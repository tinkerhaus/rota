/**
 * Control-plane coverage: health / describeCluster / stats, group config +
 * lifecycle (pause/resume/cancel/purge/reap/teardown), lane config + pause/resume
 * (with a behavioral check), programmable policy (set/get/validate), cron
 * (schedule/list/pause/delete), and singleton leases (acquire/renew/release +
 * contention + fencing).
 */

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { Control, Publisher, Worker, PolicyKind, type Message } from "../dist/index.js";
import { buildBinary, haveGo, sleep, startBroker, type Broker } from "./harness.js";

const d = haveGo() ? describe : describe.skip;

d("rota control plane", () => {
  let broker: Broker;
  let ctl: Control;
  let pub: Publisher;

  beforeAll(async () => {
    broker = await startBroker(buildBinary());
    ctl = new Control(broker.addr);
    pub = new Publisher(broker.addr);
  }, 320_000);

  afterAll(async () => {
    ctl?.close();
    pub?.close();
    await broker?.stop();
  });

  it("reports health and cluster info", async () => {
    const h = await ctl.health();
    expect(h.serving).toBe(true);
    expect(h.isLeader).toBe(true); // single node is its own leader

    const info = await ctl.describeCluster();
    expect(info.leaderId).toBeTruthy();
    expect(info.peers.length).toBeGreaterThanOrEqual(1);
  });

  it("upserts group config and round-trips it", async () => {
    const cfg = await ctl.setGroupConfig("emails", "t1", { weight: 3.0, batchSize: 2 });
    expect(cfg.weight).toBe(3.0);
    expect(cfg.batchSize).toBe(2);
    const got = await ctl.getGroupConfig("emails", "t1");
    expect(got.weight).toBe(3.0);
    expect(got.batchSize).toBe(2);
  });

  it("pauses and resumes a group", async () => {
    const paused = await ctl.pauseGroup("emails", "t1");
    expect(paused.paused).toBe(true);
    const resumed = await ctl.resumeGroup("emails", "t1");
    expect(resumed.paused).toBe(false);
  });

  it("cancels a group, dropping its pending messages", async () => {
    const lane = "cancel-lane";
    for (let i = 0; i < 5; i++) await pub.publish(lane, "victim", Buffer.from(`m${i}`));
    const res = await ctl.cancelGroup(lane, "victim");
    expect(Number(res.affectedMessages)).toBe(5);
  });

  it("purges a group's leasable messages", async () => {
    const lane = "purge-lane";
    for (let i = 0; i < 3; i++) await pub.publish(lane, "p", Buffer.from(`m${i}`));
    const res = await ctl.purgeGroup(lane, "p");
    expect(Number(res.affectedMessages)).toBe(3);
  });

  it("reaps an (absent/empty) group without error", async () => {
    const res = await ctl.reapGroup("reap-lane", "ghost");
    expect(Number(res.affectedMessages)).toBeGreaterThanOrEqual(0);
  });

  it("tears a group down across all lanes", async () => {
    await pub.publish("td-1", "shared", Buffer.from("a"));
    await pub.publish("td-1", "shared", Buffer.from("b"));
    await pub.publish("td-2", "shared", Buffer.from("c"));
    const res = await ctl.teardownGroup("shared");
    expect(Number(res.affectedMessages)).toBe(3);
    expect(res.affectedLanes.sort()).toEqual(["td-1", "td-2"]);
  });

  it("sets lane config", async () => {
    const cfg = await ctl.setLaneConfig("rl-lane", { ratePerSec: 50, burst: 10 });
    expect(cfg.ratePerSec).toBe(50);
    expect(cfg.burst).toBe(10);
  });

  it("pauses a lane (blocks leasing) and resumes it", async () => {
    const lane = "pause-lane";
    expect((await ctl.pauseLane(lane)).ok).toBe(true);
    await pub.publish(lane, "g", Buffer.from("held"));

    let got: Message | undefined;
    const worker = new Worker(broker.addr, lane, (m) => void (got = m), {
      installSignalHandler: false,
    });
    const runP = worker.run();
    await sleep(800);
    expect(got).toBeUndefined(); // paused lane delivers nothing

    expect((await ctl.resumeLane(lane)).ok).toBe(true);
    const deadline = Date.now() + 8000;
    while (!got && Date.now() < deadline) await sleep(20);
    worker.stop();
    await runP;
    expect(got?.payload.toString()).toBe("held");
  }, 20_000);

  it("installs a builtin policy and reads it back", async () => {
    const info = await ctl.setPolicy("pol-lane", { kind: PolicyKind.STRICT_PRIORITY });
    expect(Number(info.policyVersion)).toBe(1);
    const got = await ctl.getPolicy("pol-lane");
    expect(got.source?.kind).toBe(PolicyKind.STRICT_PRIORITY);

    // Hot-reload bumps the version.
    const info2 = await ctl.setPolicy("pol-lane", { kind: PolicyKind.WFQ });
    expect(Number(info2.policyVersion)).toBe(2);
  });

  it("validates CEL policy source (accepts valid, rejects malformed)", async () => {
    const bad = await ctl.validatePolicy("pol-lane", {
      kind: PolicyKind.CUSTOM,
      engine: "cel",
      code: Buffer.from("backlog +"),
    });
    expect(bad.ok).toBe(false);
    expect(bad.diagnostics.length).toBeGreaterThan(0);

    const good = await ctl.validatePolicy("pol-lane", {
      kind: PolicyKind.CUSTOM,
      engine: "cel",
      code: Buffer.from("-backlog"),
    });
    expect(good.ok).toBe(true);
  });

  it("schedules, lists, pauses, and deletes a cron", async () => {
    const info = await ctl.scheduleCron("c1", "cron-lane", "g", "@every 1s", {
      payload: Buffer.from("tick"),
    });
    expect(info.cronId).toBe("c1");

    const list = await ctl.listCron();
    expect(list.crons.some((c) => c.cronId === "c1")).toBe(true);

    const paused = await ctl.pauseCron("c1");
    expect(paused.paused).toBe(true);

    const del = await ctl.deleteCron("c1");
    expect(del.existed).toBe(true);
  });

  it("acquires, renews, and releases a singleton with fencing", async () => {
    const lease = await ctl.acquireSingleton("job", "h1", 10.0);
    expect(Number(lease.fence)).toBeGreaterThan(0);

    // A different holder cannot acquire while it is held.
    await expect(ctl.acquireSingleton("job", "h2", 10.0)).rejects.toBeTruthy();

    // The holder can renew with the current fence.
    const renewed = await ctl.renewSingleton("job", "h1", Number(lease.fence), 10.0);
    expect(Number(renewed.fence)).toBe(Number(lease.fence));

    // A stale fence is rejected.
    await expect(ctl.renewSingleton("job", "h1", Number(lease.fence) + 999, 10.0)).rejects.toBeTruthy();

    // Release, then a new holder acquires with a strictly higher fence.
    const rel = await ctl.releaseSingleton("job", "h1", Number(lease.fence));
    expect(rel.released).toBe(true);
    const reacquired = await ctl.acquireSingleton("job", "h2", 10.0);
    expect(Number(reacquired.fence)).toBeGreaterThan(Number(lease.fence));
  });

  it("returns per-lane stats", async () => {
    const lane = "stats-lane";
    await pub.publish(lane, "g", Buffer.from("x"));
    const stats = await ctl.getStats(lane);
    const ls = stats.lanes.find((l) => l.lane === lane);
    expect(ls).toBeDefined();
    expect(Number(ls!.leasable)).toBeGreaterThanOrEqual(1);
  });
});
