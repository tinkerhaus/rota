/**
 * Multi-node leader-following coverage. Boots a real 3-node Raft cluster and
 * points clients at a FOLLOWER, asserting the SDK transparently redials the
 * leader — for unary RPCs (Publisher/Control, via the `not-leader-bin` trailer)
 * and for the Work stream (via the in-stream NOT_LEADER frame).
 */

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { Control, Publisher, Worker, type Message } from "../dist/index.js";
import { buildBinary, haveGo, sleep, startCluster, type Cluster } from "./harness.js";

const d = haveGo() ? describe : describe.skip;

d("rota cluster leader-following", () => {
  let cluster: Cluster;

  beforeAll(async () => {
    cluster = await startCluster(buildBinary(), 3);
  }, 320_000);

  afterAll(async () => {
    await cluster?.stop();
  });

  it("elects a leader and reports 3 peers", async () => {
    const { addr } = await cluster.leader();
    const ctl = new Control(addr);
    try {
      const info = await ctl.describeCluster();
      expect(info.leaderId).toBeTruthy();
      expect(info.peers.length).toBe(3);
    } finally {
      ctl.close();
    }
  });

  it("follows the leader for a unary write sent to a follower", async () => {
    const follower = await cluster.follower();
    // Point the client ONLY at the follower: success requires redialing the leader.
    const ctl = new Control(follower.addr);
    try {
      const cfg = await ctl.setGroupConfig("clane", "g", { weight: 2.0 });
      expect(cfg.weight).toBe(2.0);
    } finally {
      ctl.close();
    }
  });

  it("follows the leader for publish sent to a follower", async () => {
    const follower = await cluster.follower();
    const pub = new Publisher(follower.addr);
    try {
      const id = await pub.publish("clane", "g", Buffer.from("via-follower"));
      expect(id).toBeGreaterThan(0);
    } finally {
      pub.close();
    }
  });

  it("follows the leader for the Work stream pointed at a follower", async () => {
    // Publish to the leader, then lease from a worker pointed only at a follower:
    // it must follow the in-stream NOT_LEADER redirect to actually receive it.
    const leader = await cluster.leader();
    const pub = new Publisher(leader.addr);
    await pub.publish("wlane", "g", Buffer.from("stream-redirect"));
    pub.close();

    const follower = await cluster.follower();
    let got: Message | undefined;
    const worker = new Worker(follower.addr, "wlane", (m) => void (got = m), {
      installSignalHandler: false,
    });
    const runP = worker.run();
    const deadline = Date.now() + 12_000;
    while (!got && Date.now() < deadline) await sleep(20);
    worker.stop();
    await runP;

    expect(got?.payload.toString()).toBe("stream-redirect");
  }, 20_000);
});
