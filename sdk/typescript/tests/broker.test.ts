/**
 * Broker data-plane coverage: publish, publishBatch (atomic), dedup, the full
 * Work-stream lease lifecycle (ack / Requeue / DeadLetter / RETRY), in-stream &
 * off-stream complete-by-token, and lease extension. Dead-lettering is observed
 * via the lane's DLQ depth in getStats (the dashboard read RPCs are not part of
 * the SDK surface, matching the Python SDK).
 */

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  Control,
  Publisher,
  Worker,
  Requeue,
  DeadLetter,
  ErrorCode,
  type Message,
} from "../dist/index.js";
import { buildBinary, haveGo, sleep, startBroker, type Broker } from "./harness.js";

const d = haveGo() ? describe : describe.skip;

/** Run a worker, poll until `cond` holds (or timeout), then stop it. */
async function withWorker(
  worker: Worker,
  cond: () => boolean | Promise<boolean>,
  timeoutMs = 12_000,
): Promise<void> {
  const runP = worker.run();
  const deadline = Date.now() + timeoutMs;
  while (!(await cond()) && Date.now() < deadline) await sleep(20);
  worker.stop();
  await runP;
}

d("rota broker data plane", () => {
  let broker: Broker;
  let pub: Publisher;
  let ctl: Control;

  beforeAll(async () => {
    broker = await startBroker(buildBinary());
    pub = new Publisher(broker.addr);
    ctl = new Control(broker.addr);
  }, 320_000);

  afterAll(async () => {
    pub?.close();
    ctl?.close();
    await broker?.stop();
  });

  const dlqDepth = async (lane: string): Promise<number> => {
    const stats = await ctl.getStats(lane);
    return Number(stats.lanes.find((l) => l.lane === lane)?.dlqDepth ?? 0);
  };
  const inflight = async (lane: string): Promise<number> => {
    const stats = await ctl.getStats(lane);
    return Number(stats.lanes.find((l) => l.lane === lane)?.inflight ?? 0);
  };

  it("publishes and a worker leases + acks (round-trip)", async () => {
    const lane = "round-trip";
    const id = await pub.publish(lane, "tenant-A", Buffer.from("hello-rota"), {
      headers: { trace: "abc" },
    });
    expect(id).toBeGreaterThan(0);

    let got: Message | undefined;
    const worker = new Worker(broker.addr, lane, (m) => void (got = m), {
      installSignalHandler: false,
    });
    await withWorker(worker, () => got !== undefined);

    expect(got!.payload.toString()).toBe("hello-rota");
    expect(got!.groupId).toBe("tenant-A");
    expect(got!.headers.trace).toBe("abc");
    expect(await inflight(lane)).toBe(0);
  });

  it("publishBatch atomic returns OK results with per-group unique ids", async () => {
    // message_id is a per-group monotonic sequence, so uniqueness holds within a
    // single group; assert that for the three g1 messages.
    const results = await pub.publishBatch(
      [
        { lane: "batch", groupId: "g1", payload: Buffer.from("a") },
        { lane: "batch", groupId: "g1", payload: Buffer.from("b") },
        { lane: "batch", groupId: "g1", payload: Buffer.from("c") },
      ],
      { atomic: true },
    );
    expect(results).toHaveLength(3);
    const ids = new Set<number>();
    for (const r of results) {
      expect(r.code).toBe(ErrorCode.OK);
      ids.add(Number(r.messageId));
    }
    expect(ids.size).toBe(3);
  });

  it("dedup_key collapses re-publishes within the window", async () => {
    const first = await pub.publish("dedup", "g", Buffer.from("x"), { dedupKey: "k1" });
    const second = await pub.publish("dedup", "g", Buffer.from("x"), { dedupKey: "k1" });
    expect(second).toBe(first); // same id => deduplicated
    const other = await pub.publish("dedup", "g", Buffer.from("x"), { dedupKey: "k2" });
    expect(other).not.toBe(first);
  });

  it("Requeue re-delivers with no attempt penalty", async () => {
    const lane = "requeue";
    await pub.publish(lane, "g", Buffer.from("r"));
    const attempts: number[] = [];
    const worker = new Worker(
      broker.addr,
      lane,
      (m) => {
        attempts.push(m.attempt);
        if (attempts.length === 1) throw new Requeue({ delay: 0.05 });
        // second delivery: ack
      },
      { installSignalHandler: false },
    );
    await withWorker(worker, () => attempts.length >= 2);
    expect(attempts.length).toBeGreaterThanOrEqual(2);
    expect(attempts[0]).toBe(0);
    expect(attempts[1]).toBe(0); // no penalty: attempt unchanged
  });

  it("DeadLetter terminally routes to the DLQ", async () => {
    const lane = "deadletter";
    await pub.publish(lane, "g", Buffer.from("poison"));
    const worker = new Worker(
      broker.addr,
      lane,
      () => {
        throw new DeadLetter({ meta: { why: "unparseable" } });
      },
      { installSignalHandler: false },
    );
    await withWorker(worker, async () => (await dlqDepth(lane)) >= 1);
    expect(await dlqDepth(lane)).toBe(1);
  });

  it("a generic throw RETRYs, incrementing attempt, then dead-letters at max", async () => {
    const lane = "retry";
    await pub.publish(lane, "g", Buffer.from("boom"), { maxAttempts: 3 });
    const attempts: number[] = [];
    const worker = new Worker(
      broker.addr,
      lane,
      (m) => {
        attempts.push(m.attempt);
        throw new Error("kaboom"); // => Nack(RETRY)
      },
      { installSignalHandler: false },
    );
    await withWorker(worker, async () => (await dlqDepth(lane)) >= 1, 15_000);
    expect(attempts.slice(0, 3)).toEqual([0, 1, 2]); // attempt climbs each retry
    expect(await dlqDepth(lane)).toBe(1);
  });

  it("completes by token off-stream (issueToken -> CompleteByToken)", async () => {
    const lane = "token-off";
    await pub.publish(lane, "g", Buffer.from("async"), { issueToken: true });

    let token: Buffer | undefined;
    let release!: () => void;
    const released = new Promise<void>((r) => (release = r));

    const worker = new Worker(
      broker.addr,
      lane,
      async (m) => {
        token = m.externalToken;
        await released; // hold the lease open until the test completes by token
      },
      { installSignalHandler: false },
    );
    const runP = worker.run();
    const deadline = Date.now() + 10_000;
    while (!token?.length && Date.now() < deadline) await sleep(20);
    expect(token?.length).toBeGreaterThan(0);

    const res = await ctl.completeByToken(token!, { success: true, resultMeta: { ok: "1" } });
    expect(res.resolved).toBe(true);
    expect(res.unknownToken).toBe(false);
    expect(await inflight(lane)).toBe(0);

    release();
    worker.stop();
    await runP;
  });

  it("completes by token in-stream (Message.complete) and extends a lease", async () => {
    const lane = "token-in";
    await pub.publish(lane, "g", Buffer.from("async"), { issueToken: true });

    let completed = false;
    const worker = new Worker(
      broker.addr,
      lane,
      (m) => {
        m.extend(30.0); // push the visibility deadline out
        m.complete({ success: true }); // in-stream Complete frame, keyed by token
        completed = true;
      },
      { installSignalHandler: false },
    );
    await withWorker(worker, async () => completed && (await inflight(lane)) === 0);
    expect(await inflight(lane)).toBe(0);
  });

  it("completeByToken on an unknown token is benign", async () => {
    const res = await ctl.completeByToken(Buffer.from("no-such-token"), { success: true });
    expect(res.unknownToken).toBe(true);
    expect(res.resolved).toBe(false);
  });

  it("a worker's groupAllow filter restricts which groups it leases", async () => {
    const lane = "filtered";
    for (let i = 0; i < 2; i++) await pub.publish(lane, "a", Buffer.from(`a${i}`));
    for (let i = 0; i < 2; i++) await pub.publish(lane, "b", Buffer.from(`b${i}`));

    const seen: string[] = [];
    const worker = new Worker(broker.addr, lane, (m) => void seen.push(m.groupId), {
      installSignalHandler: false,
      groupAllow: ["a"],
    });
    await withWorker(worker, () => seen.length >= 2);

    expect(seen.length).toBeGreaterThanOrEqual(2);
    expect(seen.every((g) => g === "a")).toBe(true); // group b was never delivered
  });

  it("a published TTL auto-dead-letters an undelivered message", async () => {
    const lane = "ttl-sdk";
    await pub.publish(lane, "g", Buffer.from("expireme"), { ttl: 0.08 }); // 80ms
    await sleep(400); // no worker: let the TTL elapse
    expect(await dlqDepth(lane)).toBe(1);
  });
});
