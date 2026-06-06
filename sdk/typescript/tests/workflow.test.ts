/**
 * Durable-execution coverage: the basic schedule-activity -> complete flow
 * (which proves the prefix checksum matches the Go server byte-for-byte), plus
 * signals, cancellation, durable timers, continue-as-new, fail, and listRuns.
 *
 * Each sub-test uses a distinct workflowType + consumerId and its own
 * AbortController so the poll loops don't cross-talk.
 */

import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

import {
  WorkflowClient,
  runActivityWorker,
  runWorkflowWorker,
  scheduleActivity,
  completeWorkflow,
  failWorkflow,
  startTimer,
  continueAsNew,
  HistoryEventType,
  WorkflowStatus,
  type Command,
  type Decider,
  type HistoryEvent,
} from "../dist/index.js";
import { buildBinary, haveGo, sleep, startBroker, type Broker } from "./harness.js";

const d = haveGo() ? describe : describe.skip;

d("rota durable execution", () => {
  let broker: Broker;
  let client: WorkflowClient;
  const stops: AbortController[] = [];
  const loops: Promise<unknown>[] = [];

  beforeAll(async () => {
    broker = await startBroker(buildBinary());
    client = new WorkflowClient(broker.addr);
  }, 320_000);

  afterEach(async () => {
    for (const ac of stops) ac.abort();
    stops.length = 0;
    await Promise.allSettled(loops);
    loops.length = 0;
  });

  afterAll(async () => {
    client?.close();
    await broker?.stop();
  });

  /** Start a workflow worker for `type` driven by `decide`; auto-cleaned afterEach. */
  function workflowWorker(type: string, decide: Decider): void {
    const ac = new AbortController();
    stops.push(ac);
    loops.push(runWorkflowWorker(broker.addr, type, `${type}-w`, decide, { signal: ac.signal }));
  }
  function activityWorker(
    type: string,
    handler: (input: Buffer) => [Uint8Array, boolean],
  ): void {
    const ac = new AbortController();
    stops.push(ac);
    loops.push(
      runActivityWorker(broker.addr, type, `${type}-a`, (t) => handler(t.input), {
        signal: ac.signal,
      }),
    );
  }

  async function waitStatus(runId: number, status: number, timeoutMs = 15_000): Promise<number> {
    const deadline = Date.now() + timeoutMs;
    let last = -1;
    while (Date.now() < deadline) {
      last = (await client.getRun(runId)).status as number;
      if (last === status) return last;
      await sleep(40);
    }
    return last;
  }

  const has = (h: HistoryEvent[], t: number) => h.some((e) => e.eventType === t);

  it("schedules an activity then completes (prefix checksum is correct)", async () => {
    activityWorker("charge", (input) => {
      expect(input).toBeInstanceOf(Buffer);
      return [Buffer.from("ok"), true];
    });
    workflowWorker("order", (_runId, history): Command[] => {
      if (!has(history, HistoryEventType.HET_ACTIVITY_SCHEDULED))
        return [scheduleActivity("charge", Buffer.from("100"))];
      if (has(history, HistoryEventType.HET_ACTIVITY_COMPLETED))
        return [completeWorkflow(Buffer.from("done"))];
      return [];
    });

    const runId = await client.startWorkflow("order", {
      tenantId: "tenant-1",
      input: Buffer.from("{}"),
    });
    expect(runId).toBeGreaterThan(0);
    expect(await waitStatus(runId, WorkflowStatus.WF_COMPLETED)).toBe(WorkflowStatus.WF_COMPLETED);

    const history = await client.getHistory(runId);
    expect(has(history, HistoryEventType.HET_WORKFLOW_STARTED)).toBe(true);
    expect(has(history, HistoryEventType.HET_ACTIVITY_SCHEDULED)).toBe(true);
    expect(has(history, HistoryEventType.HET_ACTIVITY_COMPLETED)).toBe(true);
    expect(has(history, HistoryEventType.HET_WORKFLOW_COMPLETED)).toBe(true);
  });

  it("reacts to a signal", async () => {
    workflowWorker("signalwf", (_runId, history): Command[] => {
      if (has(history, HistoryEventType.HET_SIGNAL_RECEIVED))
        return [completeWorkflow(Buffer.from("got-signal"))];
      return []; // wait for the signal
    });

    const runId = await client.startWorkflow("signalwf", { tenantId: "t" });
    // Give the initial (no-op) task time to be processed, then signal.
    await sleep(300);
    await client.signalWorkflow(runId, "proceed", Buffer.from("yes"));

    expect(await waitStatus(runId, WorkflowStatus.WF_COMPLETED)).toBe(WorkflowStatus.WF_COMPLETED);
    const history = await client.getHistory(runId);
    expect(has(history, HistoryEventType.HET_SIGNAL_RECEIVED)).toBe(true);
  });

  it("cancels a running workflow", async () => {
    workflowWorker("sleeper", () => []); // never completes on its own

    const runId = await client.startWorkflow("sleeper", { tenantId: "t" });
    await sleep(300); // let it reach RUNNING
    const canceled = await client.cancelWorkflow(runId, Buffer.from("operator"));
    expect(canceled).toBe(true);
    expect(await waitStatus(runId, WorkflowStatus.WF_CANCELED)).toBe(WorkflowStatus.WF_CANCELED);
  });

  it("fires a durable timer", async () => {
    workflowWorker("timerwf", (_runId, history): Command[] => {
      if (!has(history, HistoryEventType.HET_TIMER_STARTED)) return [startTimer(150)];
      if (has(history, HistoryEventType.HET_TIMER_FIRED))
        return [completeWorkflow(Buffer.from("timer-done"))];
      return [];
    });

    const runId = await client.startWorkflow("timerwf", { tenantId: "t" });
    expect(await waitStatus(runId, WorkflowStatus.WF_COMPLETED)).toBe(WorkflowStatus.WF_COMPLETED);
    const history = await client.getHistory(runId);
    expect(has(history, HistoryEventType.HET_TIMER_STARTED)).toBe(true);
    expect(has(history, HistoryEventType.HET_TIMER_FIRED)).toBe(true);
  });

  it("continues-as-new, chaining successor runs", async () => {
    workflowWorker("continuewf", (_runId, history): Command[] => {
      const seed = history.length > 0 ? history[0].attrs?.toString() || "0" : "0";
      const count = parseInt(seed, 10) || 0;
      if (count < 2) return [continueAsNew(Buffer.from(String(count + 1)))];
      return [completeWorkflow(Buffer.from("chain-done"))];
    });

    const rootId = await client.startWorkflow("continuewf", {
      tenantId: "t",
      input: Buffer.from("0"),
    });

    // Wait until a completed run for this type appears.
    let completed: number | undefined;
    const deadline = Date.now() + 15_000;
    while (Date.now() < deadline && completed === undefined) {
      const runs = (await client.listRuns({})).runs.filter((r) => r.workflowType === "continuewf");
      const done = runs.find((r) => (r.status as number) === WorkflowStatus.WF_COMPLETED);
      if (done) completed = Number(done.runId);
      else await sleep(50);
    }
    expect(completed).toBeDefined();

    const runs = (await client.listRuns({})).runs.filter((r) => r.workflowType === "continuewf");
    const continued = runs.filter((r) => (r.status as number) === WorkflowStatus.WF_CONTINUED);
    expect(continued.length).toBe(2); // count 0 -> 1 -> 2, then complete

    const root = await client.getRun(rootId);
    expect(Number(root.parentRunId)).toBe(0);
    const done = await client.getRun(completed!);
    expect(Number(done.parentRunId)).toBeGreaterThan(0);
  });

  it("fails a workflow", async () => {
    workflowWorker("failwf", () => [failWorkflow(Buffer.from("boom"))]);
    const runId = await client.startWorkflow("failwf", { tenantId: "t" });
    expect(await waitStatus(runId, WorkflowStatus.WF_FAILED)).toBe(WorkflowStatus.WF_FAILED);
  });

  it("lists and filters runs by status", async () => {
    workflowWorker("listwf", () => [completeWorkflow(Buffer.from("ok"))]);
    const ids: number[] = [];
    for (let i = 0; i < 3; i++) ids.push(await client.startWorkflow("listwf", { tenantId: "t" }));
    for (const id of ids) await waitStatus(id, WorkflowStatus.WF_COMPLETED);

    const all = await client.listRuns({});
    expect(all.runs.length).toBeGreaterThanOrEqual(3);

    const completed = await client.listRuns({ status: WorkflowStatus.WF_COMPLETED });
    expect(completed.runs.length).toBeGreaterThanOrEqual(3);
    for (const r of completed.runs) {
      expect(r.status as number).toBe(WorkflowStatus.WF_COMPLETED);
    }
  });
});
