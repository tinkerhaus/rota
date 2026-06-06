/**
 * Durable execution: workflow + activity clients and worker loops.
 *
 * This is the TypeScript side of Rota's durable-execution engine. The engine
 * itself lives in the broker; an SDK in any language just drives it over the
 * ``Workflow`` gRPC service. The vocabulary is deliberately neutral: a *workflow
 * run* has a deterministic *history* of *events*; a *worker* leases a *task*,
 * replays the history, *decides* a list of *commands*, and submits them back.
 *
 * Two worker roles:
 *
 * - A **workflow worker** ({@link runWorkflowWorker}) leases workflow tasks,
 *   replays the run's committed history, and returns the {@link Command}s the
 *   engine appends as new events. It MUST echo the *prefix checksum* it computed
 *   over the history it replayed (see {@link prefixChecksum}) or the leader's
 *   determinism gate rejects the decision as ``non_determinism``.
 * - An **activity worker** ({@link runActivityWorker}) leases activity tasks
 *   (activities ARE ordinary leases under the hood), runs side-effecting work,
 *   and reports ``[result, success]`` back into the run's history.
 *
 * Poll RPCs return ``empty: true`` when nothing is available; the loops sleep
 * ~20ms and retry.
 */

import { createHash } from "node:crypto";
import * as grpc from "@grpc/grpc-js";

import {
  LeaderClient,
  loadProto,
  type MetadataInit,
  type LeaderClientOptions,
  type Targets,
} from "./common.js";
import type { WorkflowClient as WorkflowServiceClient } from "./generated/rota/v1/Workflow.js";
import type { HistoryEvent__Output } from "./generated/rota/v1/HistoryEvent.js";
import type { WorkflowRun__Output } from "./generated/rota/v1/WorkflowRun.js";
import type { SignalWorkflowResponse__Output } from "./generated/rota/v1/SignalWorkflowResponse.js";
import type { ListWorkflowRunsResponse__Output } from "./generated/rota/v1/ListWorkflowRunsResponse.js";
import type { WorkflowCommandProto } from "./generated/rota/v1/WorkflowCommandProto.js";
import type { PolledWorkflowTask__Output } from "./generated/rota/v1/PolledWorkflowTask.js";
import type { PolledActivityTask__Output } from "./generated/rota/v1/PolledActivityTask.js";

/** Empty polls back off by this much before retrying (milliseconds). */
const POLL_IDLE_SLEEP_MS = 20;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// ── Determinism: prefix checksum ─────────────────────────────────────────────

/**
 * Compute the determinism checksum over a run's replayed history.
 *
 * This MUST be byte-identical to the broker's ``PrefixChecksumOf`` (the leader
 * recomputes the same value over the committed prefix and rejects on mismatch).
 * The algorithm hashes, for each event IN ORDER, the concatenation of:
 *
 * - ``eventId`` as a big-endian u64,
 * - the ``eventType`` enum NUMBER as a big-endian u32,
 * - the raw ``attrs`` bytes,
 *
 * and returns the 32-byte SHA-256 digest a worker echoes in
 * ``RespondWorkflowTask``.
 */
export function prefixChecksum(history: HistoryEvent__Output[]): Buffer {
  const h = createHash("sha256");
  const id = Buffer.alloc(8);
  const type = Buffer.alloc(4);
  for (const ev of history) {
    id.writeBigUInt64BE(BigInt(ev.eventId ?? 0));
    type.writeUInt32BE(Number(ev.eventType ?? 0) >>> 0);
    h.update(id);
    h.update(type);
    h.update(ev.attrs ?? Buffer.alloc(0));
  }
  return h.digest();
}

// ── Commands: a workflow decider's output ────────────────────────────────────

/**
 * One decision a workflow task emits, turned into a history event. Prefer the
 * helper constructors ({@link scheduleActivity}, {@link startTimer},
 * {@link continueAsNew}, {@link completeWorkflow}, {@link failWorkflow}) over
 * building this directly.
 */
export interface Command {
  kind:
    | "schedule_activity"
    | "start_timer"
    | "continue_as_new"
    | "complete_workflow"
    | "fail_workflow";
  activityType?: string;
  input?: Uint8Array;
  result?: Uint8Array;
  delayMs?: number;
}

/** Schedule an activity; its completion lands back in history as ACTIVITY_COMPLETED/FAILED. */
export function scheduleActivity(activityType: string, input?: Uint8Array): Command {
  return { kind: "schedule_activity", activityType, input: input ?? Buffer.alloc(0) };
}

/** Start a durable timer; a TIMER_FIRED event is appended when it elapses. */
export function startTimer(delayMs: number): Command {
  return { kind: "start_timer", delayMs: Math.trunc(delayMs) };
}

/** Close this run (status CONTINUED) and start a successor with ``input`` and a fresh history. */
export function continueAsNew(input?: Uint8Array): Command {
  return { kind: "continue_as_new", input: input ?? Buffer.alloc(0) };
}

/** Terminally complete the run with ``result`` (status WF_COMPLETED). */
export function completeWorkflow(result?: Uint8Array): Command {
  return { kind: "complete_workflow", result: result ?? Buffer.alloc(0) };
}

/** Terminally fail the run with ``result`` (status WF_FAILED). */
export function failWorkflow(result?: Uint8Array): Command {
  return { kind: "fail_workflow", result: result ?? Buffer.alloc(0) };
}

function commandToProto(c: Command): WorkflowCommandProto {
  return {
    kind: c.kind,
    activityType: c.activityType ?? "",
    input: c.input ?? Buffer.alloc(0),
    result: c.result ?? Buffer.alloc(0),
    delayMs: Math.trunc(c.delayMs ?? 0),
  };
}

// ── Activity task wrapper ────────────────────────────────────────────────────

/**
 * A leased activity task handed to an activity handler. ``runId`` +
 * ``scheduledEventId`` route the result back into the originating run's history;
 * ``activityType`` and ``input`` describe the work to do.
 */
export class ActivityTask {
  readonly runId: number;
  readonly leaseId: number;
  readonly scheduledEventId: number;
  readonly activityType: string;
  readonly input: Buffer;

  constructor(polled: PolledActivityTask__Output) {
    this.runId = polled.runId;
    this.leaseId = polled.leaseId;
    this.scheduledEventId = polled.scheduledEventId;
    this.activityType = polled.activityType;
    this.input = polled.input;
  }
}

// ── Client ───────────────────────────────────────────────────────────────────

export interface WorkflowClientOptions extends LeaderClientOptions {
  /** Per-call deadline in seconds (default 30). Pass ``null`` for no deadline. */
  timeout?: number | null;
}

/**
 * Client for the Rota ``Workflow`` service. Covers the run lifecycle (start /
 * signal / cancel) and read RPCs (get run, get history, list runs). Every call
 * follows the cluster leader on a NOT_LEADER fault.
 */
export class WorkflowClient {
  private readonly timeout: number | null;
  private readonly client: LeaderClient<WorkflowServiceClient>;

  constructor(targets: Targets = "127.0.0.1:7100", opts: WorkflowClientOptions = {}) {
    const { timeout, ...leaderOpts } = opts;
    this.timeout = timeout === undefined ? 30.0 : timeout;
    const proto = loadProto();
    this.client = new LeaderClient(targets, proto.rota.v1.Workflow, leaderOpts);
  }

  private call<Res>(method: string, request: unknown): Promise<Res> {
    return this.client.call<Res>(method, request, { timeout: this.timeout });
  }

  /**
   * Start a workflow run. Resolves to the broker-assigned ``runId``.
   * ``tenantId`` is the fairness unit (the group analogue); ``input`` is the
   * opaque starting payload replayed at the head of the run's history.
   */
  async startWorkflow(
    workflowType: string,
    opts: { tenantId?: string; input?: Uint8Array } = {},
  ): Promise<number> {
    const resp = await this.call<{ runId: number }>("StartWorkflow", {
      workflowType,
      tenantId: opts.tenantId ?? "",
      input: opts.input ?? Buffer.alloc(0),
    });
    return resp.runId;
  }

  /** Deliver an external signal to a running workflow. */
  signalWorkflow(
    runId: number,
    signalName: string,
    payload?: Uint8Array,
  ): Promise<SignalWorkflowResponse__Output> {
    return this.call("SignalWorkflow", {
      runId,
      signalName,
      payload: payload ?? Buffer.alloc(0),
    });
  }

  /** Request cancellation of a run. Resolves to whether it was canceled. */
  async cancelWorkflow(runId: number, reason?: Uint8Array): Promise<boolean> {
    const resp = await this.call<{ canceled: boolean }>("CancelWorkflow", {
      runId,
      reason: reason ?? Buffer.alloc(0),
    });
    return resp.canceled;
  }

  /** Fetch a run's quorum-written record (status, epoch, history seq, ...). */
  getRun(runId: number): Promise<WorkflowRun__Output> {
    return this.call("GetWorkflowRun", { runId });
  }

  /** Fetch a run's full history, in ``eventId`` order. */
  async getHistory(runId: number): Promise<HistoryEvent__Output[]> {
    const resp = await this.call<{ events: HistoryEvent__Output[] }>("GetWorkflowHistory", {
      runId,
    });
    return resp.events ?? [];
  }

  /**
   * List runs, optionally filtered by ``status`` (a {@link WorkflowStatus}
   * value). Paginated via ``pageToken``.
   */
  listRuns(
    opts: { status?: number; pageSize?: number; pageToken?: string } = {},
  ): Promise<ListWorkflowRunsResponse__Output> {
    const req: Record<string, unknown> = {
      pageSize: opts.pageSize ?? 0,
      pageToken: opts.pageToken ?? "",
    };
    if (opts.status !== undefined) {
      req.status = opts.status;
      req.hasStatus = true;
    }
    return this.call("ListWorkflowRuns", req);
  }

  close(): void {
    this.client.close();
  }
}

// ── Worker loops ─────────────────────────────────────────────────────────────

/** A decider replays a run's history and returns the commands to apply. */
export type Decider = (
  runId: number,
  history: HistoryEvent__Output[],
) => Command[] | Promise<Command[]>;

/** An activity handler runs side-effecting work and returns ``[result, success]``. */
export type ActivityHandler = (
  task: ActivityTask,
) => [Uint8Array, boolean] | Promise<[Uint8Array, boolean]>;

export interface WorkerLoopOptions {
  /** Set to abort the loop after the next poll cycle. */
  signal?: AbortSignal;
  channelOptions?: Partial<grpc.ClientOptions>;
  credentials?: grpc.ChannelCredentials;
  metadata?: MetadataInit;
  authToken?: string;
  /** Per-poll deadline in seconds (default 30). */
  timeout?: number | null;
}

function makeWorkflowClient(
  targets: Targets,
  opts: WorkerLoopOptions,
): LeaderClient<WorkflowServiceClient> {
  return new LeaderClient(targets, loadProto().rota.v1.Workflow, {
    channelOptions: opts.channelOptions,
    credentials: opts.credentials,
    metadata: opts.metadata,
    authToken: opts.authToken,
  });
}

/**
 * Run the workflow-task poll/decide/respond loop until ``signal`` is aborted.
 *
 * Each iteration leases one workflow task, replays the committed history, calls
 * ``decide(runId, history)``, computes the prefix checksum over that SAME
 * history (so the leader's determinism gate accepts the decision), and submits
 * the commands. Resolves when the loop stops. Poll/respond calls follow the
 * cluster leader on a NOT_LEADER fault, so the worker may target any node.
 */
export async function runWorkflowWorker(
  targets: Targets,
  workflowType: string,
  consumerId: string,
  decide: Decider,
  opts: WorkerLoopOptions = {},
): Promise<void> {
  const timeout = opts.timeout === undefined ? 30.0 : opts.timeout;
  const client = makeWorkflowClient(targets, opts);
  const req = { taskType: workflowType, consumerId };
  const stopped = () => opts.signal?.aborted ?? false;
  try {
    while (!stopped()) {
      let task: PolledWorkflowTask__Output;
      try {
        task = await client.call<PolledWorkflowTask__Output>("PollWorkflowTask", req, { timeout });
      } catch {
        if (stopped()) break;
        await sleep(POLL_IDLE_SLEEP_MS);
        continue;
      }
      if (task.empty) {
        await sleep(POLL_IDLE_SLEEP_MS);
        continue;
      }
      const history = task.history ?? [];
      const checksum = prefixChecksum(history);
      const commands = (await decide(task.runId, history)) ?? [];
      await client.call("RespondWorkflowTask", {
        runId: task.runId,
        leaseId: task.leaseId,
        runEpoch: task.runEpoch,
        historySeq: task.historySeq,
        prefixChecksum: checksum,
        commands: commands.map(commandToProto),
      }, { timeout });
    }
  } finally {
    client.close();
  }
}

/**
 * Run the activity-task poll/handle/respond loop until ``signal`` is aborted.
 *
 * Each iteration leases one activity task, runs ``handler(task)`` for its
 * ``[result, success]``, and reports the outcome back into the run's history
 * (idempotent by ``scheduledEventId``). A handler that throws is reported as a
 * failure. Resolves when the loop stops.
 */
export async function runActivityWorker(
  targets: Targets,
  activityType: string,
  consumerId: string,
  handler: ActivityHandler,
  opts: WorkerLoopOptions = {},
): Promise<void> {
  const timeout = opts.timeout === undefined ? 30.0 : opts.timeout;
  const client = makeWorkflowClient(targets, opts);
  const req = { taskType: activityType, consumerId };
  const stopped = () => opts.signal?.aborted ?? false;
  try {
    while (!stopped()) {
      let polled: PolledActivityTask__Output;
      try {
        polled = await client.call<PolledActivityTask__Output>("PollActivityTask", req, { timeout });
      } catch {
        if (stopped()) break;
        await sleep(POLL_IDLE_SLEEP_MS);
        continue;
      }
      if (polled.empty) {
        await sleep(POLL_IDLE_SLEEP_MS);
        continue;
      }
      const task = new ActivityTask(polled);
      let result: Uint8Array;
      let success: boolean;
      try {
        [result, success] = await handler(task);
      } catch {
        result = Buffer.alloc(0);
        success = false;
      }
      await client.call("RespondActivityTask", {
        runId: task.runId,
        leaseId: task.leaseId,
        scheduledEventId: task.scheduledEventId,
        success,
        result: result ?? Buffer.alloc(0),
      }, { timeout });
    }
  } finally {
    client.close();
  }
}
