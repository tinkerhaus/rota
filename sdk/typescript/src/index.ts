/**
 * Rota: a thin, domain-neutral TypeScript SDK for the Rota fair-scheduling
 * broker and durable workflow engine.
 *
 * Vocabulary is strictly generic: lane, group, message, lease, policy, cron,
 * singleton, dead-letter; and for durable execution: run, history, event, task,
 * command. Payloads are opaque bytes plus a headers map.
 *
 * Quickstart:
 *
 * ```ts
 * import { Publisher, Worker } from "rota";
 *
 * const pub = new Publisher("127.0.0.1:7100");
 * await pub.publish("emails", "tenant-1", Buffer.from("..."));
 *
 * const worker = new Worker("127.0.0.1:7100", "emails", async (msg) => {
 *   doWork(msg.payload); // resolve to ack
 * });
 * await worker.run();
 * ```
 */

// Broker (data plane).
export { Publisher } from "./publisher.js";
export type { PublishOptions, PublisherOptions, BatchItem } from "./publisher.js";
export { Worker, Message } from "./worker.js";
export type { WorkerOptions, MessageHandler } from "./worker.js";

// Control plane.
export { Control } from "./control.js";
export type { ControlOptions, PolicySpec } from "./control.js";

// Durable execution (workflows + activities).
export {
  WorkflowClient,
  ActivityTask,
  prefixChecksum,
  scheduleActivity,
  startTimer,
  continueAsNew,
  completeWorkflow,
  failWorkflow,
  runWorkflowWorker,
  runActivityWorker,
} from "./workflow.js";
export type {
  Command,
  Decider,
  ActivityHandler,
  WorkflowClientOptions,
  WorkerLoopOptions,
} from "./workflow.js";

// Signals / errors.
export { RotaError, Requeue, DeadLetter, NotLeaderError } from "./errors.js";

// Enums (runtime constant objects + their numeric union types).
export {
  HistoryEventType,
  WorkflowStatus,
  MessageState,
  NackMode,
  Outcome,
  ControlKind,
  ErrorCode,
  PolicyKind,
  PolicyMode,
  MisfirePolicy,
} from "./enums.js";

// Shared types.
export type { Targets, LeaderClientOptions } from "./common.js";

// Curated message shapes returned by the public API (the proto ``__Output``
// types, re-exported under friendly names).
export type { HistoryEvent__Output as HistoryEvent } from "./generated/rota/v1/HistoryEvent.js";
export type { WorkflowRun__Output as WorkflowRun } from "./generated/rota/v1/WorkflowRun.js";
export type { ListWorkflowRunsResponse__Output as ListWorkflowRunsResponse } from "./generated/rota/v1/ListWorkflowRunsResponse.js";
export type { PublishItemResult__Output as PublishItemResult } from "./generated/rota/v1/PublishItemResult.js";
export type { CompleteResult__Output as CompleteResult } from "./generated/rota/v1/CompleteResult.js";
export type { GroupConfig__Output as GroupConfig } from "./generated/rota/v1/GroupConfig.js";
export type { GroupOpResult__Output as GroupOpResult } from "./generated/rota/v1/GroupOpResult.js";
export type { TeardownResult__Output as TeardownResult } from "./generated/rota/v1/TeardownResult.js";
export type { LaneConfig__Output as LaneConfig } from "./generated/rota/v1/LaneConfig.js";
export type { LaneOpResult__Output as LaneOpResult } from "./generated/rota/v1/LaneOpResult.js";
export type { PolicyInfo__Output as PolicyInfo } from "./generated/rota/v1/PolicyInfo.js";
export type { ValidatePolicyResult__Output as ValidatePolicyResult } from "./generated/rota/v1/ValidatePolicyResult.js";
export type { CronInfo__Output as CronInfo } from "./generated/rota/v1/CronInfo.js";
export type { ListCronResponse__Output as ListCronResponse } from "./generated/rota/v1/ListCronResponse.js";
export type { CronOpResult__Output as CronOpResult } from "./generated/rota/v1/CronOpResult.js";
export type { SingletonLease__Output as SingletonLease } from "./generated/rota/v1/SingletonLease.js";
export type { SingletonOpResult__Output as SingletonOpResult } from "./generated/rota/v1/SingletonOpResult.js";
export type { StatsResponse__Output as StatsResponse } from "./generated/rota/v1/StatsResponse.js";
export type { ClusterInfo__Output as ClusterInfo } from "./generated/rota/v1/ClusterInfo.js";
export type { HealthResponse__Output as HealthResponse } from "./generated/rota/v1/HealthResponse.js";
