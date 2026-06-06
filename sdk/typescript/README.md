# Rota TypeScript SDK

A thin, **domain-neutral** TypeScript/Node client for [Rota](../../README.md), a
generic fair-scheduling message broker **and durable workflow engine**. The SDK
speaks only Rota's vocabulary: *lane*, *group*, *message*, *lease*, *policy*,
*cron*, *singleton*, *dead-letter*; and for durable execution: *run*, *history*,
*event*, *task*, *command*. It has no idea what your messages mean. Payloads are
opaque bytes (`Uint8Array` / `Buffer`) plus a `headers` string map.

## Install

```bash
cd sdk/typescript
npm install
npm run build        # emits dist/
```

Node 18+. Runtime deps: `@grpc/grpc-js`, `@grpc/proto-loader`. The `.proto` ships
inside the package and is loaded at runtime, so there is no codegen step to
consume the SDK. (The committed `src/generated/` typings are only *regenerated*
with `npm run gen`.)

The package is ESM (`"type": "module"`).

## Run a broker

```bash
go run ./cmd/rota serve            # Broker+Control+Workflow on 127.0.0.1:7100, metrics on :7101
```

The SDK defaults to `127.0.0.1:7100` to match. Pass your own address (or a
comma-separated seed list / array for a cluster) to any client.

For token-protected clusters, pass `{ authToken: "..." }` to `Publisher`,
`Worker`, `Control`, `WorkflowClient`, and workflow/activity worker loops. Native
`@grpc/grpc-js` credentials can be passed via `{ credentials }` for TLS/mTLS.

## Quickstart

### Publish

```ts
import { Publisher } from "rota";

const pub = new Publisher("127.0.0.1:7100"); // lazy connect; follows the leader

const msgId = await pub.publish("emails", "tenant-1", Buffer.from(JSON.stringify({ to: "a@b.com" })), {
  headers: { trace: "abc123" },
  delay: 5.0,        // become eligible in 5s (or notBefore: <unix epoch seconds>)
  weight: 2.0,       // upsert this group's scheduling weight
  maxAttempts: 3,
});
console.log("published", msgId);

// Batch (optionally all-or-nothing)
const results = await pub.publishBatch(
  [
    { lane: "emails", groupId: "t1", payload: Buffer.from("a") },
    { lane: "emails", groupId: "t2", payload: Buffer.from("b"), delay: 10.0 },
  ],
  { atomic: true },
);
pub.close();
```

### Worker loop

A worker leases from one lane and dispatches each message to your handler. Leases
are processed **serially** (one handler at a time); `credit` is the server-side
in-flight budget. Returning/resolving acks; throwing steers the outcome.

```ts
import { Worker, Requeue, DeadLetter } from "rota";

const worker = new Worker(
  "127.0.0.1:7100",
  "emails",
  async (msg) => {
    // msg.payload, msg.headers, msg.groupId, msg.leaseId, msg.attempt
    console.log("got", msg.messageId, "attempt", msg.attempt);

    if (rateLimited()) throw new Requeue({ delay: 2.0 });          // no attempt penalty
    if (poison(msg.payload)) throw new DeadLetter({ meta: { why: "unparseable" } }); // terminal -> DLQ

    msg.extend(30.0);     // need more time? push the deadline out
    await doWork(msg.payload); // resolve -> ack
  },
  { credit: 1 },
);

await worker.run(); // resolves on graceful drain / stop
```

Outcome mapping:

| handler does                       | broker frame                         |
|------------------------------------|--------------------------------------|
| returns / resolves normally        | `Ack`                                |
| throws `new Requeue({ delay })`    | `Nack(REQUEUE_NO_PENALTY, delay)`    |
| throws `new DeadLetter({ meta })`  | `Nack(DEAD_LETTER, failureMeta)`     |
| throws anything else               | `Nack(RETRY)` (attempt++)            |

`run()` resolves once a graceful drain or `stop()` completes. By default it
installs `SIGTERM`/`SIGINT` handlers for **graceful drain** (stop requesting new
credit, finish in-flight, close) — pass `{ installSignalHandler: false }` to opt
out. On a disconnect it reconnects against the leader with bounded backoff.

Call `worker.drain()` for a graceful stop, or `worker.stop()` to stop immediately.

### Off-stream completion

For complete-by-token flows (publish with `issueToken: true` or supply
`externalToken`), a process that did not hold the lease can resolve the message
later, by token alone:

```ts
await pub.complete(token, { success: true, resultMeta: { ok: "1" } });
```

Or in-stream from the handler: `msg.complete({ success: true })`.

## Control plane

```ts
import { Control, PolicyKind } from "rota";

const ctl = new Control("127.0.0.1:7100");
```

### Group config + lifecycle

```ts
await ctl.setGroupConfig("emails", "tenant-1", { weight: 3.0, batchSize: 2 });
await ctl.pauseGroup("emails", "tenant-1");  await ctl.resumeGroup("emails", "tenant-1");
await ctl.cancelGroup("emails", "tenant-1"); // drop pending; in-flight drains
await ctl.purgeGroup("emails", "tenant-1");  // drop everything leasable; keep config
await ctl.reapGroup("emails", "tenant-1");   // remove a drained group's metadata
await ctl.teardownGroup("tenant-1");         // drop a group across ALL lanes at once
```

### Back-pressure (rate-limit + dequeue-pause)

```ts
await ctl.setLaneConfig("emails", { ratePerSec: 50, burst: 10 }); // throttle DEQUEUE
await ctl.pauseLane("emails", 30.0);  // stop leasing (circuit-breaker hook); 0 = until resumed
await ctl.resumeLane("emails");
```

### Programmable scheduling policy (hot-reloadable)

```ts
// Built-in: DRR (default), strict-priority, WFQ, lottery, completion-aware.
await ctl.setPolicy("emails", { kind: PolicyKind.STRICT_PRIORITY });

// CEL expression: "shortest queue first" (score per group; higher = sooner).
await ctl.setPolicy("emails", { kind: PolicyKind.CUSTOM, engine: "cel", code: Buffer.from("-backlog") });

// WASM module (any language compiled to a wasip1 reactor exporting `score`).
await ctl.setPolicy("emails", { kind: PolicyKind.CUSTOM, engine: "wasm", code: await readFile("policy.wasm") });

await ctl.validatePolicy("emails", { kind: PolicyKind.CUSTOM, engine: "cel", code: Buffer.from("weight * 2") });
console.log((await ctl.getPolicy("emails")).policyVersion);
```

### Cron, singleton, introspection

```ts
await ctl.scheduleCron("nightly", "emails", "ops", "0 3 * * *", { payload: Buffer.from("tick") });
console.log((await ctl.listCron()).crons);
await ctl.deleteCron("nightly");

const lease = await ctl.acquireSingleton("leader-x", "host-1", 30.0);
await ctl.renewSingleton("leader-x", "host-1", lease.fence, 30.0);
await ctl.releaseSingleton("leader-x", "host-1", lease.fence);

console.log((await ctl.getStats()).lanes); // per-lane leasable / inflight / dlq depth
console.log(await ctl.describeCluster());  // leader, term, peers
console.log(await ctl.health());           // serving / hasQuorum / isLeader
```

## Durable execution (workflows + activities)

A **workflow worker** replays a run's committed history and *decides* the next
commands; it must echo a **prefix checksum** over that history (computed for you)
so the leader's determinism gate accepts the decision. An **activity worker**
runs side-effecting work and reports `[result, success]` back into the history.

```ts
import {
  WorkflowClient,
  runWorkflowWorker,
  runActivityWorker,
  scheduleActivity,
  completeWorkflow,
  HistoryEventType,
  WorkflowStatus,
  type HistoryEvent,
} from "rota";

const ac = new AbortController();

// Activity worker (run it in the background).
runActivityWorker("127.0.0.1:7100", "charge", "act-1", (task) => [Buffer.from("ok"), true], {
  signal: ac.signal,
});

// Workflow decider: replay history, decide commands (deterministic on history).
const decide = (runId: number, history: HistoryEvent[]) => {
  const scheduled = history.some((e) => e.eventType === HistoryEventType.HET_ACTIVITY_SCHEDULED);
  const done = history.some((e) => e.eventType === HistoryEventType.HET_ACTIVITY_COMPLETED);
  if (!scheduled) return [scheduleActivity("charge", Buffer.from("100"))];
  if (done) return [completeWorkflow(Buffer.from("ok"))];
  return []; // waiting on the activity
};
runWorkflowWorker("127.0.0.1:7100", "orders", "wf-1", decide, { signal: ac.signal });

// Start a run and wait for it.
const client = new WorkflowClient("127.0.0.1:7100");
const runId = await client.startWorkflow("orders", { tenantId: "tenant-A" });
// ... poll client.getRun(runId).status until WorkflowStatus.WF_COMPLETED
```

Other commands: `startTimer(ms)`, `continueAsNew(input)`, `failWorkflow(result)`.
Other client calls: `signalWorkflow`, `cancelWorkflow`, `getHistory`, `listRuns`.

Activities dispatch as ordinary, fair-scheduled leases; the worker protocol is
gRPC, so the engine inherits the broker's cross-tenant fairness. The workflow and
activity poll loops follow the cluster leader on a `NOT_LEADER` fault, so a worker
may target any node (or a seed list).

## Leader following

Rota is Raft-backed; writes must hit the leader. Every unary call and the `Work`
stream detect a `NOT_LEADER` fault (gRPC `FAILED_PRECONDITION`), re-dial the
advertised leader address, and retry with bounded exponential backoff + jitter.
Pass multiple seed addresses for a cluster: `new Publisher("a:7100,b:7100,c:7100")`
or `new Publisher(["a:7100", "b:7100"])`.

## Regenerating the typings

The typed bindings live in `src/generated/` and are committed. To regenerate
after a proto change (the `.proto` is vendored at `proto/rota/v1/rota.proto`):

```bash
npm run gen      # proto-loader-gen-types -> src/generated/
npm run build
```

## Tests

```bash
npm test         # builds, then runs the vitest e2e suite
```

The suite builds the `rota` Go binary and drives the SDK against real brokers
over gRPC (it skips gracefully if the Go toolchain is unavailable). Coverage:

- **`broker.test.ts`** — publish, `publishBatch` (atomic), `dedupKey`, the full
  Work-stream lifecycle (ack / `Requeue` / `DeadLetter` / RETRY-then-dead-letter),
  in-stream & off-stream complete-by-token, and `Message.extend`. Dead-lettering
  is observed via lane DLQ depth in `getStats`.
- **`control.test.ts`** — health / `describeCluster` / `getStats`, group config +
  lifecycle (pause/resume/cancel/purge/reap/teardown), lane config + a behavioral
  pause/resume check, policy (set/get + a CEL `validatePolicy` accept/reject),
  cron (schedule/list/pause/delete), and singleton leases (acquire/renew/release
  with contention + fencing).
- **`workflow.test.ts`** — the schedule-activity → complete flow (which proves the
  prefix checksum matches the Go server byte-for-byte), plus signals,
  cancellation, durable timers, continue-as-new, fail, and `listRuns`.
- **`cluster.test.ts`** — boots a real **3-node Raft cluster** and points clients
  at a *follower*, asserting the SDK transparently follows the leader for unary
  RPCs (`not-leader-bin` trailer) and for the Work stream (in-stream NOT_LEADER
  frame).

> The dashboard read RPCs (`ListGroups`, `ListDeadLetters`, `ListLeases`,
> `PeekMessages`, `GetLaneFairness`, `GetPolicyHealth`, `RedriveDeadLetter`) are
> intentionally not part of the SDK surface — they are served by the broker's
> HTTP/JSON gateway for the operator dashboard, matching the Python SDK.

See [`../../docs/sdk-parity.md`](../../docs/sdk-parity.md) for Python/TypeScript
feature parity and [`../../examples/`](../../examples/) for runnable examples.
