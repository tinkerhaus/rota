# Part 5 — Durable workflows

**Goal:** write a crash-proof, long-running workflow as code — with activities,
signals, durable timers, and continue-as-new — and understand the replay model
that keeps it correct.

← [Part 4 — Reliability & scheduling](04-reliability-and-scheduling.md) · Next: [Part 6 — Clustering & operations](06-clustering-and-operations.md) →

This is Rota's second layer, built on the same fair substrate. If you've used
Temporal the model will feel familiar; the difference is it runs on one binary and
your workflow's steps inherit cross-tenant fairness for free.

---

## 5.1 The model

A **workflow run** is long-running logic that survives crashes by **replaying its
history**:

- Each run has a **per-run, append-only, ordered event history** — its system of
  record (`WORKFLOW_STARTED`, `ACTIVITY_SCHEDULED`/`COMPLETED`, `TIMER_FIRED`,
  `SIGNAL_RECEIVED`, …).
- **Your workflow logic runs in a worker, never in the broker.** A worker **polls a
  task**, receives the run's history, **replays it deterministically**, and returns
  **commands** ("schedule activity X", "start a timer", "complete with result R").
- **Activities** — the side-effecting steps (call an API, charge a card) — dispatch
  as **ordinary fair-scheduled leases**. So a noisy tenant's 10k workflows can't
  starve a quiet tenant's three, on the workflow surface too.
- The **decider must be deterministic on the history**: given the same history it
  must return the same commands. Don't read the clock, RNG, or external state inside
  it — do that in *activities*. The SDK computes a **prefix checksum** of the
  history you replayed and sends it with your commands; the leader rejects any
  decision computed against divergent history (this is what makes replay safe).

Two roles, two workers:

- a **workflow worker** runs your `decide(run_id, history) -> [commands]`;
- an **activity worker** runs `handle(task) -> (result_bytes, success)`.

---

## 5.2 Your first workflow: an order that charges a card

The workflow: schedule a `charge` activity; when it completes, complete the run.

### Python

```python
import threading
from rota import (WorkflowClient, run_workflow_worker, run_activity_worker,
                  schedule_activity, complete_workflow)
from rota._gen.rota.v1 import rota_pb2 as pb

# Activity worker: the side-effecting step. Returns (result, success).
def charge(task):
    # task.input is the activity input bytes
    return b'{"charged":true}', True

# Workflow decider: deterministic function of history -> commands.
def decide(run_id, history):
    scheduled = any(e.event_type == pb.HET_ACTIVITY_SCHEDULED for e in history)
    completed = any(e.event_type == pb.HET_ACTIVITY_COMPLETED for e in history)
    if not scheduled:
        return [schedule_activity("charge", b"100")]   # step 1
    if completed:
        return [complete_workflow(b"done")]            # step 2
    return []                                          # waiting on the activity

stop = threading.Event()
threading.Thread(target=run_activity_worker,
                 args=("127.0.0.1:7100", "charge", "act-1", charge),
                 kwargs={"stop_event": stop}, daemon=True).start()
threading.Thread(target=run_workflow_worker,
                 args=("127.0.0.1:7100", "orders", "wf-1", decide),
                 kwargs={"stop_event": stop}, daemon=True).start()

client = WorkflowClient("127.0.0.1:7100")
run_id = client.start_workflow("orders", tenant_id="tenant-A", input=b"{}")

# poll until done
import time
while client.get_run(run_id).status != pb.WF_COMPLETED:
    time.sleep(0.05)
print("completed:", run_id)
for e in client.get_history(run_id):
    print(" ", pb.HistoryEventType.Name(e.event_type))
stop.set()
```

### TypeScript

```ts
import {
  WorkflowClient, runWorkflowWorker, runActivityWorker,
  scheduleActivity, completeWorkflow, HistoryEventType, WorkflowStatus,
} from "rota";

const ac = new AbortController();

// Activity worker: the side-effecting step. Returns [result, success].
runActivityWorker("127.0.0.1:7100", "charge", "act-1",
  (task) => [Buffer.from(JSON.stringify({ charged: true })), true],
  { signal: ac.signal });

// Workflow decider: deterministic function of history -> commands.
const decide = (runId, history) => {
  const scheduled = history.some((e) => e.eventType === HistoryEventType.HET_ACTIVITY_SCHEDULED);
  const completed = history.some((e) => e.eventType === HistoryEventType.HET_ACTIVITY_COMPLETED);
  if (!scheduled) return [scheduleActivity("charge", Buffer.from("100"))]; // step 1
  if (completed) return [completeWorkflow(Buffer.from("done"))];           // step 2
  return [];                                                               // waiting
};
runWorkflowWorker("127.0.0.1:7100", "orders", "wf-1", decide, { signal: ac.signal });

const client = new WorkflowClient("127.0.0.1:7100");
const runId = await client.startWorkflow("orders", { tenantId: "tenant-A", input: Buffer.from("{}") });

while ((await client.getRun(runId)).status !== WorkflowStatus.WF_COMPLETED)
  await new Promise((r) => setTimeout(r, 50));
console.log("completed:", runId);
for (const e of await client.getHistory(runId)) console.log("  eventType", e.eventType);
ac.abort();
```

The history will read: `WORKFLOW_STARTED → WORKFLOW_TASK_* → ACTIVITY_SCHEDULED →
ACTIVITY_COMPLETED → WORKFLOW_COMPLETED`. The decider ran several times (once per
task), each time replaying the full history and deciding the *next* step — that's
the replay model. You never wrote checksum or concurrency code; the SDK and the
leader's determinism gate handled it.

> **Why the decider is shaped like that.** It's not "do step 1, await, do step 2"
> imperative code — it's a *pure function of history*. On every task it re-derives
> where it is from the events and returns what to do next. That's what lets a run
> resume on any worker after any crash.

---

## 5.3 The command palette

A decider returns any of these (import the helper of the same name):

| Command | Effect |
|---|---|
| `schedule_activity(type, input)` | Dispatch an activity; its result lands back in history as `ACTIVITY_COMPLETED`/`FAILED`. |
| `start_timer(delay_ms)` | A durable timer; `TIMER_FIRED` is appended when it elapses. |
| `complete_workflow(result)` | Terminally complete the run (`WF_COMPLETED`). |
| `fail_workflow(result)` | Terminally fail the run (`WF_FAILED`). |
| `continue_as_new(input)` | Close this run (`WF_CONTINUED`) and start a fresh successor — bounds history growth for loops. |

---

## 5.4 Signals — external events into a running run

A signal appends `SIGNAL_RECEIVED` to the history and dispatches a task, so the
decider can react. Use it for human approvals, external events, etc.

### Python

```python
def decide(run_id, history):
    if any(e.event_type == pb.HET_SIGNAL_RECEIVED for e in history):
        return [complete_workflow(b"approved")]
    return []                          # wait for the signal

run_id = client.start_workflow("approvals", tenant_id="t")
# ... later, from anywhere:
client.signal_workflow(run_id, "approve", payload=b"yes")
```

### TypeScript

```ts
const decide = (runId, history) =>
  history.some((e) => e.eventType === HistoryEventType.HET_SIGNAL_RECEIVED)
    ? [completeWorkflow(Buffer.from("approved"))]
    : [];                              // wait for the signal

const runId = await client.startWorkflow("approvals", { tenantId: "t" });
// ... later, from anywhere:
await client.signalWorkflow(runId, "approve", Buffer.from("yes"));
```

---

## 5.5 Durable timers — `sleep` that survives crashes

Return `start_timer(ms)`; the leader stamps the absolute fire time, and a
`TIMER_FIRED` event is appended when it elapses — even if every worker was down in
between.

```python
def decide(run_id, history):
    started = any(e.event_type == pb.HET_TIMER_STARTED for e in history)
    fired   = any(e.event_type == pb.HET_TIMER_FIRED   for e in history)
    if not started: return [start_timer(5000)]        # sleep 5s, durably
    if fired:       return [complete_workflow(b"woke")]
    return []
```
```ts
const decide = (runId, history) => {
  const started = history.some((e) => e.eventType === HistoryEventType.HET_TIMER_STARTED);
  const fired   = history.some((e) => e.eventType === HistoryEventType.HET_TIMER_FIRED);
  if (!started) return [startTimer(5000)];            // sleep 5s, durably
  if (fired)    return [completeWorkflow(Buffer.from("woke"))];
  return [];
};
```

---

## 5.6 Cancellation, fail, and continue-as-new

**Cancel** a run from the client (leader-driven; reaches `WF_CANCELED`):

```python
client.cancel_workflow(run_id, reason=b"operator")   # -> True
```
```ts
await client.cancelWorkflow(runId, Buffer.from("operator")); // -> true
```

**Fail** terminally from the decider with `fail_workflow(result)` → `WF_FAILED`.

**Continue-as-new** closes the current run and starts a successor with fresh
history — essential for long-running loops so history doesn't grow without bound.
The successor links back via `parent_run_id`:

```python
def decide(run_id, history):
    count = int(history[0].attrs.decode() or "0")    # seed carried in WORKFLOW_STARTED
    if count < 100:
        return [continue_as_new(str(count + 1).encode())]
    return [complete_workflow(b"loop-done")]
```
```ts
const decide = (runId, history) => {
  const count = parseInt(history[0]?.attrs?.toString() || "0", 10);
  return count < 100
    ? [continueAsNew(Buffer.from(String(count + 1)))]
    : [completeWorkflow(Buffer.from("loop-done"))];
};
```

---

## 5.7 Inspecting runs

```python
runs = client.list_runs(status=pb.WF_COMPLETED).runs    # filter by status
run  = client.get_run(run_id)                           # status, epoch, history seq
hist = client.get_history(run_id)                       # full event list
```
```ts
const { runs } = await client.listRuns({ status: WorkflowStatus.WF_COMPLETED });
const run  = await client.getRun(runId);
const hist = await client.getHistory(runId);
```

The dashboard renders each run as a **swimlane timeline** — activities, timers, and
signals laid out over time — which is the easiest way to debug a stuck workflow.

---

## What you learned

- A workflow is a **deterministic decider over an append-only history**; the SDK +
  a leader-side **checksum gate** make replay crash-safe.
- **Activities** do the side effects and dispatch as **fair leases** — workflows
  inherit cross-tenant fairness.
- First-class **signals**, **durable timers**, **cancellation**, **fail**, and
  **continue-as-new**.
- The worker protocol is gRPC, so the Python and TypeScript SDKs (or any language)
  drive the same engine.

Next: run it for real — a cluster, leader-following, failover, and operations. →
[Part 6 — Clustering & operations](06-clustering-and-operations.md)
