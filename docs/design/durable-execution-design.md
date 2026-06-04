All claims verified against code. I now have complete ground truth and the adversarial verdicts. Writing the design document.

---

# Rota Durable Execution — Decision-Ready Design Document

**Status:** committed direction (maintainer-authoritative). Build a best-in-class, Temporal-grade durable-execution engine on Rota, cost no object. Primary surface = durable-execution-as-code; declarative Canvas is sugar on the same kernel. Python SDK first, Go second. Svelte UI already designed.

**Verification basis:** every load-bearing claim below was checked against the code in this commit — `internal/fsm/fsm.go` (Apply switch has **no default**, lines 60-97; unconditional `applied_index` advance, line 101; full-stream `Persist`/`DeleteRange`-`Restore`, lines 555-633), `internal/storage/keys.go` (`AppKeyspaceBounds()` = `[0x00, 0x0B)`, line 187; timer kinds end at `0x03`), `internal/raftpebble/store.go` (`StoreLogs` fsync, line 74), `internal/node/node.go` (`LeaseOne` → `Pick` → `CmdLease`, lines 296-348 — the decision-replication seam), `internal/transport/leader.go` (`mutatingMethods`), and the proto (`dedup_key` present on `MessageSpec` field 12 but dropped by `publishReqFromSpec`; no `BatchingFSM`/`ApplyBatch` anywhere). This document **incorporates the adversarial verdicts as binding**: where a verifier returned `holds=false`, the design is corrected or the gap is escalated to a blocking open risk. It does not paper over them.

---

## 1. Revised positioning & honesty bar

### 1.1 The claim we now make — and the moat that makes it winnable

**The defensible claim:** *Rota is a single-binary, zero-dependency durable-execution engine with Temporal-grade deterministic replay, and a programmable, completion-aware, cross-tenant fairness layer that no incumbent — including Temporal's 2026 fairness GA — can match.*

We lead with the **fairness moat**, not raw throughput, because that is the axis on which this is winnable:

- Temporal's fairness is **schedule-time and fixed**: the fairness key is stamped at enqueue, by Temporal's algorithm, parameterized by weights you supply. You cannot ship a fairness *function*.
- Rota's fairness is **`scheduler.Pick` — a leader-only ranking over per-group virtual-time state in `GroupMeta`, where only the resolved decision (`CmdLease`) is replicated, never the policy code** (verified: node.go:316-325, the ADR-0004 seam). The policy is hot-swappable CEL/WASM installed via `CmdSetPolicy`, replicated as a binding. It is **completion-aware** because `GroupMeta.inflight_count` (proto field 9) is a live, quorum-written cost signal the policy reads at every pick.
- Because durable execution rides the *same* broker, **one programmable fairness policy governs workflow tasks, activity tasks, and raw messages uniformly.** A noisy tenant with 10,000 in-flight workflows cannot starve a quiet tenant's three, on *either* dispatch surface — by construction, the moment `group_id = tenant` (Section 6).

That is the wedge: byzantine-safe, programmable, completion-aware fairness across durable-exec *and* messaging, in one static binary with no Cassandra/Postgres/Elasticsearch to operate. Temporal cannot reach it without adopting exactly the leader-only-eval/replicate-the-decision seam Rota is built on.

### 1.2 The honesty bar — what must be true *before* we make the durable-execution claim publicly

These are gates, not aspirations. Each maps to an adversarial finding.

1. **The correctness preconditions must ship first (Section 2.6).** The `Apply` default-case fix, the `meta:schema_ver` gate, and the `AppKeyspaceBounds()` extension are not optional — without them the engine has *latent cluster-divergence bugs of its own* that are worse than any workflow could cause.

2. **We claim "no determinism bug can commit a *re-interpretation of already-recorded history*, and no divergence is worse than a single Temporal worker" — NOT "structurally impossible to diverge."** The adversarial pass (divergence-protocol, both lenses `holds=true`) proved the absolute thesis is literally false: the **leading edge of every await-branch — the first forward decision — is committed un-validated**, exactly as Temporal commits it, because neither system can validate a forward command against a record that doesn't exist yet. We are *equal to* Temporal here, not worse, and we must word it that way. (Detail and the residual terminal-decision gap: Section 2.4, Risk R1.)

3. **Payload determinism must be enforced by byte-comparison on replay, or the safety claim must be explicitly scoped to event *shape*.** The versioning lens returned `holds=false` (high) on a genuine committed-divergence hole: a command whose *type/seq/order* match the recorded event but whose *input bytes* differ (e.g. `set`-iteration-order-dependent activity input) passes validation and commits divergent effects. Fix is mandatory (Section 2.5); until it ships we do not claim payload-determinism safety.

4. **The throughput numbers must be re-derived honestly and gated on apply-side group-commit.** The scale lens returned `holds=false` (high): the headline single-Raft starts/sec and completions/sec figures (a) undercounted entries/run by omitting lease entries (~22 not ~12 for a 10-activity run), (b) silently pre-assumed an unbuilt apply-side group-commit, and (c) presented two peak numbers that cannot co-occur on one fsync stream. Corrected targets and the group-commit prerequisite: Section 5.

5. **The snapshot/catch-up bound must be re-attributed and benchmarked honestly.** The history-growth lens returned `holds=false` (high): `pebble.Checkpoint` makes *local* snapshot creation and restore-CPU cheap but does **not** reduce InstallSnapshot bytes-on-wire (hashicorp/raft's transport is an opaque `io.Copy` byte stream; verified against the library). The real catch-up bound is set by TTL+archival and is `O(open-runs × per-run-cap)`, not a "small recent window". Corrected: Section 5.

6. **Benchmarks before claims.** No "Temporal-grade" claim ships without the published benchmark suite of Section 5.6, including the *brutal-honesty* per-device fsync table and the large-dataset failover test that distinguishes warm-follower re-election from cold-follower catch-up.

We can honestly say **today**: "durable execution with deterministic replay, single binary, zero deps, and programmable cross-tenant fairness." We may **not** yet say "infinitely scalable" or "impossible to corrupt" — those are the lies the adversarial pass caught.

---

## 2. THE COMMITTED-DIVERGENCE PROTOCOL

This is the load-bearing correctness design. The hard Rota-specific problem: in Temporal a non-deterministic workflow fails on one worker and corrupts nothing shared. In Rota, if workflow progress is recorded via *Raft-committed commands*, a naïve design could commit divergent state to **every node** — strictly worse than Temporal. The protocol makes the genuinely-worse outcomes impossible and reduces the residual to exactly Temporal's irreducible limit.

### 2.1 What is replicated: leader-validated EVENTS — never commands, never code, never a hash

Three candidate replication units; the resolution is exact:

- **Rejected — replicate worker COMMANDS.** A worker's command list is its *request* to mutate state. If raw commands were the Raft payload, a determinism bug would faithfully replicate the *wrong* stream cluster-wide. The command stream is therefore an **input to validation, never a replicated payload.**
- **Rejected — replicate a HASH of state.** A hash can detect divergence after the fact but cannot *produce* or *advance* state deterministically. Hashes are used internally (the prefix checksum, Section 2.3) but never drive the FSM.
- **CHOSEN — replicate leader-validated EVENTS.** What enters Raft is a single `CmdWFAppendEvents` carrying a vetted, leader-stamped list of `HistoryEvent`s that the leader has **proven** is a consistent extension of the already-committed history prefix. Each event is self-contained and deterministic to apply: leader-assigned `event_id`, leader-stamped `event_time_ms`, leader-stamped `fire_at`/deadlines. `Apply` does zero non-deterministic work — it is a mechanical append + index maintenance, the same shape as `applyLease`/`applyFireCron`.

This mirrors Temporal's split (worker emits *commands*; the service validates and writes *events*) with the Rota twist that validation must be **replication-safe**: it runs on the leader, off the FSM, and produces a payload every follower applies byte-identically.

### 2.2 Where user code runs: on a worker, OFF the FSM — and the proof Apply stays deterministic

User workflow code **never runs in `Apply` and never runs in the leader's proposal path.** It runs in the worker process, replaying history and emitting a command list — exactly as activity handlers run in workers today, never in `Apply`. The seam is identical to `scheduler.Pick`: the non-deterministic compute (here, the workflow replay) runs off the FSM; only the *resolved decision* (the validated event batch) is proposed; followers apply the committed outcome.

Apply determinism is then guaranteed by three structural facts, each verified:

1. **Apply only ever sees the validated event batch.** The new `case CmdWFAppendEvents` reads prior protos, appends events at leader-assigned ids, schedules derived effects (activity publishes via `publishOne`, timers via `addTimer`), and writes the run row — all inside the one atomic Pebble batch that already co-commits `applied_index` (fsm.go:101-104). No CEL, no SDK call, no clock, no RNG, no map-order iteration.

2. **The non-deterministic step is already isolated by Rota's seam** (`LeaseOne` → `Pick` → `CmdLease`, verified node.go:316-325). The decider is the new `Pick`; the validated batch is the new `LeaseCmd`.

3. **The `Apply` switch gets a hard `default` + a schema gate** — a *precondition* (Section 2.6), because today an unknown `CmdType` silently no-ops *and* advances `applied_index` (verified: no default at fsm.go:60-97, unconditional advance at line 101). That is itself a cluster-divergence vector when the command set is extended.

### 2.3 The leader-side validation gate (the crux mechanism)

Before the leader proposes anything, `node.CompleteWorkflowTask` (leader-only, off the FSM, in the gRPC handler — structurally `LeaseOne` calling `Pick` before `apply`) runs a deterministic gate. **There is no code path from a failed gate to `raft.Apply`** (verified: `apply` at node.go:323 is the sole Raft entrypoint and the gate is a plain call before it).

```
CompleteWorkflowTask(req):
  require leader                                    # LeaderGuardInterceptor (add to mutatingMethods)
  run, hist := load committed history prefix up to req.started_event_id   # leader-local, current

  # OCC GATE 1 — epoch fence (stale/racing completion)
  if req.run_epoch  != run.cur_run_epoch:   return WFTaskFailed{STALE_EPOCH,   retryable}
  # OCC GATE 2 — history-seq fence (history advanced under the worker)
  if req.history_seq!= run.cur_history_seq: return WFTaskFailed{STALE_HISTORY, retryable}
  # OCC GATE 3 — build-id/version pin (Section 2.5)
  if !versionCompatible(req.build_id, run.pinned_build): return WFTaskFailed{VERSION_MISMATCH, retryable}

  # DETERMINISM GATE — recorded-prefix re-validation
  if req.prefix_checksum != storedPrefixChecksum(run):  return WFTaskFailed{NON_DETERMINISM, retryable}
  events, err := translateAndValidate(run, hist, req.commands)   # PURE; runs NO user code
  if err != nil:                                       return WFTaskFailed{NON_DETERMINISM, retryable}

  # ONLY NOW: stamp ids/clock/tokens, propose ONE batch
  return n.apply(CmdWFAppendEvents{run_id, run_epoch, history_seq, events})
```

`translateAndValidate` checks, against the recorded prefix:
1. **Recorded-prefix byte-match** — for every command position already in history, the worker's re-emitted command must match the recorded event byte-for-byte (Temporal's replay-consistency, made replication-safe by running on the leader before the log). Catches reordered/added/removed awaits and mid-flight reinterpretation of *recorded* decisions.
2. **Prefix checksum** — `req.prefix_checksum` (a rolling hash the worker computes over command types + stable attrs of the replayed prefix) must equal the checksum stored when each decision was *originally* recorded. Cheap first tripwire.
3. **Shape/contiguity/legality** — monotone gap-free command ids; no command past a terminal; no completion-of-an-unscheduled-activity; no invented timer fire; payload-cap/claim-check compliance.

On pass, the leader stamps **forward** clock values (timer `fire_at`, activity deadlines) — inputs carried *in* the event, never recomputed in Apply — and proposes. On any fail, it proposes **nothing**; the task is a retryable `WORKFLOW_TASK_FAILED`, redelivered via the existing visibility-timeout. **The run stays exactly at `history_seq=N`, uncorrupted.**

### 2.4 The optimistic-concurrency token, and why divergence cannot be *committed* under races

Two OCC values, both echoed by the worker and checked by the gate **and re-checked deterministically inside Apply**:

- **`history_seq` (N)** — the OCC version: the `event_id` the worker replayed against.
- **`run_epoch`** — a per-run generation bumped on every workflow-task dispatch; the fencing token (same discipline as `Singleton.Fence` and message `Epoch`).

The adversarial races lens (`holds=true`, medium) attacked the leader-gate TOCTOU directly: two concurrent `CompleteWorkflowTask` calls can both pass the *leader* gate against the same `(epoch, seq)` snapshot, because the leader read is not serialized against other in-flight RPCs. **This does not admit divergence**, because the real fence is the **apply-time re-check inside the one atomic Pebble batch** — mirroring how `applyLease` re-validates `headReady` against committed state even though `Pick` is advisory:

```go
func (f *FSM) applyWFAppendEvents(b *pebble.Batch, c *WFAppendCmd) (any, error) {
    run := getRun(c.RunID)
    if run == nil { return nil, fmt.Errorf("wf: run %s missing", c.RunID) } // -> Apply error -> fail-stop
    // DETERMINISTIC re-check against COMMITTED state (idempotent under failover re-propose):
    if c.RunEpoch != run.CurRunEpoch || c.HistorySeq != run.CurHistorySeq {
        return &WFResult{Applied:false, Reason:"stale"}, nil  // benign no-op; still advances applied_index
    }
    if run.State == CLOSED { return &WFResult{Applied:false, Reason:"closed"}, nil }
    next := run.CurHistorySeq
    for _, e := range c.Events {
        next++
        if e.EventID != next { return nil, fmt.Errorf("wf: history gap") }  // -> fail-stop (forged entry)
        putProto(b, HistoryKey(c.RunID, e.EventID), e)
        switch e.Type {
        case TIMER_STARTED:        addTimer(b, e.FireAt, TimerWFFired, WFTimerRef(c.RunID, e.EventID), e.EventID)
        case ACTIVITY_SCHEDULED:   f.publishActivityTask(b, c.RunID, e)   // reuses publishOne
        case WF_COMPLETED, WF_FAILED, WF_CANCELED, WF_CONTINUED_AS_NEW: run.State = CLOSED
        }
    }
    run.CurHistorySeq = next; run.CurRunEpoch++
    if run.State != CLOSED && hasPendingDecision(run) { f.enqueueWorkflowTask(b, run) }
    putProto(b, RunKey(c.RunID), run)
    return &WFResult{Applied:true, NewSeq:next}, nil
}
```

The first entry applied bumps `(CurHistorySeq, CurRunEpoch)` atomically; any second entry against the same snapshot — duplicate dispatch, stale-cache worker, failover re-propose — fails the re-check as a **benign no-op that legitimately advances `applied_index`**. The gap-free append (`e.EventID == CurHistorySeq+1`) is an independent guard: an already-appended id can never be rewritten. **No interleaving commits two divergent batches against one OCC version.** This is the verified-true core.

**The residual the absolute thesis over-claimed (Risk R1, from the protocol lens):** a *fresh* run whose **first forward decision** is non-deterministic (`if random()<.5: charge else: refund`) commits that coin-flip un-validated — there is no recorded prefix to match it against. This is **byte-for-byte identical to Temporal**, which also commits the first decision and only catches divergence on the *next* replay; Rota's gate catches it at the same moment (the next task's prefix-match). The genuinely-worse case (re-interpreting *recorded* history under a bad deploy) **is** prevented. The one place neither system ever catches it: a **terminal** non-deterministic decision (`if random()<.5: ship; return`) — the run closes, no next task ever replays it, so it is permanently divergent-but-undetected. We mitigate, not eliminate (Section 2.7).

**Required wording correction (binding):** replace "structurally impossible / closed by construction / never reaches the log" with *"no determinism bug can commit a re-interpretation of already-recorded history, and no divergence is ever worse than a single Temporal worker."*

### 2.5 Mid-flight code change & payload-divergence defenses

**Forward-logic change without a patch (versioning lens, `holds=false`, high).** A run parked at event N awaiting a signal; a deploy changes the post-signal branch from `charge` to `refund` with no patch marker. The recorded prefix `[0..N)` is unchanged ⇒ prefix-checksum passes; there is no recorded event past N ⇒ shape-validation passes; the divergent forward tail **commits**. The honest claim is narrower than the original design stated, and we add a **fail-closed enforcement path** the original design carried but never used:

- **Build-id pinning, fail-closed (OCC Gate 3).** `WorkflowStarted` records `pinned_build` (a content hash of the workflow definition graph). The gate **refuses** a completion from a worker whose `build_id ≠ pinned_build` for an in-flight run **unless** the change is gated behind a recorded `patched()` marker. This is Temporal's worker-versioning/pinning, made a *hard gate*: old runs drain on old workers or park as `VERSION_MISMATCH`; they are never reinterpreted by new code. The `build_id`/`sdk_version` fields already specified on the completion request are now *load-bearing*, not decorative.
- **`patched()` markers** for in-place logic edits: first reach emits `MarkerRecorded{patch_id}`; replay of old runs finds no marker → old branch; new runs → new branch. The replayer (Section 3) enforces patch discipline in CI: an unpatched logic change fails replay against old histories.
- **Residual (Risk R1):** the *first* new decision after any change is unverifiable by construction. Build-id pinning reduces this to "old code must still be deployed for old runs" — strong, but it is still operator discipline, not a consensus tripwire. We state this plainly.

**Payload divergence (versioning-2 lens, `holds=false`, high — a genuine committed-divergence hole).** A command whose type/seq/order match the recorded event but whose `input` *bytes* differ (e.g. `await execute_activity(charge, list(set(items)))` — `set` iteration is `PYTHONHASHSEED`-dependent, verified) passes all current checks and commits divergent effects. **Three mandatory fixes:**

1. **Payload byte-comparison on replay.** On replay the worker re-emits the **full** command including input bytes; the leader compares re-emitted input against the recorded `*_SCHEDULED` event's input byte-for-byte (cheap — payloads are capped) and **rejects on mismatch.** This converts the silent commit into a (wedge-class) rejection. Without this, the "only validated events reach Apply" claim is unsound for content.
2. **Canonical encoding (mandated codec).** All command payloads serialize via a deterministic codec (`json.dumps(sort_keys=True)` / canonical proto), so dict/set-derived structures encode identically regardless of iteration order — neutralizes the common case at the encode boundary.
3. **Prefix-checksum must cover every observable field.** `_attr_digest` must hash *all* fields any workflow can observe (full activity result bytes, signal payloads, marker values, timer ids) — "key fields only" is a tripwire bypass.

**Residual (Risk R2):** payload determinism for activity *inputs the leader cannot recompute* is fundamentally un-validatable as a positive property; the byte-comparison only catches divergence *between a replay and its own recorded prefix*, not a first-time non-deterministic input. So §2.1's guarantee is downgraded honestly to **"event shape impossible-to-diverge by construction cluster-wide; content best-effort, worst case a rejected/wedged task rather than a silent committed divergence."**

### 2.6 Hard preconditions (blockers — no engine code until these land)

All verified against code; all flagged by multiple lenses as mandatory:

1. **`Apply` default → error + `meta:schema_ver` gate + CmdType-range fail-stop** (fsm.go:60-97/101). An unknown command must **halt the node loudly** (a returned error from `Apply` is fatal in hashicorp/raft and does *not* advance `appliedIndex`), never silently no-op-and-advance. A follower on older code must refuse a log written by newer code.
2. **Extend `AppKeyspaceBounds()`** (keys.go:187) from `[0x00, 0x0B)` to cover every new workflow tag, with a test asserting *every tag any applier writes is in-bound.* Otherwise workflow history is silently dropped from `Persist`/`Restore` (fsm.go:567,608) → instant divergence on follower catch-up. This is the single most dangerous, easiest-to-miss hole.
3. **Epoch-bind the completion token to `(run_id, step_id, attempt, run_epoch)`** (the verified hole: `tagToken` → lease id only). Fixes "a late Complete resolves a different attempt" — acute for activities feeding history.
4. **Max-lease-lifetime backstop** (verified hole: none today). A wedged async activity must not pin `inflight_count`/a run forever; the `TimerWFActStartToClose`/`TimerWFRunTimeout` kinds double as this.
5. **Wire `dedup_key`** in `publishReqFromSpec` (verified dropped, broker.go:32; Python sets it, Go drops it) for producer-idempotent workflow starts.

### 2.7 Divergence-detection tooling (closes the residual operationally)

Because the first/terminal forward decision is unverifiable by consensus, we ship the detection layer the lenses required as recommended hardening:

- **Replayer CI gate** (Section 3) — every team replays production histories against new code before deploy; green = the leader won't reject for *recorded-prefix* divergence. Explicitly **cannot** catch forward-decision or hash-seed divergence (multi-seed replay below addresses the latter).
- **Multi-seed replay** — the replayer runs each history under ≥2 distinct `PYTHONHASHSEED` values, defeating the single-seed blind spot; CI fleet pins `PYTHONHASHSEED=0` and asserts it at startup.
- **Optional periodic shadow-replay** of in-flight runs to surface forward-decision divergence that would otherwise only appear on the next organic task (and never for terminal decisions).
- **Sandbox bans ordering-unstable iteration**, not just `time`/`random`/io: bare `set`/`frozenset` iteration, set comprehensions feeding command emission, `dict(**kwargs)` from unordered sources, `os.environ` — enforced by AST analyzer + runtime check.

---

## 3. Replay core + event model + worker protocol

### 3.1 Mental model

A **workflow run** is ordinary user code whose only durable trace is a **per-run, totally-ordered event history** in Pebble (the system of record; ordering from Raft total order, never broker delivery; never exposed as a consumer feed — NON-GOAL honored). The broker is reused *verbatim as transport*: a **workflow task** and an **activity task** are each a leased `Message` on a lane. The worker replays history, re-executes the workflow function deterministically, emits commands, completes its workflow-task lease carrying the command list; the leader validates (Section 2) and proposes one `CmdWFAppendEvents`; the FSM appends events with seqs assigned in Apply and schedules effects.

### 3.2 Event model

`HistoryEvent { event_id, event_type, event_time_ms (leader-stamped), task_id (=applied-index, debug), oneof attrs }`. Three categories:

- **Lifecycle:** `WorkflowStarted` (type, input ref, parent, cron, continue-as-new predecessor, retry policy, run timeout, **pinned_build**), `WorkflowCompleted/Failed/Canceled/Terminated`, `ContinueAsNew`.
- **Workflow-task bracket** (the determinism frame): `WorkflowTaskScheduled`, `WorkflowTaskStarted` (leader stamps the *only* clock the workflow may read), `WorkflowTaskCompleted`, `WorkflowTaskFailed/TimedOut`.
- **Command-events** (produced *only* via a `WorkflowTaskCompleted`; the things validation re-derives): `ActivityScheduled`, `TimerStarted`, `ChildWorkflowInitiated`, `MarkerRecorded`, `WorkflowCompleted/Failed`, `ContinueAsNew`, `UpsertSearchAttributes`.
- **External events** (injected by the world; the leader appends on its own authority; the worker may never assert them): `ActivityStarted/Completed/Failed/TimedOut`, `TimerFired`, `SignalReceived`, `ChildWorkflow*` terminals, `WorkflowCancelRequested`, `WorkflowTaskTimedOut`.

The command-event vs external-event split is exactly what makes the validation gate tractable: only command-events are deterministic functions of (prefix + code) and so are validated; external events have no worker counterpart to assert.

### 3.3 Workflow-task lifecycle & sticky cache

```
new events appended (one atomic batch) ──► WorkflowTaskScheduled + a Message on
  lane "__wf/<type>", group_id = run_id  ──► worker leases via Work stream
  ──► WorkflowTaskStarted (leader stamps event_time_ms)
  ──► WORKER replays (sticky cache hit: only suffix; cold miss: full history scan),
      re-executes, emits commands ──► completes lease carrying command list
  ──► LEADER validates vs prefix (Section 2) ── valid → CmdWFAppendEvents | invalid → reject, park
  ──► FSM appends WorkflowTaskCompleted + command-events + schedules effects (one batch) ──► loop
```

**One-task-per-run for free:** `group_id = run_id` gives per-group head-of-line serialization (only the lowest-msgID READY message of a group is leasable, a group with an inflight lease yields none) — verified broker mechanic — so there is at-most-one outstanding workflow task per run with **zero new dispatch code.** This is Temporal's "one workflow task in flight per execution" invariant, inherited.

**Sticky cache** is pure derived state (LRU of `run_id → suspended coroutine + replayed-up-to seq`). A cache miss costs a full replay; a worker crash loses only cached objects; history in Pebble is the only system of record, so the cache can be stale with **zero durability consequence.** Sticky routing partitions the WF lane per sticky worker (`__wf/<type>/sticky/<wid>`); a `TimerWFStickyExpire` wheel timer falls back to the shared lane.

### 3.4 Worker protocol (proto, Python + Go)

A new `WorkflowService` (feature-flagged extension; the `Broker`/`Control` protos stay domain-neutral, ADR-0010). Tasks **ride the existing `Work` stream as transport** (reusing lease/credit/visibility-timeout/leader-redirect); the typed task envelope + completion are new RPCs:

```proto
service WorkflowService {
  rpc PollWorkflowTask(...)            returns (WorkflowTask);
  rpc PollWorkflowHistory(...)         returns (stream HistoryEvent);     // cold-replay fetch
  rpc RespondWorkflowTaskCompleted(RespondWFTaskCompletedRequest) returns (...); // LEADER VALIDATES
  rpc RespondWorkflowTaskFailed(...)   returns (Empty);
  rpc PollActivityTask(...)            returns (ActivityTask);
  rpc RespondActivityCompleted(...) / Failed / Canceled  returns (...);   // complete-by-token
  rpc RecordActivityHeartbeat(...)     returns (Resp{cancel_requested});  // = ExtendVisibility + cancel
  // client: StartWorkflow, SignalWorkflow, SignalWithStart, QueryWorkflow, UpdateWorkflow,
  //         CancelWorkflow, TerminateWorkflow, DescribeWorkflow, GetWorkflowResult
}
message RespondWFTaskCompletedRequest {
  bytes  task_token = 1;            // epoch-bound to (run, wft, attempt, run_epoch)
  repeated Command commands = 2;    // worker INTENT, NOT raft Command; includes FULL input bytes (§2.5)
  bytes  prefix_checksum = 3;       // over ALL observable fields (§2.5 fix 3)
  uint64 history_seq = 4;           // OCC version
  uint64 run_epoch   = 5;           // OCC fence
  string build_id = 6;             // fail-closed pin (§2.5)
  string sdk_version = 7;
}
message Command {                    // ScheduleActivity | StartTimer | CancelTimer | RequestCancelActivity
  oneof kind { ... }                 // | RecordMarker | StartChild | SignalExternal | UpsertSearchAttrs
  uint32 command_id = 20;            // monotone per run; the determinism diff key
}                                    // | CompleteWorkflow | FailWorkflow | ContinueAsNew
```

The Go SDK is the determinism-sensitive impl language, so the Go worker library and the broker-side validator **share the Go types and the `command_matches_event` / checksum functions — single source of truth, no drift** between "what the worker thinks is deterministic" and "what the leader enforces."

**SDK primitives** (first-execution emit → recorded event → replay resolution): `execute_activity`→`ScheduleActivity`→`ActivityScheduled/Completed`; `sleep`→`StartTimer`→`TimerStarted/Fired`; `now()` reads the WFT's stamped `event_time` (replay-stable; no `time.time()` in workflow code); `uuid4/random` seeded from a `MarkerRecorded`; `side_effect(fn)` runs once → `MarkerRecorded` → replay returns recorded value; `patched(id)`→`MarkerRecorded`; `execute_child_workflow`→`StartChild`; `wait_condition` re-evaluated each replay; signals drain buffered `SignalReceived` in order. **Deterministic event-loop contract (versioning-2 fix 4):** ready-queue is strict FIFO over deterministic registration order; native `asyncio.gather` completion-order is banned in favor of an SDK `workflow.gather` that resolves in command-seq order; the Go `Selector`/dispatcher carry the same obligation.

**Replay-test framework (table stakes):** `Replayer(...).replay_from_file/server` runs production histories against current code in-process, built from the *same* `WorkflowRunner` the worker uses, so green-in-replayer = the leader won't reject for recorded-prefix divergence. Runs each history under ≥2 hash seeds. `rotaviz replay` CLI steps the replay-debugger view.

**Bug fixes wired into the worker** (verified holes): lease auto-extend keeper thread (heartbeat = `ExtendVisibility` at `heartbeat_timeout/3`; today the Python worker never auto-extends) and `PAUSE_LANE` credit halt (today the worker only `logger.debug`s the control frame).

---

## 4. Full feature surface

The kernel principle: **only events cross into Raft; user code runs only on workers; the leader validates the command stream against the recorded prefix before proposing.** Every feature composes onto existing Rota primitives unless marked NEW.

| Feature | Events | Mechanism (Rota command / keyspace / tidx) | New primitive? |
|---|---|---|---|
| **Durable timers** (`workflow.sleep`) | `TimerStarted{fire_ts}` → `TimerFired` | Leader stamps `fire_ts`; `addTimer(fire_ts, TimerWFFired=0x04, WFTimerRef(run,seq))`; sweep proposes `CmdWFFireTimer`; idempotent via tidx-row presence + seq-epoch | **No** — tidx + new kind |
| **Activity retries / backoff / heartbeat / DLQ** | `ActivityScheduled/Started/Completed/Failed/TimedOut` | An activity task *is* a Rota message on `__act/<type>`, `group_id=tenant`. Reuses `applyNack` (attempt++, `backoffMs`, auto-DLQ), `applyExtend` (heartbeat=ExtendVisibility), visibility-timeout reclaim. **Ack/Nack→history bridge** (`CmdWFActivityResult`) routes the terminal outcome into history + schedules the next WF task | **Small** — the bridge + token epoch-bind |
| **Activity max-lifetime backstop** | `ActivityTimedOut{ScheduleToClose}` | `TimerWFActStartToClose=0x05` armed at schedule; force-fails a wedged activity, releases `inflight_count` (fixes named hole; also fixes it for plain messages) | **No** — new tidx kind |
| **Signals** (+ signal-with-start) | `SignalReceived{signal_id}` | `CmdWFSignal`, idempotent on `signal_id` (`tagWFSignalIdx`); schedules a WF task if idle, else buffers — the in-flight task loses the OCC fence and replays seeing it (no lost/double signals). Signal-with-start uses `tagWFRunByWF` (the natural home for the wired `dedup_key`) | **No mechanism** — command + index |
| **Queries** (read-only) | **none** (never appends) | Leader ships a query task; worker replays in side-effect-suppressed sandbox, runs the query handler; result returned out-of-band. `Barrier()` for read-your-writes. **Nothing proposed to Raft.** Any worker with the code can serve | **No** — pure worker-protocol |
| **Updates** (validated mutation + result) | `UpdateAccepted/Rejected`, `UpdateCompleted{result}` | Validator runs on worker (query-like, no write); if accepted, the same completion's command stream goes through the full gate; `CmdWFUpdate` atomically appends accept + side-effect events + completed. Idempotent + result-refetch on `update_id` (`tagWFUpdate`) | **No mechanism** — command + dedup index |
| **Child workflows** | parent: `ChildInitiated/Started/Completed/...`; child: own run with `parent_ref` | `CmdWFStartRun` for the child + parent's `ChildInitiated` in **one atomic batch** (no orphan). Child terminal cross-appends to parent + schedules a parent task. Parent-close policy (TERMINATE/REQUEST_CANCEL/ABANDON) is leader-side fan-out of `CmdWFCancel` | **No** — composition |
| **Continue-as-new** | `ContinuedAsNew{new_run_id}` (terminal) + new `WorkflowStarted` | `CmdWFContinueAsNew`: atomic close-old + create-successor (deterministic `run_id=hash(old,count)`, idempotent under failover). **Bounds per-run history** — load-bearing for snapshot cost (Section 5). Must *close* the old run so it becomes archival-eligible | **No mechanism** — leans on history-GC |
| **Cancellation** (cooperative) + cleanup | `WorkflowCancelRequested` → code cleans up → `WorkflowCanceled` | `CmdWFCancel` sets a flag + schedules a task; cancellation surfaces as a cancelled context; open activities/timers get leases/tidx range-cleaned (`dropLeasable`). `TERMINATE` variant is forceful | **No** |
| **Sagas / compensation** | only the compensating activities' events (+ optional `MarkerRecorded` for the DAG view) | **Pure SDK sugar** over activities; reverse-order is *workflow code* reconstructed by replay; the broker sees only activity tasks. Honors ADR-0010 | **None** |
| **Schedules** (cron-start) | per run: `WorkflowStarted{schedule_id, scheduled_time}` | Reuses `tagCron` + `TimerCronDue` + `applyFireCron`; the `fireCron` path branches to `CmdWFStartRun` (deterministic `run_id=hash(schedule_id,time)`). Overlap policy (Skip/BufferOne/AllowAll/CancelOther) is leader-side against `tagWFRunByWF`. **Requires wiring the stubbed `PauseCron`** (verified in `mutatingMethods` + proto but returns Unimplemented) | **None** — strongest reuse |

**Canvas sugar** (`@task`/`chain`/`group`/`chord`) compiles onto these same primitives: `chain`→sequential `execute_activity`; `group`/`chord`→one `CmdWFAppendEvents` with a 100-item `Effects.Publishes` fan-out (100 schedules = **1 entry, 1 fsync** — the single biggest throughput lever, reusing `applyPublishBatch`); `chord` fan-in→`wait_condition` over child results.

**New CmdTypes** (append after `CmdPublishBatch`; each gets a `*Cmd` pointer, an Apply case, an `apply*` handler in a new `apply_workflow.go`, and an entry in `mutatingMethods`): `CmdWFStartRun`, `CmdWFAppendEvents`, `CmdWFFireTimer`, `CmdWFSignal`, `CmdWFUpdate`, `CmdWFActivityResult`, `CmdWFCancel`, `CmdWFContinueAsNew` — plus the mandatory `default:`-error.

**New keyspace tags** (all inside the extended bound): `tagWFRun=0x0B`, `tagWFHistory=0x0C` (`0x0C ++ LP(run_id) ++ u64be(event_id)` — contiguous, range-deletable per run), `tagWFActivity=0x0D`, `tagWFTimer=0x0E`, `tagWFSignalIdx=0x0F`, `tagWFIdem=0x10` (effect idempotency `sha256(run_id|step_id|attempt)`), `tagWFTaskTok=0x11` (epoch-bound token), `tagWFRunByWF=0x12`, `tagWFUpdate=0x13`; `AppKeyspaceBounds()` → `[0x00, 0x14)`. **New tidx kinds:** `0x04`–`0x0A` (WF-fired, act-start-to-close, act-schedule-to-start, act-heartbeat, wf-task-timeout, sticky-expire, run-timeout).

---

## 5. Scale & storage — honest ceilings (reconciled with the adversarial attacks)

Two adversarial lenses returned `holds=false` (high) here. Both corrections are incorporated; the original headline numbers are **withdrawn** and replaced.

### 5.1 The cost basis (verified)

One committed entry does **two `pebble.Sync` fsyncs in series** on the leader: log-append (`StoreLogs`, store.go:74) and apply (`b.Commit(pebble.Sync)`, fsm.go:104). hashicorp/raft group-commits the *log* append across concurrent proposals, but **Rota implements `FSM`, not `BatchingFSM`** (verified: no `ApplyBatch`/`BatchApplyCh` anywhere), so `Apply` is called once per entry under `f.mu`, each with its own fsync. **The apply-fsync does not amortize today.** The single-group ceiling is `1/fsync_apply`:

| Device | fsync | Apply-bound ceiling (serial, today) |
|---|---|---|
| Consumer NVMe (no PLP) | 1–3 ms | ~300–1,000 entries/s |
| Datacenter NVMe (PLP) | 50–200 µs | ~5,000–20,000 entries/s |
| EBS gp3 / network block | 1–5 ms | ~200–1,000 entries/s |

### 5.2 Entries per run — corrected (was undercounted)

The original "~12 entries/run" **omitted lease entries**. `scheduler.Pick` runs per lease and proposes a real mutating `CmdLease` (verified). A sequential-10-activity run costs: 1 start + ~10 activity-task leases + ~10 fused result+decide entries + ~11 workflow-task-commit entries ≈ **~22 entries/run**, not 12. (Push/coalesced-lease dispatch could shave the lease entries — an open optimization, Section 8.)

### 5.3 Batching is the design, not an option

- **Coalesce a workflow-task transition into ONE entry** (`CmdWFAppendEvents`): all of its events + effects (activity publishes, timer arms, completion) in one Pebble batch, one fsync. Collapses the ~55 individual events to one entry per decision boundary.
- **Fan-out = one `PublishBatch`** (100 schedules → 1 entry).
- **Fuse result-recorded + decider-scheduled** (`CmdWFActivityResult`): recording `ActivityCompleted` and enqueuing the next WF task is one entry (mirrors `applyNack`'s record-and-requeue).
- **Never a Raft entry:** heartbeats (coalesce to a soft leader-local deadline bump, persist only at lease-renewal boundaries), query/replay reads (`Barrier()`), sticky-cache poll hits.
- **Apply-side group-commit (NOW A HARD PREREQUISITE for the quoted numbers, not a "deeper fix"):** batch N entries' Pebble batches behind one shared `b.Commit(pebble.Sync)`, ack all N after the shared fsync. Lifts the apply ceiling from `1/fsync` toward the 30–80k/s log-append ceiling. **The single-Raft targets below assume this is shipped; without it the floor is ~hundreds of starts/sec.** (Implement as `BatchingFSM`/`ApplyBatch` + grouped Sync.)

### 5.4 Snapshot / history growth — corrected attribution

`kvSnapshot.Persist` full-streams the keyspace; `Restore` is `DeleteRange` + re-ingest (verified). Retained history is in the app keyspace → in every snapshot.

**Correction (history-growth lens):** `pebble.Checkpoint` makes *local* snapshot creation O(num SSTs) and restore CPU cheap (hard-linked SSTs, `IngestExternalFiles` skips KV-by-KV re-`Set`), and we **must** ship it — but it does **not** reduce InstallSnapshot **bytes-on-wire**. hashicorp/raft's InstallSnapshot is an opaque size-prefixed byte stream (`io.Copy` both ends; `meta.Size = stat.Size()`); there is no SST-aware, incremental, or diff transport. **Drop the claims "incremental SST shipping" and "ingest rather than transfer" as transport claims.** A follower past `TrailingLogs=1024` still receives **O(total hot bytes)** on catch-up.

The real catch-up bound is set by **TTL + cold archival**, and is honestly **`O(open-runs × per-run-history-cap)`**, not "open runs + recent window". Many long-lived *open* runs (a 90-day `sleep`, a perpetual continue-as-new chain, a run awaiting human approval) keep full history hot and unsweepable until close. At M=10k open runs near the 50 MB cap that is ~500 GB shipped on every cold-follower catch-up.

Mitigations, honestly bounded:
1. **`pebble.Checkpoint` snapshots** — bounds *local* snapshot/restore cost (ship it; it is real).
2. **History TTL + cold archival** — on run close + grace, write the run's history blob to object store (content-addressed claim-check), replace hot rows with a pointer; drop the pointer after retention. Bounds the *hot* set to open + recently-closed runs. **This — not the checkpoint — is what bounds catch-up bytes.**
3. **Continue-as-new mandatory & frequent** for long-lived/looping runs, and it **must close the prior run** so its history becomes archival-eligible immediately; the live segment stays hot.
4. **Per-run caps** (Temporal parity: 51,200 events / 50 MB → force continue-as-new or terminate) + payload claim-check.
5. **Multi-Raft (Section 5.5)** is the only mechanism that *sublinearly* bounds per-group catch-up bytes as total history grows — each shard ships 1/N.

### 5.5 The multi-Raft path (sharded consensus) — and what it costs

Single global Raft is a hard ceiling: every run/tenant serializes through one leader's `f.mu` and one disk. Horizontal scale requires sharding consensus (ADR-0002 names this; dragonboat is the natural engine — built for many concurrent groups, `OnDiskStateMachine` matches the Pebble-FSM shape). Shard key, in order of atomicity preserved: **shard-by-tenant** (recommended first — all of a tenant's runs in one group, cross-run atomicity within a tenant, tenants independent, `group_id=tenant` is already the fairness unit), shard-by-workflow-class, shard-by-run-id (max parallelism, min atomicity).

**Honest cost (what you sacrifice):**
- **No cross-shard atomic batch.** A parent on shard A starting a child on shard B, a chord spanning shards, a cross-run signal — none can be one atomic commit. They become **2-phase/saga-style** async (commit intent on the source shard, relay to the target shard, reconcile on ack) — exactly Temporal's model (each workflow is its own shard), so well-trodden, but **child-start, cross-workflow signal, and cross-shard chord fan-in are eventually-consistent, not atomic.**
- **Exactly-once across shards needs idempotency, not transactions** — the `run_id+step_id+attempt` key + `dedup_key` (must be wired) + epoch-bound token become load-bearing because a relay can deliver twice on failover.
- **No global order across shards; resharding is a stop-the-world-per-tenant migration.**

**Recommendation: ship single-Raft first** (correct system of record, clean strong-atomicity model up to ~1k runs/s), add **shard-by-tenant** when one group saturates. Do not shard by run-id first.

### 5.6 Benchmark targets (corrected) + what a serious evaluator demands

**Present starts/sec and completions/sec as one shared entries/sec budget** (they draw on the same serial fsync stream and cannot be summed). All single-Raft numbers below **require apply-side group-commit** (Section 5.3) and are labeled as such.

| Metric | Single-Raft (group-commit shipped) | Multi-Raft (8 shards, tenant) | Proves |
|---|---|---|---|
| **Mixed entries/sec budget** | ~30–60k (toward log-append ceiling) | ~N× per-shard | the only honest combined number |
| **Workflow starts/sec** | ~1,000–1,500 (at ~22 entries/run, sharing the budget) | ~8k–12k | re-derived with lease entries counted |
| **Activity completions/sec** | ~8k–15k (sharing the same budget, **not additive to starts**) | ~60k–120k | fused result+decide |
| **Fan-out schedules/sec** | 50k+ (1 batch entry) | linear | `PublishBatch` |
| **p99 workflow-task latency** | 5–15 ms (2 fsync + quorum RTT) | same per shard | report p50/p99/p99.9 |
| **Sustained 1-hour mixed @ 80% peak** | no decay; local snapshot < 2 s | per shard | TTL/checkpoint closes the snapshot wall |
| **Failover — warm follower re-election** | < 3 s (no InstallSnapshot) | per shard | the fast path |
| **Catch-up — cold/lagging follower @ 50 GB hot** | **O(50 GB) stream, NOT < 3 s** | per shard (1/N) | the honest InstallSnapshot bound; **must not be conflated with warm re-election** |

**Brutal honesty (publish in the headline):** single global Raft + per-entry fsync caps you at ~5k–20k committed entries/s on the best realistic hardware; everything in 5.3 gets *more work per entry*, nothing makes one entry cheaper than one fsync; the only path past the ceiling is multi-Raft, which *sacrifices cross-run atomicity*. Report the per-device fsync table (consumer/PLP/EBS differ 10–30×) and the JSON-envelope marshal + group-meta read-modify-write CPU cost under `f.mu` that erodes the ceiling below the pure-fsync number. **Compare against:** Temporal's *single-shard* league (single-Raft Rota lands there — hundreds-to-low-thousands of starts/sec — not its multi-shard aggregate; multi-Raft is the apples-to-apples), and single-Postgres engines (Hatchet-class — single-Raft Rota with batching should beat them on activity throughput and p99). **Lead the benchmark story with operational simplicity** (one static binary, zero deps), not raw peak.

---

## 6. Fairness overlay across both surfaces — the moat preserved

The thesis: we add **no second scheduler.** The workflow engine emits work onto the same two-surface broker so every workflow task and activity task flows through `Pick` exactly like a queue message — fairness inherited as *replicated policy code*.

- **Mapping:** `lane = task-type`, `group_id = tenant` on **both** surfaces. Workflow-task lane `__wf/<type>`, activity-task lane `__act/<type>`, both `group_id = tenant`. Per-`(lane,group)` `GroupMeta` → independent virtual-time per surface (correct: decider CPU and the activity-worker pool are distinct contended resources). **Tenant is stamped by Apply from the immutable Run record, never from worker input** — so a buggy/malicious worker cannot forge a different group_id to escape its fair share (the fairness and determinism seams share the same property: the broker controls group_id; the worker controls only validated business commands).
- **Noisy-neighbor isolation by construction:** `Pick` ranks over *active* groups by virtual time; backlog *depth is not an input to ordering*. Tenant A's 10k pending workflow tasks and tenant B's 3 are each "one active group" and interleave 1:1 at equal weight — A's 10k do not buy 10k consecutive turns. Holds identically on both surfaces.
- **Completion-aware fairness (closes the lease-fair vs completion-fair gap):** lease-fairness is fair to *hand out* work, not fair in *resource-seconds*. A tenant whose leases each pin a worker for 90 s monopolizes compute under pure lease-fairness. Ship `KindCompletionAware`: rank on virtual time inflated by `inflight_count/weight` (a pure scoring function slotting into the existing `Score` contract, with the same fault→WFQ-fallback + quarantine safety net). **Zero new replicated state** — `inflight_count` is already quorum-written. Default on activity lanes; optional on decider lanes.
- **Per-tenant concurrency caps:** wire the declared-but-unimplemented `DECIDE` mode (`{group_id, take}`) for *hard* caps (`ok=false` when `InFlight ≥ cap` — a true skip a score can't express; broker still owns deficit accounting). Per-`(lane,group)` token bucket for *rate* caps (re-key the existing per-lane limiter; leader-local + advisory, rebuilt from replicated config on failover). **Rule:** rate-shaping = token bucket (soft); concurrency-correctness = DECIDE+inflight (replicated, hard).
- **Decider-loop fairness** (the hot-signal hog): one pending workflow task per run (dedup bit on the Run record — 1000 signals collapse to ≤1 in-flight decision + a tail; deterministic check in Apply), tenant turns rotate across runs (`last_served_seq`), decision-budget via the lease deadline + wheel, optional completion-aware on `__wf/*`.
- **Fairness Observatory** (existing :7101 SSE mux): per-tenant run share (decider + activity panels), starvation radar spanning *both* surfaces (catches the noisy-neighbor-holding-slow-leases signature), inflight/completion-lag panel. Resurrects the verified-dead `LaneStats` rate fields as real derived rates.

**Why no incumbent matches it:** Temporal's 2026 fairness GA is schedule-time + fixed. Rota's is **programmable** (fairness is a replicated CEL/WASM program installed via `CmdSetPolicy`, not a config knob), **completion-aware** over live replicated cost signal (reacts to how work *completes*, which a frozen-at-enqueue decision structurally cannot), and **cross-surface** (one policy governs deciders, activities, and queues via one `Pick`). And it is byzantine-safe under Raft because only the *decision* commits, never the policy code — the exact combination a fixed schedule-time GA cannot reach without adopting Rota's seam.

---

## 7. Revised phased roadmap — durable execution is the headline

Each phase ships value. The read-API/dashboard/hardening work that earlier plans deferred to Phases 5/6/7 is sequenced as *prerequisites interleaved with* the engine, because a durable-exec engine is unusable without a history-read API and a run grid.

**Phase 0 — Correctness preconditions (BLOCKER; no engine code before this merges).**
`Apply` default→error + `meta:schema_ver` gate + CmdType-range fail-stop; extend `AppKeyspaceBounds()` with the in-bound test; epoch-bind the completion token; max-lease-lifetime backstop; wire `dedup_key`. *Ships value standalone:* fixes three latent divergence/correctness bugs in the broker itself. (Also clear the small verified gaps that the engine leans on: register grpc health+reflection, wire the stubbed `PauseCron`, kill the dead `LaneStats` zero-fields or make them real.)

**Phase 1 — Replay kernel, single workflow type, happy path.**
`tagWFRun`/`tagWFHistory`, `CmdWFStartRun`/`CmdWFAppendEvents`, the leader validation gate (Section 2.3 incl. **payload byte-comparison**), `apply_workflow.go`, the worker protocol proto, Python SDK kernel + `execute_activity`/`sleep`/`now`, the **Replayer with multi-seed**. Activity tasks ride `__act/*` as messages. *Value:* run a real durable workflow end-to-end with deterministic replay and CI replay-safety. **Apply-side group-commit lands here** (gates the throughput story).

**Phase 2 — Read API + Run grid (the prerequisite UI).**
`PollWorkflowHistory`, `DescribeWorkflow`, `GetWorkflowResult`, `tagWFRunByWF` index; Svelte run grid + history view + replay-debugger over SSE. *Value:* the engine becomes observable/operable — table stakes for any user.

**Phase 3 — Full feature surface.**
Signals + signal-with-start, queries, child workflows + parent-close, cancellation, continue-as-new (+ **history TTL/archival GC reaper** — required for snapshot sanity), schedules (reuse cron), sagas (SDK sugar), Canvas sugar compiler, `patched()` + **fail-closed build-id pinning**. Go SDK reaches parity. *Value:* the complete durable-exec product.

**Phase 4 — Updates + fairness overlay + Observatory.**
`CmdWFUpdate` (validator/mutator split), `KindCompletionAware`, `DECIDE`-mode concurrency caps, per-(lane,group) token bucket, the Fairness Observatory. *Value:* the differentiating moat ships.

**Phase 5 — Scale hardening + the benchmark suite.**
`pebble.Checkpoint` snapshots, the full benchmark suite (Section 5.6) incl. the large-dataset cold-follower test, shadow-replay divergence detection. *Value:* the published, defensible "Temporal-grade" claim — gated on these numbers.

**Phase 6 — Multi-Raft (shard-by-tenant).**
dragonboat integration, cross-shard async signaling/child-start with idempotent relays. *Value:* horizontal scale past the single-group ceiling.

---

## 8. Top risks (post-verification) + open questions the maintainer must decide

### Blocking / un-closed risks (the adversarial pass could NOT fully close these)

- **R1 — Forward/terminal decision divergence is irreducible (`holds=true` but thesis over-claimed; protocol lens).** The first forward decision of a run, and *every* terminal non-deterministic decision, commit un-validated — identical to Temporal for the former, permanently-undetected for the latter. **Cannot be fixed by consensus**; mitigated only by build-id pinning + replayer CI + multi-seed + shadow-replay. *Action:* word the public claim narrowly (Section 1.2 item 2); ship the detection tooling; accept the residual as the Temporal-equivalent floor.

- **R2 — Activity-input payload determinism is un-validatable as a positive property (`holds=false`, high; versioning-2 lens).** The byte-comparison fix (Section 2.5) catches divergence *between a replay and its recorded prefix*, turning silent commits into wedge-class rejections — but a *first-time* non-deterministic input the leader cannot recompute is not caught. *Action:* mandate canonical encoding + sandbox bans + multi-seed replay; downgrade the safety claim to "shape by construction, content best-effort." **This is the most honest-to-hard residual.**

- **R3 — Poison-task liveness (`holds=true`, races lens, but flagged production-unsafe).** A genuinely non-deterministic or unpatched run fails the gate forever (retry at the same seq), pinning its `inflight_count`/fairness state indefinitely. *Required fix:* after K consecutive `NON_DETERMINISM` failures, propose a real **state-advancing** `WorkflowFailed` event that **closes** the run and releases inflight — not the state-free marker the original design used.

- **R4 — Cold-follower catch-up is O(open-runs × per-run-cap), not bounded-small (`holds=false`, high; history lens).** `pebble.Checkpoint` does not reduce InstallSnapshot bytes-on-wire. Many long-lived open runs defeat any "small window" bound. *Action:* re-attribute the bound to TTL/archival honestly; make continue-as-new mandatory-and-closing; give capacity-planning guidance (max concurrent open runs × cap = max snapshot bytes); lean on multi-Raft for sublinear per-shard catch-up. **Benchmark must distinguish warm re-election (fast) from cold catch-up (O(dataset)).**

- **R5 — Throughput numbers require unbuilt group-commit and undercounted entries (`holds=false`, high; scale lens).** *Action (done in this doc):* re-derived ~22 entries/run, made apply-side group-commit a hard prerequisite, presented one shared entries/sec budget. The risk persists until group-commit is *built and benchmarked*; until then label single-Raft as "~hundreds of starts/sec on the fsync floor."

### Open questions the maintainer must decide

1. **Build-id pinning strictness:** fail-closed on *any* un-patched binary change for in-flight runs (safest; forces old workers to stay deployed) vs. a compatibility-assertion opt-in (more operable; weaker)? R1's blast radius depends on this.
2. **Terminal-decision non-determinism:** accept the permanent-undetected residual (Temporal parity) or invest in mandatory shadow-replay of *closed* runs for audit (cost no object — is this worth it)?
3. **Multi-Raft shard key:** tenant (atomicity-preserving, recommended) vs. workflow-class vs. run-id — and *when* to introduce it (the cross-shard async-signaling complexity is permanent once shipped).
4. **History retention policy surface:** per-namespace TTL + archival store choice (S3/GCS/local), and the default per-run event/byte caps before forced continue-as-new.
5. **`Update` semantics for rejections:** write a durable `UpdateRejected` event (auditable, history growth) vs. no-trace rejection (clean, but no record)?
6. **Group-commit implementation path:** `BatchingFSM`/`ApplyBatch` (raft-native) vs. a leader-side coalescing layer over the current `FSM` — affects how invasive Phase 1 is to the broker core.
7. **ADR-0010 reversal scope:** confirm the orchestrator stays a feature-flagged extension (broker byte-for-byte identical with the flag off) and that `run`/`node`/`step` nouns living in `internal/workflow/` is the accepted boundary.

---

**Bottom line:** the committed-divergence protocol holds on its operative requirement — *no determinism bug can commit a re-interpretation of recorded history, and no divergence is ever worse than a single Temporal worker* — once the Section 2.5 payload byte-comparison and the Section 2.6 preconditions ship. The two genuinely-worse-than-Temporal holes the adversarial pass found (payload divergence; forward un-validation over-claim) are fixed or honestly scoped, not papered over. The throughput and snapshot numbers are withdrawn and re-derived against the verified single-fsync, full-stream-snapshot, opaque-InstallSnapshot reality. The fairness moat — programmable, completion-aware, cross-surface, byzantine-safe — is the defensible reason this is winnable against Temporal, and it is preserved intact because durable execution rides the same `Pick` seam rather than forking it.