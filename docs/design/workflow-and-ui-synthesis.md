Confirmed: the `Apply` switch has no `default` case (line 97 closes the switch directly), so unknown command types silently no-op and advance `applied_index`. All ground-truth facts verified. Now writing the final document.

---

# Rota Durable Workflow Engine + Dashboard — Lead Architect Design

**Status:** Decision-ready. **Scope:** Add a durable workflow engine and an operator UI to Rota, positioned against Temporal / Prefect / Celery and the 2026 durable-execution cohort (Restate, DBOS, Hatchet, Inngest, Resonate).
**Audience:** the maintainer (you), as the single decision-maker. Sections 2 and 7 are the choices I am asking you to ratify.

---

## 1. Executive summary and the honest "way better" thesis

### 1.1 What we are actually building

Rota today is a **generic, group-fair message broker** with one genuine moat: a **programmable, leader-evaluated, Raft-replicated fair scheduler** over groups, on a **single static Go binary** with embedded Pebble + Raft and zero external dependencies. It is *not* a workflow engine — there is no run / step / DAG / signal vocabulary anywhere in the proto, FSM, or SDK, and ADR-0010 makes "no business logic in the broker" a load-bearing commitment.

We are going to add a **durable workflow engine as a thin orchestration kernel on top of Rota's existing primitives** (the FSM's one-batch-atomic `Apply`, the unified `tidx` due-time wheel, lease delivery, complete-by-token, epoch fencing, the fair scheduler) plus a **single-binary operator dashboard** served from the node itself. We will do this without rewriting the broker and without abandoning the single-binary thesis.

### 1.2 The honest "way better" thesis — lead with the real wedges

The naive claim — "Rota workflows beat Temporal/Prefect/Celery" — is **false as stated and we will not make it**, because (a) Rota is not durable execution, (b) it has no replay log (Ack *deletes* the message; the Raft log is a truncated recovery window, not history), (c) it has one thin Python SDK with known holes, and (d) its read/observability surface is nearly empty. Section 7 of Proposal 4 is correct and I am adopting its discipline wholesale.

The defensible thesis is narrower and sharper:

> **Rota is the programmable fair queue and durable-timer substrate *underneath* your workers — and a workflow engine built on it inherits one structural advantage the entire field lacks: cross-group fairness as hot-reloadable, replicated *policy code*, applied to the *existing backlog* at *every dispatch*, on *one binary with no external store*.**

**The three real wedges (in priority order):**

1. **Programmable, replicated-decision fair scheduling — the only true moat.** Fairness is now table stakes (Temporal GA'd it 2026-05-12; Inngest, Hatchet, SQS all ship it), but *everyone ships one fixed discipline bolted onto an opaque scheduler*, applied at **schedule time** (Temporal explicitly cannot rebalance an existing backlog). Rota's fairness is the *serving policy itself as CEL/WASM code*, leader-evaluated, with only the resolved decision replicated (ADR-0004), applied at **every lease of the live backlog**, with the fairness ledger quorum-written in Pebble next to the work. To copy this, a competitor must run a sandboxed policy VM inside the consistency-critical dispatch path and keep it replayable — a deep architectural change, not a feature flag. **This is the screen that sells Rota** (Section 4's Fairness Observatory) and the multi-tenant property no incumbent can draw.

2. **One fsync domain, one binary, zero external deps.** Messages, scheduling state, timers, cron, leases, tokens, *and* the Raft log all live in one Pebble keyspace; every `Apply` is one atomic batch co-committing `applied_index`. This is a decisive, *architectural* answer to Temporal's 4-services-plus-Cassandra-plus-Elasticsearch sprawl, Prefect's Postgres+Redis/Docket+N-services, and Celery's broker+result-backend+Beat. **Caveat:** single-binary is now *parity* with Restate/Resonate, not a moat alone. The defensible position is the **intersection** — single-binary *and* programmable fairness, which neither of them has.

3. **The unified due-time wheel + epoch fencing as a *build-cost* moat.** Durable timers, retries, visibility deadlines, and cron are already one ordered, idempotent, epoch-fenced structure proven across failover. Workflow sleeps, step timeouts, heartbeat deadlines, and scheduled starts are *new `tidx` kinds with zero new infrastructure*. This is not a competitive moat (everyone has durable timers) — it is the single biggest piece of workflow plumbing Rota hands the engine for free.

### 1.3 Where we will NOT win — name it plainly

- **Durable mid-function replay / resume.** This is Temporal's core and we are not building it. We do step-memoization-without-a-determinism-DSL (Inngest/Hatchet style), not event-sourced replay of arbitrary user code. We will *retain a per-run history* as a first-class queryable table, but a `@task`/`@step` re-runs from the top on retry. **We will not claim Temporal-grade durable execution.**
- **Polyglot SDK ecosystem.** Temporal has 6 languages with replay-test tooling. We ship Python first, Go second. That is years behind and we say so.
- **Visibility/search at scale.** Today there are zero secondary indexes, no pagination, and a read-poor API. We are *building* this (Section 3/4), but it does not exist yet, and at-scale search remains weaker than Temporal-on-Elasticsearch for a long time.
- **Maturity / battle-testing.** Rota has uncommitted code, a live `PauseCron` `Unimplemented` bug, DESIGN-vs-code drift (claims `pebble.Checkpoint`; code full-streams the keyspace), and no benchmark against `raft-boltdb`. We are pre-1.0.

**Positioning, in one line:** *"the programmable fair queue and durable substrate underneath your workers, not a replay engine."* Complementary to Temporal; a clear upgrade over Celery; a fairness-and-ops win over Prefect.

---

## 2. Recommended workflow programming model — A vs B, resolved

The two proposals disagree on the authoring surface:

- **Proposal 1 (Approach A):** durable-execution-as-code with deterministic replay — write a normal function, the engine reconstructs its position from history; activities are leased messages; the decider is non-deterministic user code whose *resolved command list* is replicated (mirroring `LeaseCmd`).
- **Proposal 2 (Approach B):** a declarative Celery/Prefect-style layer — `@task` + `chain/group/chord`, a `@flow` body that *builds a static DAG*, a leader-side orchestrator that publishes downstream tasks as upstreams complete. No determinism contract; retries re-run from the top.

Both proposals, independently, converge on the same resolution in their own words: Proposal 2's Section 7 explicitly recommends **"one core, two surfaces"**, and Proposal 1's architecture is exactly that shared kernel. The disagreement is not "A or B" — it is "which surface ships *first* and which is *default*."

### Decision: **One orchestration kernel, declarative surface first, durable-code surface later. We do NOT ship two products.**

**The kernel (shared, built once):** a small set of new FSM commands + a `RUN` / `NODE`/`STEP` / `HISTORY` keyspace + new `tidx` timer kinds + read RPCs, implementing the **"replicate the decision, not the code"** orchestrator. Crash-atomic step transitions, durable timers, epoch-fenced idempotent fires, compensation-on-failure, per-run total-ordered history, and per-tenant fair dispatch are *identical* whether the surface above is declarative DAGs or durable code. Building them twice is the one thing we must not do — Rota's own architecture proves the principle (five "timed behaviours" are *one* `tidx` primitive; we do the same for orchestration).

**Why declarative-first is the default surface:**

1. **It is the lowest-friction onboarding and the market's actual mental model.** Celery/Prefect's millions of users already hold `@task` + `chain/group/chord`. Time-to-first-workflow is minutes, with *no forbidden-API list, no replay footgun, no `NonDeterminismError` on deploy*. Proposal 4's adversarial review is blunt that the determinism tax is Temporal's single most-attacked weakness; declarative-first sidesteps it entirely.
2. **It maps `tenant → group` directly, so it is the best fairness showcase.** A `@flow(tenant_key=…)` stamps `group_id = tenant_key` onto *every* task message a run emits. Rota's WFQ then interleaves tenants by weighted fair share over the live backlog. The durable-code model's unit is a *run*, which does not map onto a group as cleanly. Since fairness is the moat, the default surface must be the one that demonstrates it in one line.
3. **It is dramatically cheaper to build on Rota** (~one file of FSM handlers + new keyspace tags + read RPCs + a client SDK, riding existing publish/lease/complete-by-token) and it respects the non-goals: the *broker* stays generic; the orchestrator is a feature-flagged consumer/control extension.

**Why we keep the durable-code surface on the roadmap (not discard B's rival):** the pure declarative layer hits a real wall — no inline `for`/`if`/dynamic fan-out, and weak long-`await` (human-in-the-loop, multi-day waits). The *same kernel* relieves this later by exposing a **second, code-first surface** (`@durable` functions whose `await activity()` / `workflow.sleep()` compile down to the *same* `CmdStepCompleted` / node-state machine and the *same* `RUN`/`HISTORY` tables). Code ergonomics on top, a diffable/versionable artifact underneath, one determinism boundary, no engine fork.

### Resolving the specific A-vs-B technical disagreements

| Disagreement | Resolution | Rationale |
|---|---|---|
| Static DAG (B) vs arbitrary control flow (A) | **Default declarative/static; controlled `dynamic_group` expansion from a completed upstream's result as the escape hatch; full inline control flow only on the later `@durable` surface.** | Dynamic runtime graphs require running user code inside the durability boundary — the determinism tax we are avoiding. Static edges let the leader walk the DAG with zero user code on the hot path. |
| Retry semantics | **Re-run from the top (B), made safe by an engine-owned idempotency key `run_id+step_id+attempt` + a cache-key result store (Prefect's decomposed durability).** | We are honest that this is not mid-function resume. The idempotency key (Section 6 of Proposal 4) closes the at-least-once double-effect hole. |
| Where orchestration runs | **Leader-side propose path only; followers apply the resolved decision; `Apply` never runs user code or reads a clock (A and B agree).** | This is Rota's existing `LeaseDecision` seam. Non-negotiable for replay/snapshot determinism. |
| History | **Retain a first-class, append-only, per-run, totally-ordered `HISTORY` table (both proposals agree).** | Fixes Rota's "scan-hint, not FIFO" ordering gap for workflow events; answers Celery's unanswerable "what step is run X on?" |
| Versioning | **Run-pinning at start (`code_version` + `code_hash`, content-addressed like policies); patch markers for the `@durable` surface only.** | Borrow Rota's content-addressed policy-versioning wholesale for the long-lived entity. |

**Net:** Author as code (declarative Canvas now, `@durable` later), persist as a content-addressed explicit state machine into shared `RUN`/`NODE`/`HISTORY` tables, replicate the decision not the code. One core. Declarative-first. Durable-code-capable.

---

## 3. Architecture — concrete mapping onto Rota primitives

Everything below is grounded in verified symbols: `AppKeyspaceBounds()` returns `0x00..0x0B` today (`keys.go:187`); the 18 `CmdType`s end at `CmdPublishBatch` with a JSON envelope and **no schema version** (`command.go`); the `Apply` switch has **no `default` case** (`fsm.go:97`); `tidx` kinds are `0x01..0x03` (`keys.go:27-29`).

### 3.1 New Pebble keyspaces and indexes

Add new table tags **inside** `AppKeyspaceBounds()`, then **extend the upper bound** — this is load-bearing: a tag outside `[lo,hi)` is silently dropped from snapshot/restore and lost on follower catch-up.

```
0x0B  tagRun       run:  LP(tenant) ++ LP(workflow_type) ++ u64be(run_id)   -> RunMeta (proto)
0x0C  tagNode      node: u64be(run_id) ++ LP(node_id)                       -> NodeState (proto)
0x0D  tagHistory   hist: u64be(run_id) ++ u64be(seq)                        -> HistoryEvent (proto, append-only)
0x0E  tagFlowDef   fdef: sha256(canonical_dag)                              -> FlowDef (content-addressed, immutable)
0x0F  tagRunResult rres: u64be(run_id) ++ LP(node_id)                       -> inline bytes | blob pointer
0x10  tagRunIdx    ridx: LP(tenant) ++ status_byte ++ u64be(updated_ts) ++ u64be(run_id) -> primary run key  [secondary index]
0x11  tagIdemp     idem: sha256(idempotency_key)                            -> u64be(run_id)   [start/signal dedup]
// AppKeyspaceBounds() now returns {tagMeta} .. {tagIdemp + 1}   ← MUST be updated in the same change
```

- **`run_id`** is a global counter `meta:next_run_id`, assigned inside `Apply` exactly like `meta:next_lease_id` (`fsm.go` `nextLeaseID`), so it survives snapshot/restore and never collides. Likewise a per-run `next_seq` for history ordering, mirroring `GroupMeta.NextSeq`.
- **`RunMeta`** is the quorum-written run record (the `GroupMeta` analogue): `run_id, workflow_type (= lane), tenant_id (= group_id), status {PENDING|RUNNING|COMPLETED|FAILED|COMPENSATING|COMPENSATED|CANCELLED|CONTINUED}, run_epoch (uint32), history_len, code_version, code_hash, flow_def_hash, open_nodes, pending_timers, nodes_total/done/failed, started_ms, last_event_ms, parent_run_id, parent_node_id`.
- **`NodeState`** keyed `(run_id, node_id)`: `BLOCKED → READY → DISPATCHED → {SUCCEEDED, FAILED, COMPENSATED}`. `BLOCKED` until all `deps` are `SUCCEEDED`.
- **`HISTORY` is the system of record**, not the Raft log (which is truncated at `TrailingLogs=1024` / `SnapshotThreshold=8192`). `seq` is assigned in `Apply` in the *same batch* as the transition, giving a **guaranteed per-run total order** — the property Rota's scan-hint in-group ordering does not provide.
- **`ridx`** is the secondary index that makes `ListRuns(tenant, status)` an index scan, not a full keyspace scan. Maintained in the same batch as every status transition (insert new key, delete old), exactly the way the lease-deadline `tidx` row is maintained on extend. **All list responses get a `page_token`/`page_size` envelope** — the current unbounded `ListCron` pattern is not repeated.
- **Large payloads:** Rota payloads are KB-scale by design. Activity inputs/outputs and run results store `*_ref` content-hash pointers; bytes go to `rres:` inline only when small, else to an external/CAS blob store. We flag this as a real (small) re-introduction of a second system, not hide it.

### 3.2 New Raft command types

Append to the `iota` block in `command.go` (**append-only**, to keep JSON-tag-stable decoding of older committed entries), each with a `*Cmd` pointer field on `Command`, a `case` in `fsm.go` `Apply`, and an `apply*` handler mirroring `apply_phase4.go`:

| New `CmdType` | Leader-side (propose path) | `Apply` — one atomic batch |
|---|---|---|
| `CmdStartRun` | assign `run_id`; load `FlowDef` by hash; compute root nodes; leader-stamp `now` | write `RunMeta` RUNNING + `ridx`; write `NodeState` rows; **publish root task messages** via `publishOne`; arm SLA timer; `WorkflowStarted` + `NodeDispatched` history; `idem:` insert |
| `CmdStepCompleted` | resolve which downstream nodes unblock; store `result_ref` | flip node `SUCCEEDED`; **publish newly-READY task messages**; bump `run_epoch`; update counts + `ridx`; history; if all terminal → `COMPLETED` |
| `CmdNodeFailed` | compute reverse-topo compensators | flip node `FAILED`; run → `COMPENSATING`/`FAILED`; **publish first compensation message** into a dedicated compensation group; history |
| `CmdSignal` | leader-stamp; resolve run by `run_id` or `idem:` corr-id | `SignalReceived` history event (or buffer); arm a workflow/decider task; `idem:` dedup |
| `CmdCancelRun` | — | bump `run_epoch` (fences all in-flight timers/leases/tasks); `CancelRequested` history; arm decider for cooperative cleanup; optional `dropLeasable`; **purge outstanding `tok:` rows** (close the existing teardown leak) |
| `CmdContinueAsNew` | assign fresh `run_id` | mark old run `CONTINUED`; `applyStartRun` a fresh run with empty history carrying forward state; `idem:` chain |
| (extend `CmdFireTimer`) | new `TimerWorkflow` branch in `sweepTimers` | `TimerFired` history event; arm decider; **epoch-fenced** against `run_epoch` |
| (extend `CmdComplete`) | on activity complete, add `result_ref` | fuse: `ActivityCompleted` history + clear `open_nodes[node]` + **arm decider** in the *same batch* |

**Mandatory companion change (a real bug fix, not optional):** the `Apply` switch has no `default` case, so an unknown `CmdType` *silently commits an empty batch and advances `applied_index`* (verified `fsm.go:97`). A mixed-version rollout of these new commands would make an old node silently diverge. Before any new command ships we add:
1. `default: return fmt.Errorf("unknown CmdType %d", cmd.Type)` so a node that cannot apply a command **halts loudly instead of diverging**.
2. A `meta:schema_ver` gate checked at `Apply` start, refusing commands above the node's known version.
3. Freeze JSON tags (or migrate the command envelope to protobuf) before the first new command type ships.

### 3.3 New gRPC RPCs

All workflow control is server-side and leader-stamped, so it must be FSM commands — you cannot get crash-atomic "step done → next step published" purely client-side. Mutating RPCs go in the `Control` service and **must be added to `mutatingMethods` in `leader.go`** so followers redirect via the existing `not-leader-bin` contract. Read RPCs are deliberately left out of that set so followers serve them.

**Mutating (leader-guarded):**
- `StartRun(flow_def_hash | inline_dag, params, tenant_key, idempotency_key) -> {run_id}`
- `RegisterFlowDef(dag) -> {hash}`
- `SignalRun(run_id | corr_id, signal_name, payload_ref)`
- `CancelRun(run_id)` / `TerminateRun(run_id)`
- `RedriveDeadLetter(...)` (also serves the DLQ inspector in Section 4)
- extend `CompleteByToken` with `result_ref` for activity results

**Read (follower-servable, with a leader+`Barrier()` opt-in for read-your-writes — `node.Barrier()` already exists):**
- `GetRun(run_id) -> RunDetail`
- `GetRunGraph(run_id) -> [NodeState]` (the DAG view)
- `GetRunHistory(run_id, from_seq, page_size) -> [HistoryEvent]` (paginated)
- `ListRuns(tenant?, status?, page_token) -> [RunSummary]` (served off `ridx`, never a full scan)
- `GetNodeResult(run_id, node_id)`
- Plus the broker read RPCs the dashboard needs (Section 4.2): `ListLanes`, `ListGroups`, `GetGroupStats` (honor the ignored `group_id`), `ListLeases`/`GetLease`, `PeekMessages`/`GetMessage`, `ListDeadLetters`/`GetDeadLetter`, `GetCron`, `ListSingletonLeases`/`GetSingletonLease`, `GetLaneFairness`, `GetPolicyHealth`.

**Consistency story (must be explicit, currently unwired):** reads are follower-served off a possibly-stale local Pebble replica with no read-index. For v1, **route dashboard/run reads to the leader** (the gateway knows the leader via `DescribeCluster`) for read-your-writes after an action, and **paginate every scan** to bound leader load. Phase-later: allow follower reads carrying `applied_index` + `as_of_ts` with a UI staleness badge, and wire `Barrier()` for opt-in strong single-entity reads.

### 3.4 SDK / worker protocol

The workflow SDK is a **new layer above** the existing `Publisher`/`Worker`/`LeaderClient` (subclassed, not forked), consistent with ADR-0010 keeping the broker generic.

- **Declarative authoring (default):** `@task(retries, retry_backoff, timeout, rate_limit, compensate, cache_key)` and `@flow(tenant_key=…)`. A **signature** (`task.s(args)`) is a serializable JSON partial call `{task_name, args, kwargs, options}` — never a closure. Canvas constructors (`chain`, `group`, `chord`, `|`, `.map()`, `chunks`, `link_error`) compose signatures into a content-addressed DAG. Submitting compiles the DAG, `RegisterFlowDef`s it, and `StartRun`s it, returning a `FlowRun` future.
- **Worker:** `flow.Worker` subclasses `rota.Worker`. Per leased message: deserialize the task envelope → look up `task_name` in the registry → inject upstream results from `rres:` → run the (ordinary, non-deterministic) Python → on return, complete the lease with `result_ref` (driving `CmdStepCompleted` via the complete-by-token path). Exceptions map to nacks: transient → `RETRY`, backpressure/`Requeue` → `REQUEUE_NO_PENALTY`, terminal/`DeadLetter` → `DEAD_LETTER`.
- **Two SDK fixes that become mandatory** (DESIGN-promised, currently missing — the worker only `logger.debug`s these): (a) **auto-extend the lease at ~2/3 of the deadline** (long activities heartbeat via `ExtendVisibility`), and (b) **halt credit on `PAUSE_LANE`** so server-pushed backpressure actually slows the consumer.
- **Activity = lease:** an activity step is a published message on lane `act:<type>`, group `<tenant>`, leased with a visibility/heartbeat timeout. Worker crash ⇒ `TimerLeaseDeadline` fires ⇒ no-penalty requeue ⇒ redelivery. Async activities (webhook/human) use complete-by-token from any process.

### 3.5 How fair scheduling makes workflows multi-tenant-fair — the exact point

**The exact instruction: `Node.LeaseOne` → `scheduler.Pick`, per dispatch of any task.** The activating mapping:

- **lane = task queue / workflow type** (`act:<type>`, `wft:<type>`).
- **group = tenant** (`group_id = tenant_key`), stamped by the orchestrator onto *every* task and compensation message a run emits.

When a worker calls `Work` and `LeaseOne` runs, the leader builds the active-group set (`ListGroups` filtered to `Ready>0 && !Paused`), and `scheduler.Pick` runs the lane's WFQ virtual-time (or hot-reloaded CEL/WASM policy) to choose *which tenant's* next task to serve, advancing that tenant's VT by `1/weight`. New tenants join at min-VT (no cold-start penalty). **This is the precise point where a noisy tenant flooding 10k tasks cannot starve a quiet tenant's handful** — they interleave by weighted fair share of the *live* backlog, and the fairness state (`virtual_time`, weights, counts in `GroupMeta`) is quorum-written in Pebble, so failover never loses it (the failure mode of a bolt-on Redis fairness layer).

**Two honest caveats baked into the design (not oversold):**
1. **Lease-fair, not completion-fair.** Default WFQ advances VT at *lease* time regardless of task duration, so a tenant with many *long* in-flight activities is not throttled by the default. Mitigation: ship a built-in **`completion_aware`** policy scoring on `inflight` (already an available ABI input; per-tenant inflight is exactly `GroupMeta.InflightCount`) and make it the recommended default for workflow lanes.
2. **No native per-tenant concurrency cap.** A tenant can hold unbounded in-flight leases even while lease-turns stay fair. The real fix is **DECIDE mode** (`{group_id, take}`, declared in proto, unimplemented) or a per-group token bucket — both are *policy-engine extensions, not new broker machinery*. Until then, lane-wide rate-limit is the blunt instrument.

---

## 4. UI dashboard

### 4.1 Thesis and serving architecture

The dashboard is a **fairness observatory**, not a workflow UI bolted onto a broker. The differentiating screen — *service proportional to weight, nobody starving, live* — is one **no competitor can draw**, because none model cross-group fairness as a first-class, programmable, replicated thing.

**Serving (preserve the single-binary moat — non-negotiable):** the SPA is compiled to static assets and embedded into the Go binary via `//go:embed`, served by the **same process** off the existing side HTTP mux (`:7101`, currently `/metrics` + `/healthz`). **Zero new daemon, no new DB, no proxy.** A `--ui=on|off|addr` flag, default on, bound to loopback like `/metrics`; auth is a later bearer-token/reverse-proxy concern.

**Stack:** **Svelte 5 + SvelteKit `adapter-static`**. Justification: the embedded-binary profile makes bundle size literally part of the download (Svelte compiles the runtime away → ~30-60% smaller JS), and the highest-value screens are high-frequency streaming counters (the fairness ribbon ticks many times/sec), which is exactly Svelte's surgical-reactivity strength. The DAG/graph libs React holds an edge on are framework-agnostic (`@dagrejs/dagre` + SVG). **The architecture below is framework-neutral**; React + Vite is an acceptable de-risk fallback if the team has zero Svelte experience.

**Read/query API:** the dashboard is **gated entirely on the new read RPCs of Section 3.3** — the frontend is the easy half. These are pure reads over *existing* Pebble iterators (`Store.ListGroups`, `CountDLQ`, `MessagePrefix`, `DLQLanePrefix`, `LeaseKey`), so they add no FSM commands and no determinism risk. A thin in-process **HTTP/JSON gateway** (grpc-gateway or a ~200-line shim) exposes them to the SPA; reads are direct method calls, not a network hop. Mutating actions follow the `NOT_LEADER` redirect (port the Python `LeaderClient`).

**Three contract fixes the UI depends on (or it binds to zeros/errors):**
- **Populate the dead `LaneStats` fields** (`publish_rate/lease_rate/ack_rate` EWMA, `oldest_age_ms`) at the existing `observe.*` increment sites — or mark them deprecated and serve rates from the new per-group RPC. Do not leave them silently zero.
- **Implement the missing `PauseCron` handler** — it is declared in proto + generated server + `mutatingMethods` + the Python SDK but has no implementation, so it returns `codes.Unimplemented` at runtime. The cron page's pause toggle hits this live bug.
- **Register `grpc.health.v1.Health` + server reflection** in `main.go` (DESIGN promises it; unwired).

### 4.2 Realtime: SSE (chosen)

**Server-Sent Events**, not WebSocket or server-streaming gRPC. SSE matches the dashboard's one-directional shape (counters/fairness/lease events stream down; actions go up as ordinary POSTs), rides the existing HTTP mux with **no proxy** (grpc-web would need Envoy — a dependency that violates the thesis), and gives **free auto-reconnect with `Last-Event-ID`** — important because the SPA must resubscribe to a new leader on failover. The leader maintains an **in-memory fairness projection** (the same data `rotaviz` keeps) updated at the `observe.*` sites and `scheduler.Pick`; a fan-out hub pushes **coalesced deltas every ~250-500ms** (never per-lease). This reads the leader's *in-memory* projection, not Pebble — the explicit "beat the incumbents' DB-polling grids" advantage. Topics: `cluster`, `stats`, `fairness`, `lease`, `dlq`, `policy`. Fallback to interval polling if SSE is blocked.

### 4.3 Key views

**Information architecture — one opinionated home, strict drill-down (no Airflow tab-sprawl):**
```
Cluster/Raft header (always)        ← DescribeCluster + Health, cluster SSE
└─ HOME: Lanes Overview             ← ListLanes
   └─ Lane Detail [THE GRID]        ← GetLaneFairness + ListGroups
      ├─ Group/Tenant Detail        ← GetGroupStats + PeekMessages + ListLeases
      ├─ Leases (+ lifecycle timeline) ← ListLeases
      ├─ DLQ Inspector              ← ListDeadLetters + RedriveDeadLetter
      └─ Policy Lab                 ← GetPolicyHealth + ValidatePolicy
   └─ Fairness Observatory          ← GetLaneFairness + fairness SSE   [THE DIFFERENTIATOR]
   └─ Cron / Schedules              ← ListCron + GetCron
   └─ Tenant cross-lane view        ← ListGroups across lanes + TeardownGroup
[Phase 8] Workflows: Run grid → DAG → Gantt → Step detail/actions
```

**Broker observability (buildable after the read RPCs land):**
- **Cluster/Raft header** (every page): leader id/term/`applied_index`/quorum, per-peer suffrage, a "reading from leader/follower-N" chip, live failover flip via `cluster` SSE — a screen no DB-backed incumbent draws as cleanly.
- **Lanes Overview** (home): per-lane policy + quarantine chip, depths, rates, paused; pause/resume + rate-limit actions; shared color language (green=served, blue=inflight, orange=delayed, red=DLQ/faulted, grey=paused).
- **Lane Detail — THE GRID** (the web promotion of `rotaviz`): rows = groups, columns = recent time-buckets, cell color = served/starved/paused/DLQ'd, intensity = throughput. The literal picture of fairness. Includes the policy Code tab + a Policy-Lab launchpad.
- **Group/Tenant Detail:** weight-vs-actual-service bar, VT, deficit, depths; non-destructive **queue peek** (`PeekMessages`); in-flight leases with deadline countdown; pause/cancel/purge/reap/set-config actions.
- **Leases + per-lease lifecycle timeline:** `enqueue → lease → extend → nack(no-penalty|terminal) → redeliver → DLQ` with attempt + epoch — Rota's correct analogue to per-task logs (Rota runs no user code, so there is no stdout; show the lease lifecycle).
- **DLQ Inspector** (first-class, headline — nobody else does this well): list dead letters with `failure_headers`/`final_attempt`/`reason`; **one-click redrive with a preview** (Dagster backfill-preview discipline); "republish like this" launchpad.
- **Cron/Schedules:** list with next-/last-fire, pause/delete (fix `PauseCron`!), volume heatmap; honestly surface that misfire/coalesce/timezone are accepted-but-ignored.

**Fairness Observatory (the differentiator):**
- **Share bars side by side:** expected share (weight/total_weight) vs actual served share — when they match, fairness is *visibly* working. The live, continuous "500-vs-20, the big backlog never starved the small one" demo.
- **Live interleave ribbon:** the colored `A A B C A B …` served-order stream straight from `rotaviz`.
- **Starvation radar:** a computed `starvation_score` per tenant (time-since-last-served × backlog × expected-vs-actual gap); breaching tenants light red — a question fixed-scheduler incumbents *cannot even ask*.
- **Policy Lab:** live CEL/WASM editor wired to `ValidatePolicy` dry-run ("here's the serving order this policy would produce on the current snapshot"); loud quarantine/fault panel (the UI *is* the alert the code only flags); hot-swap and watch the ribbon rebalance.
- **Tenant cross-lane view:** one `group_id` across every lane + one-call `TeardownGroup` — an operation DAG-centric tools structurally cannot offer (their unit is the workflow, not the tenant).

**Workflow surface (later phase):** Run grid → DAG graph (`@dagrejs/dagre`, live node animation via SSE) → Gantt timeline → Step detail (the lease/attempt/nack/DLQ event log + payload-in/result-out toggle) → actions (retry → redrive/re-publish, cancel → `CancelRun`, signal → `SignalRun`). The view is identical whether the run data comes from the server-side `RUN`/`HISTORY` tables or, in a feature-off deployment, a client-side header projection.

---

## 5. Phased roadmap (continues Rota's Phase 0-4)

Rota has shipped Phases 0-4 (broker, scheduler, policy engine, cron/singleton/complete-by-token/Control gRPC/Python SDK). New phases continue the numbering. **Every phase ships standalone value; Phase 5 is buildable on what exists today.**

**Phase 5 — Read API + Operator Dashboard core (buildable on today's code; no FSM changes).**
Pure reads over existing iterators + three bug/contract fixes. Ship: paginated `ListLanes`/`ListGroups`/`GetGroupStats`/`ListLeases`/`PeekMessages`/`ListDeadLetters`+`RedriveDeadLetter`/`GetCron`/`ListSingletonLeases`; populate dead `LaneStats` rates + `oldest_age_ms`; **fix the `PauseCron` `Unimplemented` bug**; register `grpc.health.v1` + reflection; `go:embed` Svelte SPA on `:7101`; HTTP/JSON gateway; SSE hub with `cluster`/`stats`/`dlq` topics; the cluster header, Lanes Overview, Lane Detail (THE GRID), Group Detail, Leases, DLQ Inspector, Cron pages. **Value:** Rota's operability beats Celery's Flower and matches Argo/Dagster ops surface — with zero new infrastructure and zero broker risk.

**Phase 6 — Fairness Observatory (the differentiating screen).**
`GetLaneFairness`/`GetPolicyHealth`; leader in-memory fairness projection + `fairness` SSE; share bars, interleave ribbon, starvation radar, Policy Lab with `ValidatePolicy` dry-run + loud quarantine alerting, tenant cross-lane view. Plus the `completion_aware` built-in policy. **Value:** the screen that sells Rota; closes the lease-fair gap with a recommended policy.

**Phase 7 — Durability & correctness hardening (prerequisites for any workflow state).**
The non-negotiable safety work before workflow commands ship: add the `Apply` `default`-error case + `meta:schema_ver` gate; freeze/migrate the command envelope; implement the **complete-by-token max-lease-lifetime backstop** + epoch-bind the token to the attempt; wire `dedup_key` into `applyPublish` as a bounded TTL-swept dedup index; implement the SDK auto-extend + `PAUSE_LANE` credit-halt; **decide and implement history retention** (real `pebble.Checkpoint` snapshots OR a TTL/cold-archive tier) before any monotonically-growing table exists; publish a single-node + HA throughput/latency benchmark. **Value:** Rota becomes correctness-trustworthy for money-moving work even *without* workflows, and the snapshot/throughput ceilings are measured, not guessed.

**Phase 8 — Workflow orchestration kernel + declarative SDK (the engine).**
New keyspace tags `0x0B..0x11` (and the mandatory `AppKeyspaceBounds()` bump); `CmdStartRun`/`CmdStepCompleted`/`CmdNodeFailed`/`CmdSignal`/`CmdCancelRun`/`CmdContinueAsNew` + the `TimerWorkflow` `tidx` kind + fused `CmdComplete`; `RUN`/`NODE`/`HISTORY`/`ridx` materialization; `StartRun`/`SignalRun`/`CancelRun`/`GetRun`/`GetRunGraph`/`GetRunHistory`/`ListRuns` RPCs; the `rota.flow` Python SDK (`@task`/`@flow`/Canvas, `group_id=tenant_key` fairness, saga/compensation via reverse-topo on terminal nack); the Workflow surface in the dashboard (Run grid → DAG → Gantt → Step detail). Feature-flagged so the generic broker thesis is preserved. **Value:** a durable, fair, single-binary workflow engine — the headline deliverable.

**Phase 9 — Per-tenant concurrency caps + DECIDE mode + `@durable` surface.**
Implement DECIDE mode (`{group_id, take}`) for per-tenant max-inflight caps; honor `batch_size` in `Pick`; add the code-first `@durable` surface (inline control flow + long `await`) compiling to the same kernel; patch-point versioning markers. **Value:** completion-fair capacity guarantees and the expressiveness ceiling lifted for the workloads that need it.

**Phase 10 (deferred, as before) — multi-raft (dragonboat) for per-workflow-class consensus** to lift the single-group throughput ceiling, accepting the cross-run atomicity split that comes with it.

---

## 6. Top risks and mitigations

1. **Single-Raft-group throughput ceiling under workflow fan-out.** One global commit pipeline with per-entry `fsync` (ADR-0002). A history-heavy engine produces many small events/sec — the inverse of the broker's "seconds-to-minutes" profile — and hits the ceiling fast; the Postgres crowd hits the same class at hundreds-to-40k/s. *Mitigations:* batch a decider's whole command list into one `CmdStepCompleted`; emit parallel fan-out as one `PublishBatch` entry, not N; fuse "result recorded + decider scheduled" into one batch; borrow Temporal's synchronous matching (hand a lease to a waiting long-poller without a separate persist); **publish a real benchmark** (the market rewards concrete numbers; "no benchmark" against a per-entry-fsync single group is a credibility hole); treat multi-raft as the eventual answer (Phase 10).

2. **Whole-keyspace snapshot cost vs growing history.** Confirmed in code: snapshots full-stream the application keyspace and `Restore` does `DeleteRange` + re-ingest — **not** the cheap `pebble.Checkpoint` DESIGN claims. A retained `hist:` table makes every snapshot and every `InstallSnapshot` to a lagging follower **O(total history)**. *Mitigations:* this is make-or-break and is **gated into Phase 7 before any workflow table ships** — implement the real `Checkpoint` snapshot path *or* hard-bound history with TTL + cold-archive (content-addressed pointers, continue-as-new keeping live histories bounded). Plus the one-line catastrophic-if-missed footgun: **new tags must be inside `AppKeyspaceBounds()`** or history silently won't snapshot.

3. **Determinism drift corrupting replicated state.** Any wall-clock/RNG/map-order leak, or a mid-flight code change, makes a decider's command stream diverge from history — and here a *committed* divergent event becomes replicated cluster state (worse than Temporal's single-run runtime error). *Mitigations:* the leader-side determinism-validation gate (validate the new command stream extends the recorded prefix consistently *before* proposing; reject non-conforming tasks, never commit them) is load-bearing, mirroring `policy.Validate`'s smoke-eval-before-install; plus run-pinning and patch markers; plus the `default`-error/`schema_ver` gate from Phase 7.

4. **Exactly-once *effects* — at-least-once with known holes.** Confirmed: the completion token is **not epoch-bound to the attempt** (survives re-lease → a late `Complete` can resolve a *different* attempt), there is **no max-lease-lifetime backstop** (a wedged async activity pins `inflight_count` forever), and **`dedup_key` is dropped in `publishReqFromSpec`** (no producer idempotency for run starts). *Mitigations (Phase 7):* thread an engine-owned idempotency key `run_id+step_id+attempt` into every activity + own an effect-dedup table (reuse the tag-`0x09` token pattern); epoch-bind the token and check epoch in `applyComplete`; implement the max-lease-lifetime backstop (a second `tidx` row that dead-letters regardless of extend); wire `dedup_key` into `applyPublish`. **Until these ship, do not claim exactly-once.**

5. **Leader-serial decider/validation/policy path.** Workflow-task scheduling, determinism validation, and policy evaluation all run only on the leader, serially (heavy WASM already serializes the leader tick). A busy engine funnels every run's "advance" through one node. *Mitigations:* keep validation O(delta); push everything possible to the parallel workers; accept this as the per-leader ceiling until multi-raft; surface leader load in the dashboard.

6. **Backpressure × fairness interaction is partial.** The SDK ignores `PAUSE_LANE` (only logs it), so server-pushed backpressure doesn't slow consumers; the scheduler is lease-fair while rate-limiting is lane-wide, so a tenant can monopolize *capacity* while lease-*turns* look fair; `batch_size` is stored but never batches a lease. *Mitigations:* implement SDK credit-halt-on-`PAUSE_LANE` + auto-extend (Phase 7); per-group concurrency cap + DECIDE mode (Phase 9); honor `batch_size` in `Pick`. Describe fairness-under-backpressure as a **partial** guarantee until then.

7. **ADR-0010 reversal (strategic, not technical).** `run`/`node` are domain nouns; adding them to the FSM reverses the zero-business-logic boundary. *Mitigation:* keep the **broker** generic; ship the orchestrator as a feature-flagged consumer/control extension behind the `schema_ver` gate, so the core thesis and the snapshot-balloon risk are both contained to deployments that opt in.

---

## 7. Open questions you must decide

1. **Programming-model surface order — confirm the recommendation.** I recommend **one kernel, declarative `rota.flow` (`@task`/`@flow`/Canvas) as the default surface, `@durable` code-first surface deferred to Phase 9.** Do you ratify declarative-first, or do you want the code-first surface co-equal at launch (more build cost, weaker fairness showcase)?

2. **"Workflow" vs "task queue" scope — how far do we cross the ADR-0010 line?** Three positions: **(a)** stay a fair task queue + canvas (chain/group/chord) with *no* server-side run record (client-side header projection only — no broker change, but no durable queryable history); **(b)** the recommended kernel with server-side `RUN`/`HISTORY` (durable, queryable, but reverses ADR-0010 and incurs the snapshot/throughput risk); **(c)** full durable-execution with the `@durable` surface. I recommend **(b)**, gated behind Phase 7 hardening and a feature flag. Confirm.

3. **SDK languages.** Python-first is assumed (existing SDK). Is **Go second** (the implementation language, natural for the worker protocol) the right call, or is **TypeScript** higher priority for the target market? This affects the worker-protocol proto design now.

4. **UI stack — Svelte vs React.** I recommend **Svelte 5 + SvelteKit `adapter-static`** on bundle-size + streaming-reactivity grounds, with React as a de-risk fallback. Do you have a team familiarity constraint that should override this?

5. **History retention policy (gates Phase 7→8).** Which path: **real `pebble.Checkpoint` snapshots** (decouples snapshot cost from state size — more engine work) or **TTL + cold-archive of terminal-run history** (bounds the hot keyspace — needs an external archive tier)? One of these *must* be chosen before any growing table ships.

6. **Read consistency for the dashboard/run views.** Leader-routed reads (read-your-writes, more leader load) for v1, or follower reads with a staleness badge + opt-in `Barrier()`? I recommend leader-routed + pagination for v1.

7. **Benchmark + claims gate.** Will you commit to **publishing a single-node and HA throughput/latency number** before any public "beats Temporal/Prefect/Celery" positioning? The adversarial review (Proposal 4) is explicit that the unbenchmarked single-group ceiling and at-least-once-with-holes effects are what a serious evaluator finds first; I recommend the public claim be capped at *"the programmable fair queue underneath your workers"* until Phase 7 lands.