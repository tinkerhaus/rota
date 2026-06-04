# Rota — Workflow Engine + Dashboard Roadmap

> Reconciles two design passes (broker+UI synthesis, and the full durable-execution
> design with its adversarial verification) into one phase plan. The broker has shipped
> Phases 0–4 (see `ROADMAP.md`); this continues the global numbering at **Phase 5**.
>
> Full design docs (working copies): the durable-execution design (committed-divergence
> protocol, replay core, scale analysis) and the non-goals re-evaluation. This file is the
> executable sequencing of those.

## Direction (maintainer-ratified)

- Build a **best-in-class, Temporal-grade durable-execution engine** on Rota — cost no object.
- **Primary surface = durable-execution-as-code**; declarative Canvas (`@task`/`chain`/`group`/`chord`)
  is sugar compiling onto the **same kernel**. One engine, not two.
- **SDKs:** Python first, Go second (Go is the impl language; the validator and worker share Go types).
- **UI:** Svelte 5 + SvelteKit `adapter-static`, `//go:embed` into the binary, SSE on `:7101`.

## The defensible thesis (what we may claim)

*Rota is a single-binary, zero-dependency durable-execution engine with deterministic replay and a
**programmable, completion-aware, cross-tenant fairness** layer no incumbent — including Temporal's
2026 fairness GA (schedule-time + fixed) — can match.* We lead with the **fairness moat**, not raw
throughput. We do **not** claim "impossible to diverge" or "infinitely scalable" (the adversarial pass
proved both false; see Risks).

## Non-goals decision (carried forward)

The broker stays **delete-on-ack with no public replayable/seekable stream** ("a work-queue, not Kafka").
The **workflow engine** keeps a private, per-run, totally-ordered **event history** as its system of
record — full event-sourcing *internal* to the engine, never a consumer feed. Ordering comes from the
Raft total order, not broker delivery. Large payloads stay out of messages (claim-check). Three of four
non-goals survive verbatim at the broker boundary; only event-sourcing is relaxed, engine-internal only.

---

## Phase 5 — Correctness preconditions  **(BLOCKER — no engine code before this lands)**

All verified against code; each fixes a latent broker bug and ships value standalone.

1. **`Apply` fail-stop on unknown `CmdType`** (`fsm.go:60-97`) + a `meta:schema_ver` gate. Today an
   unknown command silently no-ops *and* advances `applied_index` (line 101) → cluster divergence on any
   mixed-version rollout. A node that cannot apply a command must **halt loudly**, never silently advance.
2. **`AppKeyspaceBounds()` guard test** (`keys.go:187`). Add a test asserting *every* tag any applier
   writes is within `[lo, hi)`; extend the bound as workflow tags are added. A tag outside the bound is
   silently dropped from snapshot/restore → instant divergence on follower catch-up.
3. **Epoch-bind the completion token** to `(lease, attempt, epoch)` (today `tagToken` → lease id only),
   so a late `Complete(token)` for a superseded attempt is rejected.
4. **Max-lease-lifetime backstop** (new tidx kind): a wedged async activity must not pin `inflight_count`
   forever; force-fail/DLQ regardless of extends.
5. **Wire `dedup_key`** in `publishReqFromSpec` (proto field present, dropped on the Go path) → producer
   idempotency for workflow starts.
6. **Small gaps the engine + UI lean on:** implement the stubbed `PauseCron` (returns `Unimplemented`);
   register gRPC health + reflection; populate or remove the dead `LaneStats` rate fields.

**Deliverable:** broker is correctness-trustworthy for money-moving work *without* workflows; the
divergence vectors that would otherwise be latent in the engine are closed first.

## Phase 6 — Read API + Operator Dashboard core  *(no FSM changes; buildable on today's code)*

Paginated read RPCs over existing Pebble iterators: `ListLanes`, `ListGroups`, `GetGroupStats`,
`ListLeases`/`GetLease`, `PeekMessages`/`GetMessage`, `ListDeadLetters`/`RedriveDeadLetter`, `GetCron`,
`ListSingletonLeases`. Thin in-process HTTP/JSON gateway; `go:embed` Svelte SPA on `:7101`; SSE hub
(`cluster`/`stats`/`dlq`). Views: cluster/Raft header, Lanes Overview, **Lane Detail (the fairness grid)**,
Group/Tenant Detail, Leases + lifecycle timeline, **DLQ Inspector**, Cron. All list responses paginated.

**Deliverable:** Rota's operability beats Celery/Flower and matches Argo/Dagster — zero new infra, zero broker risk.

## Phase 7 — Fairness Observatory  *(the differentiating screen)*

`GetLaneFairness`/`GetPolicyHealth`; leader in-memory fairness projection + `fairness` SSE; share bars,
live interleave ribbon, starvation radar, **Policy Lab** (live CEL/WASM dry-run via `ValidatePolicy` +
loud quarantine alerting), tenant cross-lane view. Ship the **`completion_aware`** built-in policy.

**Deliverable:** the screen that sells Rota; closes the lease-fair vs completion-fair gap.

## Phase 8 — Durable-execution replay kernel  *(single workflow type, happy path)*

New tags `tagWFRun`/`tagWFHistory` (+ `AppKeyspaceBounds()` bump); `CmdWFStartRun`/`CmdWFAppendEvents`;
the **leader-side validation gate** (Section 2 of the design — recorded-prefix byte-match incl. **payload
byte-comparison**, OCC fences on `history_seq`/`run_epoch`); `apply_workflow.go`; the worker-protocol
proto; Python SDK kernel (`execute_activity`/`sleep`/`now`); the **Replayer with multi-seed**. Activity
tasks ride `__act/*` as messages. **Apply-side group-commit (`BatchingFSM`) lands here** (gates the
throughput story).

**Deliverable:** a real durable workflow end-to-end with deterministic replay and CI replay-safety.

## Phase 9 — Full feature surface

Signals (+ signal-with-start), queries, child workflows (+ parent-close), cancellation, continue-as-new
(+ **history TTL/archival GC reaper**), schedules (reuse cron), sagas (SDK sugar), the **Canvas sugar
compiler**, `patched()` + **fail-closed build-id pinning**. Go SDK reaches parity.

**Deliverable:** the complete durable-exec product.

## Phase 10 — Updates + fairness overlay for workflows + Observatory extensions

`CmdWFUpdate` (validator/mutator split); `KindCompletionAware` on workflow + activity lanes; **`DECIDE`-mode
concurrency caps** ({group_id, take}); per-`(lane,group)` token bucket; workflow panels in the Observatory
(per-tenant run share, starvation radar across both task surfaces).

**Deliverable:** the differentiating moat, applied to durable execution.

## Phase 11 — Scale hardening + benchmark suite

Real `pebble.Checkpoint` snapshots (replace the full-stream path); the full benchmark suite (per-device
fsync table, large-dataset **cold-follower** catch-up test distinguished from warm re-election, mixed
entries/sec budget); shadow-replay divergence detection.

**Deliverable:** the published, defensible "Temporal-grade" claim — gated on these numbers.

## Phase 12 — Multi-Raft (shard-by-tenant)

dragonboat integration; cross-shard async signaling/child-start with idempotent relays. Shard by **tenant**
(atomicity-preserving) first — never by run-id.

**Deliverable:** horizontal scale past the single-group ceiling.

---

## Blocking / un-closed risks (the adversarial pass could not fully close these)

- **R1 — Forward/terminal decision divergence is irreducible.** The first forward decision of a run, and
  every terminal non-deterministic decision, commit un-validated — identical to Temporal for the former,
  permanently-undetected for the latter. Mitigated by build-id pinning + replayer CI + multi-seed +
  shadow-replay; **not** eliminable by consensus. Word the public claim narrowly.
- **R2 — Activity-input payload determinism is un-validatable as a positive property.** Byte-comparison
  (Phase 8) turns silent commits into wedge-class rejections, but a *first-time* non-deterministic input
  is not caught. Mandate canonical encoding + sandbox bans + multi-seed replay; scope the safety claim to
  "event shape by construction, content best-effort."
- **R3 — Poison-task liveness.** A forever-failing run pins `inflight_count`/fairness state. Fix: after K
  `NON_DETERMINISM` failures, propose a state-advancing `WorkflowFailed` that **closes** the run.
- **R4 — Cold-follower catch-up is O(open-runs × per-run-cap).** `pebble.Checkpoint` does not reduce
  InstallSnapshot bytes-on-wire; the real bound is set by TTL+archival. Make continue-as-new
  mandatory-and-closing; benchmark must distinguish warm re-election from cold catch-up.
- **R5 — Throughput requires unbuilt group-commit and was undercounted.** ~22 entries/run (not ~12);
  apply-side group-commit is a hard prerequisite. Until built+benchmarked, label single-Raft as
  "~hundreds of starts/sec on the fsync floor."

## Open questions for the maintainer

1. Build-id pinning strictness: fail-closed on any un-patched change vs. compatibility-assertion opt-in.
2. Terminal-decision non-determinism: accept Temporal-parity residual vs. invest in closed-run shadow-replay.
3. Multi-Raft shard key + when to introduce it (cross-shard async complexity is permanent once shipped).
4. History retention: per-namespace TTL + archival store (S3/GCS/local) + default per-run event/byte caps.
5. `Update` rejection semantics: durable `UpdateRejected` event vs. no-trace rejection.
6. Group-commit path: raft-native `BatchingFSM`/`ApplyBatch` vs. a leader-side coalescing layer.
7. ADR-0010 scope: confirm the orchestrator stays a feature-flagged extension (broker byte-identical with the flag off).
