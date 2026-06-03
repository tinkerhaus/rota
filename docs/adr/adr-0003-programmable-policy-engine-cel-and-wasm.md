# 0003. Programmable policy engine: pure SCORE/DECIDE ABI, CEL default + WASM first-class

## Decision

Adopt a PURE scoring/decision contract — `score(group, lane, consumer) -> float` (SCORE mode) or
`{group_id, take}` (DECIDE mode) — as the single ABI for every policy engine. The broker owns all
DRR/WFQ/turn bookkeeping in the FSM. Ship two first-class engines: **CEL** as the lightweight default
for inline scoring, and **WASM (wazero)** for full, any-language custom policies. Starlark is kept as
an optional middle tier and may be dropped if it earns no adoption. Reject Lua and expr-lang.

## Rationale

Only the leader evaluates the policy and only the decision is Raft-replicated, so cross-node
bit-determinism is not required. But the state the policy reads (deficits, turn counters,
virtual_time, last_served) MUST be reconstructable on failover, which means it must live in the FSM,
not the engine's heap. That kills any model where the policy mutates scheduler state in its own
memory: every engine, WASM included, is constrained to a pure function whose only output is
scores/decisions. A pure function of broker-owned state needs no persistence and no mutation — exactly
CEL's non-Turing-complete, cost-bounded, stateless-cachable design center for the common case. WASM via
wazero is first-class (not deferred) so operators can author policies in any language (Rust, Go/TinyGo,
AssemblyScript, C) with the strongest sandbox available: a hard linear-memory cap, a fuel/CPU bound, and
crash containment, all in pure Go with no external toolchain at runtime. Its per-call instantiation +
host-call marshalling cost is acceptable because evaluation is leader-only and the broker is not
throughput-bound.

## Consequences

Some genuinely cross-group strategies (e.g. cap total in-flight across a tag set) are awkward in
per-group SCORE and push operators to DECIDE mode (CEL DECIDE or a WASM module). Supporting CEL + WASM
(+ optional Starlark) means multiple validators/cost-models/test-matrices and ABI-drift risk; the ABI
schema must be single-sourced and every engine binding generated from it. The WASM host boundary must
marshal the lane snapshot efficiently (reused buffers) and enforce fuel/memory limits per evaluation.
Fallback-to-DRR can mask a quietly-faulting policy, so faults must alert loudly and auto-quarantine.
