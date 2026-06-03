# 0004. Leader-only policy evaluation; replicate the decision, not the code

## Decision

The Raft leader is the sole evaluator of policy and the sole consumer of the wall clock for scheduling/timers. It appends the OUTCOME (LeaseDecision with fairness mutations; FireTimer/FireCron with a leader-stamped fire_at) to the log. Followers apply outcomes deterministically and never run policy or fire on their own clock.

## Rationale

FSM.Apply must be deterministic and identical on every peer (hashicorp/raft requirement). Wall clock and policy execution are inherently non-deterministic across nodes. Stamping the decision/fire_at into the committed entry relaxes determinism exactly where it is impossible (clock, policy) while keeping the FSM bit-deterministic everywhere it matters. It also bounds the blast radius of a bad policy to the leader (worst case: a clean step-down + failover).

## Consequences

The leader's scheduler tick + fire loop are serial bottlenecks and fairness-fidelity knobs (need demand-driven ticking + batched assignments). The leader's clock is authoritative, so a badly-skewed leader clock could fire early/stall everything — needs a sanity guard (reject backwards jumps, monitor fire latency). Lottery/jitter non-determinism is fine only because evaluation is leader-only — a hard, documented dependency that breaks if speculative read-replica scheduling is ever added.
