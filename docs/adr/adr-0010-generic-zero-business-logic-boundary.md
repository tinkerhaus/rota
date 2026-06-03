# 0010. Generic, zero-business-logic boundary

(validated by mapping representative real-world consumer patterns onto the broker, without baking any of them in)

## Decision

Rota's vocabulary is strictly Lane/Group/Message/Lease/Policy/Consumer/Cron/Singleton/DeadLetter. Application-specific state — credential/connection pools, circuit-breaker STATE, business retry policy — stays consumer-side; the broker offers only generic dequeue-pause + per-key rate-limit hooks. Representative consumer mappings (a job/tenant/batch = group, a worktype.variant.priority tuple = lane, relative weight = a float, an external async call = complete-by-token, a periodic job = cron + singleton, a failed-message sink = an ordinary consumer reading the dead-letter stream) live in an appendix as worked examples, never compiled into the broker.

## Rationale

The whole value proposition is a generic broker that owns fairness/delivery/scheduling/coordination while owning zero domain concepts. To prove the generic model is *sufficient* (not just elegant), we ground it by mapping several representative real-world consumer patterns onto it end to end: a per-group relative weight that defaults to 1.0 and is tuned to express "this group should be served N times as often"; a fan-out worker whose queue identity is a `worktype.variant.priority` tuple folded into the Lane name with `group_id` carrying an opaque group id (e.g. a job, tenant, session, or batch id); one-call cross-lane teardown of a group; and a circuit breaker that keeps its STATE in the consumer's own store. Each of these maps onto Lane/Group/weight/complete-by-token/cron/singleton without leaking a single domain noun into the broker. The mappings demonstrate adequacy; they are deliberately kept *out* of the broker so no one consumer's workload is privileged.

## Consequences

A genuine gap remains: some workloads want fairness on TWO orthogonal axes at once (e.g. round-robin across groups *and* across a sub-key within a group, such as a per-(group, downstream-target) rotation). Rota expresses a second axis only because that sub-key can be folded into the Lane name (`<worktype>.<sub-key>.<priority>`); a single opaque `group_id` cannot carry two independent fairness keys within one lane. Composing the extra axis into the Lane name works for low-cardinality, statically-known sub-keys (target/variant/priority); a composite `group_id` would lose independent weighting of the sub-axis. Consumers with a true two-dimensional fairness requirement must confirm that folding one axis into the lane is acceptable, or accept that the second axis is unweighted.
