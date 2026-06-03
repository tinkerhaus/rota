# 0009. Complete-by-token as a broker-native async-completion primitive

## Decision

Provide a complete-by-token primitive: a leased message can be held in-flight with long/extendable visibility while a consumer hands work to an external system, then completed/failed later by an opaque server-minted token (looked up by stored sha256 hash), in-stream or via a unary CompleteByToken RPC from any process. Bounded by a max-lease-lifetime backstop.

## Rationale

Any submit-now / callback-later integration faces the same problem: the consumer dispatches work to an external system (a third-party API job, a long-running task runner, a webhook-driven service) and cannot finish the message until a result arrives, possibly minutes or hours later and possibly on a different process. A broker whose only contract is "hold the lease while the worker is alive and processing" cannot model this, so applications bolt on an external async-completion side-table plus a polling worker to remember in-flight submissions and reconcile them when results land — carrying all the retention, cleanup, and two-stores-disagreeing risk that implies.

Making the broker hold the in-flight lease + the token (in the replicated FSM) eliminates that entire side-table and its retention/cleanup logic: the in-flight state lives durably in the broker alongside the message, not in a separate store that can drift. Lookup by stored hash (not HMAC re-derivation) lets any node complete a token after failover, so the completing process need not be the one that minted it.

## Consequences

An unresolved token must still eventually dead-letter (max-lease-lifetime) so a wedged external job cannot pin a group's inflight count forever. A late Complete after a lease expiry+re-lease is epoch-rejected, but the external system may then have two in-flight submissions — inherent to at-least-once; idempotency stays the consumer's job.
