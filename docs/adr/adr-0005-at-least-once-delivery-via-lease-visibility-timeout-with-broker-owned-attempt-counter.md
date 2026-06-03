# 0005. At-least-once delivery via lease/visibility-timeout with broker-owned attempt counter

## Decision

Deliver at-least-once via SQS-style leases with a per-message visibility deadline. The broker owns the attempt counter. Negative-ack is two-outcome: no-penalty requeue (attempt unchanged, optional delay) and terminal dead-letter; bounded retry auto-promotes to native DLQ. Visibility timeout defaults to no-penalty requeue (penalty if the queue/group is configured to penalise lease expiry). Not broker exactly-once.

## Rationale

This maps cleanly onto a typical consumer contract: a thrown error from the handler triggers a retry, and an explicit no-penalty requeue (attempt unchanged, optional delay) lets a consumer return work to the queue without burning a retry. It also maps onto a strictly-serial consumer (prefetch=1) via a credit of 1. A broker-owned attempt counter removes the consumer-managed retry-count header pattern (e.g. an `x-retry-count` header round-tripped through every message), which is brittle and easy to lose. Consumers are responsible for being idempotent, so exactly-once is an explicit non-goal.

## Consequences

Re-leasing on timeout means a slow consumer can have its message worked twice (inherent to at-least-once); consumers must tolerate it. Complete-by-token holds need a max-lease-lifetime backstop so a never-completed token eventually dead-letters instead of pinning a group's inflight count forever.
