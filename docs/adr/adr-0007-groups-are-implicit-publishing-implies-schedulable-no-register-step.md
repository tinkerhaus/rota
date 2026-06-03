# 0007. Groups are implicit: publishing implies schedulable, no register step

## Decision

A Group is implied by publishing a Message tagged with an opaque group id (e.g. a job, tenant, session, or batch id), created in the same atomic batch that stores the message; schedulable membership is DERIVED from message presence, never from a separate registry. Leasing an empty/absent group is a benign empty result. Groups are high-cardinality, ephemeral, auto-reaped when drained+idle; pause/resume/cancel/purge in place; one-call teardown across all lanes. Group knobs (weight/batch_size/paused) are stored in the same Raft-replicated store as messages.

## Rationale

A whole class of fairness/scheduling failures traces to two stores disagreeing — an out-of-band group registry held in a separate cache versus the in-band queue of messages — or to a blind fetch against a missing queue killing the consumer's channel. Deriving membership from message presence and storing knobs durably (not in a separate cache) makes drained-group-retry-invisible, missing-queue-kills-channel, paused-group-race, and the cache-flush-loses-state class structurally impossible rather than handled by careful code. That last class is the failure mode where fairness/scheduling state kept in a SEPARATE cache is lost on a cache flush or restart while the messages themselves remain queued; folding the knobs into the same replicated store as the messages removes the second store entirely.

## Consequences

Reaping a drained+idle group GCs its weight, so a later retry-republish defaults to weight 1.0 unless re-supplied — the publisher must pass weight on every publish (a load-bearing convention; optionally add a sticky-weight-until-explicit-purge mode). Cancel/teardown must purge outstanding completion tokens in the same batch so a late CompleteByToken after a cancel is a benign no-op.
