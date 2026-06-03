"""Signal exceptions a worker handler can raise to steer message outcome.

These are domain-neutral control-flow signals, not real errors:

- Raise :class:`Requeue` to put the message back with no attempt penalty
  (transient back-pressure: shutting down, rate-limited, circuit open).
- Raise :class:`DeadLetter` to terminally fail the message into the DLQ.

Returning normally from a handler acks the message. Raising any *other*
exception nacks it with RETRY (attempt++ and auto-dead-letter at max attempts).
"""

from __future__ import annotations

from typing import Mapping, Optional


class RotaError(Exception):
    """Base class for all SDK-raised errors and signals."""


class Requeue(RotaError):
    """Requeue the in-flight message without penalizing its attempt count.

    Maps to a Nack with mode ``REQUEUE_NO_PENALTY``. An optional ``delay``
    (seconds, float) requests a redelivery delay.
    """

    def __init__(self, delay: Optional[float] = None, reason: str = ""):
        super().__init__(reason or "requeue")
        self.delay = delay
        self.reason = reason


class DeadLetter(RotaError):
    """Terminally fail the in-flight message into the dead-letter queue.

    Maps to a Nack with mode ``DEAD_LETTER``. Optional ``meta`` is attached
    as failure metadata and carried into the DLQ record.
    """

    def __init__(self, meta: Optional[Mapping[str, str]] = None, reason: str = ""):
        super().__init__(reason or "dead_letter")
        self.meta = dict(meta) if meta else {}
        self.reason = reason


class NotLeaderError(RotaError):
    """Raised internally when an RPC hits a follower; carries the leader addr."""

    def __init__(self, leader_addr: str = "", detail: str = ""):
        super().__init__(detail or "not leader")
        self.leader_addr = leader_addr
        self.detail = detail
