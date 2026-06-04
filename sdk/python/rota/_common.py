"""Internal helpers shared by the Publisher, Worker, and Control clients.

Domain-neutral plumbing only: target parsing, lazy channel creation, leader
following (NOT_LEADER -> retry against the advertised leader), and conversion
helpers for the protobuf well-known Duration / Timestamp types.
"""

from __future__ import annotations

import random
import time
from typing import Callable, List, Optional, Sequence, TypeVar, Union

import grpc
from google.protobuf import duration_pb2, timestamp_pb2

from rota._gen.rota.v1 import rota_pb2 as pb

T = TypeVar("T")

Targets = Union[str, Sequence[str], grpc.Channel]


def normalize_targets(targets: Targets) -> List[str]:
    """Coerce a target spec into a list of ``host:port`` strings."""
    if isinstance(targets, str):
        return [t.strip() for t in targets.split(",") if t.strip()]
    return [str(t).strip() for t in targets if str(t).strip()]


def to_duration(seconds: Optional[float]) -> Optional[duration_pb2.Duration]:
    """Convert a float number of seconds to a protobuf Duration (or None)."""
    if seconds is None:
        return None
    d = duration_pb2.Duration()
    d.FromNanoseconds(int(seconds * 1e9))
    return d


def to_timestamp(epoch_seconds: Optional[float]) -> Optional[timestamp_pb2.Timestamp]:
    """Convert a unix epoch (float seconds) to a protobuf Timestamp (or None)."""
    if epoch_seconds is None:
        return None
    ts = timestamp_pb2.Timestamp()
    ts.FromNanoseconds(int(epoch_seconds * 1e9))
    return ts


def extract_not_leader(err: grpc.RpcError) -> Optional[str]:
    """If ``err`` is a NOT_LEADER fault, return the advertised leader address.

    Rota signals leadership redirection with a FAILED_PRECONDITION status. The
    leader address is carried either in a binary ``NotLeader`` error detail
    (trailing metadata key ``not-leader-bin``) or, failing that, parsed out of
    the status details string. Returns ``None`` if this is not a NOT_LEADER
    error.
    """
    if err.code() != grpc.StatusCode.FAILED_PRECONDITION:
        return None

    # Preferred: structured NotLeader detail in trailing metadata.
    try:
        for key, value in err.trailing_metadata() or ():
            if key in ("not-leader-bin", "rota-not-leader-bin"):
                nl = pb.NotLeader()
                nl.ParseFromString(value)
                if nl.leader_addr:
                    return nl.leader_addr
    except Exception:  # pragma: no cover - defensive
        pass

    # Fallback: the human-readable details string. We only treat it as a
    # leader redirect if it actually looks like one.
    details = (err.details() or "")
    low = details.lower()
    if "not_leader" in low or "not leader" in low:
        # Try to recover an address token (host:port) from the message.
        for token in details.replace(",", " ").split():
            if ":" in token and not token.endswith(":"):
                host, _, port = token.partition(":")
                if port.isdigit() and host:
                    return token
        return ""  # NOT_LEADER but no usable address
    return None


class LeaderClient:
    """Lazy gRPC channel that follows the cluster leader on NOT_LEADER.

    Holds an ordered list of candidate targets, lazily dials the first, and
    reuses one channel. On a NOT_LEADER fault it re-dials the advertised leader
    (promoting it to the head of the candidate list) and retries the call with
    bounded exponential backoff + jitter.
    """

    def __init__(
        self,
        targets: Targets,
        stub_factory: Callable[[grpc.Channel], object],
        *,
        channel_options: Optional[Sequence] = None,
        max_retries: int = 5,
        base_backoff: float = 0.1,
        max_backoff: float = 5.0,
        credentials: Optional[grpc.ChannelCredentials] = None,
    ):
        self._stub_factory = stub_factory
        self._channel_options = list(channel_options or [])
        self._max_retries = max_retries
        self._base_backoff = base_backoff
        self._max_backoff = max_backoff
        self._credentials = credentials

        # Allow passing a pre-built channel directly (tests / shared channel).
        if isinstance(targets, grpc.Channel):
            self._external_channel = targets
            self._candidates: List[str] = []
            self._channel: Optional[grpc.Channel] = targets
            self._stub = stub_factory(targets)
        else:
            self._external_channel = None
            self._candidates = normalize_targets(targets)
            if not self._candidates:
                raise ValueError("at least one target address is required")
            self._channel = None
            self._stub = None

    # -- channel lifecycle --------------------------------------------------

    def _dial(self, target: str) -> grpc.Channel:
        if self._credentials is not None:
            return grpc.secure_channel(target, self._credentials, self._channel_options)
        return grpc.insecure_channel(target, self._channel_options)

    @property
    def channel(self) -> grpc.Channel:
        if self._channel is None:
            self._channel = self._dial(self._candidates[0])
            self._stub = self._stub_factory(self._channel)
        return self._channel

    @property
    def stub(self):
        # Touch .channel to trigger lazy connect.
        self.channel
        return self._stub

    def _switch_to(self, leader_addr: str) -> None:
        """Re-dial against a new leader address and rebuild the stub."""
        if self._external_channel is not None:
            # Caller owns the channel; we can't redial. Just retry on it.
            return
        if leader_addr and leader_addr not in self._candidates:
            self._candidates.insert(0, leader_addr)
        if self._channel is not None:
            try:
                self._channel.close()
            except Exception:  # pragma: no cover
                pass
        self._channel = self._dial(leader_addr or self._candidates[0])
        self._stub = self._stub_factory(self._channel)

    def _rotate(self) -> None:
        """Move to the next candidate target (e.g. the current one is down)."""
        if self._external_channel is not None or len(self._candidates) <= 1:
            # Nothing to rotate to: just re-dial the same target (may have recovered).
            self._switch_to("")
            return
        self._candidates.append(self._candidates.pop(0))
        self._switch_to("")

    def _backoff(self, attempt: int) -> None:
        delay = min(self._max_backoff, self._base_backoff * (2 ** attempt))
        time.sleep(delay * (0.5 + random.random() * 0.5))

    # -- call wrapper -------------------------------------------------------

    def call(self, method_name: str, request, *, timeout: Optional[float] = None):
        """Invoke a unary stub method, following the leader on NOT_LEADER."""
        last_err: Optional[Exception] = None
        for attempt in range(self._max_retries + 1):
            method = getattr(self.stub, method_name)
            try:
                return method(request, timeout=timeout)
            except grpc.RpcError as err:
                last_err = err
                leader = extract_not_leader(err)
                if leader:
                    # NOT_LEADER with an advertised address: redial the leader.
                    self._switch_to(leader)
                elif leader == "" or err.code() in (
                    grpc.StatusCode.UNAVAILABLE,
                    grpc.StatusCode.DEADLINE_EXCEEDED,
                ):
                    # Leaderless redirect, or the node is down/unreachable: try the
                    # next candidate (this is what makes dead-leader failover work).
                    self._rotate()
                else:
                    raise  # genuine application error — do not retry
                if attempt < self._max_retries:
                    self._backoff(attempt)
                    continue
                raise
        if last_err is not None:
            raise last_err
        raise RuntimeError("call exhausted retries without an error")  # pragma: no cover

    def close(self) -> None:
        if self._external_channel is None and self._channel is not None:
            try:
                self._channel.close()
            finally:
                self._channel = None
                self._stub = None

    def __enter__(self) -> "LeaderClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
