"""Worker: lease messages off a Rota lane and dispatch them to a handler.

The worker opens the bidirectional ``Work`` stream, advertises demand
(``credit``), and dispatches each delivered lease to ``handler(message)``. With
``credit=1`` (the default) it is strictly serial: exactly one in-flight message
at a time.

Handler outcome mapping:

- returns normally           -> Ack
- raises :class:`Requeue`    -> Nack(REQUEUE_NO_PENALTY, delay)
- raises :class:`DeadLetter` -> Nack(DEAD_LETTER, failure_meta)
- raises anything else       -> Nack(RETRY)

The worker installs a SIGTERM handler for graceful drain: it stops requesting
new credit, lets the in-flight message finish, then closes. On a stream
disconnect (or a NOT_LEADER redirect) it reconnects against the leader with
bounded backoff and resumes.
"""

from __future__ import annotations

import logging
import queue
import random
import signal
import socket
import threading
import time
import uuid
from typing import Callable, List, Mapping, Optional, Sequence

import grpc

from rota._common import (
    LeaderClient,
    Targets,
    auth_metadata,
    extract_not_leader,
    normalize_targets,
    to_duration,
)
from rota._gen.rota.v1 import rota_pb2 as pb
from rota._gen.rota.v1 import rota_pb2_grpc as pb_grpc
from rota.exceptions import DeadLetter, Requeue

logger = logging.getLogger("rota.worker")

# Sentinel pushed onto the outbound queue to close the request iterator.
_CLOSE = object()


class Message:
    """A leased message handed to the handler.

    Exposes the delivery fields and two helper methods. ``extend(ttl)`` pushes
    the visibility deadline out; ``complete(external_token=...)`` resolves a
    complete-by-token message in-stream.
    """

    __slots__ = ("_worker", "lease_id", "message_id", "lane", "group_id",
                 "payload", "headers", "attempt", "external_token",
                 "visibility_deadline")

    def __init__(self, worker: "Worker", leased: pb.LeasedMessage):
        self._worker = worker
        self.lease_id: int = leased.lease_id
        self.message_id: int = leased.message_id
        self.lane: str = leased.lane
        self.group_id: str = leased.group_id
        self.payload: bytes = leased.payload
        self.headers: Mapping[str, str] = dict(leased.headers)
        self.attempt: int = leased.attempt
        self.external_token: bytes = leased.external_token
        self.visibility_deadline = (
            leased.visibility_deadline if leased.HasField("visibility_deadline") else None
        )

    def extend(self, ttl: float) -> None:
        """Extend this lease's visibility deadline by ``ttl`` seconds."""
        self._worker._send(
            pb.WorkClientMsg(
                extend=pb.ExtendVisibility(
                    lease_id=self.lease_id, ttl=to_duration(ttl)
                )
            )
        )

    def complete(
        self,
        external_token: Optional[bytes] = None,
        *,
        success: bool = True,
        result_meta: Optional[Mapping[str, str]] = None,
        delay: Optional[float] = None,
    ) -> None:
        """Send an in-stream Complete frame for this message's token."""
        token = external_token if external_token is not None else self.external_token
        frame = pb.Complete(
            external_token=token,
            outcome=pb.SUCCESS if success else pb.FAILURE,
        )
        if result_meta:
            for k, v in result_meta.items():
                frame.result_meta[k] = v
        if delay is not None:
            frame.delay.CopyFrom(to_duration(delay))
        self._worker._send(pb.WorkClientMsg(complete=frame))


class Worker:
    """Leases and processes messages from one lane via the Work stream.

    :param targets_or_channel: a ``host:port`` string, comma list, sequence, or
        a pre-built :class:`grpc.Channel`.
    :param lane: the lane to lease from.
    :param handler: called as ``handler(message: Message)`` for each lease.
    :param credit: in-flight budget. ``1`` (default) means strictly serial.
    """

    def __init__(
        self,
        targets_or_channel: Targets,
        lane: str,
        handler: Callable[[Message], None],
        *,
        credit: int = 1,
        consumer_id: Optional[str] = None,
        group_allow: Optional[Sequence[str]] = None,
        group_deny: Optional[Sequence[str]] = None,
        install_signal_handler: bool = True,
        reconnect_base_backoff: float = 0.2,
        reconnect_max_backoff: float = 10.0,
        channel_options: Optional[Sequence] = None,
        credentials=None,
        metadata=None,
        auth_token: Optional[str] = None,
    ):
        if credit < 1:
            raise ValueError("credit must be >= 1")
        self._lane = lane
        self._handler = handler
        self._credit = credit
        self._consumer_id = consumer_id or f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        self._group_allow = list(group_allow or [])
        self._group_deny = list(group_deny or [])
        self._install_signal_handler = install_signal_handler
        self._reconnect_base = reconnect_base_backoff
        self._reconnect_max = reconnect_max_backoff
        self._channel_options = list(channel_options or [])
        self._credentials = credentials
        self._metadata = auth_metadata(auth_token, metadata)

        if isinstance(targets_or_channel, grpc.Channel):
            self._external_channel: Optional[grpc.Channel] = targets_or_channel
            self._candidates: List[str] = []
        else:
            self._external_channel = None
            self._candidates = normalize_targets(targets_or_channel)
            if not self._candidates:
                raise ValueError("at least one target address is required")

        # Lifecycle flags.
        self._draining = threading.Event()   # stop requesting new credit, finish in-flight
        self._stopped = threading.Event()     # fully stop (no reconnect)
        self._outbound: "queue.Queue[object]" = queue.Queue()
        self._channel: Optional[grpc.Channel] = None
        self._lock = threading.Lock()

    # -- outbound frame plumbing -------------------------------------------

    def _send(self, msg) -> None:
        self._outbound.put(msg)

    def _request_iterator(self):
        """Yield client->server frames: an initial LeaseRequest then acks/nacks.

        Returns when a ``_CLOSE`` sentinel is dequeued (half-close).
        """
        # Advertise demand up front. While draining we still need the iterator
        # alive to flush the final ack/nack, but we do NOT request fresh credit.
        if not self._draining.is_set():
            yield pb.WorkClientMsg(
                lease_request=pb.LeaseRequest(
                    lane=self._lane,
                    credit=self._credit,
                    consumer_id=self._consumer_id,
                    group_allow=self._group_allow,
                    group_deny=self._group_deny,
                )
            )
        while True:
            item = self._outbound.get()
            if item is _CLOSE:
                return
            yield item

    # -- outcome mapping ----------------------------------------------------

    def _dispatch(self, leased: pb.LeasedMessage) -> None:
        """Run the handler for one lease and emit the matching ack/nack."""
        message = Message(self, leased)
        try:
            self._handler(message)
        except Requeue as sig:
            nack = pb.Nack(lease_id=leased.lease_id, mode=pb.REQUEUE_NO_PENALTY)
            if sig.delay is not None:
                nack.delay.CopyFrom(to_duration(sig.delay))
            self._send(pb.WorkClientMsg(nack=nack))
        except DeadLetter as sig:
            nack = pb.Nack(lease_id=leased.lease_id, mode=pb.DEAD_LETTER)
            for k, v in (sig.meta or {}).items():
                nack.failure_meta[k] = v
            if sig.reason:
                nack.failure_meta.setdefault("reason", sig.reason)
            self._send(pb.WorkClientMsg(nack=nack))
        except Exception:  # noqa: BLE001 - any other error => RETRY
            logger.exception(
                "handler raised on lane=%s group=%s lease=%s; nacking RETRY",
                leased.lane, leased.group_id, leased.lease_id,
            )
            self._send(
                pb.WorkClientMsg(nack=pb.Nack(lease_id=leased.lease_id, mode=pb.RETRY))
            )
        else:
            self._send(pb.WorkClientMsg(ack=pb.Ack(lease_id=leased.lease_id)))

    # -- connection lifecycle ----------------------------------------------

    def _dial(self, target: str) -> grpc.Channel:
        if self._credentials is not None:
            return grpc.secure_channel(target, self._credentials, self._channel_options)
        return grpc.insecure_channel(target, self._channel_options)

    def _open_channel(self) -> grpc.Channel:
        if self._external_channel is not None:
            return self._external_channel
        return self._dial(self._candidates[0])

    def _promote_leader(self, leader_addr: str) -> None:
        if leader_addr and leader_addr not in self._candidates:
            self._candidates.insert(0, leader_addr)
        elif leader_addr:
            self._candidates.remove(leader_addr)
            self._candidates.insert(0, leader_addr)

    def _rotate_candidates(self) -> None:
        """Move the current head target to the back, so the next reconnect tries
        a different node (the prior one may be down)."""
        if self._external_channel is None and len(self._candidates) > 1:
            self._candidates.append(self._candidates.pop(0))

    def _run_one_session(self) -> Optional[str]:
        """Open one Work stream and pump it until it ends.

        Returns a leader address to redirect to (NOT_LEADER), or ``None`` for a
        plain disconnect / clean drain.
        """
        # Fresh outbound queue per session (drop stale frames from a dead stream).
        self._outbound = queue.Queue()
        self._channel = self._open_channel()
        stub = pb_grpc.BrokerStub(self._channel)

        responses = stub.Work(self._request_iterator(), metadata=self._metadata or None)
        try:
            for server_msg in responses:
                which = server_msg.WhichOneof("msg")
                if which == "lease":
                    self._dispatch(server_msg.lease)
                    # In drain mode, finish this one then half-close.
                    if self._draining.is_set():
                        self._send(_CLOSE)
                elif which == "error":
                    err = server_msg.error
                    if err.code == pb.NOT_LEADER:
                        self._send(_CLOSE)
                        return err.leader_addr
                    logger.warning(
                        "stream error code=%s detail=%s lease=%s",
                        err.code, err.detail, err.lease_id,
                    )
                elif which == "credit":
                    logger.debug("credit grant lane=%s granted=%s",
                                 server_msg.credit.lane, server_msg.credit.granted)
                elif which == "control":
                    logger.debug("control frame kind=%s lane=%s group=%s",
                                 server_msg.control.kind, server_msg.control.lane,
                                 server_msg.control.group_id)
            return None
        except grpc.RpcError as err:
            leader = extract_not_leader(err)
            if leader is not None:
                return leader
            if self._stopped.is_set() or self._draining.is_set():
                return None
            logger.warning("Work stream error: %s", err)
            raise
        finally:
            # Ensure the request iterator can terminate.
            self._send(_CLOSE)

    # -- public API ---------------------------------------------------------

    def run(self) -> None:
        """Run the lease/dispatch loop until drained or stopped.

        Blocks the calling thread. Reconnects on disconnect (following the
        leader) with bounded exponential backoff. Returns once a graceful drain
        or :meth:`stop` has completed.
        """
        prev_sigterm = None
        prev_sigint = None
        if self._install_signal_handler and threading.current_thread() is threading.main_thread():
            prev_sigterm = signal.signal(signal.SIGTERM, self._on_signal)
            try:
                prev_sigint = signal.signal(signal.SIGINT, self._on_signal)
            except ValueError:  # pragma: no cover
                prev_sigint = None

        backoff_attempt = 0
        try:
            while not self._stopped.is_set():
                redirect: Optional[str] = None
                try:
                    redirect = self._run_one_session()
                    backoff_attempt = 0
                except grpc.RpcError:
                    # transient: fall through to backoff + reconnect
                    pass
                except Exception:  # noqa: BLE001
                    logger.exception("unexpected error in Work session")

                if self._draining.is_set():
                    # We finished the in-flight message; done.
                    break

                if redirect is not None:
                    self._promote_leader(redirect)
                    self._close_channel()
                    continue

                if self._stopped.is_set():
                    break

                # Plain disconnect: the current target may be down. Rotate to the
                # next candidate (a follower redirects us to the leader), back off,
                # then reconnect — this is what makes dead-leader failover work.
                self._close_channel()
                self._rotate_candidates()
                self._sleep_backoff(backoff_attempt)
                backoff_attempt += 1
        finally:
            self._close_channel()
            if prev_sigterm is not None:
                signal.signal(signal.SIGTERM, prev_sigterm)
            if prev_sigint is not None:
                signal.signal(signal.SIGINT, prev_sigint)

    def _sleep_backoff(self, attempt: int) -> None:
        delay = min(self._reconnect_max, self._reconnect_base * (2 ** attempt))
        time.sleep(delay * (0.5 + random.random() * 0.5))

    def _on_signal(self, signum, frame) -> None:  # pragma: no cover - signal path
        logger.info("received signal %s; draining", signum)
        self.drain()

    def drain(self) -> None:
        """Begin a graceful drain: stop requesting credit, finish in-flight."""
        self._draining.set()
        # Nudge the request iterator so a half-close can propagate even if idle.
        self._send(_CLOSE)

    def stop(self) -> None:
        """Stop immediately (no reconnect). In-flight may be cut off."""
        self._stopped.set()
        self._draining.set()
        self._send(_CLOSE)
        self._close_channel()

    def _close_channel(self) -> None:
        with self._lock:
            if self._external_channel is None and self._channel is not None:
                try:
                    self._channel.close()
                except Exception:  # pragma: no cover
                    pass
            self._channel = None
