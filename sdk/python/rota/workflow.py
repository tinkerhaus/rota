"""Durable execution: workflow + activity clients and worker loops.

This is the Python side of Rota's Phase-8 durable-execution engine. The engine
itself lives in the broker; an SDK in any language just drives it over the
``Workflow`` gRPC service. The vocabulary is deliberately neutral: a *workflow
run* has a deterministic *history* of *events*; a *worker* leases a *task*,
replays the history, *decides* a list of *commands*, and submits them back.

Two worker roles:

- A **workflow worker** (:func:`run_workflow_worker`) leases workflow tasks,
  replays the run's committed history, and returns a list of :class:`Command`
  the engine appends as new events. It MUST echo the *prefix checksum* it
  computed over the history it replayed (see :func:`prefix_checksum`) or the
  leader's determinism gate rejects the decision as ``non_determinism``.
- An **activity worker** (:func:`run_activity_worker`) leases activity tasks
  (activities ARE ordinary leases under the hood), runs side-effecting work,
  and reports ``(result_bytes, success)`` back into the run's history.

Poll RPCs return ``empty=True`` when nothing is available; the loops sleep
~20ms and retry.
"""

from __future__ import annotations

import hashlib
import struct
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Sequence, Tuple, Union

import grpc

from rota._common import LeaderClient, Targets, normalize_targets
from rota._gen.rota.v1 import rota_pb2 as pb
from rota._gen.rota.v1 import rota_pb2_grpc as pb_grpc

# Empty polls back off by this much before retrying (seconds).
_POLL_IDLE_SLEEP = 0.02


# ─────────────────────────────────────────────────────────────────────────────
# Determinism: prefix checksum
# ─────────────────────────────────────────────────────────────────────────────


def prefix_checksum(history: Sequence[pb.HistoryEvent]) -> bytes:
    """Compute the determinism checksum over a run's replayed history.

    This MUST be byte-identical to the broker's ``PrefixChecksumOf`` (the leader
    recomputes the same value over the committed prefix and rejects on mismatch).
    The algorithm hashes, for each event IN ORDER, the concatenation of:

    - ``event_id`` as a big-endian u64,
    - the ``event_type`` ENUM NUMBER as a big-endian u32,
    - the raw ``attrs`` bytes,

    and returns the 32-byte SHA-256 digest.

    :param history: the run's history events, in ``event_id`` order (exactly as
        returned by :meth:`WorkflowClient.get_history` / a polled task).
    :returns: the 32-byte digest a worker echoes in ``RespondWorkflowTask``.
    """
    h = hashlib.sha256()
    for ev in history:
        # event_type carries the proto enum NUMBER (read from the event, never
        # hardcoded) so this tracks the wire definition.
        h.update(struct.pack(">Q", int(ev.event_id)))
        h.update(struct.pack(">I", int(ev.event_type)))
        h.update(ev.attrs)
    return h.digest()


# ─────────────────────────────────────────────────────────────────────────────
# Commands: a workflow decider's output
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class Command:
    """One decision a workflow task emits, turned into a history event.

    Prefer the helper constructors (:func:`schedule_activity`, :func:`start_timer`,
    :func:`continue_as_new`, :func:`complete_workflow`, :func:`fail_workflow`)
    over building this directly. ``kind`` is one of the engine's command kinds:
    ``schedule_activity | start_timer | continue_as_new | complete_workflow |
    fail_workflow``.
    """

    kind: str
    activity_type: str = ""
    input: bytes = b""
    result: bytes = b""
    delay_ms: int = 0

    def to_proto(self) -> pb.WorkflowCommandProto:
        return pb.WorkflowCommandProto(
            kind=self.kind,
            activity_type=self.activity_type,
            input=self.input or b"",
            result=self.result or b"",
            delay_ms=int(self.delay_ms),
        )


def schedule_activity(activity_type: str, input: bytes = b"") -> Command:
    """Schedule an activity. Its completion lands back in history as an
    ``ACTIVITY_COMPLETED`` (or ``ACTIVITY_FAILED``) event."""
    return Command(kind="schedule_activity", activity_type=activity_type, input=input or b"")


def start_timer(delay_ms: int) -> Command:
    """Start a durable timer. The engine leader-stamps the absolute fire time;
    a ``TIMER_FIRED`` event is appended when it elapses."""
    return Command(kind="start_timer", delay_ms=int(delay_ms))


def continue_as_new(input: bytes = b"") -> Command:
    """Close this run (status CONTINUED) and start a successor with ``input``
    and a fresh, empty history — bounding per-run history for looping workflows."""
    return Command(kind="continue_as_new", input=input or b"")


def complete_workflow(result: bytes = b"") -> Command:
    """Terminally complete the run with ``result`` (status WF_COMPLETED)."""
    return Command(kind="complete_workflow", result=result or b"")


def fail_workflow(result: bytes = b"") -> Command:
    """Terminally fail the run with ``result`` (status WF_FAILED)."""
    return Command(kind="fail_workflow", result=result or b"")


# ─────────────────────────────────────────────────────────────────────────────
# Activity task wrapper
# ─────────────────────────────────────────────────────────────────────────────


class ActivityTask:
    """A leased activity task handed to an activity handler.

    Exposes the dispatch fields. ``run_id`` + ``scheduled_event_id`` route the
    result back into the originating run's history; ``activity_type`` and
    ``input`` describe the work to do.
    """

    __slots__ = ("run_id", "lease_id", "scheduled_event_id", "activity_type", "input")

    def __init__(self, polled: pb.PolledActivityTask):
        self.run_id: int = polled.run_id
        self.lease_id: int = polled.lease_id
        self.scheduled_event_id: int = polled.scheduled_event_id
        self.activity_type: str = polled.activity_type
        self.input: bytes = polled.input


# ─────────────────────────────────────────────────────────────────────────────
# Client
# ─────────────────────────────────────────────────────────────────────────────


class WorkflowClient:
    """Client for the Rota ``Workflow`` service.

    Covers the run lifecycle (start / signal / cancel) and read RPCs (get run,
    get history, list runs). Every call follows the cluster leader on a
    NOT_LEADER fault. Methods return raw protobuf responses so nothing is lost.

    :param targets: a ``host:port`` string, comma list, sequence, or a pre-built
        :class:`grpc.Channel`.
    """

    def __init__(
        self,
        targets: Targets = "127.0.0.1:7100",
        *,
        timeout: Optional[float] = 30.0,
        max_retries: int = 5,
        channel_options: Optional[Sequence] = None,
        credentials=None,
    ):
        self._timeout = timeout
        self._client = LeaderClient(
            targets,
            pb_grpc.WorkflowStub,
            channel_options=channel_options,
            max_retries=max_retries,
            credentials=credentials,
        )

    def _call(self, method: str, request):
        return self._client.call(method, request, timeout=self._timeout)

    # -- lifecycle ----------------------------------------------------------

    def start_workflow(
        self, workflow_type: str, *, tenant_id: str = "", input: bytes = b""
    ) -> int:
        """Start a workflow run. Returns the broker-assigned ``run_id``.

        ``tenant_id`` is the fairness unit (the group analogue); ``input`` is the
        opaque starting payload replayed at the head of the run's history.
        """
        resp = self._call(
            "StartWorkflow",
            pb.StartWorkflowRequest(
                workflow_type=workflow_type, tenant_id=tenant_id, input=input or b""
            ),
        )
        return resp.run_id

    def signal_workflow(
        self, run_id: int, signal_name: str, payload: bytes = b""
    ) -> pb.SignalWorkflowResponse:
        """Deliver an external signal to a running workflow. The engine appends a
        SIGNAL_RECEIVED event and dispatches a workflow task to react to it."""
        return self._call(
            "SignalWorkflow",
            pb.SignalWorkflowRequest(
                run_id=run_id, signal_name=signal_name, payload=payload or b""
            ),
        )

    def cancel_workflow(self, run_id: int, reason: bytes = b"") -> bool:
        """Request cancellation of a run. Returns whether it was canceled."""
        resp = self._call(
            "CancelWorkflow",
            pb.CancelWorkflowRequest(run_id=run_id, reason=reason or b""),
        )
        return resp.canceled

    # -- reads --------------------------------------------------------------

    def get_run(self, run_id: int) -> pb.WorkflowRun:
        """Fetch a run's quorum-written record (status, epoch, history seq, ...)."""
        return self._call("GetWorkflowRun", pb.WorkflowRunRef(run_id=run_id))

    def get_history(self, run_id: int) -> List[pb.HistoryEvent]:
        """Fetch a run's full history, in ``event_id`` order."""
        resp = self._call("GetWorkflowHistory", pb.WorkflowRunRef(run_id=run_id))
        return list(resp.events)

    def list_runs(
        self,
        *,
        status: Optional[int] = None,
        page_size: int = 0,
        page_token: str = "",
    ) -> pb.ListWorkflowRunsResponse:
        """List runs, optionally filtered by ``status`` (a ``WorkflowStatus``
        enum value, e.g. ``pb.WF_RUNNING``). Paginated via ``page_token``."""
        req = pb.ListWorkflowRunsRequest(page_size=page_size, page_token=page_token)
        if status is not None:
            req.status = status
            req.has_status = True
        return self._call("ListWorkflowRuns", req)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "WorkflowClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


# ─────────────────────────────────────────────────────────────────────────────
# Worker loops
# ─────────────────────────────────────────────────────────────────────────────

# A decider replays a run's history and returns the commands to apply.
Decider = Callable[[int, List[pb.HistoryEvent]], Sequence[Command]]
# An activity handler runs side-effecting work and returns (result, success).
ActivityHandler = Callable[[ActivityTask], Tuple[bytes, bool]]


def _resolve_channel(
    channel_or_addr: Union[str, Sequence[str], grpc.Channel],
    channel_options: Optional[Sequence],
    credentials,
) -> Tuple[grpc.Channel, bool]:
    """Return ``(channel, owned)``; ``owned`` is True if we created it."""
    if isinstance(channel_or_addr, grpc.Channel):
        return channel_or_addr, False
    targets = normalize_targets(channel_or_addr)
    if not targets:
        raise ValueError("at least one target address is required")
    if credentials is not None:
        return grpc.secure_channel(targets[0], credentials, channel_options or []), True
    return grpc.insecure_channel(targets[0], channel_options or []), True


def run_workflow_worker(
    channel_or_addr: Union[str, Sequence[str], grpc.Channel],
    workflow_type: str,
    consumer_id: str,
    decide: Decider,
    *,
    stop_event: Optional[threading.Event] = None,
    channel_options: Optional[Sequence] = None,
    credentials=None,
    timeout: Optional[float] = 30.0,
) -> None:
    """Run the workflow-task poll/decide/respond loop until ``stop_event`` is set.

    Each iteration leases one workflow task, replays the committed history, calls
    ``decide(run_id, history) -> list[Command]``, computes the prefix checksum
    over that SAME history (so the leader's determinism gate accepts the
    decision), and submits the commands. Blocks the calling thread; run it in a
    thread if you want it in the background.

    :param channel_or_addr: an address (string / comma list / sequence) or a
        pre-built :class:`grpc.Channel`.
    :param workflow_type: the workflow type to poll (the dispatch lane).
    :param consumer_id: this worker's stable identity.
    :param decide: the decider; returns a sequence of :class:`Command`.
    :param stop_event: set it to stop the loop after the next poll cycle.
    """
    stop = stop_event or threading.Event()
    channel, owned = _resolve_channel(channel_or_addr, channel_options, credentials)
    stub = pb_grpc.WorkflowStub(channel)
    req = pb.PollTaskRequest(task_type=workflow_type, consumer_id=consumer_id)
    try:
        while not stop.is_set():
            try:
                task = stub.PollWorkflowTask(req, timeout=timeout)
            except grpc.RpcError:
                if stop.is_set():
                    break
                time.sleep(_POLL_IDLE_SLEEP)
                continue
            if task.empty:
                time.sleep(_POLL_IDLE_SLEEP)
                continue

            history = list(task.history)
            checksum = prefix_checksum(history)
            commands = decide(task.run_id, history) or []
            cmd_protos = [c.to_proto() for c in commands]
            stub.RespondWorkflowTask(
                pb.RespondWorkflowTaskRequest(
                    run_id=task.run_id,
                    lease_id=task.lease_id,
                    run_epoch=task.run_epoch,
                    history_seq=task.history_seq,
                    prefix_checksum=checksum,
                    commands=cmd_protos,
                ),
                timeout=timeout,
            )
    finally:
        if owned:
            channel.close()


def run_activity_worker(
    addr: Union[str, Sequence[str], grpc.Channel],
    activity_type: str,
    consumer_id: str,
    handler: ActivityHandler,
    *,
    stop_event: Optional[threading.Event] = None,
    channel_options: Optional[Sequence] = None,
    credentials=None,
    timeout: Optional[float] = 30.0,
) -> None:
    """Run the activity-task poll/handle/respond loop until ``stop_event`` is set.

    Each iteration leases one activity task, runs ``handler(task) ->
    (result_bytes, success)``, and reports the outcome back into the run's
    history (idempotent by ``scheduled_event_id``). A handler that raises is
    reported as a failure. Blocks the calling thread.

    :param addr: an address (string / comma list / sequence) or a pre-built
        :class:`grpc.Channel`.
    :param activity_type: the activity type to poll.
    :param consumer_id: this worker's stable identity.
    :param handler: the activity handler; returns ``(result_bytes, success)``.
    :param stop_event: set it to stop the loop after the next poll cycle.
    """
    stop = stop_event or threading.Event()
    channel, owned = _resolve_channel(addr, channel_options, credentials)
    stub = pb_grpc.WorkflowStub(channel)
    req = pb.PollTaskRequest(task_type=activity_type, consumer_id=consumer_id)
    try:
        while not stop.is_set():
            try:
                polled = stub.PollActivityTask(req, timeout=timeout)
            except grpc.RpcError:
                if stop.is_set():
                    break
                time.sleep(_POLL_IDLE_SLEEP)
                continue
            if polled.empty:
                time.sleep(_POLL_IDLE_SLEEP)
                continue

            task = ActivityTask(polled)
            try:
                result, success = handler(task)
            except Exception:  # noqa: BLE001 - any handler error => failed activity
                result, success = b"", False
            stub.RespondActivityTask(
                pb.RespondActivityTaskRequest(
                    run_id=task.run_id,
                    lease_id=task.lease_id,
                    scheduled_event_id=task.scheduled_event_id,
                    success=success,
                    result=result or b"",
                ),
                timeout=timeout,
            )
    finally:
        if owned:
            channel.close()
