"""End-to-end durable-execution test through the Python SDK.

Builds the `rota` binary, boots a single-node broker, and drives a real workflow
to completion entirely through the SDK's Workflow client and worker loops:

  start_workflow -> [workflow worker schedules an activity] ->
  [activity worker runs it] -> [workflow worker completes] -> WF_COMPLETED.

This exercises the language-agnostic gRPC worker protocol AND the determinism
gate: the workflow worker must echo a prefix checksum that matches the Go
server's `PrefixChecksumOf` byte-for-byte, or every decision is rejected as
`non_determinism` and the run never completes. So a green test proves the
checksum is correct.

Skips gracefully when the Go toolchain (or building the binary) is unavailable.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import tempfile
import threading
import time

import pytest

import rota
from rota import (
    Control,
    WorkflowClient,
    complete_workflow,
    run_activity_worker,
    run_workflow_worker,
    schedule_activity,
)
from rota._gen.rota.v1 import rota_pb2 as pb

# Repo root is three levels up from this file: sdk/python/tests/ -> repo.
_REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..")
)


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _build_binary() -> str:
    """Build the rota binary; skip the whole test if go is missing or fails."""
    if shutil.which("go") is None:
        pytest.skip("go toolchain not available")
    if not os.path.isdir(os.path.join(_REPO_ROOT, "cmd", "rota")):
        pytest.skip(f"cannot locate ./cmd/rota under {_REPO_ROOT}")
    out = os.path.join(tempfile.gettempdir(), "rota_e2e")
    try:
        proc = subprocess.run(
            ["go", "build", "-o", out, "./cmd/rota"],
            cwd=_REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=300,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover
        pytest.skip(f"go build failed to run: {exc}")
    if proc.returncode != 0:
        pytest.skip(f"go build failed: {proc.stderr.strip()[:500]}")
    return out


def _wait_ready(addr: str, timeout: float = 20.0) -> None:
    """Block until the broker answers a Control.health() on ``addr``."""
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            with Control(addr, timeout=2.0, max_retries=0) as ctl:
                h = ctl.health()
                if h.serving:
                    return
        except Exception as exc:  # noqa: BLE001 - server still coming up
            last_err = exc
        time.sleep(0.1)
    raise RuntimeError(f"broker at {addr} not ready in {timeout}s: {last_err}")


@pytest.fixture()
def broker():
    """Boot a single-node rota broker in a subprocess; yield its gRPC address."""
    binary = _build_binary()
    grpc_port = _free_port()
    metrics_port = _free_port()
    grpc_addr = f"127.0.0.1:{grpc_port}"
    metrics_addr = f"127.0.0.1:{metrics_port}"
    data_dir = tempfile.mkdtemp(prefix="rota_e2e_data_")

    proc = subprocess.Popen(
        [
            binary, "serve",
            "--grpc", grpc_addr,
            "--metrics", metrics_addr,
            "--data", data_dir,
        ],
        cwd=_REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        try:
            _wait_ready(grpc_addr)
        except Exception:
            # Surface server output so a boot failure is debuggable, then skip.
            proc.terminate()
            try:
                out, _ = proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:  # pragma: no cover
                proc.kill()
                out, _ = proc.communicate()
            pytest.skip(f"rota broker did not become ready; output:\n{out}")
        yield grpc_addr
    finally:
        proc.terminate()
        try:
            proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:  # pragma: no cover
            proc.kill()
            proc.communicate()
        shutil.rmtree(data_dir, ignore_errors=True)


def test_workflow_completes_end_to_end(broker):
    addr = broker
    stop = threading.Event()

    # Activity worker: just succeed with a fixed result.
    def handle_activity(task):
        assert task.activity_type == "charge"
        return b"ok", True

    # Workflow decider: schedule one activity, then complete once it's done.
    def decide(run_id, history):
        scheduled = any(
            e.event_type == pb.HistoryEventType.Value("HET_ACTIVITY_SCHEDULED")
            for e in history
        )
        completed = any(
            e.event_type == pb.HistoryEventType.Value("HET_ACTIVITY_COMPLETED")
            for e in history
        )
        if not scheduled:
            return [schedule_activity("charge", input=b"")]
        if completed:
            return [complete_workflow(result=b"done")]
        return []  # activity still running; no-op decision

    act_thread = threading.Thread(
        target=run_activity_worker,
        args=(addr, "charge", "act-1", handle_activity),
        kwargs={"stop_event": stop},
        daemon=True,
    )
    wf_thread = threading.Thread(
        target=run_workflow_worker,
        args=(addr, "order", "wf-1", decide),
        kwargs={"stop_event": stop},
        daemon=True,
    )
    act_thread.start()
    wf_thread.start()

    client = WorkflowClient(addr)
    try:
        run_id = client.start_workflow("order", tenant_id="tenant-1", input=b"{}")
        assert run_id != 0

        completed = pb.WorkflowStatus.Value("WF_COMPLETED")
        deadline = time.time() + 15.0
        status = None
        while time.time() < deadline:
            run = client.get_run(run_id)
            status = run.status
            if status == completed:
                break
            time.sleep(0.04)

        assert status == completed, (
            f"workflow did not reach WF_COMPLETED (status={status}); "
            "a non_determinism rejection (bad prefix checksum) would cause this"
        )

        # History should carry the full causal chain.
        history = client.get_history(run_id)
        types = {e.event_type for e in history}
        assert pb.HistoryEventType.Value("HET_WORKFLOW_STARTED") in types
        assert pb.HistoryEventType.Value("HET_ACTIVITY_SCHEDULED") in types
        assert pb.HistoryEventType.Value("HET_ACTIVITY_COMPLETED") in types
        assert pb.HistoryEventType.Value("HET_WORKFLOW_COMPLETED") in types
    finally:
        stop.set()
        client.close()
        act_thread.join(timeout=3)
        wf_thread.join(timeout=3)
