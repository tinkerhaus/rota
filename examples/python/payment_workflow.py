#!/usr/bin/env python3
"""Durable workflow example: schedule one payment activity and complete."""

from __future__ import annotations

import argparse
import os
import threading
import time
from typing import Sequence

from rota import WorkflowClient, complete_workflow, run_activity_worker, run_workflow_worker
from rota.workflow import schedule_activity
from rota._gen.rota.v1 import rota_pb2 as pb


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--addr", default=os.getenv("ROTA_ADDR", "127.0.0.1:7100"))
    p.add_argument("--workflow-type", default="example.payment")
    p.add_argument("--activity-type", default="example.charge-card")
    p.add_argument("--tenant", default="tenant-a")
    p.add_argument("--timeout", type=float, default=15.0)
    p.add_argument("--auth-token", default=os.getenv("ROTA_TOKEN", ""))
    return p.parse_args()


def main() -> None:
    args = parse_args()
    stop = threading.Event()
    auth_token = args.auth_token or None

    def decide(run_id: int, history: Sequence[pb.HistoryEvent]):
        scheduled = any(e.event_type == pb.HET_ACTIVITY_SCHEDULED for e in history)
        completed = any(e.event_type == pb.HET_ACTIVITY_COMPLETED for e in history)
        failed = any(e.event_type == pb.HET_ACTIVITY_FAILED for e in history)
        if failed:
            return [complete_workflow(b"payment failed")]
        if not scheduled:
            return [schedule_activity(args.activity_type, b'{"amount": 4200}')]
        if completed:
            return [complete_workflow(b"payment captured")]
        return []

    def charge(task):
        print(f"charging card run={task.run_id} event={task.scheduled_event_id}")
        return b"charge-id=ch_123", True

    wf_thread = threading.Thread(
        target=run_workflow_worker,
        args=(args.addr, args.workflow_type, "wf-example-1", decide),
        kwargs={"stop_event": stop, "auth_token": auth_token},
        daemon=True,
    )
    act_thread = threading.Thread(
        target=run_activity_worker,
        args=(args.addr, args.activity_type, "act-example-1", charge),
        kwargs={"stop_event": stop, "auth_token": auth_token},
        daemon=True,
    )
    wf_thread.start()
    act_thread.start()

    with WorkflowClient(args.addr, auth_token=auth_token) as client:
        run_id = client.start_workflow(args.workflow_type, tenant_id=args.tenant, input=b"order-123")
        print(f"started workflow run={run_id}")
        deadline = time.time() + args.timeout
        while time.time() < deadline:
            run = client.get_run(run_id)
            print(f"run={run_id} status={pb.WorkflowStatus.Name(run.status)} seq={run.cur_history_seq}")
            if run.status == pb.WF_COMPLETED:
                break
            time.sleep(0.2)
        else:
            raise TimeoutError(f"workflow {run_id} did not complete")

    stop.set()
    wf_thread.join(timeout=2.0)
    act_thread.join(timeout=2.0)
    print("workflow completed")


if __name__ == "__main__":
    main()
