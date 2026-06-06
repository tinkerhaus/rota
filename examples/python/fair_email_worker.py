#!/usr/bin/env python3
"""Fair broker example: publish email jobs across tenants and drain them."""

from __future__ import annotations

import argparse
import json
import os
import threading
import time
from typing import Any

from rota import Publisher, Worker


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--addr", default=os.getenv("ROTA_ADDR", "127.0.0.1:7100"))
    p.add_argument("--lane", default="example.emails")
    p.add_argument("--messages", type=int, default=18)
    p.add_argument("--timeout", type=float, default=10.0)
    p.add_argument("--auth-token", default=os.getenv("ROTA_TOKEN", ""))
    return p.parse_args()


def main() -> None:
    args = parse_args()
    tenants = ["tenant-a", "tenant-b", "tenant-c"]
    done = threading.Event()
    lock = threading.Lock()
    processed = 0

    def handle(msg: Any) -> None:
        nonlocal processed
        job = json.loads(msg.payload.decode("utf-8"))
        print(f"sent email msg={msg.message_id} tenant={msg.group_id} to={job['to']}")
        with lock:
            processed += 1
            if processed >= args.messages:
                done.set()

    worker = Worker(
        args.addr,
        lane=args.lane,
        handler=handle,
        credit=1,
        install_signal_handler=False,
        auth_token=args.auth_token or None,
    )
    thread = threading.Thread(target=worker.run, name="rota-email-worker", daemon=True)
    thread.start()

    with Publisher(args.addr, auth_token=args.auth_token or None) as pub:
        for i in range(args.messages):
            tenant = tenants[i % len(tenants)]
            payload = json.dumps({"to": f"user-{i}@example.com", "template": "welcome"}).encode()
            pub.publish(
                args.lane,
                tenant,
                payload,
                headers={"example": "fair-email-worker"},
                dedup_key=f"email-{i}",
            )
            print(f"queued email tenant={tenant} index={i}")

    if not done.wait(args.timeout):
        raise TimeoutError(f"processed {processed}/{args.messages} messages before timeout")

    worker.stop()
    thread.join(timeout=2.0)
    print(f"drained {processed} email jobs from {len(tenants)} tenants")


if __name__ == "__main__":
    main()
