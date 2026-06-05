"""Rota: a thin, domain-neutral Python SDK for the Rota fair-scheduling broker.

Vocabulary is strictly generic: lane, group, message, lease, policy, cron,
singleton, dead-letter. The SDK does not know what a message *means*; payloads
are opaque bytes plus a headers map.

Quickstart::

    from rota import Publisher, Worker, Requeue, DeadLetter

    pub = Publisher("localhost:9090")
    pub.publish("emails", group_id="tenant-1", payload=b"...")

    def handle(msg):
        do_work(msg.payload)        # ack on return

    Worker("localhost:9090", lane="emails", handler=handle, credit=1).run()
"""

from rota.control import Control
from rota.exceptions import DeadLetter, NotLeaderError, Requeue, RotaError
from rota.publisher import Publisher
from rota.worker import Message, Worker
from rota.workflow import (
    ActivityTask,
    Command,
    WorkflowClient,
    complete_workflow,
    continue_as_new,
    fail_workflow,
    prefix_checksum,
    run_activity_worker,
    run_workflow_worker,
    schedule_activity,
    start_timer,
)

__all__ = [
    "Publisher",
    "Worker",
    "Message",
    "Control",
    "Requeue",
    "DeadLetter",
    "RotaError",
    "NotLeaderError",
    # Durable execution (workflows + activities).
    "WorkflowClient",
    "Command",
    "ActivityTask",
    "prefix_checksum",
    "schedule_activity",
    "start_timer",
    "continue_as_new",
    "complete_workflow",
    "fail_workflow",
    "run_workflow_worker",
    "run_activity_worker",
]

__version__ = "0.1.0"
