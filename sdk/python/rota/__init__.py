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

__all__ = [
    "Publisher",
    "Worker",
    "Message",
    "Control",
    "Requeue",
    "DeadLetter",
    "RotaError",
    "NotLeaderError",
]

__version__ = "0.1.0"
