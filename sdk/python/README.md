# Rota Python SDK

A thin, **domain-neutral** Python client for [Rota](../../README.md), a generic
fair-scheduling message broker. The SDK speaks only Rota's vocabulary: *lane*,
*group*, *message*, *lease*, *policy*, *cron*, *singleton*, *dead-letter*. It has
no idea what your messages mean. Payloads are opaque `bytes` plus a `headers`
string map.

## Install

```bash
pip install -e sdk/python          # editable, from the repo root
# or just the runtime deps
pip install grpcio protobuf
```

Python 3.9+. Runtime deps: `grpcio`, `protobuf`. (`grpcio-tools` is only needed
to *regenerate* the stubs.)

## Quickstart

### Publish

```python
from rota import Publisher

pub = Publisher("localhost:9090")          # lazy connect; follows the leader

msg_id = pub.publish(
    lane="emails",
    group_id="tenant-1",
    payload=b'{"to": "a@b.com"}',
    headers={"trace": "abc123"},
    delay=5.0,            # become eligible in 5s (or not_before=<unix epoch>)
    weight=2.0,           # upsert this group's scheduling weight
    max_attempts=3,
)
print("published", msg_id)

# Batch (optionally all-or-nothing)
results = pub.publish_batch(
    [
        {"lane": "emails", "group_id": "t1", "payload": b"a"},
        {"lane": "emails", "group_id": "t2", "payload": b"b", "delay": 10.0},
    ],
    atomic=True,
)
pub.close()
```

### Worker loop

A worker leases from one lane and dispatches each message to your handler **one
at a time** when `credit=1` (strictly serial). Returning acks; raising steers the
outcome.

```python
from rota import Worker, Requeue, DeadLetter

def handle(msg):
    # msg.payload, msg.headers, msg.group_id, msg.lease_id, msg.attempt
    print("got", msg.message_id, "attempt", msg.attempt)

    if rate_limited():
        raise Requeue(delay=2.0)          # no attempt penalty, redeliver later

    if poison(msg.payload):
        raise DeadLetter(meta={"why": "unparseable"})   # terminal -> DLQ

    msg.extend(30.0)                       # need more time? push the deadline out
    do_work(msg.payload)                   # return normally -> ack

Worker("localhost:9090", lane="emails", handler=handle, credit=1).run()
```

Outcome mapping:

| handler does                       | broker frame                         |
|------------------------------------|--------------------------------------|
| returns normally                   | `Ack`                                |
| raises `Requeue(delay=...)`        | `Nack(REQUEUE_NO_PENALTY, delay)`    |
| raises `DeadLetter(meta=...)`      | `Nack(DEAD_LETTER, failure_meta)`    |
| raises any other exception         | `Nack(RETRY)` (attempt++)            |

`run()` blocks. It installs a `SIGTERM`/`SIGINT` handler for **graceful drain**:
it stops requesting new credit, lets the in-flight message finish, then closes.
On a disconnect it reconnects against the leader with bounded backoff. Pass
`install_signal_handler=False` to manage that yourself, then call `worker.drain()`
or `worker.stop()`.

### Off-stream completion

For complete-by-token flows (publish with `issue_token=True` or supply
`external_token=...`), a process that did not hold the lease can resolve the
message:

```python
pub.complete(external_token=b"tok", success=True, result_meta={"ok": "1"})
```

Or in-stream from the handler: `msg.complete(success=True)`.

### Control plane

```python
from rota import Control

ctl = Control("localhost:9090")

ctl.set_group_config("emails", "tenant-1", weight=3.0, batch_size=2)
ctl.pause_group("emails", "tenant-1"); ctl.resume_group("emails", "tenant-1")

# Programmable scheduling policy (hot-reloadable)
from rota._gen.rota.v1 import rota_pb2 as pb
ctl.set_policy("emails", kind=pb.WFQ, mode=pb.SCORE, params={"quantum": "16"})

# Cron (generic recurring publish)
ctl.schedule_cron("nightly", lane="emails", group_id="ops",
                  schedule="0 3 * * *", payload=b"tick")

# Singleton lease (cluster-wide single-instance coordination)
lease = ctl.acquire_singleton("leader-x", holder="host-1", ttl=30.0)
ctl.renew_singleton("leader-x", "host-1", lease.fence, ttl=30.0)

print(ctl.get_stats().lanes)
print(ctl.health())          # serving / has_quorum / is_leader
```

## Leader following

Rota is Raft-backed; writes must hit the leader. Every unary call and the Work
stream detect a `NOT_LEADER` fault (gRPC `FAILED_PRECONDITION`), re-dial the
advertised leader address, and retry with bounded exponential backoff + jitter.
You can pass multiple seed addresses: `Publisher("a:9090,b:9090,c:9090")`.

## Regenerating the gRPC stubs

The generated code lives in `rota/_gen/rota/v1/` and is committed. To regenerate
after a proto change, from the repo root:

```bash
pip install grpcio-tools
python3 -m grpc_tools.protoc -I proto \
  --python_out=sdk/python/rota/_gen \
  --grpc_python_out=sdk/python/rota/_gen \
  proto/rota/v1/rota.proto
```

Then rewrite the one absolute import in `rota_pb2_grpc.py`
(`from rota.v1 import rota_pb2` -> `from rota._gen.rota.v1 import rota_pb2`) and
ensure the `_gen/**/__init__.py` files exist. See `buf.gen.python.yaml` at the
repo root for the buf-driven equivalent.

## Tests

```bash
pip install pytest
pytest sdk/python/tests          # import + construction smoke tests (no server)
```
