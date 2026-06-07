"""Smoke tests: the package imports and core objects construct without a server.

Nothing here touches the network. Constructing a Publisher / Worker / Control
only sets up lazy state; no gRPC connection is dialed until the first call.
"""

from rota import Control, DeadLetter, Message, Publisher, Requeue, Worker
from rota._common import auth_metadata


def test_top_level_exports():
    # Every promised symbol is importable from the package root.
    assert Publisher is not None
    assert Worker is not None
    assert Control is not None
    assert issubclass(Requeue, Exception)
    assert issubclass(DeadLetter, Exception)


def test_generated_stubs_import():
    from rota._gen.rota.v1 import rota_pb2, rota_pb2_grpc

    assert hasattr(rota_pb2, "MessageSpec")
    assert hasattr(rota_pb2, "WorkClientMsg")
    assert hasattr(rota_pb2_grpc, "BrokerStub")
    assert hasattr(rota_pb2_grpc, "ControlStub")


def test_publisher_constructs_lazily():
    pub = Publisher("localhost:9090")
    # Lazy: no channel dialed yet.
    assert pub._client._channel is None
    pub.close()


def test_auth_token_metadata():
    md = auth_metadata("secret", (("x-app", "demo"),))
    assert ("x-app", "demo") in md
    assert ("authorization", "Bearer secret") in md
    assert ("x-rota-token", "secret") in md


def test_publisher_build_spec_rejects_double_eligibility():
    import pytest

    with pytest.raises(ValueError):
        Publisher._build_spec("lane", "g", b"x", delay=1.0, not_before=123.0)


def test_publisher_build_spec_fields():
    spec = Publisher._build_spec(
        "emails",
        "tenant-1",
        b"hello",
        headers={"k": "v"},
        delay=2.5,
        weight=3.0,
        batch_size=4,
        max_attempts=5,
    )
    assert spec.lane == "emails"
    assert spec.group_id == "tenant-1"
    assert spec.payload == b"hello"
    assert spec.headers["k"] == "v"
    assert spec.weight == 3.0
    assert spec.batch_size == 4
    assert spec.max_attempts == 5
    # delay set, `at` (absolute) not set
    assert spec.HasField("delay")
    assert not spec.HasField("at")


def test_worker_constructs_lazily():
    seen = []
    w = Worker(
        "localhost:9090",
        lane="emails",
        handler=lambda m: seen.append(m),
        credit=1,
        install_signal_handler=False,
    )
    assert w._lane == "emails"
    assert w._credit == 1
    assert w._channel is None  # nothing dialed


def test_worker_rejects_zero_credit():
    import pytest

    with pytest.raises(ValueError):
        Worker("localhost:9090", "lane", lambda m: None, credit=0)


def test_control_constructs_lazily():
    ctl = Control("localhost:9090")
    assert ctl._client._channel is None
    ctl.close()


def test_message_namespace_kept_generic():
    # No business vocabulary leaked into the Message surface.
    fields = set(Message.__slots__)
    assert {"payload", "headers", "group_id", "lease_id", "attempt"} <= fields
