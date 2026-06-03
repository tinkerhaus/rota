"""Publisher: submit messages to a Rota lane/group and complete by token.

Domain-neutral. A message is opaque ``payload`` bytes plus a ``headers`` map,
addressed to a (``lane``, ``group_id``) pair. Eligibility can be deferred via a
relative ``delay`` (seconds) or an absolute ``not_before`` (unix epoch seconds).
"""

from __future__ import annotations

from typing import List, Mapping, Optional, Sequence, Union

from rota._common import LeaderClient, Targets, to_duration, to_timestamp
from rota._gen.rota.v1 import rota_pb2 as pb
from rota._gen.rota.v1 import rota_pb2_grpc as pb_grpc


class Publisher:
    """Publishes messages to a Rota broker.

    The gRPC channel is dialed lazily on first use. On a NOT_LEADER fault the
    publisher transparently re-dials the advertised leader and retries with
    bounded exponential backoff.

    :param targets: a ``host:port`` string, a comma-separated list, a sequence
        of such strings, or a pre-built :class:`grpc.Channel`.
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
            pb_grpc.BrokerStub,
            channel_options=channel_options,
            max_retries=max_retries,
            credentials=credentials,
        )

    # -- spec construction --------------------------------------------------

    @staticmethod
    def _build_spec(
        lane: str,
        group_id: str,
        payload: bytes,
        *,
        headers: Optional[Mapping[str, str]] = None,
        delay: Optional[float] = None,
        not_before: Optional[float] = None,
        weight: Optional[float] = None,
        batch_size: Optional[int] = None,
        max_attempts: Optional[int] = None,
        ttl: Optional[float] = None,
        dedup_key: Optional[str] = None,
        issue_token: bool = False,
        external_token: Optional[bytes] = None,
    ) -> pb.MessageSpec:
        if delay is not None and not_before is not None:
            raise ValueError("set at most one of `delay` or `not_before`")

        spec = pb.MessageSpec(
            lane=lane,
            group_id=group_id,
            payload=payload or b"",
        )
        if headers:
            for k, v in headers.items():
                spec.headers[k] = v
        if delay is not None:
            spec.delay.CopyFrom(to_duration(delay))
        if not_before is not None:
            spec.at.CopyFrom(to_timestamp(not_before))
        if max_attempts is not None:
            spec.max_attempts = max_attempts
        if ttl is not None:
            spec.ttl.CopyFrom(to_duration(ttl))
        if weight is not None:
            spec.weight = weight
        if batch_size is not None:
            spec.batch_size = batch_size
        if dedup_key is not None:
            spec.dedup_key = dedup_key
        if issue_token:
            spec.issue_token = True
        if external_token is not None:
            spec.external_token = external_token
        return spec

    # -- public API ---------------------------------------------------------

    def publish(
        self,
        lane: str,
        group_id: str,
        payload: bytes,
        *,
        headers: Optional[Mapping[str, str]] = None,
        delay: Optional[float] = None,
        not_before: Optional[float] = None,
        weight: Optional[float] = None,
        batch_size: Optional[int] = None,
        max_attempts: Optional[int] = None,
        ttl: Optional[float] = None,
        dedup_key: Optional[str] = None,
        issue_token: bool = False,
        external_token: Optional[bytes] = None,
    ) -> int:
        """Publish one message. Returns the broker-assigned ``message_id``.

        :param delay: relative eligibility delay in seconds.
        :param not_before: absolute eligibility instant as a unix epoch (seconds).
            Mutually exclusive with ``delay``.
        :param weight: upsert hint for the group's scheduling weight.
        :param batch_size: upsert hint for the group's per-turn batch size.
        :param max_attempts: 0 (or unset) = lane default.
        """
        spec = self._build_spec(
            lane,
            group_id,
            payload,
            headers=headers,
            delay=delay,
            not_before=not_before,
            weight=weight,
            batch_size=batch_size,
            max_attempts=max_attempts,
            ttl=ttl,
            dedup_key=dedup_key,
            issue_token=issue_token,
            external_token=external_token,
        )
        resp = self._client.call(
            "Publish", pb.PublishRequest(message=spec), timeout=self._timeout
        )
        return resp.message_id

    def publish_batch(
        self,
        messages: Sequence[Union[pb.MessageSpec, Mapping]],
        *,
        atomic: bool = False,
    ) -> List[pb.PublishItemResult]:
        """Publish many messages in one call.

        Each item may be a pre-built :class:`MessageSpec` or a mapping of the
        same keyword arguments accepted by :meth:`publish` (plus ``lane``,
        ``group_id``, ``payload``). With ``atomic=True`` the broker applies them
        as a single all-or-nothing write.

        Returns the list of per-item results.
        """
        specs: List[pb.MessageSpec] = []
        for item in messages:
            if isinstance(item, pb.MessageSpec):
                specs.append(item)
            elif isinstance(item, Mapping):
                kwargs = dict(item)
                lane = kwargs.pop("lane")
                group_id = kwargs.pop("group_id")
                payload = kwargs.pop("payload", b"")
                specs.append(self._build_spec(lane, group_id, payload, **kwargs))
            else:
                raise TypeError(
                    "batch items must be MessageSpec or a mapping, got "
                    f"{type(item).__name__}"
                )

        req = pb.PublishBatchRequest(messages=specs, atomic=atomic)
        resp = self._client.call("PublishBatch", req, timeout=self._timeout)
        return list(resp.results)

    def complete(
        self,
        external_token: bytes,
        *,
        success: bool = True,
        result_meta: Optional[Mapping[str, str]] = None,
        delay: Optional[float] = None,
    ) -> pb.CompleteResult:
        """Complete a message off-stream by its external completion token.

        Twin of the in-stream Complete frame: lets a process that did not hold
        the lease (e.g. an async callback) resolve a message keyed by token.
        ``success=False`` records a failure outcome.
        """
        req = pb.CompleteByTokenRequest(
            external_token=external_token,
            outcome=pb.SUCCESS if success else pb.FAILURE,
        )
        if result_meta:
            for k, v in result_meta.items():
                req.result_meta[k] = v
        if delay is not None:
            req.delay.CopyFrom(to_duration(delay))
        return self._client.call("CompleteByToken", req, timeout=self._timeout)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "Publisher":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
