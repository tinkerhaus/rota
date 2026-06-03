"""Control: thin client over Rota's control plane.

Covers programmable policy, group lifecycle, cron, singleton leases,
introspection, and health. Every call follows the cluster leader on a
NOT_LEADER fault. Methods return the raw protobuf responses so nothing is lost;
the SDK does not editorialize the control surface.
"""

from __future__ import annotations

from typing import Mapping, Optional, Sequence

from rota._common import LeaderClient, Targets, to_duration, to_timestamp
from rota._gen.rota.v1 import rota_pb2 as pb
from rota._gen.rota.v1 import rota_pb2_grpc as pb_grpc


class Control:
    """Client for the Rota ``Control`` service.

    :param targets: a ``host:port`` string, comma list, sequence, or a
        pre-built :class:`grpc.Channel`.
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
            pb_grpc.ControlStub,
            channel_options=channel_options,
            max_retries=max_retries,
            credentials=credentials,
        )

    def _call(self, method: str, request):
        return self._client.call(method, request, timeout=self._timeout)

    # -- group config -------------------------------------------------------

    def set_group_config(
        self,
        lane: str,
        group_id: str,
        *,
        weight: Optional[float] = None,
        batch_size: Optional[int] = None,
    ) -> pb.GroupConfig:
        """Set (upsert) a group's scheduling knobs. Returns the live config."""
        req = pb.SetGroupConfigRequest(lane=lane, group_id=group_id)
        if weight is not None:
            req.weight = weight
        if batch_size is not None:
            req.batch_size = batch_size
        return self._call("SetGroupConfig", req)

    def get_group_config(self, lane: str, group_id: str) -> pb.GroupConfig:
        return self._call("GetGroupConfig", pb.GroupRef(lane=lane, group_id=group_id))

    # -- group lifecycle ----------------------------------------------------

    def pause_group(self, lane: str, group_id: str) -> pb.GroupConfig:
        return self._call("PauseGroup", pb.GroupRef(lane=lane, group_id=group_id))

    def resume_group(self, lane: str, group_id: str) -> pb.GroupConfig:
        return self._call("ResumeGroup", pb.GroupRef(lane=lane, group_id=group_id))

    def cancel_group(self, lane: str, group_id: str) -> pb.GroupOpResult:
        """Drop READY/DELAYED messages and drain in-flight for the group."""
        return self._call("CancelGroup", pb.GroupRef(lane=lane, group_id=group_id))

    def purge_group(self, lane: str, group_id: str) -> pb.GroupOpResult:
        """Drop everything leasable in the group, keeping its config."""
        return self._call("PurgeGroup", pb.GroupRef(lane=lane, group_id=group_id))

    def reap_group(self, lane: str, group_id: str) -> pb.GroupOpResult:
        """Idle-reap a group (asserts it is empty)."""
        return self._call("ReapGroup", pb.GroupRef(lane=lane, group_id=group_id))

    def teardown_group(self, group_id: str) -> pb.TeardownResult:
        """Tear a group down across ALL lanes in one call."""
        return self._call("TeardownGroup", pb.TeardownRequest(group_id=group_id))

    # -- lane config + back-pressure ---------------------------------------

    def set_lane_config(
        self, lane: str, *, rate_per_sec: float = 0.0, burst: int = 0
    ) -> pb.LaneConfig:
        """Set a lane's dequeue rate limit. ``rate_per_sec`` <= 0 means unlimited."""
        return self._call(
            "SetLaneConfig",
            pb.SetLaneConfigRequest(lane=lane, rate_per_sec=rate_per_sec, burst=burst),
        )

    def pause_lane(self, lane: str, duration: float = 0.0) -> pb.LaneOpResult:
        """Stop leasing a lane. ``duration`` in seconds; 0 = until ``resume_lane``."""
        req = pb.PauseLaneRequest(lane=lane)
        if duration:
            req.duration.CopyFrom(to_duration(duration))
        return self._call("PauseLane", req)

    def resume_lane(self, lane: str) -> pb.LaneOpResult:
        """Resume leasing a paused lane."""
        return self._call("ResumeLane", pb.LaneRef(lane=lane))

    # -- policy -------------------------------------------------------------

    def set_policy(
        self,
        lane: str,
        *,
        kind: int = pb.DRR,
        mode: int = pb.POLICY_MODE_UNSPECIFIED,
        engine: str = "builtin",
        code: bytes = b"",
        params: Optional[Mapping[str, str]] = None,
    ) -> pb.PolicyInfo:
        """Install (hot-reload) a scheduling policy on a lane.

        ``kind``/``mode`` are values from the proto enums (e.g. ``pb.DRR``,
        ``pb.WFQ``, ``pb.SCORE``). ``engine`` is one of ``builtin|cel|wasm|
        starlark``; ``code`` is the policy source (empty for pure built-ins).
        """
        source = pb.PolicySource(kind=kind, mode=mode, engine=engine, code=code)
        if params:
            for k, v in params.items():
                source.params[k] = v
        return self._call("SetPolicy", pb.SetPolicyRequest(lane=lane, source=source))

    def get_policy(self, lane: str) -> pb.PolicyInfo:
        return self._call("GetPolicy", pb.LaneRef(lane=lane))

    def validate_policy(
        self,
        lane: str,
        *,
        kind: int = pb.CUSTOM,
        mode: int = pb.POLICY_MODE_UNSPECIFIED,
        engine: str = "builtin",
        code: bytes = b"",
        params: Optional[Mapping[str, str]] = None,
    ) -> pb.ValidatePolicyResult:
        """Compile + dry-run a policy without installing it."""
        source = pb.PolicySource(kind=kind, mode=mode, engine=engine, code=code)
        if params:
            for k, v in params.items():
                source.params[k] = v
        return self._call("ValidatePolicy", pb.SetPolicyRequest(lane=lane, source=source))

    # -- cron ---------------------------------------------------------------

    def schedule_cron(
        self,
        cron_id: str,
        lane: str,
        group_id: str,
        schedule: str,
        *,
        payload: bytes = b"",
        headers: Optional[Mapping[str, str]] = None,
        timezone: str = "",
        misfire: int = pb.MISFIRE_POLICY_UNSPECIFIED,
        coalesce: bool = False,
        start_at: Optional[float] = None,
        end_at: Optional[float] = None,
    ) -> pb.CronInfo:
        """Schedule a recurring publish (idempotent on ``cron_id``).

        ``schedule`` is a UTC crontab expression. ``start_at`` / ``end_at`` are
        optional unix epoch (seconds) bounds.
        """
        req = pb.ScheduleCronRequest(
            cron_id=cron_id,
            lane=lane,
            group_id=group_id,
            payload=payload,
            schedule=schedule,
            timezone=timezone,
            misfire=misfire,
            coalesce=coalesce,
        )
        if headers:
            for k, v in headers.items():
                req.headers[k] = v
        if start_at is not None:
            req.start_at.CopyFrom(to_timestamp(start_at))
        if end_at is not None:
            req.end_at.CopyFrom(to_timestamp(end_at))
        return self._call("ScheduleCron", req)

    def list_cron(self, lane: str = "", next_n: int = 0) -> pb.ListCronResponse:
        return self._call("ListCron", pb.ListCronRequest(lane=lane, next_n=next_n))

    def delete_cron(self, cron_id: str) -> pb.CronOpResult:
        return self._call("DeleteCron", pb.CronRef(cron_id=cron_id))

    def pause_cron(self, cron_id: str) -> pb.CronInfo:
        return self._call("PauseCron", pb.CronRef(cron_id=cron_id))

    # -- singleton leases ---------------------------------------------------

    def acquire_singleton(self, name: str, holder: str, ttl: float) -> pb.SingletonLease:
        """Acquire a cluster-wide singleton lease. ``ttl`` is in seconds."""
        return self._call(
            "AcquireSingletonLease",
            pb.AcquireSingletonRequest(name=name, holder=holder, ttl=to_duration(ttl)),
        )

    def renew_singleton(
        self, name: str, holder: str, fence: int, ttl: float
    ) -> pb.SingletonLease:
        """Renew a held singleton lease using its fencing token."""
        return self._call(
            "RenewSingletonLease",
            pb.RenewSingletonRequest(
                name=name, holder=holder, fence=fence, ttl=to_duration(ttl)
            ),
        )

    def release_singleton(
        self, name: str, holder: str, fence: int
    ) -> pb.SingletonOpResult:
        return self._call(
            "ReleaseSingletonLease",
            pb.ReleaseSingletonRequest(name=name, holder=holder, fence=fence),
        )

    # -- introspection ------------------------------------------------------

    def get_stats(self, lane: str = "", group_id: str = "") -> pb.StatsResponse:
        """Fetch per-lane stats, optionally filtered by lane and/or group."""
        return self._call("GetStats", pb.GetStatsRequest(lane=lane, group_id=group_id))

    def describe_cluster(self) -> pb.ClusterInfo:
        return self._call("DescribeCluster", pb.DescribeClusterRequest())

    def health(self) -> pb.HealthResponse:
        """Liveness/leadership probe: serving / has_quorum / is_leader."""
        return self._call("Health", pb.HealthRequest())

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "Control":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
