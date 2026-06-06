/**
 * Control: thin client over Rota's control plane.
 *
 * Covers programmable policy, group config + lifecycle, lane back-pressure,
 * cron, singleton leases, off-stream completion, introspection, and health.
 * Every call follows the cluster leader on a NOT_LEADER fault. Methods resolve
 * to the raw protobuf responses so nothing is lost.
 */

import {
  LeaderClient,
  loadProto,
  toDuration,
  toTimestamp,
  type LeaderClientOptions,
  type Targets,
} from "./common.js";
import { Outcome } from "./generated/rota/v1/Outcome.js";
import { PolicyKind } from "./generated/rota/v1/PolicyKind.js";
import { PolicyMode } from "./generated/rota/v1/PolicyMode.js";
import { MisfirePolicy } from "./generated/rota/v1/MisfirePolicy.js";
import type { ControlClient } from "./generated/rota/v1/Control.js";
import type { GroupConfig__Output } from "./generated/rota/v1/GroupConfig.js";
import type { GroupOpResult__Output } from "./generated/rota/v1/GroupOpResult.js";
import type { TeardownResult__Output } from "./generated/rota/v1/TeardownResult.js";
import type { LaneConfig__Output } from "./generated/rota/v1/LaneConfig.js";
import type { LaneOpResult__Output } from "./generated/rota/v1/LaneOpResult.js";
import type { PolicyInfo__Output } from "./generated/rota/v1/PolicyInfo.js";
import type { ValidatePolicyResult__Output } from "./generated/rota/v1/ValidatePolicyResult.js";
import type { CronInfo__Output } from "./generated/rota/v1/CronInfo.js";
import type { ListCronResponse__Output } from "./generated/rota/v1/ListCronResponse.js";
import type { CronOpResult__Output } from "./generated/rota/v1/CronOpResult.js";
import type { SingletonLease__Output } from "./generated/rota/v1/SingletonLease.js";
import type { SingletonOpResult__Output } from "./generated/rota/v1/SingletonOpResult.js";
import type { CompleteResult__Output } from "./generated/rota/v1/CompleteResult.js";
import type { StatsResponse__Output } from "./generated/rota/v1/StatsResponse.js";
import type { ClusterInfo__Output } from "./generated/rota/v1/ClusterInfo.js";
import type { HealthResponse__Output } from "./generated/rota/v1/HealthResponse.js";

export interface ControlOptions extends LeaderClientOptions {
  /** Per-call deadline in seconds (default 30). Pass ``null`` for no deadline. */
  timeout?: number | null;
}

/** A scheduling policy source for {@link Control.setPolicy} / {@link Control.validatePolicy}. */
export interface PolicySpec {
  kind?: number;
  mode?: number;
  engine?: string;
  code?: Uint8Array;
  params?: Record<string, string>;
}

/** Client for the Rota ``Control`` service. */
export class Control {
  private readonly timeout: number | null;
  private readonly client: LeaderClient<ControlClient>;

  constructor(targets: Targets = "127.0.0.1:7100", opts: ControlOptions = {}) {
    const { timeout, ...leaderOpts } = opts;
    this.timeout = timeout === undefined ? 30.0 : timeout;
    const proto = loadProto();
    this.client = new LeaderClient(targets, proto.rota.v1.Control, leaderOpts);
  }

  private call<Res>(method: string, request: unknown): Promise<Res> {
    return this.client.call<Res>(method, request, { timeout: this.timeout });
  }

  // ── group config ───────────────────────────────────────────────────────────

  /** Set (upsert) a group's scheduling knobs. Resolves to the live config. */
  setGroupConfig(
    lane: string,
    groupId: string,
    opts: { weight?: number; batchSize?: number } = {},
  ): Promise<GroupConfig__Output> {
    const req: Record<string, unknown> = { lane, groupId };
    if (opts.weight !== undefined) req.weight = opts.weight;
    if (opts.batchSize !== undefined) req.batchSize = opts.batchSize;
    return this.call("SetGroupConfig", req);
  }

  getGroupConfig(lane: string, groupId: string): Promise<GroupConfig__Output> {
    return this.call("GetGroupConfig", { lane, groupId });
  }

  // ── group lifecycle ──────────────────────────────────────────────────────────

  pauseGroup(lane: string, groupId: string): Promise<GroupConfig__Output> {
    return this.call("PauseGroup", { lane, groupId });
  }

  resumeGroup(lane: string, groupId: string): Promise<GroupConfig__Output> {
    return this.call("ResumeGroup", { lane, groupId });
  }

  /** Drop READY/DELAYED messages and drain in-flight for the group. */
  cancelGroup(lane: string, groupId: string): Promise<GroupOpResult__Output> {
    return this.call("CancelGroup", { lane, groupId });
  }

  /** Drop everything leasable in the group, keeping its config. */
  purgeGroup(lane: string, groupId: string): Promise<GroupOpResult__Output> {
    return this.call("PurgeGroup", { lane, groupId });
  }

  /** Idle-reap a group (asserts it is empty). */
  reapGroup(lane: string, groupId: string): Promise<GroupOpResult__Output> {
    return this.call("ReapGroup", { lane, groupId });
  }

  /** Tear a group down across ALL lanes in one call. */
  teardownGroup(groupId: string): Promise<TeardownResult__Output> {
    return this.call("TeardownGroup", { groupId });
  }

  // ── lane config + back-pressure ──────────────────────────────────────────────

  /** Set a lane's dequeue rate limit. ``ratePerSec`` <= 0 means unlimited. */
  setLaneConfig(
    lane: string,
    opts: { ratePerSec?: number; burst?: number } = {},
  ): Promise<LaneConfig__Output> {
    return this.call("SetLaneConfig", {
      lane,
      ratePerSec: opts.ratePerSec ?? 0,
      burst: opts.burst ?? 0,
    });
  }

  /** Stop leasing a lane. ``duration`` in seconds; 0 = until {@link resumeLane}. */
  pauseLane(lane: string, duration = 0): Promise<LaneOpResult__Output> {
    const req: Record<string, unknown> = { lane };
    if (duration) req.duration = toDuration(duration);
    return this.call("PauseLane", req);
  }

  resumeLane(lane: string): Promise<LaneOpResult__Output> {
    return this.call("ResumeLane", { lane });
  }

  // ── policy ───────────────────────────────────────────────────────────────────

  /**
   * Install (hot-reload) a scheduling policy on a lane. ``kind``/``mode`` are
   * values from the {@link PolicyKind} / {@link PolicyMode} enums; ``engine`` is
   * one of ``builtin|cel|wasm|starlark``; ``code`` is the policy source (empty
   * for pure built-ins).
   */
  setPolicy(lane: string, spec: PolicySpec = {}): Promise<PolicyInfo__Output> {
    return this.call("SetPolicy", { lane, source: Control.buildSource(spec, PolicyKind.DRR) });
  }

  getPolicy(lane: string): Promise<PolicyInfo__Output> {
    return this.call("GetPolicy", { lane });
  }

  /** Compile + dry-run a policy without installing it. */
  validatePolicy(lane: string, spec: PolicySpec = {}): Promise<ValidatePolicyResult__Output> {
    return this.call("ValidatePolicy", {
      lane,
      source: Control.buildSource(spec, PolicyKind.CUSTOM),
    });
  }

  private static buildSource(spec: PolicySpec, defaultKind: number): Record<string, unknown> {
    const source: Record<string, unknown> = {
      kind: spec.kind ?? defaultKind,
      mode: spec.mode ?? PolicyMode.POLICY_MODE_UNSPECIFIED,
      engine: spec.engine ?? "builtin",
      code: spec.code ?? Buffer.alloc(0),
    };
    if (spec.params) source.params = { ...spec.params };
    return source;
  }

  // ── cron ─────────────────────────────────────────────────────────────────────

  /**
   * Schedule a recurring publish (idempotent on ``cronId``). ``schedule`` is a
   * UTC crontab expression. ``startAt`` / ``endAt`` are optional unix epoch
   * (seconds) bounds.
   */
  scheduleCron(
    cronId: string,
    lane: string,
    groupId: string,
    schedule: string,
    opts: {
      payload?: Uint8Array;
      headers?: Record<string, string>;
      timezone?: string;
      misfire?: number;
      coalesce?: boolean;
      startAt?: number;
      endAt?: number;
    } = {},
  ): Promise<CronInfo__Output> {
    const req: Record<string, unknown> = {
      cronId,
      lane,
      groupId,
      schedule,
      payload: opts.payload ?? Buffer.alloc(0),
      timezone: opts.timezone ?? "",
      misfire: opts.misfire ?? MisfirePolicy.MISFIRE_POLICY_UNSPECIFIED,
      coalesce: opts.coalesce ?? false,
    };
    if (opts.headers) req.headers = { ...opts.headers };
    if (opts.startAt !== undefined) req.startAt = toTimestamp(opts.startAt);
    if (opts.endAt !== undefined) req.endAt = toTimestamp(opts.endAt);
    return this.call("ScheduleCron", req);
  }

  listCron(lane = "", nextN = 0): Promise<ListCronResponse__Output> {
    return this.call("ListCron", { lane, nextN });
  }

  deleteCron(cronId: string): Promise<CronOpResult__Output> {
    return this.call("DeleteCron", { cronId });
  }

  pauseCron(cronId: string): Promise<CronInfo__Output> {
    return this.call("PauseCron", { cronId });
  }

  // ── singleton leases ─────────────────────────────────────────────────────────

  /** Acquire a cluster-wide singleton lease. ``ttl`` is in seconds. */
  acquireSingleton(name: string, holder: string, ttl: number): Promise<SingletonLease__Output> {
    return this.call("AcquireSingletonLease", { name, holder, ttl: toDuration(ttl) });
  }

  /** Renew a held singleton lease using its fencing token. */
  renewSingleton(
    name: string,
    holder: string,
    fence: number,
    ttl: number,
  ): Promise<SingletonLease__Output> {
    return this.call("RenewSingletonLease", { name, holder, fence, ttl: toDuration(ttl) });
  }

  releaseSingleton(name: string, holder: string, fence: number): Promise<SingletonOpResult__Output> {
    return this.call("ReleaseSingletonLease", { name, holder, fence });
  }

  // ── async completion ─────────────────────────────────────────────────────────

  /**
   * Resolve a complete-by-token message off-stream by its token. ``success:
   * false`` records a failure. An unknown token is benign (``unknownToken:
   * true``) so at-least-once callbacks can fire more than once safely.
   */
  completeByToken(
    externalToken: Uint8Array,
    opts: { success?: boolean; resultMeta?: Record<string, string>; delay?: number } = {},
  ): Promise<CompleteResult__Output> {
    const req: Record<string, unknown> = {
      externalToken,
      outcome: opts.success === false ? Outcome.FAILURE : Outcome.SUCCESS,
    };
    if (opts.resultMeta) req.resultMeta = { ...opts.resultMeta };
    if (opts.delay !== undefined) req.delay = toDuration(opts.delay);
    return this.call("CompleteByToken", req);
  }

  // ── introspection ────────────────────────────────────────────────────────────

  /** Fetch per-lane stats, optionally filtered by lane and/or group. */
  getStats(lane = "", groupId = ""): Promise<StatsResponse__Output> {
    return this.call("GetStats", { lane, groupId });
  }

  describeCluster(): Promise<ClusterInfo__Output> {
    return this.call("DescribeCluster", {});
  }

  /** Liveness/leadership probe: serving / hasQuorum / isLeader. */
  health(): Promise<HealthResponse__Output> {
    return this.call("Health", {});
  }

  close(): void {
    this.client.close();
  }
}
