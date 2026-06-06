/**
 * Publisher: submit messages to a Rota lane/group and complete by token.
 *
 * Domain-neutral. A message is opaque ``payload`` bytes plus a ``headers`` map,
 * addressed to a (``lane``, ``groupId``) pair. Eligibility can be deferred via a
 * relative ``delay`` (seconds) or an absolute ``notBefore`` (unix epoch seconds).
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
import type { BrokerClient } from "./generated/rota/v1/Broker.js";
import type { ControlClient } from "./generated/rota/v1/Control.js";
import type { MessageSpec } from "./generated/rota/v1/MessageSpec.js";
import type { PublishItemResult__Output } from "./generated/rota/v1/PublishItemResult.js";
import type { CompleteResult__Output } from "./generated/rota/v1/CompleteResult.js";

/** Optional per-message knobs accepted by {@link Publisher.publish}. */
export interface PublishOptions {
  /** App metadata; surfaced on delivery and carried into the DLQ. */
  headers?: Record<string, string>;
  /** Relative eligibility delay in seconds. Mutually exclusive with ``notBefore``. */
  delay?: number;
  /** Absolute eligibility instant as a unix epoch (seconds). Mutually exclusive with ``delay``. */
  notBefore?: number;
  /** Upsert hint for the group's scheduling weight. */
  weight?: number;
  /** Upsert hint for the group's per-turn batch size. */
  batchSize?: number;
  /** 0 (or unset) = lane default. */
  maxAttempts?: number;
  /** Time-to-live in seconds; auto-drop / DLQ if undelivered by then. */
  ttl?: number;
  /** Producer-side idempotency window. */
  dedupKey?: string;
  /** Mint a completion token at lease time. */
  issueToken?: boolean;
  /** Supply your own external completion token. */
  externalToken?: Uint8Array;
}

/** One item for {@link Publisher.publishBatch}. */
export type BatchItem = {
  lane: string;
  groupId: string;
  payload?: Uint8Array | string;
} & PublishOptions;

export interface PublisherOptions extends LeaderClientOptions {
  /** Per-call deadline in seconds (default 30). Pass ``null`` for no deadline. */
  timeout?: number | null;
}

/**
 * Publishes messages to a Rota broker.
 *
 * The gRPC client is dialed lazily on first use. On a NOT_LEADER fault the
 * publisher transparently re-dials the advertised leader and retries with
 * bounded exponential backoff.
 */
export class Publisher {
  private readonly timeout: number | null;
  private readonly client: LeaderClient<BrokerClient>;
  private readonly targets: Targets;
  private readonly leaderOpts: LeaderClientOptions;
  private control: LeaderClient<ControlClient> | undefined;

  constructor(targets: Targets = "127.0.0.1:7100", opts: PublisherOptions = {}) {
    const { timeout, ...leaderOpts } = opts;
    this.timeout = timeout === undefined ? 30.0 : timeout;
    this.targets = targets;
    this.leaderOpts = leaderOpts;
    const proto = loadProto();
    this.client = new LeaderClient(targets, proto.rota.v1.Broker, leaderOpts);
  }

  private static buildSpec(
    lane: string,
    groupId: string,
    payload: Uint8Array | string,
    opts: PublishOptions = {},
  ): MessageSpec {
    if (opts.delay !== undefined && opts.notBefore !== undefined) {
      throw new Error("set at most one of `delay` or `notBefore`");
    }
    const spec: MessageSpec = {
      lane,
      groupId,
      payload: payload ?? Buffer.alloc(0),
    };
    if (opts.headers) spec.headers = { ...opts.headers };
    if (opts.delay !== undefined) spec.delay = toDuration(opts.delay);
    if (opts.notBefore !== undefined) spec.at = toTimestamp(opts.notBefore);
    if (opts.maxAttempts !== undefined) spec.maxAttempts = opts.maxAttempts;
    if (opts.ttl !== undefined) spec.ttl = toDuration(opts.ttl);
    if (opts.weight !== undefined) spec.weight = opts.weight;
    if (opts.batchSize !== undefined) spec.batchSize = opts.batchSize;
    if (opts.dedupKey !== undefined) spec.dedupKey = opts.dedupKey;
    if (opts.issueToken) spec.issueToken = true;
    if (opts.externalToken !== undefined) spec.externalToken = opts.externalToken;
    return spec;
  }

  /**
   * Publish one message. Resolves to the broker-assigned ``messageId``.
   */
  async publish(
    lane: string,
    groupId: string,
    payload: Uint8Array | string,
    opts: PublishOptions = {},
  ): Promise<number> {
    const spec = Publisher.buildSpec(lane, groupId, payload, opts);
    const resp = await this.client.call<{ messageId: number }>(
      "Publish",
      { message: spec },
      { timeout: this.timeout },
    );
    return resp.messageId;
  }

  /**
   * Publish many messages in one call. Each item carries ``lane``, ``groupId``,
   * an optional ``payload``, and the same options as {@link publish}. With
   * ``atomic: true`` the broker applies them as a single all-or-nothing write.
   * Resolves to the per-item results.
   */
  async publishBatch(
    messages: BatchItem[],
    opts: { atomic?: boolean } = {},
  ): Promise<PublishItemResult__Output[]> {
    const specs: MessageSpec[] = messages.map(({ lane, groupId, payload, ...rest }) =>
      Publisher.buildSpec(lane, groupId, payload ?? Buffer.alloc(0), rest),
    );
    const resp = await this.client.call<{ results: PublishItemResult__Output[] }>(
      "PublishBatch",
      { messages: specs, atomic: opts.atomic ?? false },
      { timeout: this.timeout },
    );
    return resp.results ?? [];
  }

  /**
   * Complete a message off-stream by its external completion token.
   *
   * Twin of the in-stream Complete frame: lets a process that did not hold the
   * lease (e.g. an async callback) resolve a message keyed by token alone.
   */
  async complete(
    externalToken: Uint8Array,
    opts: { success?: boolean; resultMeta?: Record<string, string>; delay?: number } = {},
  ): Promise<CompleteResult__Output> {
    const req: Record<string, unknown> = {
      externalToken,
      outcome: opts.success === false ? Outcome.FAILURE : Outcome.SUCCESS,
    };
    if (opts.resultMeta) req.resultMeta = { ...opts.resultMeta };
    if (opts.delay !== undefined) req.delay = toDuration(opts.delay);
    if (!this.control) {
      const proto = loadProto();
      this.control = new LeaderClient(this.targets, proto.rota.v1.Control, this.leaderOpts);
    }
    return this.control.call<CompleteResult__Output>("CompleteByToken", req, {
      timeout: this.timeout,
    });
  }

  close(): void {
    this.client.close();
    this.control?.close();
  }
}
