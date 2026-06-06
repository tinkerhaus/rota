/**
 * Worker: lease messages off a Rota lane and dispatch them to a handler.
 *
 * The worker opens the bidirectional ``Work`` stream, advertises demand
 * (``credit``), and dispatches each delivered lease to ``handler(message)``.
 * Leases are processed serially (one handler at a time); ``credit`` is the
 * server-side in-flight budget. With ``credit: 1`` (default) the broker grants
 * at most one outstanding lease.
 *
 * Handler outcome mapping:
 *
 * - returns / resolves normally  -> Ack
 * - throws {@link Requeue}       -> Nack(REQUEUE_NO_PENALTY, delay)
 * - throws {@link DeadLetter}    -> Nack(DEAD_LETTER, failureMeta)
 * - throws anything else         -> Nack(RETRY)
 *
 * ``run()`` resolves once a graceful drain or {@link Worker.stop} completes. It
 * optionally installs SIGTERM/SIGINT handlers for graceful drain: stop
 * requesting new credit, let the in-flight message finish, then close. On a
 * disconnect (or a NOT_LEADER redirect) it reconnects against the leader with
 * bounded backoff and resumes.
 */

import { hostname } from "node:os";
import * as grpc from "@grpc/grpc-js";

import {
  extractNotLeader,
  loadProto,
  normalizeTargets,
  toDuration,
  type Targets,
} from "./common.js";
import { NackMode } from "./generated/rota/v1/NackMode.js";
import { Outcome } from "./generated/rota/v1/Outcome.js";
import { ErrorCode } from "./generated/rota/v1/ErrorCode.js";
import { DeadLetter, Requeue } from "./errors.js";
import type { BrokerClient } from "./generated/rota/v1/Broker.js";
import type { WorkClientMsg } from "./generated/rota/v1/WorkClientMsg.js";
import type { WorkServerMsg__Output } from "./generated/rota/v1/WorkServerMsg.js";
import type { LeasedMessage__Output } from "./generated/rota/v1/LeasedMessage.js";
import type { Timestamp__Output } from "./generated/google/protobuf/Timestamp.js";

type WorkStream = grpc.ClientDuplexStream<WorkClientMsg, WorkServerMsg__Output>;

/** Called for each leased message. May be sync or async. */
export type MessageHandler = (message: Message) => void | Promise<void>;

export interface WorkerOptions {
  /** In-flight budget advertised to the broker. ``1`` (default) = strictly serial. */
  credit?: number;
  /** Stable consumer identity (default: ``<hostname>-<random>``). */
  consumerId?: string;
  /** Optional group filters. */
  groupAllow?: string[];
  groupDeny?: string[];
  /** Install SIGTERM/SIGINT graceful-drain handlers (default true). */
  installSignalHandler?: boolean;
  reconnectBaseBackoff?: number;
  reconnectMaxBackoff?: number;
  channelOptions?: Partial<grpc.ClientOptions>;
  credentials?: grpc.ChannelCredentials;
}

/** A leased message handed to the handler. */
export class Message {
  readonly leaseId: number;
  readonly messageId: number;
  readonly lane: string;
  readonly groupId: string;
  readonly payload: Buffer;
  readonly headers: Record<string, string>;
  readonly attempt: number;
  readonly externalToken: Buffer;
  readonly visibilityDeadline: Timestamp__Output | null;
  private readonly stream: WorkStream;

  constructor(stream: WorkStream, leased: LeasedMessage__Output) {
    this.stream = stream;
    this.leaseId = leased.leaseId;
    this.messageId = leased.messageId;
    this.lane = leased.lane;
    this.groupId = leased.groupId;
    this.payload = leased.payload;
    this.headers = { ...leased.headers };
    this.attempt = leased.attempt;
    this.externalToken = leased.externalToken;
    this.visibilityDeadline = leased.visibilityDeadline ?? null;
  }

  /** Extend this lease's visibility deadline by ``ttl`` seconds. */
  extend(ttl: number): void {
    this.stream.write({ extend: { leaseId: this.leaseId, ttl: toDuration(ttl) } });
  }

  /** Send an in-stream Complete frame for this message's token. */
  complete(
    opts: {
      externalToken?: Uint8Array;
      success?: boolean;
      resultMeta?: Record<string, string>;
      delay?: number;
    } = {},
  ): void {
    const frame: Record<string, unknown> = {
      externalToken: opts.externalToken ?? this.externalToken,
      outcome: opts.success === false ? Outcome.FAILURE : Outcome.SUCCESS,
    };
    if (opts.resultMeta) frame.resultMeta = { ...opts.resultMeta };
    if (opts.delay !== undefined) frame.delay = toDuration(opts.delay);
    this.stream.write({ complete: frame });
  }
}

interface SessionResult {
  /** Non-null (possibly "") => redirect to this leader; null => plain disconnect. */
  redirect: string | null;
  /** Whether at least one lease was handled this session. */
  progressed: boolean;
}

/** Leases and processes messages from one lane via the Work stream. */
export class Worker {
  private readonly lane: string;
  private readonly handler: MessageHandler;
  private readonly credit: number;
  private readonly consumerId: string;
  private readonly groupAllow: string[];
  private readonly groupDeny: string[];
  private readonly installSignalHandler: boolean;
  private readonly reconnectBase: number;
  private readonly reconnectMax: number;
  private readonly channelOptions: Partial<grpc.ClientOptions>;
  private readonly credentials: grpc.ChannelCredentials;

  private candidates: string[];
  private client: BrokerClient | undefined;
  private stream: WorkStream | undefined;
  private draining = false;
  private stopped = false;

  constructor(targets: Targets, lane: string, handler: MessageHandler, opts: WorkerOptions = {}) {
    const credit = opts.credit ?? 1;
    if (credit < 1) throw new Error("credit must be >= 1");
    this.lane = lane;
    this.handler = handler;
    this.credit = credit;
    this.consumerId =
      opts.consumerId ?? `${hostname()}-${Math.random().toString(16).slice(2, 10)}`;
    this.groupAllow = opts.groupAllow ?? [];
    this.groupDeny = opts.groupDeny ?? [];
    this.installSignalHandler = opts.installSignalHandler ?? true;
    this.reconnectBase = opts.reconnectBaseBackoff ?? 0.2;
    this.reconnectMax = opts.reconnectMaxBackoff ?? 10.0;
    this.channelOptions = opts.channelOptions ?? {};
    this.credentials = opts.credentials ?? grpc.credentials.createInsecure();
    this.candidates = normalizeTargets(targets);
  }

  private dial(addr: string): BrokerClient {
    const proto = loadProto();
    return new proto.rota.v1.Broker(addr, this.credentials, this.channelOptions);
  }

  /** Run the handler for one lease and emit the matching ack/nack. */
  private async dispatch(stream: WorkStream, leased: LeasedMessage__Output): Promise<void> {
    const message = new Message(stream, leased);
    try {
      await this.handler(message);
    } catch (err) {
      if (err instanceof Requeue) {
        const nack: Record<string, unknown> = {
          leaseId: leased.leaseId,
          mode: NackMode.REQUEUE_NO_PENALTY,
        };
        if (err.delay !== undefined) nack.delay = toDuration(err.delay);
        stream.write({ nack });
      } else if (err instanceof DeadLetter) {
        const failureMeta = { ...err.meta };
        if (err.reason && !("reason" in failureMeta)) failureMeta.reason = err.reason;
        stream.write({ nack: { leaseId: leased.leaseId, mode: NackMode.DEAD_LETTER, failureMeta } });
      } else {
        console.error(
          `rota.worker: handler threw on lane=${leased.lane} group=${leased.groupId} ` +
            `lease=${leased.leaseId}; nacking RETRY`,
          err,
        );
        stream.write({ nack: { leaseId: leased.leaseId, mode: NackMode.RETRY } });
      }
      return;
    }
    stream.write({ ack: { leaseId: leased.leaseId } });
  }

  /** Open one Work stream and pump it until it ends. */
  private runOneSession(): Promise<SessionResult> {
    return new Promise<SessionResult>((resolve) => {
      const client = this.dial(this.candidates[0]);
      this.client = client;
      const stream = client.Work();
      this.stream = stream;

      let settled = false;
      let progressed = false;
      let chain: Promise<void> = Promise.resolve();

      const done = (redirect: string | null) => {
        if (settled) return;
        settled = true;
        resolve({ redirect, progressed });
      };

      // Advertise demand up front (unless already draining).
      if (!this.draining) {
        stream.write({
          leaseRequest: {
            lane: this.lane,
            credit: this.credit,
            consumerId: this.consumerId,
            groupAllow: this.groupAllow,
            groupDeny: this.groupDeny,
          },
        });
      }

      stream.on("data", (server: WorkServerMsg__Output) => {
        switch (server.msg) {
          case "lease": {
            const leased = server.lease!;
            progressed = true;
            // Process serially: chain handler execution so only one runs at a time.
            chain = chain
              .then(() => this.dispatch(stream, leased))
              .then(() => {
                if (this.draining) {
                  try {
                    stream.end();
                  } catch {
                    // ignore
                  }
                }
              });
            break;
          }
          case "error": {
            const e = server.error!;
            if (e.code === ErrorCode.NOT_LEADER) {
              try {
                stream.end();
              } catch {
                // ignore
              }
              done(e.leaderAddr || "");
            }
            // other stream errors are informational; the stream stays open
            break;
          }
          case "credit":
          case "control":
          default:
            break;
        }
      });

      stream.on("error", (err: grpc.ServiceError) => {
        const leader = extractNotLeader(err);
        if (leader !== null) {
          done(leader);
          return;
        }
        done(null); // transient / clean teardown — reconnect with backoff
      });

      stream.on("end", () => done(null));
    });
  }

  /** Run the lease/dispatch loop until drained or stopped. */
  async run(): Promise<void> {
    const onSignal = () => this.drain();
    if (this.installSignalHandler) {
      process.on("SIGTERM", onSignal);
      process.on("SIGINT", onSignal);
    }

    let backoffAttempt = 0;
    try {
      while (!this.stopped) {
        let result: SessionResult;
        try {
          result = await this.runOneSession();
        } catch {
          result = { redirect: null, progressed: false };
        }

        if (this.draining) break;

        if (result.redirect !== null) {
          this.promoteLeader(result.redirect);
          this.closeClient();
          backoffAttempt = 0;
          continue;
        }

        if (this.stopped) break;

        // Plain disconnect: rotate to the next candidate (a follower redirects
        // us to the leader), back off, then reconnect.
        this.closeClient();
        this.rotateCandidates();
        if (result.progressed) backoffAttempt = 0;
        await this.sleepBackoff(backoffAttempt++);
      }
    } finally {
      this.closeClient();
      if (this.installSignalHandler) {
        process.off("SIGTERM", onSignal);
        process.off("SIGINT", onSignal);
      }
    }
  }

  /** Begin a graceful drain: stop requesting credit, finish in-flight, then close. */
  drain(): void {
    this.draining = true;
    if (this.stream) {
      try {
        this.stream.end();
      } catch {
        // ignore
      }
    }
  }

  /** Stop immediately (no reconnect). In-flight may be cut off. */
  stop(): void {
    this.stopped = true;
    this.draining = true;
    if (this.stream) {
      try {
        this.stream.cancel();
      } catch {
        // ignore
      }
    }
    this.closeClient();
  }

  private promoteLeader(leaderAddr: string): void {
    if (!leaderAddr) return;
    const idx = this.candidates.indexOf(leaderAddr);
    if (idx >= 0) this.candidates.splice(idx, 1);
    this.candidates.unshift(leaderAddr);
  }

  private rotateCandidates(): void {
    if (this.candidates.length > 1) {
      this.candidates.push(this.candidates.shift() as string);
    }
  }

  private async sleepBackoff(attempt: number): Promise<void> {
    const delay = Math.min(this.reconnectMax, this.reconnectBase * 2 ** attempt);
    const jittered = delay * (0.5 + Math.random() * 0.5);
    await new Promise((resolve) => setTimeout(resolve, jittered * 1000));
  }

  private closeClient(): void {
    this.stream = undefined;
    if (this.client) {
      try {
        this.client.close();
      } catch {
        // ignore
      }
      this.client = undefined;
    }
  }
}
