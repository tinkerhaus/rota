/**
 * Internal helpers shared by the Publisher, Worker, Control, and Workflow
 * clients.
 *
 * Domain-neutral plumbing only: proto loading, target parsing, leader following
 * (NOT_LEADER -> retry against the advertised leader), and conversion helpers
 * for the protobuf well-known Duration / Timestamp types.
 */

import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";

import type { ProtoGrpcType } from "./generated/rota.js";
import type { Duration } from "./generated/google/protobuf/Duration.js";
import type { Timestamp } from "./generated/google/protobuf/Timestamp.js";

/** A target spec: a single ``host:port``, a comma-separated list, or an array. */
export type Targets = string | string[];

/** A service-client constructor as produced by ``loadPackageDefinition``. */
export type ServiceCtor<T extends grpc.Client> = new (
  address: string,
  credentials: grpc.ChannelCredentials,
  options?: Partial<grpc.ClientOptions>,
) => T;

/** Per-call metadata accepted by SDK clients. */
export type MetadataInit = grpc.Metadata | Record<string, string> | Array<[string, string]>;

// ── proto loading ────────────────────────────────────────────────────────────

let cachedProto: ProtoGrpcType | undefined;

/**
 * Load (once, cached) the Rota proto and return the typed package root. The
 * ``.proto`` is shipped inside the package alongside the compiled output; it is
 * resolved relative to this module so it works from both ``src`` and ``dist``.
 */
export function loadProto(): ProtoGrpcType {
  if (cachedProto) return cachedProto;
  const protoPath = fileURLToPath(
    new URL("../proto/rota/v1/rota.proto", import.meta.url),
  );
  const def = protoLoader.loadSync(protoPath, {
    // keepCase:false => idiomatic camelCase fields (groupId, runId, ...).
    keepCase: false,
    // numbers (not Long objects / strings) for int64/uint64 — ergonomic, and
    // the determinism checksum re-widens via BigInt so it stays exact.
    longs: Number,
    // numeric enum values, matching the generated `--enums=Number` types.
    bytes: Buffer,
    defaults: true,
    oneofs: true,
  });
  cachedProto = grpc.loadPackageDefinition(def) as unknown as ProtoGrpcType;
  return cachedProto;
}

// ── target parsing ───────────────────────────────────────────────────────────

/** Coerce a target spec into a list of ``host:port`` strings. */
export function normalizeTargets(targets: Targets): string[] {
  const list = Array.isArray(targets) ? targets : targets.split(",");
  const out = list.map((t) => String(t).trim()).filter((t) => t.length > 0);
  if (out.length === 0) {
    throw new Error("at least one target address is required");
  }
  return out;
}

// ── well-known type conversions ──────────────────────────────────────────────

/** Convert a float number of seconds to a protobuf Duration. */
export function toDuration(seconds: number): Duration {
  const whole = Math.trunc(seconds);
  const nanos = Math.round((seconds - whole) * 1e9);
  return { seconds: whole, nanos };
}

/** Convert a unix epoch (float seconds) to a protobuf Timestamp. */
export function toTimestamp(epochSeconds: number): Timestamp {
  const whole = Math.trunc(epochSeconds);
  const nanos = Math.round((epochSeconds - whole) * 1e9);
  return { seconds: whole, nanos };
}

/** Build call metadata, including Rota's static bearer token when set. */
export function buildMetadata(metadata?: MetadataInit, authToken?: string): grpc.Metadata {
  let md: grpc.Metadata;
  if (metadata instanceof grpc.Metadata) {
    md = metadata.clone();
  } else {
    md = new grpc.Metadata();
    if (Array.isArray(metadata)) {
      for (const [key, value] of metadata) md.add(key, value);
    } else if (metadata) {
      for (const [key, value] of Object.entries(metadata)) md.set(key, value);
    }
  }
  if (authToken) {
    md.set("authorization", `Bearer ${authToken}`);
    md.set("x-rota-token", authToken);
  }
  return md;
}

// ── leader following ─────────────────────────────────────────────────────────

/**
 * Minimal protobuf decode of a ``NotLeader`` message, pulling out field 1
 * (``leader_addr``, a length-delimited string). Avoids a runtime protobuf
 * decoder dependency for the one message we must read off trailing metadata.
 */
function decodeNotLeaderAddr(buf: Buffer): string {
  let i = 0;
  const readVarint = (): number => {
    let value = 0;
    let shift = 0;
    while (i < buf.length) {
      const b = buf[i++];
      value += (b & 0x7f) * 2 ** shift;
      if ((b & 0x80) === 0) break;
      shift += 7;
    }
    return value;
  };
  while (i < buf.length) {
    const tag = readVarint();
    const field = tag >>> 3;
    const wire = tag & 0x7;
    if (wire === 2) {
      const len = readVarint();
      const bytes = buf.subarray(i, i + len);
      i += len;
      if (field === 1) return bytes.toString("utf8");
    } else if (wire === 0) {
      readVarint();
    } else if (wire === 5) {
      i += 4;
    } else if (wire === 1) {
      i += 8;
    } else {
      break;
    }
  }
  return "";
}

/**
 * If ``err`` is a NOT_LEADER fault, return the advertised leader address (which
 * may be ``""`` if none was supplied). Returns ``null`` when this is not a
 * NOT_LEADER error.
 *
 * Rota signals leadership redirection with FAILED_PRECONDITION. The leader
 * address rides in a binary ``NotLeader`` detail (trailing metadata key
 * ``not-leader-bin``) or, failing that, is parsed out of the details string
 * (``"not_leader: host:port"``).
 */
export function extractNotLeader(err: grpc.ServiceError): string | null {
  if (err.code !== grpc.status.FAILED_PRECONDITION) return null;

  try {
    const md = err.metadata;
    if (md) {
      for (const key of ["not-leader-bin", "rota-not-leader-bin"]) {
        const values = md.get(key);
        if (values && values.length > 0) {
          const v = values[0];
          const buf = Buffer.isBuffer(v) ? v : Buffer.from(String(v));
          const addr = decodeNotLeaderAddr(buf);
          if (addr) return addr;
        }
      }
    }
  } catch {
    // defensive: fall through to the string parse
  }

  const details = err.details ?? "";
  const low = details.toLowerCase();
  if (low.includes("not_leader") || low.includes("not leader")) {
    for (const token of details.replace(/,/g, " ").split(/\s+/)) {
      if (token.includes(":") && !token.endsWith(":")) {
        const [host, port] = token.split(":");
        if (host && /^\d+$/.test(port)) return token;
      }
    }
    return ""; // NOT_LEADER but no usable address
  }
  return null;
}

export interface LeaderClientOptions {
  channelOptions?: Partial<grpc.ClientOptions>;
  maxRetries?: number;
  baseBackoff?: number;
  maxBackoff?: number;
  credentials?: grpc.ChannelCredentials;
  metadata?: MetadataInit;
  authToken?: string;
}

/**
 * A lazy gRPC client that follows the cluster leader on NOT_LEADER.
 *
 * Holds an ordered list of candidate targets, lazily dials the first, and
 * reuses one client. On a NOT_LEADER fault it re-dials the advertised leader
 * (promoting it to the head of the candidate list) and retries the call with
 * bounded exponential backoff + jitter. Transient transport failures rotate to
 * the next candidate, which is what makes dead-leader failover work.
 */
export class LeaderClient<T extends grpc.Client> {
  private readonly ctor: ServiceCtor<T>;
  private readonly channelOptions: Partial<grpc.ClientOptions>;
  private readonly maxRetries: number;
  private readonly baseBackoff: number;
  private readonly maxBackoff: number;
  private readonly credentials: grpc.ChannelCredentials;
  private readonly metadata: grpc.Metadata;
  private candidates: string[];
  private client: T | undefined;

  constructor(targets: Targets, ctor: ServiceCtor<T>, opts: LeaderClientOptions = {}) {
    this.ctor = ctor;
    this.channelOptions = opts.channelOptions ?? {};
    this.maxRetries = opts.maxRetries ?? 5;
    this.baseBackoff = opts.baseBackoff ?? 0.1;
    this.maxBackoff = opts.maxBackoff ?? 5.0;
    this.credentials = opts.credentials ?? grpc.credentials.createInsecure();
    this.metadata = buildMetadata(opts.metadata, opts.authToken);
    this.candidates = normalizeTargets(targets);
  }

  private dial(addr: string): T {
    return new this.ctor(addr, this.credentials, this.channelOptions);
  }

  private getClient(): T {
    if (!this.client) {
      this.client = this.dial(this.candidates[0]);
    }
    return this.client;
  }

  private switchTo(leaderAddr: string): void {
    if (leaderAddr && !this.candidates.includes(leaderAddr)) {
      this.candidates.unshift(leaderAddr);
    }
    this.close();
    this.client = this.dial(leaderAddr || this.candidates[0]);
  }

  private rotate(): void {
    if (this.candidates.length > 1) {
      this.candidates.push(this.candidates.shift() as string);
    }
    this.close();
    this.client = this.dial(this.candidates[0]);
  }

  private async backoff(attempt: number): Promise<void> {
    const delay = Math.min(this.maxBackoff, this.baseBackoff * 2 ** attempt);
    const jittered = delay * (0.5 + Math.random() * 0.5);
    await new Promise((resolve) => setTimeout(resolve, jittered * 1000));
  }

  private invoke<Res>(
    client: T,
    method: string,
    request: unknown,
    timeout: number | null | undefined,
  ): Promise<Res> {
    return new Promise<Res>((resolve, reject) => {
      const options: grpc.CallOptions = {};
      if (timeout != null) options.deadline = Date.now() + timeout * 1000;
      const metadata = this.metadata.clone();
      const fn = (client as unknown as Record<string, unknown>)[method];
      if (typeof fn !== "function") {
        reject(new Error(`unknown method: ${method}`));
        return;
      }
      (fn as (...a: unknown[]) => void).call(
        client,
        request,
        metadata,
        options,
        (err: grpc.ServiceError | null, resp: Res) => {
          if (err) reject(err);
          else resolve(resp);
        },
      );
    });
  }

  /** Invoke a unary method, following the leader on NOT_LEADER. */
  async call<Res>(
    method: string,
    request: unknown,
    opts: { timeout?: number | null } = {},
  ): Promise<Res> {
    let lastErr: unknown;
    for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
      try {
        return await this.invoke<Res>(this.getClient(), method, request, opts.timeout);
      } catch (err) {
        lastErr = err;
        const e = err as grpc.ServiceError;
        const leader = extractNotLeader(e);
        if (leader) {
          this.switchTo(leader);
        } else if (
          leader === "" ||
          e.code === grpc.status.UNAVAILABLE ||
          e.code === grpc.status.DEADLINE_EXCEEDED
        ) {
          this.rotate();
        } else {
          throw e; // genuine application error — do not retry
        }
        if (attempt < this.maxRetries) {
          await this.backoff(attempt);
          continue;
        }
        throw e;
      }
    }
    throw lastErr ?? new Error("call exhausted retries without an error");
  }

  close(): void {
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
