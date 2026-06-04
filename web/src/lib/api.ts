// Typed client for the Rota control-plane JSON gateway.
//
// The Go binary marshals proto responses with protojson, so wire field names
// are camelCase and proto uint64 fields arrive as decimal STRINGS (protojson
// encodes 64-bit ints as strings to avoid JS precision loss). Helpers below
// normalise those into numbers/bigints at the edge so views stay clean.
//
// Same-origin in production; a Vite proxy forwards /api -> :7101 in dev.

export class ApiError extends Error {
  status: number;
  body: string;
  constructor(status: number, body: string, message?: string) {
    super(message ?? `HTTP ${status}: ${body || 'request failed'}`);
    this.name = 'ApiError';
    this.status = status;
    this.body = body;
  }
}

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    ...init,
    headers: {
      Accept: 'application/json',
      ...(init?.body ? { 'Content-Type': 'application/json' } : {}),
      ...init?.headers
    }
  });
  const text = await res.text();
  if (!res.ok) {
    throw new ApiError(res.status, text);
  }
  if (!text) return undefined as unknown as T;
  try {
    return JSON.parse(text) as T;
  } catch {
    throw new ApiError(res.status, text, 'malformed JSON response');
  }
}

// protojson encodes uint64/int64 as strings; coerce defensively (some encoders
// emit plain numbers). Returns a JS number — fine for display/count magnitudes.
export function num(v: string | number | null | undefined): number {
  if (v === null || v === undefined || v === '') return 0;
  return typeof v === 'number' ? v : Number(v);
}

// ── Wire shapes (camelCase, post-protojson) ──────────────────────────────────

export interface PeerInfo {
  id?: string;
  addr?: string;
  suffrage?: string; // "Voter" | "Nonvoter"
}

export interface ClusterInfo {
  leaderId?: string;
  leaderAddr?: string;
  term?: string | number;
  appliedIndex?: string | number;
  peers?: PeerInfo[];
}

export interface HealthResponse {
  serving?: boolean;
  hasQuorum?: boolean;
  isLeader?: boolean;
}

export interface LaneStats {
  lane?: string;
  leasable?: string | number;
  delayed?: string | number;
  inflight?: string | number;
  dlqDepth?: string | number;
  publishRate?: number;
  leaseRate?: number;
  ackRate?: number;
  oldestAgeMs?: string | number;
  groupCount?: string | number;
  policyVersion?: string | number;
}

export interface StatsResponse {
  lanes?: LaneStats[];
}

export interface GroupStats {
  lane?: string;
  groupId?: string;
  weight?: number;
  paused?: boolean;
  ready?: string | number;
  delayed?: string | number;
  inflight?: string | number;
  total?: string | number;
  virtualTime?: number;
  deficit?: number;
  lastActivityMs?: string | number;
}

export interface ListGroupsResponse {
  groups?: GroupStats[];
  nextPageToken?: string;
}

export interface DeadLetterInfo {
  lane?: string;
  groupId?: string;
  msgId?: string | number;
  finalAttempt?: number;
  reason?: string;
  deadAtMs?: string | number;
  failureHeaders?: Record<string, string>;
  headers?: Record<string, string>;
  payload?: string; // base64 (protojson encodes bytes as base64)
}

export interface ListDeadLettersResponse {
  deadLetters?: DeadLetterInfo[];
  nextPageToken?: string;
}

// Leases (ListLeases) — mirrors the proto Lease value type. Field names follow
// the same protojson camelCase convention.
export interface LeaseInfo {
  leaseId?: string | number;
  msgId?: string | number;
  lane?: string;
  groupId?: string;
  consumerId?: string;
  deadlineMs?: string | number;
  attemptAtLease?: number;
  attempt?: number; // tolerate either spelling
  epoch?: number;
  extendCount?: number;
  grantedMs?: string | number;
}

export interface ListLeasesResponse {
  leases?: LeaseInfo[];
  nextPageToken?: string;
}

export interface PeekedMessage {
  msgId?: string | number;
  lane?: string;
  groupId?: string;
  state?: string;
  attempt?: number;
  notBeforeMs?: string | number;
  enqueueMs?: string | number;
  headers?: Record<string, string>;
  payload?: string;
}

export interface PeekMessagesResponse {
  messages?: PeekedMessage[];
}

export interface RedriveResult {
  // RedriveDeadLetter response — tolerant of either an ok flag or a count.
  ok?: boolean;
  messageId?: string | number;
  redriven?: string | number;
}

// ── Fairness Observatory ─────────────────────────────────────────────────────

// Control.GetLaneFairness — per-group fairness snapshot. expectedShare/actualShare
// are fractions in [0,1]; starvationScore is normalised to [0,1] across the lane.
export interface FairnessGroup {
  groupId?: string;
  weight?: number;
  expectedShare?: number;
  actualShare?: number;
  virtualTime?: number;
  deficit?: number;
  served?: string | number;
  starvationScore?: number;
  paused?: boolean;
}

export interface LaneFairness {
  groups?: FairnessGroup[];
  totalServed?: string | number;
}

// Control.GetPolicyHealth — quarantine + fault surface for a lane's policy.
export interface PolicyHealth {
  lane?: string;
  version?: string | number;
  quarantined?: boolean;
  engine?: string; // "cel" | "wasm" | ...
  faults?: string[];
}

// Dry-run validation result. The gateway may not implement this yet; callers
// tolerate a 404/501 and fall back to the policy-health signal.
export interface PolicyValidation {
  ok?: boolean;
  valid?: boolean;
  engine?: string;
  errors?: string[];
  diagnostics?: string[];
  message?: string;
}

// ── Endpoints ────────────────────────────────────────────────────────────────

function qs(params: Record<string, string | number | undefined>): string {
  const u = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== '' && v !== null) u.set(k, String(v));
  }
  const s = u.toString();
  return s ? `?${s}` : '';
}

const enc = encodeURIComponent;

export const api = {
  cluster: () => req<ClusterInfo>('/api/cluster'),

  health: () => req<HealthResponse>('/api/health'),

  stats: () => req<StatsResponse>('/api/stats'),

  groups: (lane: string, opts: { pageToken?: string; pageSize?: number } = {}) =>
    req<ListGroupsResponse>(
      `/api/lanes/${enc(lane)}/groups${qs({ page_token: opts.pageToken, page_size: opts.pageSize })}`
    ),

  dlq: (lane: string, opts: { pageToken?: string; pageSize?: number } = {}) =>
    req<ListDeadLettersResponse>(
      `/api/lanes/${enc(lane)}/dlq${qs({ page_token: opts.pageToken, page_size: opts.pageSize })}`
    ),

  leases: (lane: string, opts: { pageToken?: string; pageSize?: number } = {}) =>
    req<ListLeasesResponse>(
      `/api/lanes/${enc(lane)}/leases${qs({ page_token: opts.pageToken, page_size: opts.pageSize })}`
    ),

  peekMessages: (lane: string, group: string, limit = 20) =>
    req<PeekMessagesResponse>(
      `/api/lanes/${enc(lane)}/groups/${enc(group)}/messages${qs({ limit })}`
    ),

  redrive: (lane: string, groupId: string, msgId: string | number) =>
    req<RedriveResult>(`/api/lanes/${enc(lane)}/dlq/redrive`, {
      method: 'POST',
      body: JSON.stringify({ group_id: groupId, msg_id: String(msgId) })
    }),

  // ── Fairness Observatory ──
  fairness: (lane: string) => req<LaneFairness>(`/api/lanes/${enc(lane)}/fairness`),

  // EventSource URL for the live interleave ribbon (event: served).
  fairnessStreamUrl: (lane: string) => `/api/lanes/${enc(lane)}/fairness/stream`,

  policyHealth: (lane: string) => req<PolicyHealth>(`/api/policy/${enc(lane)}/health`),

  // Dry-run a candidate policy. The gateway route may not exist yet; callers
  // catch ApiError(404/405/501) and degrade to a TODO note + health check.
  validatePolicy: (lane: string, source: string, engine = 'cel') =>
    req<PolicyValidation>(`/api/lanes/${enc(lane)}/policy/validate`, {
      method: 'POST',
      body: JSON.stringify({ source, engine })
    })
};
