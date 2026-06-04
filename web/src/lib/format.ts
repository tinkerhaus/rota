// Small formatting helpers shared across views. Operator-focused: terse,
// unambiguous, monospace-friendly.

const DASH = '–'; // en dash, used as the "no value" glyph
const ELLIPSIS = '…';
const MIDDOT = '·';

export function fmtInt(n: number | string | undefined | null): string {
  const v = typeof n === 'string' ? Number(n) : (n ?? 0);
  if (!Number.isFinite(v)) return DASH;
  return v.toLocaleString('en-US');
}

export function fmtFloat(n: number | undefined | null, dp = 2): string {
  if (n === undefined || n === null || !Number.isFinite(n)) return DASH;
  return n.toFixed(dp);
}

export function fmtRate(n: number | undefined | null): string {
  if (n === undefined || n === null || !Number.isFinite(n)) return DASH;
  if (n === 0) return '0/s';
  if (n < 1) return `${n.toFixed(2)}/s`;
  if (n < 100) return `${n.toFixed(1)}/s`;
  return `${Math.round(n).toLocaleString('en-US')}/s`;
}

// Human "5m ago" from a unix-ms timestamp (string or number). 0/empty => em dash.
export function fmtAgo(ms: number | string | undefined | null, nowMs = Date.now()): string {
  const t = typeof ms === 'string' ? Number(ms) : (ms ?? 0);
  if (!t || !Number.isFinite(t)) return '—';
  const d = nowMs - t;
  if (d < 0) return 'just now';
  return `${humanDuration(d)} ago`;
}

// Signed countdown to a future unix-ms deadline. Past => expired.
export function fmtCountdown(
  deadlineMs: number | string | undefined | null,
  nowMs = Date.now()
): { text: string; expired: boolean; urgent: boolean } {
  const t = typeof deadlineMs === 'string' ? Number(deadlineMs) : (deadlineMs ?? 0);
  if (!t || !Number.isFinite(t)) return { text: '—', expired: false, urgent: false };
  const d = t - nowMs;
  if (d <= 0) return { text: `-${humanDuration(-d)}`, expired: true, urgent: true };
  return { text: humanDuration(d), expired: false, urgent: d < 5000 };
}

export function humanDuration(ms: number): string {
  const s = Math.floor(ms / 1000);
  if (s < 1) return `${ms}ms`;
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ${m % 60}m`;
  const days = Math.floor(h / 24);
  return `${days}d ${h % 24}h`;
}

// Decode a protojson base64 bytes field to a short, printable preview.
export function previewPayload(b64: string | undefined, max = 160): string {
  if (!b64) return '';
  let decoded: string;
  try {
    const bin = atob(b64);
    const bytes = Uint8Array.from(bin, (c) => c.charCodeAt(0));
    decoded = new TextDecoder('utf-8', { fatal: false }).decode(bytes);
  } catch {
    return `(${b64.length} b64 chars)`;
  }
  // Collapse non-printable control chars (0x00-0x1F and 0x7F) to a middle dot.
  // Built from escape sequences so no literal control byte lives in the source.
  const ctrl = new RegExp('[\\u0000-\\u001f\\u007f]', 'g');
  const printable = decoded.replace(ctrl, MIDDOT);
  return printable.length > max ? printable.slice(0, max) + ELLIPSIS : printable;
}

export function shortHash(s: string | undefined, n = 8): string {
  if (!s) return '';
  return s.length > n ? s.slice(0, n) : s;
}
