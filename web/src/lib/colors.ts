// Stable per-group color mapping for the Fairness Observatory.
//
// Every view that paints "group A vs group B vs ..." (share bars, the live
// interleave ribbon, the starvation radar) must agree on which hue belongs to
// which group id. A pure hash of the group id gives a deterministic mapping
// that is stable across renders, reconnects, and pagination — no shared state,
// no allocation order dependence.
//
// The palette is hand-tuned to sit alongside the dashboard's dark surfaces and
// to stay distinct from the reserved semantic colors (red = starvation/dlq).

// 12 evenly-spread, readable hues. Avoids pure red (reserved for danger) and
// muddy mid-greys. Picked for contrast against --bg (#0a0c10).
const PALETTE = [
  '#58a6ff', // blue
  '#3fb950', // green
  '#d29922', // amber
  '#a371f7', // violet
  '#56d4dd', // cyan
  '#f778ba', // pink
  '#7ee787', // mint
  '#ffa657', // orange
  '#79c0ff', // sky
  '#d2a8ff', // lavender
  '#e3b341', // gold
  '#6ee7b7' // teal
] as const;

// Deterministic 32-bit FNV-1a hash of the group id.
function hashId(id: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < id.length; i++) {
    h ^= id.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

export function groupColor(groupId: string | undefined | null): string {
  const id = groupId ?? '';
  return PALETTE[hashId(id) % PALETTE.length];
}

// A translucent variant of the group color, for fills/backgrounds.
export function groupColorBg(groupId: string | undefined | null, alpha = 0.16): string {
  const c = groupColor(groupId);
  // c is "#rrggbb"
  const r = parseInt(c.slice(1, 3), 16);
  const g = parseInt(c.slice(3, 5), 16);
  const b = parseInt(c.slice(5, 7), 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}
