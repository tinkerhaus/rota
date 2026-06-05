// Display helpers for the durable-execution (workflow) views.
//
// protojson encodes WorkflowStatus / HistoryEventType as their NAME strings
// (e.g. "WF_RUNNING", "HET_ACTIVITY_SCHEDULED"). These helpers map those onto
// the dashboard's shared semantic color language and produce terse labels +
// a best-effort preview of each event's base64 `attrs` blob.

import type { HistoryEvent, WorkflowStatus, HistoryEventType } from './api';
import { previewPayload } from './format';

// Semantic tone reused by the .pill / .c-* atoms in app.css.
export type Tone = 'ready' | 'inflight' | 'delayed' | 'dlq' | 'paused';

// RUNNING blue, COMPLETED green, FAILED/CANCELED red, CONTINUED grey, PENDING amber.
export function statusTone(status: WorkflowStatus | string | undefined): Tone {
  switch (status) {
    case 'WF_RUNNING':
      return 'inflight';
    case 'WF_COMPLETED':
      return 'ready';
    case 'WF_FAILED':
    case 'WF_CANCELED':
      return 'dlq';
    case 'WF_CONTINUED':
      return 'paused';
    case 'WF_PENDING':
      return 'delayed';
    default:
      return 'paused';
  }
}

// "WF_RUNNING" -> "running"
export function statusLabel(status: WorkflowStatus | string | undefined): string {
  if (!status) return 'unknown';
  return status.replace(/^WF_/, '').toLowerCase();
}

export function isRunning(status: WorkflowStatus | string | undefined): boolean {
  return status === 'WF_RUNNING' || status === 'WF_PENDING';
}

// "HET_ACTIVITY_SCHEDULED" -> "activity scheduled"
export function eventLabel(t: HistoryEventType | string | undefined): string {
  if (!t) return 'unspecified';
  return t.replace(/^HET_/, '').replace(/_/g, ' ').toLowerCase();
}

// Tone for the event-timeline dot, grouped by family.
export function eventTone(t: HistoryEventType | string | undefined): Tone {
  switch (t) {
    case 'HET_WORKFLOW_STARTED':
    case 'HET_WORKFLOW_COMPLETED':
    case 'HET_ACTIVITY_COMPLETED':
      return 'ready';
    case 'HET_ACTIVITY_SCHEDULED':
    case 'HET_WORKFLOW_TASK_SCHEDULED':
    case 'HET_WORKFLOW_TASK_COMPLETED':
      return 'inflight';
    case 'HET_TIMER_STARTED':
    case 'HET_TIMER_FIRED':
    case 'HET_SIGNAL_RECEIVED':
    case 'HET_MARKER_RECORDED':
      return 'delayed';
    case 'HET_ACTIVITY_FAILED':
    case 'HET_WORKFLOW_FAILED':
    case 'HET_WORKFLOW_CANCELED':
      return 'dlq';
    case 'HET_WORKFLOW_CONTINUED_AS_NEW':
      return 'paused';
    default:
      return 'paused';
  }
}

// Best-effort decode of an event's base64 attrs blob. The kernel keeps attrs
// opaque (the SDK encodes them, often protobuf), so we render a printable
// preview rather than pretend to parse a fixed schema.
export function eventAttrsPreview(ev: HistoryEvent, max = 200): string {
  return previewPayload(ev.attrs, max);
}

// ── Swimlane timeline model ──────────────────────────────────────────────────
//
// The run-detail view lays history events on a horizontal time axis by their
// eventTimeMs. We CANNOT decode the proto `attrs` in JS, so activities are
// paired purely BY ORDER: each ACTIVITY_SCHEDULED is matched with the next
// ACTIVITY_COMPLETED / ACTIVITY_FAILED in sequence and drawn as a duration bar.
// Unmatched scheduled activities are still "running".

function toMs(v: HistoryEvent['eventTimeMs']): number {
  return typeof v === 'string' ? Number(v) : (v ?? 0);
}

export type ActivityState = 'done' | 'failed' | 'running';

export interface ActivitySeg {
  index: number; // 1-based activity ordinal (label "activity N")
  startMs: number;
  endMs: number; // === startMs while running (caller extends to "now")
  state: ActivityState;
}

export interface DecisionMark {
  ms: number;
}

export interface SignalMark {
  ms: number;
  kind: 'signal' | 'timer-start' | 'timer-fired';
  label: string;
}

export interface TerminalMark {
  ms: number;
  kind: 'completed' | 'failed' | 'canceled' | 'continued';
  label: string;
}

export interface TimelineModel {
  activities: ActivitySeg[];
  decisions: DecisionMark[];
  signals: SignalMark[];
  terminal: TerminalMark | undefined;
  startMs: number; // axis origin (first event)
  endMs: number; // axis end (last event)
  hasRunning: boolean;
}

// Build the swimlane model from raw history. Events are expected pre-sorted by
// eventId; we re-sort defensively. Activity pairing is strictly by order.
export function buildTimeline(events: HistoryEvent[]): TimelineModel {
  const evs = [...events].sort((a, b) => {
    const d = toMs(a.eventTimeMs) - toMs(b.eventTimeMs);
    return d !== 0 ? d : Number(a.eventId ?? 0) - Number(b.eventId ?? 0);
  });

  const activities: ActivitySeg[] = [];
  const decisions: DecisionMark[] = [];
  const signals: SignalMark[] = [];
  let terminal: TimelineModel['terminal'] = undefined;

  // FIFO queue of scheduled-but-unmatched activities (by insertion order).
  const open: ActivitySeg[] = [];
  let actIndex = 0;

  let first = Infinity;
  let last = 0;

  for (const ev of evs) {
    const ms = toMs(ev.eventTimeMs);
    if (ms > 0) {
      if (ms < first) first = ms;
      if (ms > last) last = ms;
    }
    switch (ev.eventType) {
      case 'HET_ACTIVITY_SCHEDULED': {
        actIndex += 1;
        const seg: ActivitySeg = { index: actIndex, startMs: ms, endMs: ms, state: 'running' };
        activities.push(seg);
        open.push(seg);
        break;
      }
      case 'HET_ACTIVITY_COMPLETED':
      case 'HET_ACTIVITY_FAILED': {
        const seg = open.shift();
        if (seg) {
          seg.endMs = ms;
          seg.state = ev.eventType === 'HET_ACTIVITY_FAILED' ? 'failed' : 'done';
        }
        break;
      }
      case 'HET_WORKFLOW_TASK_COMPLETED':
        decisions.push({ ms });
        break;
      case 'HET_SIGNAL_RECEIVED':
        signals.push({ ms, kind: 'signal', label: 'signal' });
        break;
      case 'HET_TIMER_STARTED':
        signals.push({ ms, kind: 'timer-start', label: 'timer' });
        break;
      case 'HET_TIMER_FIRED':
        signals.push({ ms, kind: 'timer-fired', label: 'timer fired' });
        break;
      case 'HET_WORKFLOW_COMPLETED':
        terminal = { ms, kind: 'completed', label: 'completed' };
        break;
      case 'HET_WORKFLOW_FAILED':
        terminal = { ms, kind: 'failed', label: 'failed' };
        break;
      case 'HET_WORKFLOW_CANCELED':
        terminal = { ms, kind: 'canceled', label: 'canceled' };
        break;
      case 'HET_WORKFLOW_CONTINUED_AS_NEW':
        terminal = { ms, kind: 'continued', label: 'continued' };
        break;
      default:
        break;
    }
  }

  if (!Number.isFinite(first)) first = 0;
  return {
    activities,
    decisions,
    signals,
    terminal,
    startMs: first,
    endMs: last,
    hasRunning: open.length > 0
  };
}
