<script lang="ts">
  import type { HistoryEvent } from '$lib/api';
  import { buildTimeline } from '$lib/workflow';
  import { humanDuration } from '$lib/format';

  // SWIMLANE TIMELINE — lays the run's real history on a horizontal time axis by
  // eventTimeMs. Lanes:
  //   · activities — each ACTIVITY_SCHEDULED paired IN ORDER with the next
  //     COMPLETED/FAILED draws a duration bar (green done, red failed, amber
  //     pulsing while still running).
  //   · decisions  — ticks for WORKFLOW_TASK_COMPLETED.
  //   · signals/timers — markers for SIGNAL_RECEIVED / TIMER_STARTED / FIRED.
  //   · terminal   — an end marker for COMPLETED/FAILED/CANCELED/CONTINUED.
  // attrs are opaque (cannot be decoded in JS) so activities are labelled by
  // index and laid out purely from timestamps.
  interface Props {
    events: HistoryEvent[];
    running: boolean;
    now: number;
  }
  let { events, running, now }: Props = $props();

  const model = $derived(buildTimeline(events));

  // Axis: from the first event to the later of (last event, now-if-running).
  const t0 = $derived(model.startMs || 0);
  const t1 = $derived.by(() => {
    let end = model.endMs || model.startMs || now;
    if (running) end = Math.max(end, now);
    // Guard against a zero-width axis (single event / all same ms).
    if (end <= t0) end = t0 + 1000;
    return end;
  });
  const span = $derived(Math.max(1, t1 - t0));

  function leftPct(ms: number): number {
    return ((Math.max(t0, Math.min(t1, ms)) - t0) / span) * 100;
  }
  // A duration bar's width, with a small floor so instant activities stay visible.
  function widthPct(startMs: number, endMs: number): number {
    const w = ((Math.max(0, endMs - startMs)) / span) * 100;
    return Math.max(0.8, w);
  }

  // Effective end of a running activity is "now" (clamped to the axis).
  function actEnd(state: string, endMs: number): number {
    return state === 'running' ? Math.min(t1, now) : endMs;
  }

  const nowLeft = $derived(running ? leftPct(now) : 100);
  const totalDur = $derived(humanDuration(span));
</script>

<div class="tl">
  <div class="tlhead">
    <div class="h">timeline</div>
    <div class="span mono">{totalDur} span · {events.length} events</div>
  </div>

  {#if events.length === 0}
    <div class="empty">no history events yet.</div>
  {:else}
    <div class="swim">
      <!-- activities -->
      <div class="lanrow">
        <div class="nm">activities</div>
        <div class="track2">
          {#if model.activities.length === 0}
            <span class="laneempty">no activities</span>
          {/if}
          {#each model.activities as a (a.index)}
            {@const end = actEnd(a.state, a.endMs)}
            <div
              class="seg {a.state}"
              style="left:{leftPct(a.startMs)}%; width:{widthPct(a.startMs, end)}%"
              title="activity {a.index} · {a.state}{a.state !== 'running'
                ? ' · ' + humanDuration(Math.max(0, a.endMs - a.startMs))
                : ''}"
            ></div>
          {/each}
          {#if running}<div class="nowline" style="left:{nowLeft}%"></div>{/if}
        </div>
      </div>

      <!-- decisions -->
      <div class="lanrow">
        <div class="nm">decisions</div>
        <div class="track2">
          {#if model.decisions.length === 0}
            <span class="laneempty">—</span>
          {/if}
          {#each model.decisions as d, i (i)}
            <div class="seg dec" style="left:{leftPct(d.ms)}%" title="workflow task completed"></div>
          {/each}
          {#if running}<div class="nowline" style="left:{nowLeft}%"></div>{/if}
        </div>
      </div>

      <!-- signals / timers -->
      <div class="lanrow">
        <div class="nm">signals</div>
        <div class="track2">
          {#if model.signals.length === 0}
            <span class="laneempty">—</span>
          {/if}
          {#each model.signals as s, i (i)}
            <div
              class="mark {s.kind}"
              style="left:{leftPct(s.ms)}%"
              title={s.label}
            ></div>
          {/each}
          {#if model.terminal}
            <div
              class="endmark {model.terminal.kind}"
              style="left:{leftPct(model.terminal.ms)}%"
              title={model.terminal.label}
            ></div>
          {/if}
          {#if running}<div class="nowline" style="left:{nowLeft}%"></div>{/if}
        </div>
      </div>
    </div>

    <div class="tlfoot">
      <span class="l-done">activity done</span>
      <span class="l-run">running</span>
      <span class="l-fail">failed</span>
      <span class="l-dec">decision</span>
      <span class="l-sig">signal / timer</span>
      {#if model.terminal}<span class="l-end {model.terminal.kind}">{model.terminal.label}</span>{/if}
    </div>
  {/if}
</div>

<style>
  .tl {
    border: 1px solid var(--line);
    background: linear-gradient(180deg, var(--panel), var(--bg2));
    border-radius: var(--radius);
    padding: 16px 18px;
  }
  .tlhead {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    margin-bottom: 14px;
  }
  .tlhead .h {
    font-family: var(--serif);
    font-size: 19px;
  }
  .span {
    font-size: 10.5px;
    color: var(--faint);
  }
  .empty {
    padding: 24px 8px;
    text-align: center;
    color: var(--faint);
    font-size: 12px;
  }

  .swim {
    display: flex;
    flex-direction: column;
    gap: 9px;
  }
  .lanrow {
    display: grid;
    grid-template-columns: 86px 1fr;
    gap: 12px;
    align-items: center;
  }
  .nm {
    font-size: 10.5px;
    color: var(--dim);
    text-align: right;
    white-space: nowrap;
  }
  .track2 {
    position: relative;
    height: 16px;
    background: rgba(255, 255, 255, 0.035);
    border-radius: 4px;
    overflow: hidden;
  }
  .laneempty {
    position: absolute;
    left: 8px;
    top: 50%;
    transform: translateY(-50%);
    font-size: 10px;
    color: var(--ghost);
  }

  .seg {
    position: absolute;
    top: 2px;
    height: 12px;
    border-radius: 3px;
    opacity: 0.92;
  }
  .seg.done {
    background: var(--served);
  }
  .seg.running {
    background: var(--active);
    box-shadow: 0 0 10px var(--active);
    animation: pulsebar 1.1s infinite;
  }
  .seg.failed {
    background: var(--fail);
    box-shadow: 0 0 8px rgba(251, 106, 134, 0.5);
  }
  .seg.dec {
    background: var(--delayed);
    width: 3px;
    opacity: 0.75;
  }

  /* signal / timer markers — thin pins */
  .mark {
    position: absolute;
    top: 1px;
    width: 3px;
    height: 14px;
    border-radius: 2px;
  }
  .mark.signal {
    background: var(--delayed);
    box-shadow: 0 0 8px var(--delayed);
  }
  .mark.timer-start {
    background: var(--pause);
  }
  .mark.timer-fired {
    background: var(--active);
  }

  /* terminal end-cap */
  .endmark {
    position: absolute;
    top: -1px;
    width: 5px;
    height: 18px;
    border-radius: 2px;
  }
  .endmark.completed {
    background: var(--served);
    box-shadow: 0 0 10px var(--served);
  }
  .endmark.failed,
  .endmark.canceled {
    background: var(--fail);
    box-shadow: 0 0 10px var(--fail);
  }
  .endmark.continued {
    background: var(--pause);
  }

  .nowline {
    position: absolute;
    top: -3px;
    bottom: -3px;
    width: 1px;
    background: var(--ink);
    opacity: 0.4;
  }

  .tlfoot {
    display: flex;
    flex-wrap: wrap;
    gap: 16px;
    font-size: 10px;
    color: var(--faint);
    margin-top: 14px;
  }
  .tlfoot span::before {
    content: '';
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 2px;
    margin-right: 5px;
    vertical-align: middle;
  }
  .l-done::before {
    background: var(--served);
  }
  .l-run::before {
    background: var(--active);
  }
  .l-fail::before {
    background: var(--fail);
  }
  .l-dec::before {
    background: var(--delayed);
  }
  .l-sig::before {
    background: var(--delayed);
  }
  .l-end.completed::before {
    background: var(--served);
  }
  .l-end.failed::before,
  .l-end.canceled::before {
    background: var(--fail);
  }
  .l-end.continued::before {
    background: var(--pause);
  }
</style>
