<script lang="ts">
  import { num, type FairnessGroup } from '$lib/api';
  import { groupColor } from '$lib/colors';
  import { fmtInt } from '$lib/format';

  // SHARE BARS — for each active group, expected share (weight-derived) sits
  // beside actual served share (rolling window). When the pair lines up, WFQ is
  // doing its job: the "drift" badge goes green. Widths animate via CSS so the
  // bars visibly converge as fairness settles.
  interface Props {
    groups: FairnessGroup[];
    totalServed: number;
  }
  let { groups, totalServed }: Props = $props();

  // Match threshold: within 2 percentage points reads as "fair".
  const MATCH_EPS = 0.02;

  function pct(v: number | undefined): number {
    const n = v ?? 0;
    if (!Number.isFinite(n)) return 0;
    return Math.max(0, Math.min(1, n)) * 100;
  }

  // Sort biggest-expected-share first so the dominant groups anchor the top.
  const rows = $derived(
    [...groups].sort((a, b) => (b.expectedShare ?? 0) - (a.expectedShare ?? 0))
  );

  function drift(g: FairnessGroup): number {
    return (g.actualShare ?? 0) - (g.expectedShare ?? 0);
  }
  function fair(g: FairnessGroup): boolean {
    return Math.abs(drift(g)) <= MATCH_EPS;
  }
  function fmtPct(v: number | undefined): string {
    return `${((v ?? 0) * 100).toFixed(1)}%`;
  }
  function fmtSigned(v: number): string {
    const p = v * 100;
    const s = p >= 0 ? '+' : '';
    return `${s}${p.toFixed(1)}pp`;
  }
</script>

<div class="sharebars">
  {#if rows.length === 0}
    <div class="card empty">no active groups to compare.</div>
  {:else}
    <div class="head">
      <span class="lbl">expected (weight)</span>
      <span class="lbl">vs</span>
      <span class="lbl">actual (served)</span>
      <span class="grow"></span>
      <span class="total mono">{fmtInt(totalServed)} served</span>
    </div>
    <div class="card list">
      {#each rows as g (g.groupId)}
        {@const col = groupColor(g.groupId)}
        {@const ok = fair(g)}
        <div class="row" class:paused={g.paused}>
          <div class="gid">
            <span class="dot" style="background:{col}; box-shadow:0 0 7px {col}"></span>
            <span class="name mono">{g.groupId || '(default)'}</span>
            {#if g.paused}<span class="pill paused"><span class="dot small"></span>paused</span>{/if}
          </div>

          <div class="bars">
            <!-- expected -->
            <div class="track" title="expected {fmtPct(g.expectedShare)}">
              <span
                class="fill expected"
                style="width:{pct(g.expectedShare)}%; background:{col}"
              ></span>
              <span class="vlabel mono">{fmtPct(g.expectedShare)}</span>
            </div>
            <!-- actual -->
            <div class="track" title="actual {fmtPct(g.actualShare)}">
              <span
                class="fill actual"
                class:short={drift(g) < -MATCH_EPS}
                style="width:{pct(g.actualShare)}%; background:{col}"
              ></span>
              <span class="vlabel mono">{fmtPct(g.actualShare)}</span>
            </div>
          </div>

          <div class="drift">
            <span class="pill" class:ready={ok} class:delayed={!ok && drift(g) > 0} class:dlq={!ok && drift(g) < 0}>
              <span class="dot small"></span>
              {ok ? 'fair' : fmtSigned(drift(g))}
            </span>
            <span class="served mono">{fmtInt(g.served)}</span>
          </div>
        </div>
      {/each}
    </div>
  {/if}
</div>

<style>
  .head {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 10px;
    font-size: 10.5px;
    text-transform: uppercase;
    letter-spacing: 0.07em;
    color: var(--fg-faint);
  }
  .head .lbl + .lbl {
    color: var(--fg-ghost);
  }
  .head .grow {
    flex: 1;
  }
  .head .total {
    color: var(--fg-dim);
    text-transform: none;
    letter-spacing: 0;
    font-size: 12px;
  }

  .list {
    padding: 4px 0;
  }
  .row {
    display: grid;
    grid-template-columns: 200px 1fr 132px;
    align-items: center;
    gap: 16px;
    padding: 11px 16px;
    border-bottom: 1px solid var(--border);
  }
  .row:last-child {
    border-bottom: none;
  }
  .row.paused {
    opacity: 0.6;
  }

  .gid {
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
  }
  .dot {
    width: 9px;
    height: 9px;
    border-radius: 50%;
    flex: none;
  }
  .dot.small {
    width: 6px;
    height: 6px;
    box-shadow: none;
  }
  .name {
    font-weight: 600;
    color: var(--fg);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .bars {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  .track {
    position: relative;
    height: 16px;
    width: 100%;
    background: var(--bg-3);
    border: 1px solid var(--border);
    border-radius: 5px;
    overflow: hidden;
  }
  .fill {
    position: absolute;
    inset: 0 auto 0 0;
    height: 100%;
    border-radius: 4px 0 0 4px;
    transition: width 0.6s cubic-bezier(0.22, 1, 0.36, 1);
  }
  .fill.expected {
    opacity: 0.42;
  }
  .fill.actual {
    opacity: 0.9;
  }
  /* under-served bars get a faint diagonal hatch so a shortfall reads instantly */
  .fill.actual.short {
    background-image: repeating-linear-gradient(
      45deg,
      rgba(0, 0, 0, 0) 0,
      rgba(0, 0, 0, 0) 5px,
      rgba(0, 0, 0, 0.28) 5px,
      rgba(0, 0, 0, 0.28) 10px
    );
  }
  .vlabel {
    position: absolute;
    right: 6px;
    top: 50%;
    transform: translateY(-50%);
    font-size: 10.5px;
    color: var(--fg);
    text-shadow: 0 1px 2px rgba(0, 0, 0, 0.7);
    pointer-events: none;
  }

  .drift {
    display: flex;
    align-items: center;
    justify-content: flex-end;
    gap: 10px;
  }
  .drift .served {
    font-size: 11.5px;
    color: var(--fg-faint);
    min-width: 44px;
    text-align: right;
  }
  .pill .dot.small {
    background: currentColor;
  }
</style>
