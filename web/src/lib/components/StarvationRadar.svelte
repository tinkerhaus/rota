<script lang="ts">
  import { num, type FairnessGroup } from '$lib/api';
  import { groupColor } from '$lib/colors';
  import { fmtInt, fmtFloat } from '$lib/format';

  // STARVATION RADAR — surfaces groups the scheduler is leaving behind. The
  // starvationScore (age × backlog × under-share, normalised [0,1]) drives both
  // sort order (worst first) and a red→amber→calm color ramp, so a starving
  // group is impossible to miss.
  interface Props {
    groups: FairnessGroup[];
  }
  let { groups }: Props = $props();

  // Thresholds for the severity ramp.
  const CRIT = 0.66;
  const WARN = 0.33;

  const rows = $derived(
    [...groups].sort((a, b) => (b.starvationScore ?? 0) - (a.starvationScore ?? 0))
  );

  const worst = $derived(rows[0]?.starvationScore ?? 0);
  const starving = $derived(rows.filter((g) => (g.starvationScore ?? 0) >= WARN).length);

  function sev(s: number): 'crit' | 'warn' | 'ok' {
    if (s >= CRIT) return 'crit';
    if (s >= WARN) return 'warn';
    return 'ok';
  }
  function deficitOf(g: FairnessGroup): number {
    // Under-served by expected − actual share (only the positive shortfall).
    return Math.max(0, (g.expectedShare ?? 0) - (g.actualShare ?? 0));
  }
</script>

<div class="radar">
  <div class="head">
    <span class="title">
      starvation radar
      {#if starving > 0}
        <span class="pill dlq"><span class="dot small"></span>{starving} at risk</span>
      {:else}
        <span class="pill ready"><span class="dot small"></span>all fed</span>
      {/if}
    </span>
    <span class="grow"></span>
    <span class="hint mono">worst {(worst * 100).toFixed(0)}%</span>
  </div>

  {#if rows.length === 0}
    <div class="card empty">no groups to scan.</div>
  {:else}
    <div class="card list">
      {#each rows as g (g.groupId)}
        {@const s = g.starvationScore ?? 0}
        {@const level = sev(s)}
        <div class="srow" class:crit={level === 'crit'} class:warn={level === 'warn'}>
          <div class="gid">
            <span class="dot" style="background:{groupColor(g.groupId)}"></span>
            <span class="name mono">{g.groupId || '(default)'}</span>
            {#if g.paused}<span class="tag">paused</span>{/if}
          </div>

          <div class="meter" title="starvation score {fmtFloat(s, 3)}">
            <span class="mfill" class:crit={level === 'crit'} class:warn={level === 'warn'} style="width:{Math.min(100, s * 100)}%"></span>
          </div>

          <span class="score num" class:crit={level === 'crit'} class:warn={level === 'warn'}>
            {(s * 100).toFixed(0)}%
          </span>

          <div class="why mono" title="why">
            <span class="w" title="served">{fmtInt(g.served)} served</span>
            <span class="sepdot">·</span>
            <span class="w" class:short={deficitOf(g) > 0.02} title="expected − actual share">
              −{(deficitOf(g) * 100).toFixed(1)}pp
            </span>
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
  }
  .title {
    display: inline-flex;
    align-items: center;
    gap: 10px;
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--fg-faint);
  }
  .grow {
    flex: 1;
  }
  .hint {
    color: var(--fg-faint);
    font-size: 11px;
  }
  .dot.small {
    width: 6px;
    height: 6px;
    background: currentColor;
  }

  .list {
    padding: 4px 0;
  }
  .srow {
    display: grid;
    grid-template-columns: 200px 1fr 56px 180px;
    align-items: center;
    gap: 14px;
    padding: 10px 16px;
    border-bottom: 1px solid var(--border);
    border-left: 2px solid transparent;
    transition: background 0.2s ease;
  }
  .srow:last-child {
    border-bottom: none;
  }
  .srow.warn {
    border-left-color: var(--delayed);
    background: var(--delayed-bg);
  }
  .srow.crit {
    border-left-color: var(--dlq);
    background: var(--dlq-bg);
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
  .name {
    font-weight: 600;
    color: var(--fg);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .tag {
    font-size: 10px;
    color: var(--paused);
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }

  .meter {
    height: 8px;
    border-radius: 5px;
    background: var(--bg-3);
    border: 1px solid var(--border);
    overflow: hidden;
  }
  .mfill {
    display: block;
    height: 100%;
    background: var(--paused);
    border-radius: 4px;
    transition: width 0.5s cubic-bezier(0.22, 1, 0.36, 1);
  }
  .mfill.warn {
    background: var(--delayed);
  }
  .mfill.crit {
    background: var(--dlq);
    box-shadow: 0 0 8px var(--dlq);
  }

  .score {
    text-align: right;
    font-size: 12.5px;
    font-weight: 600;
    color: var(--fg-dim);
  }
  .score.warn {
    color: var(--delayed);
  }
  .score.crit {
    color: var(--dlq);
  }

  .why {
    display: flex;
    align-items: center;
    gap: 7px;
    justify-content: flex-end;
    font-size: 11px;
    color: var(--fg-faint);
  }
  .why .sepdot {
    color: var(--fg-ghost);
  }
  .why .short {
    color: var(--delayed);
  }
</style>
