<script lang="ts">
  import { goto } from '$app/navigation';
  import { api, num, type LaneStats } from '$lib/api';
  import { createPoll } from '$lib/poll.svelte';
  import { fmtInt, fmtRate } from '$lib/format';
  import Count from '$lib/components/Count.svelte';

  // Lanes overview — the home view. Polls /api/stats every ~2s.
  const stats = createPoll(() => api.stats(), 2000);
  $effect(() => {
    stats.start();
    return () => stats.stop();
  });

  const lanes = $derived(
    [...(stats.data?.lanes ?? [])].sort((a, b) => (a.lane ?? '').localeCompare(b.lane ?? ''))
  );

  // Cluster-wide rollups for the summary strip.
  const totals = $derived.by(() => {
    const acc = { leasable: 0, delayed: 0, inflight: 0, dlq: 0, groups: 0 };
    for (const l of lanes) {
      acc.leasable += num(l.leasable);
      acc.delayed += num(l.delayed);
      acc.inflight += num(l.inflight);
      acc.dlq += num(l.dlqDepth);
      acc.groups += num(l.groupCount);
    }
    return acc;
  });

  function laneHref(l: LaneStats): string {
    return `/lanes/${encodeURIComponent(l.lane ?? '')}`;
  }
</script>

<svelte:head><title>Rota — lanes</title></svelte:head>

<div class="page">
  <!-- Summary strip -->
  <div class="summary card">
    <div class="s">
      <span class="lbl">lanes</span><span class="big num">{fmtInt(lanes.length)}</span>
    </div>
    <div class="s">
      <span class="lbl">groups</span><span class="big num">{fmtInt(totals.groups)}</span>
    </div>
    <div class="s">
      <span class="lbl c-ready">leasable</span
      ><span class="big num c-ready">{fmtInt(totals.leasable)}</span>
    </div>
    <div class="s">
      <span class="lbl c-inflight">inflight</span
      ><span class="big num c-inflight">{fmtInt(totals.inflight)}</span>
    </div>
    <div class="s">
      <span class="lbl c-delayed">delayed</span
      ><span class="big num c-delayed">{fmtInt(totals.delayed)}</span>
    </div>
    <div class="s">
      <span class="lbl c-dlq">dlq</span><span class="big num c-dlq">{fmtInt(totals.dlq)}</span>
    </div>
  </div>

  <div class="section-title">
    lanes
    {#if stats.loading && !stats.data}<span class="spin"></span>{/if}
    {#if stats.stale && stats.data}<span class="pill delayed" style="margin-left:auto"
        ><span class="dot"></span>stale</span
      >{/if}
  </div>

  {#if stats.error && !stats.data}
    <div class="err-banner">failed to load stats — {stats.error}</div>
  {:else if lanes.length === 0 && stats.data}
    <div class="card empty">no lanes yet. Publish a message to spin one up.</div>
  {:else}
    <div class="card lanes-card">
      <table class="tbl">
        <thead>
          <tr>
            <th>lane</th>
            <th class="r">leasable</th>
            <th class="r">delayed</th>
            <th class="r">inflight</th>
            <th class="r">dlq</th>
            <th class="r">groups</th>
            <th class="r">pub</th>
            <th class="r">lease</th>
            <th class="r">ack</th>
            <th class="r">policy</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {#each lanes as l (l.lane)}
            <tr class="lane-row" onclick={() => goto(laneHref(l))}>
              <td>
                <a class="lane-name mono" href={laneHref(l)}>
                  <span class="lane-dot" class:hot={num(l.dlqDepth) > 0}></span>{l.lane || '(default)'}
                </a>
              </td>
              <td class="num"><Count value={l.leasable} kind="ready" /></td>
              <td class="num"><Count value={l.delayed} kind="delayed" /></td>
              <td class="num"><Count value={l.inflight} kind="inflight" /></td>
              <td class="num"><Count value={l.dlqDepth} kind="dlq" /></td>
              <td class="num"><Count value={l.groupCount} /></td>
              <td class="num rate">{fmtRate(l.publishRate)}</td>
              <td class="num rate">{fmtRate(l.leaseRate)}</td>
              <td class="num rate">{fmtRate(l.ackRate)}</td>
              <td class="num"><span class="pol">v{fmtInt(num(l.policyVersion))}</span></td>
              <td class="r"><span class="chev">&rsaquo;</span></td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
</div>

<style>
  .summary {
    display: flex;
    gap: 0;
    padding: 4px 0;
    margin-bottom: 4px;
    overflow-x: auto;
  }
  .summary .s {
    flex: 1;
    min-width: 110px;
    display: flex;
    flex-direction: column;
    gap: 4px;
    padding: 14px 20px;
    border-right: 1px solid var(--border);
  }
  .summary .s:last-child {
    border-right: none;
  }
  .lbl {
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--fg-faint);
  }
  .big {
    font-size: 22px;
    font-weight: 600;
    letter-spacing: -0.02em;
  }

  .lanes-card {
    overflow: hidden;
  }
  .lane-row {
    cursor: pointer;
  }
  .lane-name {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    font-weight: 600;
    color: var(--fg);
  }
  .lane-row:hover .lane-name {
    color: var(--accent);
  }
  .lane-dot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: var(--ready);
    box-shadow: 0 0 6px rgba(63, 185, 80, 0.5);
  }
  .lane-dot.hot {
    background: var(--dlq);
    box-shadow: 0 0 7px var(--dlq);
  }
  .rate {
    color: var(--fg-dim);
    font-size: 11.5px;
  }
  .pol {
    font-size: 11px;
    color: var(--fg-faint);
    padding: 1px 6px;
    border-radius: 4px;
    background: var(--bg-3);
    border: 1px solid var(--border);
  }
  .chev {
    color: var(--fg-ghost);
    font-size: 18px;
  }
  .lane-row:hover .chev {
    color: var(--accent);
  }
</style>
