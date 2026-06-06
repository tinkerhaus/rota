<script lang="ts">
  import { api, num, type LaneStats } from '$lib/api';
  import { createPoll } from '$lib/poll.svelte';
  import { fmtInt, fmtRate } from '$lib/format';
  import DoctorPanel from '$lib/components/DoctorPanel.svelte';
  import LaneCard from '$lib/components/LaneCard.svelte';
  import WorkflowsSummary from '$lib/components/WorkflowsSummary.svelte';

  // HOME — the lanes overview. Polls /api/stats every ~2s and keeps a short
  // rolling history of each lane's lease rate client-side to drive the per-card
  // throughput sparklines. A periodic fairness sample per lane feeds the mini
  // served-order ribbons (cheap: just the group colors, refreshed slowly).
  const SPARK_LEN = 28;

  const stats = createPoll(() => api.stats(), 2000);
  $effect(() => {
    stats.start();
    return () => stats.stop();
  });

  const lanes = $derived(
    [...(stats.data?.lanes ?? [])].sort((a, b) => (a.lane ?? '').localeCompare(b.lane ?? ''))
  );

  // Rolling sparkline history, keyed by lane name. Appended on each fresh poll.
  let history = $state<Record<string, number[]>>({});
  let lastSampleAt = 0;
  $effect(() => {
    // Touch lastFetched so the effect re-runs whenever a poll completes.
    const at = stats.lastFetched;
    if (!at || at === lastSampleAt) return;
    lastSampleAt = at;
    const next: Record<string, number[]> = {};
    for (const l of stats.data?.lanes ?? []) {
      const key = l.lane ?? '';
      const prev = history[key] ?? new Array(SPARK_LEN).fill(0);
      const sample = l.leaseRate ?? l.publishRate ?? 0;
      next[key] = [...prev, sample].slice(-SPARK_LEN);
    }
    history = next;
  });

  // Mini served-order ribbon samples per lane, refreshed on a slow cadence so we
  // don't hammer the fairness endpoint. Each value is a list of group ids; the
  // card paints each in its stable color.
  const MINI = 18;
  let ribbons = $state<Record<string, string[]>>({});
  $effect(() => {
    let stopped = false;
    async function sampleAll() {
      const ls = stats.data?.lanes ?? [];
      await Promise.all(
        ls.map(async (l) => {
          const lane = l.lane ?? '';
          try {
            const f = await api.fairness(lane);
            const groups = (f.groups ?? []).filter((g) => num(g.served) > 0 || (g.actualShare ?? 0) > 0);
            if (groups.length === 0) return;
            // Weight the sample by actual share so the ribbon mirrors who's served.
            const pool: string[] = [];
            for (const g of groups) {
              const share = Math.max(0.02, g.actualShare ?? 0);
              const n = Math.max(1, Math.round(share * MINI));
              for (let i = 0; i < n; i++) pool.push(g.groupId ?? '');
            }
            const cells: string[] = [];
            for (let i = 0; i < MINI; i++) cells.push(pool[(Math.random() * pool.length) | 0] ?? '');
            if (!stopped) ribbons = { ...ribbons, [lane]: cells };
          } catch {
            /* lane may have no fairness data yet; leave its ribbon empty */
          }
        })
      );
    }
    void sampleAll();
    const t = setInterval(sampleAll, 4000);
    return () => {
      stopped = true;
      clearInterval(t);
    };
  });

  // Cluster-wide rollups for the summary strip.
  const totals = $derived.by(() => {
    const acc = { leasable: 0, delayed: 0, inflight: 0, dlq: 0, groups: 0, pub: 0 };
    for (const l of lanes) {
      acc.leasable += num(l.leasable);
      acc.delayed += num(l.delayed);
      acc.inflight += num(l.inflight);
      acc.dlq += num(l.dlqDepth);
      acc.groups += num(l.groupCount);
      acc.pub += l.publishRate ?? 0;
    }
    return acc;
  });

  function sparkFor(l: LaneStats): number[] {
    const h = history[l.lane ?? ''];
    return h && h.length ? h : new Array(SPARK_LEN).fill(0);
  }
</script>

<svelte:head><title>Rota — observatory</title></svelte:head>

<div class="page">
  <!-- Cluster rollup strip -->
  <div class="summary panel">
    <div class="s"><span class="lbl">lanes</span><span class="big num">{fmtInt(lanes.length)}</span></div>
    <div class="s"><span class="lbl">groups</span><span class="big num">{fmtInt(totals.groups)}</span></div>
    <div class="s"><span class="lbl c-ready">leasable</span><span class="big num c-ready">{fmtInt(totals.leasable)}</span></div>
    <div class="s"><span class="lbl c-inflight">inflight</span><span class="big num c-inflight">{fmtInt(totals.inflight)}</span></div>
    <div class="s"><span class="lbl c-delayed">delayed</span><span class="big num c-delayed">{fmtInt(totals.delayed)}</span></div>
    <div class="s"><span class="lbl c-dlq">dlq</span><span class="big num c-dlq">{fmtInt(totals.dlq)}</span></div>
    <div class="s"><span class="lbl">throughput</span><span class="big num">{stats.data ? fmtRate(totals.pub) : '—'}</span></div>
  </div>

  <div class="home">
    <!-- Lanes -->
    <section class="lanes-col">
      <h2 class="section-title">
        Lanes
        {#if stats.loading && !stats.data}<span class="spin"></span>{/if}
        {#if stats.stale && stats.data}<span class="pill delayed"><span class="dot"></span>stale</span>{/if}
      </h2>

      {#if stats.error && !stats.data}
        <div class="err-banner">failed to load stats — {stats.error}</div>
      {:else if lanes.length === 0 && stats.data}
        <div class="card empty">no lanes yet. Publish a message to spin one up.</div>
      {:else}
        <div class="lanegrid">
          {#each lanes as l, i (l.lane)}
            <LaneCard lane={l} history={sparkFor(l)} ribbon={ribbons[l.lane ?? ''] ?? []} index={i} />
          {/each}
        </div>
      {/if}
    </section>

    <!-- Workflows summary -->
    <aside class="wf-col">
      <DoctorPanel />
      <WorkflowsSummary />
    </aside>
  </div>
</div>

<style>
  .summary {
    display: flex;
    gap: 0;
    padding: 0;
    margin-bottom: 6px;
    overflow-x: auto;
    animation-delay: 0.04s;
  }
  .summary .s {
    flex: 1;
    min-width: 116px;
    display: flex;
    flex-direction: column;
    gap: 5px;
    padding: 15px 20px;
    border-right: 1px solid var(--line);
  }
  .summary .s:last-child {
    border-right: none;
  }
  .lbl {
    font-size: 9.5px;
    text-transform: uppercase;
    letter-spacing: 0.18em;
    color: var(--faint);
  }
  .big {
    font-size: 24px;
    font-weight: 500;
    letter-spacing: -0.01em;
  }

  .home {
    display: grid;
    grid-template-columns: 1.55fr 1fr;
    gap: 18px;
    align-items: start;
    margin-top: 10px;
  }
  .section-title {
    margin-top: 16px;
  }
  .lanes-col .section-title {
    margin-top: 16px;
  }
  .lanegrid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
    gap: 14px;
  }
  .wf-col {
    margin-top: 16px;
    position: sticky;
    top: 110px;
    display: flex;
    flex-direction: column;
    gap: 14px;
  }

  @media (max-width: 1040px) {
    .home {
      grid-template-columns: 1fr;
    }
    .wf-col {
      position: static;
    }
  }
</style>
