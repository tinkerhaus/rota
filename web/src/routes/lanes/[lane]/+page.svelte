<script lang="ts">
  import { page } from '$app/state';
  import { goto } from '$app/navigation';
  import { api, num } from '$lib/api';
  import { createPoll } from '$lib/poll.svelte';
  import { fmtInt, fmtRate } from '$lib/format';
  import GroupsGrid from '$lib/components/GroupsGrid.svelte';
  import DlqInspector from '$lib/components/DlqInspector.svelte';
  import LeasesPanel from '$lib/components/LeasesPanel.svelte';
  import FairnessObservatory from '$lib/components/FairnessObservatory.svelte';

  const lane = $derived(decodeURIComponent(page.params.lane ?? ''));

  type Tab = 'groups' | 'fairness' | 'leases' | 'dlq';
  const tabs: { id: Tab; label: string }[] = [
    { id: 'groups', label: 'fairness grid' },
    { id: 'fairness', label: 'observatory' },
    { id: 'leases', label: 'leases' },
    { id: 'dlq', label: 'dead letters' }
  ];
  const tab = $derived(((page.url.searchParams.get('tab') as Tab) || 'groups') as Tab);

  function setTab(t: Tab) {
    const u = new URL(page.url);
    u.searchParams.set('tab', t);
    void goto(u, { replaceState: true, keepFocus: true, noScroll: true });
  }

  // Per-lane headline numbers (filter /api/stats by lane).
  const stats = createPoll(() => api.stats(), 2500);
  $effect(() => {
    stats.start();
    return () => stats.stop();
  });
  const me = $derived((stats.data?.lanes ?? []).find((l) => (l.lane ?? '') === lane));
</script>

<svelte:head><title>Rota — {lane}</title></svelte:head>

<div class="page">
  <nav class="crumb">
    <a href="/" class="back">&larr; lanes</a>
    <span class="slash">/</span>
    <span class="cur mono">{lane || '(default)'}</span>
  </nav>

  <!-- Lane summary -->
  <div class="lane-summary card">
    <div class="ls title">
      <span class="lane-dot" class:hot={num(me?.dlqDepth) > 0}></span>
      <h1 class="mono">{lane || '(default)'}</h1>
      <span class="pol">policy v{fmtInt(num(me?.policyVersion))}</span>
    </div>
    <div class="ls-stats">
      <div class="m"><span class="k c-ready">leasable</span><span class="v num c-ready">{fmtInt(num(me?.leasable))}</span></div>
      <div class="m"><span class="k c-inflight">inflight</span><span class="v num c-inflight">{fmtInt(num(me?.inflight))}</span></div>
      <div class="m"><span class="k c-delayed">delayed</span><span class="v num c-delayed">{fmtInt(num(me?.delayed))}</span></div>
      <div class="m"><span class="k c-dlq">dlq</span><span class="v num c-dlq">{fmtInt(num(me?.dlqDepth))}</span></div>
      <div class="m"><span class="k">groups</span><span class="v num">{fmtInt(num(me?.groupCount))}</span></div>
      <div class="m sep"></div>
      <div class="m"><span class="k">pub</span><span class="v num rate">{fmtRate(me?.publishRate)}</span></div>
      <div class="m"><span class="k">lease</span><span class="v num rate">{fmtRate(me?.leaseRate)}</span></div>
      <div class="m"><span class="k">ack</span><span class="v num rate">{fmtRate(me?.ackRate)}</span></div>
    </div>
  </div>

  <!-- Tabs -->
  <div class="tabbar">
    {#each tabs as t (t.id)}
      <button class="tab" class:active={tab === t.id} onclick={() => setTab(t.id)}>
        {t.label}
        {#if t.id === 'dlq' && num(me?.dlqDepth) > 0}
          <span class="tab-badge dlq">{fmtInt(num(me?.dlqDepth))}</span>
        {/if}
        {#if t.id === 'leases' && num(me?.inflight) > 0}
          <span class="tab-badge inflight">{fmtInt(num(me?.inflight))}</span>
        {/if}
        {#if t.id === 'groups' && num(me?.groupCount) > 0}
          <span class="tab-badge">{fmtInt(num(me?.groupCount))}</span>
        {/if}
        {#if t.id === 'fairness'}
          <span class="tab-badge live">live</span>
        {/if}
      </button>
    {/each}
  </div>

  <div class="panel">
    {#key lane}
      {#if tab === 'groups'}
        <GroupsGrid {lane} />
      {:else if tab === 'fairness'}
        <FairnessObservatory {lane} />
      {:else if tab === 'leases'}
        <LeasesPanel {lane} />
      {:else}
        <DlqInspector {lane} />
      {/if}
    {/key}
  </div>
</div>

<style>
  .crumb {
    display: flex;
    align-items: center;
    gap: 9px;
    margin-bottom: 14px;
    font-size: 12px;
  }
  .back {
    color: var(--fg-dim);
  }
  .back:hover {
    color: var(--accent);
  }
  .slash {
    color: var(--fg-ghost);
  }
  .cur {
    color: var(--fg);
    font-weight: 600;
  }

  .lane-summary {
    padding: 16px 20px;
    margin-bottom: 18px;
  }
  .ls.title {
    display: flex;
    align-items: center;
    gap: 11px;
    margin-bottom: 14px;
  }
  .ls.title h1 {
    font-size: 19px;
  }
  .lane-dot {
    width: 9px;
    height: 9px;
    border-radius: 50%;
    background: var(--ready);
    box-shadow: 0 0 8px rgba(63, 185, 80, 0.5);
  }
  .lane-dot.hot {
    background: var(--dlq);
    box-shadow: 0 0 9px var(--dlq);
  }
  .pol {
    font-size: 11px;
    color: var(--fg-faint);
    padding: 2px 8px;
    border-radius: 5px;
    background: var(--bg-3);
    border: 1px solid var(--border);
  }
  .ls-stats {
    display: flex;
    flex-wrap: wrap;
    align-items: flex-end;
    gap: 26px;
  }
  .m {
    display: flex;
    flex-direction: column;
    gap: 3px;
  }
  .m.sep {
    width: 1px;
    align-self: stretch;
    background: var(--border);
    gap: 0;
  }
  .k {
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--fg-faint);
  }
  .v {
    font-size: 18px;
    font-weight: 600;
  }
  .v.rate {
    font-size: 14px;
    color: var(--fg-dim);
    font-weight: 500;
  }

  .tabbar {
    display: flex;
    gap: 4px;
    border-bottom: 1px solid var(--border);
    margin-bottom: 20px;
  }
  .tab {
    display: inline-flex;
    align-items: center;
    gap: 7px;
    padding: 9px 16px;
    background: none;
    border: none;
    border-bottom: 2px solid transparent;
    color: var(--fg-faint);
    font-weight: 500;
    font-size: 13px;
    margin-bottom: -1px;
    transition: color 0.1s ease;
  }
  .tab:hover {
    color: var(--fg-dim);
  }
  .tab.active {
    color: var(--fg);
    border-bottom-color: var(--accent);
  }
  .tab-badge {
    font-family: var(--mono);
    font-size: 10.5px;
    padding: 1px 6px;
    border-radius: 999px;
    background: var(--bg-3);
    border: 1px solid var(--border-strong);
    color: var(--fg-dim);
  }
  .tab-badge.dlq {
    color: var(--dlq);
    background: var(--dlq-bg);
    border-color: var(--dlq-bd);
  }
  .tab-badge.inflight {
    color: var(--inflight);
    background: var(--inflight-bg);
    border-color: var(--inflight-bd);
  }
  .tab-badge.live {
    color: var(--ready);
    background: var(--ready-bg);
    border-color: var(--ready-bd);
    text-transform: uppercase;
    letter-spacing: 0.05em;
    font-size: 9px;
  }
</style>
