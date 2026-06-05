<script lang="ts">
  import { page } from '$app/state';
  import { api, num } from '$lib/api';
  import { createPoll } from '$lib/poll.svelte';
  import { fmtInt, fmtRate } from '$lib/format';

  // Persistent "instrument readout" header. The ROTA wordmark in Instrument
  // Serif sits beside a live cluster console — leader / term / applied-index /
  // quorum (from /api/cluster + /api/health) and aggregate throughput (summed
  // publish rate across lanes from /api/stats). All three poll on ~2s.
  const nav = [
    { href: '/', label: 'lanes', seg: '' },
    { href: '/workflows', label: 'workflows', seg: 'workflows' }
  ];
  const activeSeg = $derived((page.url.pathname.split('/')[1] ?? '').toLowerCase());

  const cluster = createPoll(() => api.cluster(), 2000);
  const health = createPoll(() => api.health(), 2000);
  const stats = createPoll(() => api.stats(), 2000);

  $effect(() => {
    cluster.start();
    health.start();
    stats.start();
    return () => {
      cluster.stop();
      health.stop();
      stats.stop();
    };
  });

  const c = $derived(cluster.data);
  const h = $derived(health.data);
  const peers = $derived(c?.peers ?? []);
  const voters = $derived(peers.filter((p) => (p.suffrage ?? '').toLowerCase() === 'voter').length);
  const connOk = $derived(!cluster.error && !health.error);

  // Aggregate throughput = sum of per-lane publish rates.
  const tput = $derived(
    (stats.data?.lanes ?? []).reduce((a, l) => a + (l.publishRate ?? 0), 0)
  );

  // Overall serving verdict drives the leader dot color.
  const serving = $derived(!!(h?.serving && h?.hasQuorum));
</script>

<header class="top">
  <div class="brand">
    <a href="/" class="mark" aria-label="Rota home">ROTA<em>.</em></a>
    <nav class="nav">
      {#each nav as item (item.href)}
        <a class="navlink" class:active={activeSeg === item.seg} href={item.href}>{item.label}</a>
      {/each}
    </nav>
  </div>

  <div class="readout">
    <div class="kv">
      <span class="k">Leader</span>
      <span class="v">
        {#if !connOk}
          <span class="dot down"></span><span class="c-dlq">unreachable</span>
        {:else if c?.leaderId}
          <span class="dot" class:ok={serving} class:warn={!serving}></span>{c.leaderId}{#if h?.isLeader}<span
              class="self">self</span
            >{/if}
        {:else}
          <span class="dot warn"></span><span class="c-delayed">electing…</span>
        {/if}
      </span>
    </div>

    <div class="kv">
      <span class="k">Term</span>
      <span class="v num">{fmtInt(num(c?.term))}</span>
    </div>

    <div class="kv">
      <span class="k">Applied</span>
      <span class="v num">{fmtInt(num(c?.appliedIndex))}</span>
    </div>

    <div class="kv">
      <span class="k">Quorum</span>
      <span
        class="v num"
        class:ok={connOk && (h?.hasQuorum ?? false)}
        class:bad={connOk && !(h?.hasQuorum ?? true)}
        title={peers.map((p) => `${p.id} · ${p.suffrage ?? '?'}`).join('\n')}
      >
        {voters} / {peers.length || '?'}
      </span>
    </div>

    <div class="kv">
      <span class="k">Throughput</span>
      <span class="v num">{stats.data ? fmtRate(tput) : '—'}</span>
    </div>
  </div>
</header>

<style>
  .top {
    position: sticky;
    top: 0;
    z-index: 50;
    display: flex;
    align-items: flex-end;
    justify-content: space-between;
    gap: 24px;
    max-width: 1480px;
    margin: 0 auto;
    padding: 18px 26px 14px;
    border-bottom: 1px solid var(--line);
    background: rgba(7, 8, 12, 0.78);
    backdrop-filter: blur(14px) saturate(140%);
    animation: rise 0.7s both;
  }

  .brand {
    display: flex;
    align-items: baseline;
    gap: 18px;
  }
  .mark {
    font-family: var(--serif);
    font-size: 38px;
    line-height: 0.8;
    letter-spacing: 0.5px;
    color: var(--ink);
  }
  .mark em {
    font-style: italic;
    color: var(--served);
  }

  .nav {
    display: flex;
    align-items: center;
    gap: 4px;
    padding-bottom: 4px;
  }
  .navlink {
    padding: 4px 11px;
    border-radius: 999px;
    font-size: 11px;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    color: var(--faint);
    transition:
      color 0.1s ease,
      background 0.1s ease,
      border-color 0.1s ease;
    border: 1px solid transparent;
  }
  .navlink:hover {
    color: var(--dim);
  }
  .navlink.active {
    color: var(--ink);
    border-color: var(--line2);
    background: var(--panel2);
  }

  .readout {
    display: flex;
    gap: 26px;
    flex-wrap: wrap;
    justify-content: flex-end;
  }
  .kv {
    display: flex;
    flex-direction: column;
    gap: 3px;
    align-items: flex-end;
  }
  .kv .k {
    font-size: 9.5px;
    letter-spacing: 0.22em;
    color: var(--faint);
    text-transform: uppercase;
  }
  .kv .v {
    font-size: 15px;
    color: var(--ink);
    font-variant-numeric: tabular-nums;
    display: inline-flex;
    align-items: center;
    gap: 6px;
    white-space: nowrap;
  }
  .kv .v.ok {
    color: var(--served);
  }
  .kv .v.bad {
    color: var(--fail);
  }

  .dot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    display: inline-block;
    background: var(--pause);
  }
  .dot.ok {
    background: var(--served);
    box-shadow: var(--glow) var(--served);
    animation: beat 1.8s infinite;
  }
  .dot.warn {
    background: var(--active);
    box-shadow: var(--glow) var(--active);
    animation: beat 1.8s infinite;
  }
  .dot.down {
    background: var(--fail);
    box-shadow: var(--glow) var(--fail);
  }

  .self {
    margin-left: 2px;
    font-size: 8.5px;
    padding: 1px 5px;
    border-radius: 4px;
    background: var(--ready-bg);
    color: var(--served);
    border: 1px solid var(--ready-bd);
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  @media (max-width: 820px) {
    .top {
      flex-direction: column;
      align-items: stretch;
      gap: 14px;
    }
    .readout {
      justify-content: flex-start;
    }
  }
</style>
