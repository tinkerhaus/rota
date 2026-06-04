<script lang="ts">
  import { api, num } from '$lib/api';
  import { createPoll } from '$lib/poll.svelte';
  import { fmtInt, shortHash } from '$lib/format';

  // Persistent Raft/cluster header — polls /api/cluster + /api/health every ~2s.
  const cluster = createPoll(() => api.cluster(), 2000);
  const health = createPoll(() => api.health(), 2000);

  $effect(() => {
    cluster.start();
    health.start();
    return () => {
      cluster.stop();
      health.stop();
    };
  });

  const c = $derived(cluster.data);
  const h = $derived(health.data);
  const peers = $derived(c?.peers ?? []);
  const voters = $derived(peers.filter((p) => (p.suffrage ?? '').toLowerCase() === 'voter').length);

  const connOk = $derived(!cluster.error && !health.error);
</script>

<header class="hdr">
  <div class="brand">
    <a href="/" class="logo" aria-label="Rota home">
      <span class="mark"></span>
      <span class="wordmark">ROTA</span>
    </a>
    <span class="tag">operator console</span>
  </div>

  <div class="cluster">
    <!-- Serving / quorum status -->
    <div class="stat">
      <span class="lbl">cluster</span>
      {#if !connOk}
        <span class="pill dlq"><span class="dot"></span>unreachable</span>
      {:else if h?.serving && h?.hasQuorum}
        <span class="pill ready"><span class="dot"></span>serving</span>
      {:else if h?.hasQuorum}
        <span class="pill delayed"><span class="dot"></span>quorum, not serving</span>
      {:else}
        <span class="pill dlq"><span class="dot"></span>no quorum</span>
      {/if}
    </div>

    <div class="sep"></div>

    <!-- Leader -->
    <div class="stat">
      <span class="lbl">leader</span>
      <span class="val mono" title={c?.leaderAddr}>
        {#if c?.leaderId}
          <span class="led">{c.leaderId}</span>{#if h?.isLeader}<span class="self">self</span>{/if}
        {:else}
          <span class="c-delayed">electing…</span>
        {/if}
      </span>
    </div>

    <div class="sep"></div>

    <div class="stat">
      <span class="lbl">term</span>
      <span class="val num">{fmtInt(num(c?.term))}</span>
    </div>

    <div class="stat">
      <span class="lbl">applied</span>
      <span class="val num">{fmtInt(num(c?.appliedIndex))}</span>
    </div>

    <div class="sep"></div>

    <!-- Per-peer suffrage strip -->
    <div class="peers" title={`${voters} voter${voters === 1 ? '' : 's'} of ${peers.length} peer${peers.length === 1 ? '' : 's'}`}>
      <span class="lbl">peers</span>
      <div class="peerdots">
        {#each peers as p (p.id)}
          {@const isVoter = (p.suffrage ?? '').toLowerCase() === 'voter'}
          {@const isLeader = p.id === c?.leaderId}
          <span
            class="peer"
            class:voter={isVoter}
            class:leader={isLeader}
            title={`${p.id} · ${p.addr ?? ''} · ${p.suffrage ?? 'Unknown'}${isLeader ? ' · LEADER' : ''}`}
          >
            <span class="pdot"></span>
            <span class="pid">{shortHash(p.id, 10)}</span>
          </span>
        {/each}
        {#if peers.length === 0}
          <span class="faint mono">{connOk ? 'no peers' : '—'}</span>
        {/if}
      </div>
      <span class="quorum num">{voters}/{peers.length || '?'}</span>
    </div>
  </div>
</header>

<style>
  .hdr {
    position: sticky;
    top: 0;
    z-index: 50;
    display: flex;
    align-items: center;
    gap: 22px;
    padding: 0 22px;
    height: 52px;
    background: rgba(13, 17, 23, 0.86);
    backdrop-filter: blur(14px) saturate(140%);
    border-bottom: 1px solid var(--border);
    box-shadow: 0 1px 0 rgba(0, 0, 0, 0.5);
  }

  .brand {
    display: flex;
    align-items: baseline;
    gap: 10px;
  }
  .logo {
    display: flex;
    align-items: center;
    gap: 9px;
  }
  .mark {
    width: 16px;
    height: 16px;
    border-radius: 50%;
    border: 2.5px solid var(--ready);
    position: relative;
    box-shadow: 0 0 10px rgba(63, 185, 80, 0.4);
  }
  .mark::before {
    content: '';
    position: absolute;
    width: 5px;
    height: 5px;
    border-radius: 50%;
    background: var(--inflight);
    top: -3px;
    right: -3px;
    box-shadow: 0 0 6px var(--inflight);
  }
  .wordmark {
    font-family: var(--mono);
    font-weight: 700;
    letter-spacing: 0.22em;
    font-size: 14px;
  }
  .tag {
    font-size: 10.5px;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    color: var(--fg-ghost);
  }

  .cluster {
    display: flex;
    align-items: center;
    gap: 18px;
    margin-left: auto;
    overflow-x: auto;
    padding: 6px 0;
  }
  .sep {
    width: 1px;
    height: 22px;
    background: var(--border);
  }
  .stat {
    display: flex;
    flex-direction: column;
    gap: 2px;
    white-space: nowrap;
  }
  .lbl {
    font-size: 9px;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: var(--fg-ghost);
  }
  .val {
    font-size: 12.5px;
    color: var(--fg);
  }
  .led {
    color: var(--ready);
    font-weight: 600;
  }
  .self {
    margin-left: 6px;
    font-size: 9px;
    padding: 1px 5px;
    border-radius: 4px;
    background: var(--ready-bg);
    color: var(--ready);
    border: 1px solid var(--ready-bd);
    letter-spacing: 0.05em;
    text-transform: uppercase;
  }

  .peers {
    display: flex;
    align-items: center;
    gap: 9px;
  }
  .peerdots {
    display: flex;
    gap: 6px;
    align-items: center;
  }
  .peer {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    padding: 2px 7px 2px 6px;
    border-radius: 999px;
    background: var(--bg-3);
    border: 1px solid var(--border-strong);
  }
  .pdot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: var(--paused);
  }
  .peer.voter .pdot {
    background: var(--ready);
    box-shadow: 0 0 6px rgba(63, 185, 80, 0.6);
  }
  .peer.leader {
    border-color: var(--ready-bd);
    background: var(--ready-bg);
  }
  .peer.leader .pdot {
    background: var(--ready);
    box-shadow: 0 0 9px var(--ready);
  }
  .pid {
    font-family: var(--mono);
    font-size: 11px;
    color: var(--fg-dim);
  }
  .quorum {
    font-size: 12px;
    color: var(--fg-dim);
  }
</style>
