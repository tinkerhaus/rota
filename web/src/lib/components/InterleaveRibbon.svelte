<script lang="ts">
  import { groupColor } from '$lib/colors';

  // LIVE INTERLEAVE RIBBON — a scrolling stream of the most-recently-served
  // group ids, newest on the right, each tile colored by its stable group hue.
  // A well-mixed ribbon (no long single-color runs) is the visual signature of
  // working fairness. Fed by the SSE `served` event's `ribbon` array.
  interface ShareLite {
    expected?: number;
    actual?: number;
    served?: number;
  }
  interface Props {
    ribbon: string[]; // last ~200 served group ids, oldest → newest
    shares?: Record<string, ShareLite>;
    connected: boolean;
    error?: string;
  }
  let { ribbon, shares = {}, connected, error = '' }: Props = $props();

  // Cap the rendered tiles; the stream already coalesces to ~200.
  const MAX = 200;
  const tiles = $derived(ribbon.slice(-MAX));

  // Stable, de-duplicated legend of the groups currently present in the ribbon,
  // ordered by first appearance so colors are easy to learn.
  const legend = $derived.by(() => {
    const seen = new Set<string>();
    const out: string[] = [];
    for (const id of tiles) {
      if (!seen.has(id)) {
        seen.add(id);
        out.push(id);
      }
    }
    return out;
  });
</script>

<div class="ribbon-wrap">
  <div class="rib-head">
    <span class="status" class:live={connected} class:down={!connected}>
      <span class="sdot"></span>{connected ? 'live' : error ? 'reconnecting…' : 'connecting…'}
    </span>
    {#if error}<span class="err mono">{error}</span>{/if}
    <span class="grow"></span>
    <span class="hint">recently served · newest →</span>
  </div>

  <div class="card stream" class:idle={tiles.length === 0}>
    {#if tiles.length === 0}
      <div class="waiting">
        {connected ? 'waiting for the lane to serve a message…' : 'awaiting stream…'}
      </div>
    {:else}
      <div class="track">
        {#each tiles as id, i (`${i}-${id}`)}
          <span
            class="tile"
            class:fresh={i >= tiles.length - 3}
            style="background:{groupColor(id)}"
            title={id || '(default)'}
          ></span>
        {/each}
      </div>
    {/if}
  </div>

  {#if legend.length > 0}
    <div class="rlegend">
      {#each legend as id (id)}
        {@const sh = shares[id]}
        <span class="litem" title={id || '(default)'}>
          <span class="lsw" style="background:{groupColor(id)}"></span>
          <span class="lname mono">{id || '(default)'}</span>
          {#if sh}<span class="lshare mono">{((sh.actual ?? 0) * 100).toFixed(0)}%</span>{/if}
        </span>
      {/each}
    </div>
  {/if}
</div>

<style>
  .rib-head {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 10px;
    font-size: 11px;
  }
  .grow {
    flex: 1;
  }
  .status {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    font-size: 10.5px;
  }
  .status .sdot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: var(--paused);
  }
  .status.live {
    color: var(--ready);
  }
  .status.live .sdot {
    background: var(--ready);
    box-shadow: 0 0 7px var(--ready);
    animation: pulse 1.6s ease-in-out infinite;
  }
  .status.down {
    color: var(--delayed);
  }
  .status.down .sdot {
    background: var(--delayed);
    box-shadow: 0 0 7px var(--delayed);
  }
  .err {
    color: var(--fg-faint);
    font-size: 11px;
  }
  .hint {
    color: var(--fg-faint);
  }

  .stream {
    padding: 12px 14px;
    overflow: hidden;
  }
  .track {
    display: flex;
    gap: 2px;
    align-items: stretch;
    height: 40px;
  }
  .tile {
    flex: 1 1 0;
    min-width: 3px;
    border-radius: 2px;
    transition: opacity 0.3s ease;
  }
  /* the newest few tiles flare in as they arrive */
  .tile.fresh {
    animation: flare 0.45s ease-out;
  }
  @keyframes flare {
    from {
      opacity: 0.15;
      transform: scaleY(0.4);
    }
    to {
      opacity: 1;
      transform: scaleY(1);
    }
  }
  .waiting {
    color: var(--fg-faint);
    font-size: 12px;
    padding: 14px 2px;
  }

  .rlegend {
    display: flex;
    flex-wrap: wrap;
    gap: 8px 14px;
    margin-top: 12px;
  }
  .litem {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-size: 11px;
    color: var(--fg-dim);
  }
  .lsw {
    width: 10px;
    height: 10px;
    border-radius: 3px;
  }
  .lname {
    color: var(--fg);
  }
  .lshare {
    color: var(--fg-faint);
    font-size: 10.5px;
  }
</style>
