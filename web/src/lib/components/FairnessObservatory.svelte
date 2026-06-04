<script lang="ts">
  import { api, num, type FairnessGroup } from '$lib/api';
  import { createPoll } from '$lib/poll.svelte';
  import { fmtInt } from '$lib/format';
  import ShareBars from './ShareBars.svelte';
  import InterleaveRibbon from './InterleaveRibbon.svelte';
  import StarvationRadar from './StarvationRadar.svelte';
  import PolicyLab from './PolicyLab.svelte';

  // FAIRNESS OBSERVATORY — the Phase 7 view. Stitches four signals together:
  //   1. share bars        (snapshot: expected vs actual served share)
  //   2. interleave ribbon (SSE: live stream of recently-served group ids)
  //   3. starvation radar   (snapshot: who's being left behind)
  //   4. policy lab         (editor + dry-run + health banner)
  // The snapshot is polled; the ribbon rides a long-lived EventSource that the
  // leader coalesces to ~300ms ticks. Both share the stable per-group color map.
  interface Props {
    lane: string;
  }
  let { lane }: Props = $props();

  // ── Snapshot poll (share bars + radar) ──
  const snap = createPoll(() => api.fairness(lane), 2500);
  $effect(() => {
    snap.start();
    return () => snap.stop();
  });
  const groups = $derived<FairnessGroup[]>(snap.data?.groups ?? []);
  const totalServed = $derived(num(snap.data?.totalServed));

  // ── Live ribbon via SSE EventSource ──
  interface ShareLite {
    expected?: number;
    actual?: number;
    served?: number;
  }
  let ribbon = $state<string[]>([]);
  let liveShares = $state<Record<string, ShareLite>>({});
  let liveTotal = $state(0);
  let connected = $state(false);
  let streamErr = $state('');

  $effect(() => {
    // Re-subscribe whenever the lane changes.
    const url = api.fairnessStreamUrl(lane);
    let es: EventSource | null = null;
    let closed = false;

    function open() {
      if (closed) return;
      es = new EventSource(url);

      es.addEventListener('open', () => {
        connected = true;
        streamErr = '';
      });

      // The leader emits `event: served` with a coalesced payload.
      es.addEventListener('served', (ev: MessageEvent) => {
        try {
          const d = JSON.parse(ev.data);
          if (Array.isArray(d.ribbon)) ribbon = d.ribbon.map((x: unknown) => String(x));
          if (d.shares && typeof d.shares === 'object') liveShares = d.shares;
          if (d.totalServed !== undefined) liveTotal = num(d.totalServed);
          connected = true;
          streamErr = '';
        } catch {
          // Ignore a malformed frame; the next tick replaces it wholesale.
        }
      });

      // Some servers emit default (unnamed) `message` events instead of named
      // ones — accept those too, with the same shape.
      es.addEventListener('message', (ev: MessageEvent) => {
        try {
          const d = JSON.parse(ev.data);
          if (Array.isArray(d.ribbon)) {
            ribbon = d.ribbon.map((x: unknown) => String(x));
            if (d.shares) liveShares = d.shares;
            if (d.totalServed !== undefined) liveTotal = num(d.totalServed);
          }
        } catch {
          /* keepalive comments never reach here */
        }
      });

      es.addEventListener('error', () => {
        // EventSource auto-reconnects on its own; reflect the gap in the UI.
        connected = false;
        streamErr = 'stream interrupted';
        // If the browser fully closed it (CLOSED), retry manually after a beat.
        if (es && es.readyState === EventSource.CLOSED) {
          es.close();
          es = null;
          if (!closed) setTimeout(open, 2000);
        }
      });
    }

    open();
    return () => {
      closed = true;
      connected = false;
      es?.close();
      es = null;
    };
  });

  // Prefer the live total once the stream has produced one; else the snapshot.
  const servedTotal = $derived(liveTotal > 0 ? liveTotal : totalServed);

  // Fairness verdict for the section header: are all active groups within band?
  const aligned = $derived(
    groups
      .filter((g) => !g.paused)
      .every((g) => Math.abs((g.actualShare ?? 0) - (g.expectedShare ?? 0)) <= 0.03)
  );
  const haveData = $derived(snap.data !== undefined);
</script>

<div class="obs">
  {#if snap.error && !snap.data}
    <div class="err-banner">failed to load fairness snapshot — {snap.error}</div>
  {/if}

  <!-- 1. SHARE BARS -->
  <section>
    <h3 class="section-title">
      served share
      {#if haveData && groups.length > 0}
        {#if aligned}
          <span class="pill ready"><span class="dot"></span>fairness converged</span>
        {:else}
          <span class="pill delayed"><span class="dot"></span>drifting</span>
        {/if}
      {/if}
      {#if snap.stale && snap.data}<span class="pill delayed"><span class="dot"></span>stale</span>{/if}
    </h3>
    {#if haveData}
      <ShareBars {groups} totalServed={servedTotal} />
    {:else}
      <div class="card empty"><span class="spin"></span> loading fairness snapshot…</div>
    {/if}
  </section>

  <!-- 2. LIVE INTERLEAVE RIBBON -->
  <section>
    <h3 class="section-title">live interleave</h3>
    <InterleaveRibbon {ribbon} shares={liveShares} {connected} error={streamErr} />
  </section>

  <!-- 3. STARVATION RADAR -->
  <section>
    <h3 class="section-title">starvation</h3>
    {#if haveData}
      <StarvationRadar {groups} />
    {:else}
      <div class="card empty"><span class="spin"></span> scanning…</div>
    {/if}
  </section>

  <!-- 4. POLICY LAB -->
  <section>
    <h3 class="section-title">policy lab</h3>
    <PolicyLab {lane} />
  </section>
</div>

<style>
  .obs {
    display: flex;
    flex-direction: column;
  }
  section {
    margin-bottom: 6px;
  }
  .section-title {
    margin-top: 18px;
  }
  section:first-child .section-title {
    margin-top: 0;
  }
  .empty .spin {
    margin-right: 8px;
    vertical-align: middle;
  }
</style>
