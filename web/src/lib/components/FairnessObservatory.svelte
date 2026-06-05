<script lang="ts">
  import { api, num, type FairnessGroup } from '$lib/api';
  import { createPoll } from '$lib/poll.svelte';
  import { groupColorHsl } from '$lib/colors';
  import { fmtInt } from '$lib/format';
  import StarvationRadar from './StarvationRadar.svelte';
  import PolicyLab from './PolicyLab.svelte';

  // FAIRNESS OBSERVATORY — the hero. A live SERVED-ORDER RIBBON (each cell is a
  // served group in its stable hue, newest at right, sliding in) rides the SSE
  // stream; below it, expected-vs-actual SHARE BARS per group (hatched expected,
  // solid actual, a vertical "fair line" at the expected share, ▲/▼ for drift,
  // and a red pulsing "starving" flag). A stat row summarises the window.
  interface Props {
    lane: string;
  }
  let { lane }: Props = $props();

  // ── Snapshot poll (share bars + stat row + radar) ──
  const snap = createPoll(() => api.fairness(lane), 2500);
  $effect(() => {
    snap.start();
    return () => snap.stop();
  });
  const groups = $derived<FairnessGroup[]>(snap.data?.groups ?? []);
  const snapTotal = $derived(num(snap.data?.totalServed));

  // ── Live ribbon via SSE EventSource ──
  interface ShareLite {
    expected?: number;
    actual?: number;
    served?: number;
  }
  const RIB_CAP = 80;
  let ribbon = $state<string[]>([]);
  let liveShares = $state<Record<string, ShareLite>>({});
  let liveTotal = $state(0);
  let connected = $state(false);
  let streamErr = $state('');
  // Per-cell render keys so Svelte animates only freshly-arrived tiles.
  let seq = 0;
  let cells = $state<{ key: number; gid: string; h: number; fresh: boolean }[]>([]);

  function applyRibbon(ids: string[]) {
    const trimmed = ids.slice(-RIB_CAP);
    const prev = cells;
    // The stream is append-mostly: each tick the ribbon array gains a few new
    // ids at the tail and may drop a few from the head. Re-key by aligning the
    // existing tiles to the new tail so on-screen tiles keep their key & height
    // (no re-animation) and only freshly-appended tiles slide in.
    const grew = Math.max(1, trimmed.length - prev.length);
    const next: { key: number; gid: string; h: number; fresh: boolean }[] = [];
    for (let i = 0; i < trimmed.length; i++) {
      const isNew = i >= trimmed.length - grew;
      // Map this position back onto the previous list (shifted by how much the
      // head dropped) to reuse a matching tile.
      const reuse = prev[i + (prev.length - (trimmed.length - grew))];
      if (!isNew && reuse && reuse.gid === trimmed[i]) {
        next.push({ ...reuse, fresh: false });
      } else {
        const h = 44 + Math.sin((seq + i) * 0.6) * 10 + Math.random() * 16;
        next.push({ key: seq++, gid: trimmed[i], h, fresh: isNew });
      }
    }
    cells = next;
  }

  $effect(() => {
    const url = api.fairnessStreamUrl(lane);
    let es: EventSource | null = null;
    let closed = false;

    function ingest(raw: string) {
      try {
        const d = JSON.parse(raw);
        if (Array.isArray(d.ribbon)) {
          ribbon = d.ribbon.map((x: unknown) => String(x));
          applyRibbon(ribbon);
        }
        if (d.shares && typeof d.shares === 'object') liveShares = d.shares;
        if (d.totalServed !== undefined) liveTotal = num(d.totalServed);
        connected = true;
        streamErr = '';
      } catch {
        /* malformed frame or keepalive comment — ignore */
      }
    }

    function open() {
      if (closed) return;
      es = new EventSource(url);
      es.addEventListener('open', () => {
        connected = true;
        streamErr = '';
      });
      es.addEventListener('served', (ev: MessageEvent) => ingest(ev.data));
      es.addEventListener('message', (ev: MessageEvent) => ingest(ev.data));
      es.addEventListener('error', () => {
        connected = false;
        streamErr = 'stream interrupted';
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

  const servedTotal = $derived(liveTotal > 0 ? liveTotal : snapTotal);

  // ── Share rows (sorted by actual share desc, like the concept) ──
  const MATCH = 0.02;
  const rows = $derived(
    [...groups].sort((a, b) => (b.actualShare ?? 0) - (a.actualShare ?? 0))
  );
  function pct(v: number | undefined): number {
    const n = v ?? 0;
    return Math.max(0, Math.min(1, Number.isFinite(n) ? n : 0)) * 100;
  }
  function drift(g: FairnessGroup): number {
    return (g.actualShare ?? 0) - (g.expectedShare ?? 0);
  }
  // A group is "starving" when it's materially under-share AND its score is high.
  function starving(g: FairnessGroup): boolean {
    return !g.paused && (g.starvationScore ?? 0) >= 0.5 && drift(g) < -MATCH;
  }

  // ── Stat row ──
  const activeTenants = $derived(rows.filter((g) => !g.paused).length);
  const starvingCount = $derived(rows.filter((g) => starving(g)).length);

  // Pull inflight from /api/stats for this lane.
  const stats = createPoll(() => api.stats(), 2500);
  $effect(() => {
    stats.start();
    return () => stats.stop();
  });
  const laneInflight = $derived(
    num((stats.data?.lanes ?? []).find((l) => (l.lane ?? '') === lane)?.inflight)
  );

  const haveData = $derived(snap.data !== undefined);
  const aligned = $derived(
    rows.filter((g) => !g.paused).every((g) => Math.abs(drift(g)) <= 0.03)
  );
</script>

<div class="obs">
  {#if snap.error && !snap.data}
    <div class="err-banner">failed to load fairness snapshot — {snap.error}</div>
  {/if}

  <!-- HERO: ribbon + shares + stat row -->
  <div class="panel hero delay1">
    <div class="phead">
      <div>
        <div class="ptitle">Fairness — <span class="dim mono">{lane || '(default)'}</span></div>
        <div class="psub">
          served share ·
          {#if haveData && rows.length > 0}
            {aligned ? 'converged' : 'drifting'}
          {:else}
            scanning
          {/if}
        </div>
      </div>
      <span class="chip" class:live={connected} class:idle={!connected}>
        {connected ? 'live' : streamErr ? 'reconnecting' : 'connecting'}
      </span>
    </div>

    <!-- SERVED-ORDER RIBBON -->
    <div class="ribwrap">
      <div class="riblabel">served order →</div>
      <div class="ribbon">
        {#if cells.length === 0}
          <div class="ribwait">{connected ? 'waiting for the lane to serve…' : 'awaiting stream…'}</div>
        {:else}
          {#each cells as c (c.key)}
            <i
              class="cell"
              class:fresh={c.fresh}
              style="height:{c.h.toFixed(0)}px; background:{groupColorHsl(c.gid)}; color:{groupColorHsl(
                c.gid
              )}; animation-delay:{c.fresh ? '0s' : '-0.42s'}"
              title={c.gid || '(default)'}
            ></i>
          {/each}
        {/if}
      </div>
      <div class="ribfoot">
        <span>oldest</span>
        <span>who got served, in order — newest at right</span>
        <span>now</span>
      </div>
    </div>

    <!-- SHARE BARS -->
    {#if haveData && rows.length > 0}
      <div class="shares">
        {#each rows as g (g.groupId)}
          {@const col = groupColorHsl(g.groupId)}
          {@const exp = g.expectedShare ?? 0}
          {@const act = g.actualShare ?? 0}
          {@const gap = drift(g)}
          {@const starve = starving(g)}
          <div class="srow" class:starve>
            <div class="tname" title={g.groupId || '(default)'}>
              <span class="swatch" style="background:{col}; color:{col}"></span>
              <span class="tn">{g.groupId || '(default)'}</span>
              {#if starve}<span class="flag">starving</span>{/if}
              {#if g.paused}<span class="flag paused">paused</span>{/if}
            </div>
            <div class="bars">
              <div class="track">
                <div class="bar-exp" style="width:{pct(exp)}%"></div>
                <div
                  class="bar-act"
                  style="width:{pct(act)}%; background:{col}; box-shadow:0 0 12px {col}"
                ></div>
              </div>
              <div class="fairmark" style="left:calc({pct(exp).toFixed(1)}% - 1px)"></div>
            </div>
            <div class="snum">
              <span class="pct">{(act * 100).toFixed(0)}%</span>
              {#if gap > 0.02}<span class="up">▲</span>{:else if gap < -0.02}<span class="dn">▼</span
                >{/if}
              <span class="faint">exp {(exp * 100).toFixed(0)}%</span>
            </div>
          </div>
        {/each}
      </div>
    {:else if !snap.error}
      <div class="empty"><span class="spin"></span> loading fairness snapshot…</div>
    {/if}

    <!-- STAT ROW -->
    <div class="statrow">
      <div class="stat">
        <div class="n c-ready">{fmtInt(servedTotal)}</div>
        <div class="l">served / window</div>
      </div>
      <div class="stat">
        <div class="n c-inflight">{fmtInt(laneInflight)}</div>
        <div class="l">in flight</div>
      </div>
      <div class="stat">
        <div class="n">{fmtInt(activeTenants)}</div>
        <div class="l">active tenants</div>
      </div>
      <div class="stat">
        <div class="n" class:c-dlq={starvingCount > 0}>{fmtInt(starvingCount)}</div>
        <div class="l">starving</div>
      </div>
    </div>
  </div>

  <!-- Starvation radar (worst-first scan) -->
  <section>
    <h3 class="section-title">starvation radar</h3>
    {#if haveData}
      <StarvationRadar {groups} />
    {:else}
      <div class="card empty"><span class="spin"></span> scanning…</div>
    {/if}
  </section>

  <!-- Policy lab -->
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

  .hero {
    padding: 16px 18px;
  }
  .phead {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 14px;
  }
  .ptitle {
    font-family: var(--serif);
    font-size: 24px;
    letter-spacing: 0.3px;
  }
  .psub {
    font-size: 10px;
    letter-spacing: 0.16em;
    color: var(--faint);
    text-transform: uppercase;
    margin-top: 2px;
  }
  .chip.idle {
    color: var(--dim);
  }

  /* ── ribbon (hero) ── */
  .ribwrap {
    position: relative;
    border: 1px solid var(--line);
    border-radius: 10px;
    background: #06070b;
    padding: 12px 0 12px 14px;
    overflow: hidden;
  }
  .ribwrap::after {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    width: 60px;
    height: 100%;
    background: linear-gradient(90deg, #06070b, transparent);
    pointer-events: none;
  }
  .riblabel {
    position: absolute;
    top: 8px;
    left: 14px;
    font-size: 9px;
    letter-spacing: 0.2em;
    color: var(--faint);
    text-transform: uppercase;
    z-index: 2;
  }
  .ribbon {
    display: flex;
    gap: 5px;
    align-items: flex-end;
    height: 74px;
    margin-top: 14px;
    overflow: hidden;
  }
  .cell {
    flex: 0 0 auto;
    width: 17px;
    border-radius: 4px;
    opacity: 0.92;
    transform-origin: bottom;
    animation: slidein 0.42s cubic-bezier(0.2, 0.8, 0.2, 1) both;
  }
  .cell.fresh {
    box-shadow: 0 0 14px currentColor;
  }
  .ribwait {
    color: var(--faint);
    font-size: 12px;
    align-self: center;
    padding-bottom: 24px;
  }
  .ribfoot {
    display: flex;
    justify-content: space-between;
    margin-top: 10px;
    padding-right: 14px;
    font-size: 10px;
    color: var(--faint);
  }

  /* ── share bars ── */
  .shares {
    margin-top: 18px;
    display: flex;
    flex-direction: column;
    gap: 12px;
  }
  .srow {
    display: grid;
    grid-template-columns: 150px 1fr auto;
    align-items: center;
    gap: 12px;
  }
  .tname {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 12.5px;
    color: var(--ink);
    min-width: 0;
  }
  .tname .tn {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .swatch {
    width: 9px;
    height: 9px;
    border-radius: 2px;
    flex: 0 0 auto;
    box-shadow: 0 0 8px currentColor;
  }
  .bars {
    position: relative;
    height: 22px;
  }
  .track {
    position: absolute;
    inset: 0;
    background: rgba(255, 255, 255, 0.04);
    border-radius: 5px;
    overflow: hidden;
  }
  .bar-exp {
    position: absolute;
    top: 0;
    height: 100%;
    border-radius: 5px;
    background: repeating-linear-gradient(
      90deg,
      rgba(255, 255, 255, 0.14) 0 2px,
      transparent 2px 6px
    );
    transition: width 0.6s;
  }
  .bar-act {
    position: absolute;
    top: 3px;
    height: 16px;
    border-radius: 4px;
    transition: width 0.55s cubic-bezier(0.3, 0.9, 0.3, 1);
    opacity: 0.95;
  }
  .fairmark {
    position: absolute;
    top: -3px;
    width: 2px;
    height: 28px;
    background: var(--ink);
    opacity: 0.55;
    transition: left 0.6s;
  }
  .snum {
    font-size: 12px;
    font-variant-numeric: tabular-nums;
    color: var(--dim);
    min-width: 120px;
    text-align: right;
    white-space: nowrap;
  }
  .snum .pct {
    color: var(--ink);
  }
  .snum .up {
    color: var(--served);
  }
  .snum .dn {
    color: var(--fail);
  }
  .starve {
    animation: warn 1.1s infinite;
  }
  .flag {
    font-size: 9px;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--fail);
    border: 1px solid rgba(251, 106, 134, 0.45);
    border-radius: 4px;
    padding: 1px 5px;
    margin-left: 4px;
    flex: 0 0 auto;
  }
  .flag.paused {
    color: var(--pause);
    border-color: var(--paused-bd);
  }

  /* ── stat row ── */
  .statrow {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 10px;
    margin-top: 18px;
  }
  .stat {
    background: #0a0c12;
    border: 1px solid var(--line);
    border-radius: 9px;
    padding: 11px 12px;
  }
  .stat .n {
    font-size: 24px;
    font-variant-numeric: tabular-nums;
    line-height: 1;
  }
  .stat .l {
    font-size: 9px;
    letter-spacing: 0.16em;
    text-transform: uppercase;
    color: var(--faint);
    margin-top: 7px;
  }

  .empty {
    padding: 24px 8px;
    color: var(--faint);
    font-size: 12px;
  }
  .empty .spin {
    margin-right: 8px;
    vertical-align: middle;
  }

  @media (max-width: 720px) {
    .srow {
      grid-template-columns: 110px 1fr auto;
    }
    .statrow {
      grid-template-columns: repeat(2, 1fr);
    }
  }
</style>
