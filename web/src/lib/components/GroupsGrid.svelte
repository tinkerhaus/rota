<script lang="ts">
  import { api, num, type GroupStats } from '$lib/api';
  import { createPoll } from '$lib/poll.svelte';
  import { fmtInt, fmtFloat, fmtAgo } from '$lib/format';
  import Count from './Count.svelte';
  import Pager from './Pager.svelte';
  import Legend from './Legend.svelte';

  // THE FAIRNESS GRID. Rows = groups in a lane, with the shared color language
  // and the WFQ virtual-time / DRR-deficit signals that explain scheduling order.
  interface Props {
    lane: string;
  }
  let { lane }: Props = $props();

  const PAGE_SIZE = 100;

  // Token stack drives prev/next. Index 0 is the first page (empty token).
  let stack = $state<string[]>(['']);
  let pageIdx = $state(0);
  const currentToken = $derived(stack[pageIdx] ?? '');

  // Poll the current page. Re-created implicitly: the loader closes over
  // currentToken via a getter so each tick fetches the active page.
  const poll = createPoll(() => api.groups(lane, { pageToken: currentToken, pageSize: PAGE_SIZE }), 2500);
  $effect(() => {
    poll.start();
    return () => poll.stop();
  });

  const groups = $derived(poll.data?.groups ?? []);
  const nextToken = $derived(poll.data?.nextPageToken ?? '');
  const hasNext = $derived(!!nextToken);
  const hasPrev = $derived(pageIdx > 0);

  // Sort by virtual_time so the row most "owed" service (lowest VT among the
  // backlogged) floats to the top — the operator's natural reading order.
  const rows = $derived(
    [...groups].sort((a, b) => {
      const ab = num(a.ready) + num(a.inflight) > 0 ? 0 : 1;
      const bb = num(b.ready) + num(b.inflight) > 0 ? 0 : 1;
      if (ab !== bb) return ab - bb; // backlogged groups first
      return (a.virtualTime ?? 0) - (b.virtualTime ?? 0);
    })
  );

  // Max ready+inflight, for the inline backlog bar scale.
  const maxBacklog = $derived(
    Math.max(1, ...rows.map((g) => num(g.ready) + num(g.delayed) + num(g.inflight)))
  );

  function next() {
    if (!hasNext) return;
    if (pageIdx === stack.length - 1) stack = [...stack, nextToken];
    pageIdx += 1;
    void poll.refresh();
  }
  function prev() {
    if (!hasPrev) return;
    pageIdx -= 1;
    void poll.refresh();
  }

  function status(g: GroupStats): 'paused' | 'inflight' | 'ready' | 'delayed' | 'idle' {
    if (g.paused) return 'paused';
    if (num(g.inflight) > 0) return 'inflight';
    if (num(g.ready) > 0) return 'ready';
    if (num(g.delayed) > 0) return 'delayed';
    return 'idle';
  }

  function barSegs(g: GroupStats) {
    const total = maxBacklog;
    return {
      ready: (num(g.ready) / total) * 100,
      inflight: (num(g.inflight) / total) * 100,
      delayed: (num(g.delayed) / total) * 100
    };
  }
</script>

<div class="grid-head">
  <Legend />
  {#if poll.stale && poll.data}
    <span class="pill delayed"><span class="dot"></span>stale</span>
  {/if}
</div>

{#if poll.error && !poll.data}
  <div class="err-banner">failed to load groups — {poll.error}</div>
{:else if rows.length === 0 && poll.data}
  <div class="card empty">no groups in this lane.</div>
{:else}
  <div class="card">
    <table class="tbl grid">
      <thead>
        <tr>
          <th>group</th>
          <th style="width:160px">backlog</th>
          <th class="r">ready</th>
          <th class="r">inflight</th>
          <th class="r">delayed</th>
          <th class="r">total</th>
          <th class="r">weight</th>
          <th class="r" title="WFQ virtual finish time — fairness clock">virtual&nbsp;time</th>
          <th class="r" title="DRR deficit counter">deficit</th>
          <th class="r">last&nbsp;activity</th>
          <th>state</th>
        </tr>
      </thead>
      <tbody>
        {#each rows as g (g.groupId)}
          {@const st = status(g)}
          {@const segs = barSegs(g)}
          <tr class:paused-row={g.paused}>
            <td class="gname mono">{g.groupId || '(default)'}</td>
            <td>
              <div class="bar" title={`ready ${fmtInt(num(g.ready))} · inflight ${fmtInt(num(g.inflight))} · delayed ${fmtInt(num(g.delayed))}`}>
                <span class="seg ready" style="width:{segs.ready}%"></span>
                <span class="seg inflight" style="width:{segs.inflight}%"></span>
                <span class="seg delayed" style="width:{segs.delayed}%"></span>
              </div>
            </td>
            <td class="num"><Count value={g.ready} kind="ready" /></td>
            <td class="num"><Count value={g.inflight} kind="inflight" /></td>
            <td class="num"><Count value={g.delayed} kind="delayed" /></td>
            <td class="num"><Count value={g.total} /></td>
            <td class="num weight">{fmtFloat(g.weight, 2)}</td>
            <td class="num vt" title={String(g.virtualTime ?? 0)}>{fmtFloat(g.virtualTime, 1)}</td>
            <td class="num">{fmtFloat(g.deficit, 1)}</td>
            <td class="num ago">{fmtAgo(g.lastActivityMs)}</td>
            <td>
              {#if st === 'paused'}
                <span class="pill paused"><span class="dot"></span>paused</span>
              {:else if st === 'inflight'}
                <span class="pill inflight"><span class="dot"></span>serving</span>
              {:else if st === 'ready'}
                <span class="pill ready"><span class="dot"></span>ready</span>
              {:else if st === 'delayed'}
                <span class="pill delayed"><span class="dot"></span>delayed</span>
              {:else}
                <span class="pill"><span class="dot"></span>idle</span>
              {/if}
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
    <Pager
      page={pageIdx + 1}
      {hasNext}
      {hasPrev}
      loading={poll.loading}
      count={rows.length}
      onnext={next}
      onprev={prev}
    />
  </div>
{/if}

<style>
  .grid-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 12px;
  }
  .grid .gname {
    font-weight: 600;
    color: var(--fg);
    max-width: 220px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .paused-row {
    opacity: 0.62;
  }
  .bar {
    display: flex;
    height: 9px;
    width: 100%;
    border-radius: 5px;
    overflow: hidden;
    background: var(--bg-3);
    border: 1px solid var(--border);
  }
  .seg {
    height: 100%;
    transition: width 0.3s ease;
  }
  .seg.ready {
    background: var(--ready);
  }
  .seg.inflight {
    background: var(--inflight);
  }
  .seg.delayed {
    background: var(--delayed);
  }
  .weight {
    color: var(--accent);
  }
  .vt {
    color: var(--fg-dim);
  }
  .ago {
    color: var(--fg-faint);
    font-size: 11px;
  }
</style>
