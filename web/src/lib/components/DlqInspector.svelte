<script lang="ts">
  import { api, num, ApiError, type DeadLetterInfo } from '$lib/api';
  import { createPoll } from '$lib/poll.svelte';
  import { fmtInt, fmtAgo, previewPayload } from '$lib/format';
  import Pager from './Pager.svelte';
  import HeaderMap from './HeaderMap.svelte';

  // DLQ inspector — dead letters with a one-click redrive (POST to the gateway).
  interface Props {
    lane: string;
  }
  let { lane }: Props = $props();

  const PAGE_SIZE = 50;
  let stack = $state<string[]>(['']);
  let pageIdx = $state(0);
  const currentToken = $derived(stack[pageIdx] ?? '');

  const poll = createPoll(() => api.dlq(lane, { pageToken: currentToken, pageSize: PAGE_SIZE }), 3000);
  $effect(() => {
    poll.start();
    return () => poll.stop();
  });

  const items = $derived(poll.data?.deadLetters ?? []);
  const nextToken = $derived(poll.data?.nextPageToken ?? '');
  const hasNext = $derived(!!nextToken);
  const hasPrev = $derived(pageIdx > 0);

  function key(d: DeadLetterInfo): string {
    return `${d.groupId ?? ''}/${d.msgId ?? ''}`;
  }

  // Per-row redrive state, keyed by group/msg. Optimistic: hide on success.
  let busy = $state<Record<string, boolean>>({});
  let done = $state<Record<string, boolean>>({});
  let failed = $state<Record<string, string>>({});
  let expanded = $state<Record<string, boolean>>({});

  async function redrive(d: DeadLetterInfo) {
    const k = key(d);
    if (busy[k] || done[k]) return;
    busy = { ...busy, [k]: true };
    failed = { ...failed, [k]: '' };
    try {
      await api.redrive(lane, d.groupId ?? '', d.msgId ?? '');
      done = { ...done, [k]: true };
      // Pull a fresh page shortly after so the redriven letter drops off.
      setTimeout(() => void poll.refresh(), 600);
    } catch (e) {
      const msg = e instanceof ApiError ? `${e.status} ${e.body || e.message}` : String(e);
      failed = { ...failed, [k]: msg };
    } finally {
      busy = { ...busy, [k]: false };
    }
  }

  function toggle(k: string) {
    expanded = { ...expanded, [k]: !expanded[k] };
  }

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

  function reasonClass(reason: string | undefined): string {
    switch (reason) {
      case 'ttl':
        return 'delayed';
      case 'terminal_nack':
        return 'dlq';
      case 'max_lease_lifetime':
        return 'inflight';
      default:
        return 'dlq'; // max_attempts and unknowns
    }
  }
</script>

{#if poll.error && !poll.data}
  <div class="err-banner">failed to load DLQ — {poll.error}</div>
{:else if items.length === 0 && poll.data}
  <div class="card empty">
    <span class="c-ready" style="font-size:15px">&#10003;</span> dead-letter queue is empty for this lane.
  </div>
{:else}
  <div class="card">
    <table class="tbl dlq">
      <thead>
        <tr>
          <th style="width:30px"></th>
          <th>group</th>
          <th class="r">msg id</th>
          <th>reason</th>
          <th class="r">attempt</th>
          <th>dead</th>
          <th>payload</th>
          <th class="r">redrive</th>
        </tr>
      </thead>
      <tbody>
        {#each items as d (key(d))}
          {@const k = key(d)}
          <tr class:done={done[k]}>
            <td>
              <button class="exp" onclick={() => toggle(k)} aria-label="expand">
                {expanded[k] ? '▾' : '▸'}
              </button>
            </td>
            <td class="mono gname">{d.groupId || '(default)'}</td>
            <td class="num msgid">{fmtInt(num(d.msgId))}</td>
            <td>
              <span class="pill {reasonClass(d.reason)}"
                ><span class="dot"></span>{d.reason || 'unknown'}</span
              >
            </td>
            <td class="num">{fmtInt(d.finalAttempt ?? 0)}</td>
            <td class="ago">{fmtAgo(d.deadAtMs)}</td>
            <td class="payload mono" title={previewPayload(d.payload, 600)}>
              {previewPayload(d.payload, 80) || '—'}
            </td>
            <td class="r">
              {#if done[k]}
                <span class="pill ready"><span class="dot"></span>redriven</span>
              {:else}
                <button class="btn danger sm" disabled={busy[k]} onclick={() => redrive(d)}>
                  {#if busy[k]}<span class="spin"></span>{:else}&#8635; redrive{/if}
                </button>
              {/if}
            </td>
          </tr>
          {#if failed[k]}
            <tr class="detail-row">
              <td></td>
              <td colspan="7"><span class="err-banner">redrive failed — {failed[k]}</span></td>
            </tr>
          {/if}
          {#if expanded[k]}
            <tr class="detail-row">
              <td></td>
              <td colspan="7">
                <div class="detail">
                  <div class="dcol">
                    <div class="dlbl">original headers</div>
                    <HeaderMap map={d.headers} empty="(none)" />
                  </div>
                  <div class="dcol">
                    <div class="dlbl c-dlq">failure headers</div>
                    <HeaderMap map={d.failureHeaders} tone="fail" empty="(none)" />
                  </div>
                  <div class="dcol wide">
                    <div class="dlbl">payload preview</div>
                    <pre class="payload-full mono">{previewPayload(d.payload, 1200) || '(empty)'}</pre>
                  </div>
                </div>
              </td>
            </tr>
          {/if}
        {/each}
      </tbody>
    </table>
    <Pager
      page={pageIdx + 1}
      {hasNext}
      {hasPrev}
      loading={poll.loading}
      count={items.length}
      onnext={next}
      onprev={prev}
    />
  </div>
{/if}

<style>
  .dlq .gname {
    color: var(--fg);
    max-width: 180px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .msgid {
    color: var(--fg-dim);
  }
  .ago {
    color: var(--fg-faint);
    font-size: 11px;
    white-space: nowrap;
  }
  .payload {
    max-width: 280px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    color: var(--fg-dim);
    font-size: 11px;
  }
  tr.done {
    opacity: 0.5;
  }
  .exp {
    background: none;
    border: none;
    color: var(--fg-faint);
    font-size: 11px;
    padding: 0 2px;
  }
  .exp:hover {
    color: var(--accent);
  }
  .detail-row td {
    background: var(--bg);
  }
  .detail {
    display: flex;
    flex-wrap: wrap;
    gap: 22px;
    padding: 6px 4px 10px;
  }
  .dcol {
    min-width: 200px;
  }
  .dcol.wide {
    flex: 1;
    min-width: 320px;
  }
  .dlbl {
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.07em;
    color: var(--fg-faint);
    margin-bottom: 6px;
  }
  .payload-full {
    margin: 0;
    padding: 9px 11px;
    background: var(--bg-2);
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    font-size: 11.5px;
    color: var(--fg);
    white-space: pre-wrap;
    word-break: break-all;
    max-height: 220px;
    overflow: auto;
  }
</style>
