<script lang="ts">
  import { api, num, type LeaseInfo } from '$lib/api';
  import { createPoll, createClock } from '$lib/poll.svelte';
  import { fmtInt, fmtCountdown, fmtAgo } from '$lib/format';
  import Pager from './Pager.svelte';

  // Live leases — in-flight messages with a per-lease visibility-deadline
  // countdown. A 1s clock ticks the countdowns between the slower data polls.
  interface Props {
    lane: string;
  }
  let { lane }: Props = $props();

  const PAGE_SIZE = 100;
  let stack = $state<string[]>(['']);
  let pageIdx = $state(0);
  const currentToken = $derived(stack[pageIdx] ?? '');

  const poll = createPoll(() => api.leases(lane, { pageToken: currentToken, pageSize: PAGE_SIZE }), 2000);
  const clock = createClock(1000);
  $effect(() => {
    poll.start();
    clock.start();
    return () => {
      poll.stop();
      clock.stop();
    };
  });

  const leases = $derived(poll.data?.leases ?? []);
  const nextToken = $derived(poll.data?.nextPageToken ?? '');
  const hasNext = $derived(!!nextToken);
  const hasPrev = $derived(pageIdx > 0);

  // Soonest-to-expire first — the leases an operator worries about.
  const rows = $derived(
    [...leases].sort((a, b) => num(a.deadlineMs) - num(b.deadlineMs))
  );

  function attemptOf(l: LeaseInfo): number {
    return l.attemptAtLease ?? l.attempt ?? 0;
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
</script>

{#if poll.error && !poll.data}
  <div class="err-banner">failed to load leases — {poll.error}</div>
{:else if rows.length === 0 && poll.data}
  <div class="card empty">no active leases in this lane.</div>
{:else}
  <div class="card">
    <table class="tbl leases">
      <thead>
        <tr>
          <th class="r">lease id</th>
          <th class="r">msg id</th>
          <th>group</th>
          <th>consumer</th>
          <th class="r">attempt</th>
          <th class="r">granted</th>
          <th class="r" style="width:130px">deadline</th>
        </tr>
      </thead>
      <tbody>
        {#each rows as l (l.leaseId)}
          {@const cd = fmtCountdown(l.deadlineMs, clock.now)}
          <tr class:expired={cd.expired}>
            <td class="num lid">{fmtInt(num(l.leaseId))}</td>
            <td class="num msgid">{fmtInt(num(l.msgId))}</td>
            <td class="mono gname">{l.groupId || '(default)'}</td>
            <td class="mono consumer">{l.consumerId || '—'}</td>
            <td class="num">
              {#if attemptOf(l) > 1}
                <span class="retry">{fmtInt(attemptOf(l))}</span>
              {:else}
                {fmtInt(attemptOf(l))}
              {/if}
            </td>
            <td class="num ago">{fmtAgo(l.grantedMs, clock.now)}</td>
            <td class="r">
              <span
                class="cd num"
                class:expired={cd.expired}
                class:urgent={cd.urgent && !cd.expired}
              >
                {cd.text}
              </span>
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
  .lid {
    color: var(--fg-dim);
  }
  .msgid {
    color: var(--inflight);
  }
  .gname {
    color: var(--fg);
    max-width: 180px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .consumer {
    color: var(--fg-dim);
    font-size: 11.5px;
    max-width: 200px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .retry {
    color: var(--delayed);
    font-weight: 600;
  }
  .ago {
    color: var(--fg-faint);
    font-size: 11px;
    white-space: nowrap;
  }
  .cd {
    font-weight: 600;
    color: var(--inflight);
    padding: 2px 8px;
    border-radius: 5px;
    background: var(--inflight-bg);
    border: 1px solid var(--inflight-bd);
    display: inline-block;
    min-width: 64px;
    text-align: center;
  }
  .cd.urgent {
    color: var(--delayed);
    background: var(--delayed-bg);
    border-color: var(--delayed-bd);
    animation: pulse 1.4s ease-in-out infinite;
  }
  .cd.expired {
    color: var(--dlq);
    background: var(--dlq-bg);
    border-color: var(--dlq-bd);
  }
  tr.expired {
    background: rgba(248, 81, 73, 0.05);
  }
</style>
