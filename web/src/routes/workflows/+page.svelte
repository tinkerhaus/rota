<script lang="ts">
  import { api, num, type WorkflowRun } from '$lib/api';
  import { createPoll } from '$lib/poll.svelte';
  import { groupColorHsl } from '$lib/colors';
  import { fmtInt, fmtAgo } from '$lib/format';
  import { statusTone, statusLabel, isRunning } from '$lib/workflow';
  import Pager from '$lib/components/Pager.svelte';
  import StartWorkflowForm from '$lib/components/StartWorkflowForm.svelte';

  // WORKFLOWS — runs as "reactor" rows. Each row is a status-colored filling
  // bar: running pipelines have no known target so they show an animated active
  // bar, terminal runs a full bar (no faked percentage). curHistorySeq is the
  // magnitude label. Polls /api/workflows every ~2.5s with status filter +
  // token pagination.
  const PAGE_SIZE = 50;

  const STATUSES = [
    { id: '', label: 'all' },
    { id: 'WF_RUNNING', label: 'running' },
    { id: 'WF_PENDING', label: 'pending' },
    { id: 'WF_COMPLETED', label: 'completed' },
    { id: 'WF_FAILED', label: 'failed' },
    { id: 'WF_CANCELED', label: 'canceled' },
    { id: 'WF_CONTINUED', label: 'continued' }
  ];

  let status = $state('');
  let stack = $state<string[]>(['']);
  let pageIdx = $state(0);
  const currentToken = $derived(stack[pageIdx] ?? '');

  const poll = createPoll(
    () => api.workflows({ status, pageToken: currentToken, pageSize: PAGE_SIZE }),
    2500
  );
  $effect(() => {
    poll.start();
    return () => poll.stop();
  });

  const runs = $derived<WorkflowRun[]>(poll.data?.runs ?? []);
  const nextToken = $derived(poll.data?.nextPageToken ?? '');
  const hasNext = $derived(!!nextToken);
  const hasPrev = $derived(pageIdx > 0);
  const runningCount = $derived(runs.filter((r) => isRunning(r.status)).length);

  function setStatus(s: string) {
    if (s === status) return;
    status = s;
    stack = [''];
    pageIdx = 0;
    void poll.refresh();
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
  function runHref(r: WorkflowRun): string {
    return `/workflows/${encodeURIComponent(String(r.runId ?? ''))}`;
  }
  function onStarted() {
    if (status !== '' || pageIdx !== 0) {
      status = '';
      stack = [''];
      pageIdx = 0;
    }
    void poll.refresh();
  }

  // A waiting-on-signal run is one that's running with a pending task — render it
  // as the indigo "waiting" tone per the concept.
  function tone(r: WorkflowRun) {
    if (r.status === 'WF_RUNNING' && r.wfTaskPending) return 'delayed';
    return statusTone(r.status);
  }
  function label(r: WorkflowRun) {
    if (r.status === 'WF_RUNNING' && r.wfTaskPending) return 'waiting · task';
    return statusLabel(r.status);
  }
</script>

<svelte:head><title>Rota — workflows</title></svelte:head>

<div class="page">
  <h2 class="section-title">Reactor</h2>
  <StartWorkflowForm onstarted={onStarted} />

  <h2 class="section-title">
    Runs
    <span class="run-count">{runningCount} running</span>
    {#if poll.loading && !poll.data}<span class="spin"></span>{/if}
    {#if poll.stale && poll.data}<span class="pill delayed"><span class="dot"></span>stale</span>{/if}
  </h2>

  <div class="filters">
    {#each STATUSES as s (s.id)}
      <button class="fchip" class:active={status === s.id} onclick={() => setStatus(s.id)}>
        {s.label}
      </button>
    {/each}
  </div>

  {#if poll.error && !poll.data}
    <div class="err-banner">failed to load workflows — {poll.error}</div>
  {:else if runs.length === 0 && poll.data}
    <div class="card empty">
      no workflow runs{status ? ' for this status' : ''}. Start one above to spin one up.
    </div>
  {:else}
    <div class="reactor">
      {#each runs as r, i (r.runId)}
        {@const t = tone(r)}
        {@const active = isRunning(r.status)}
        <a
          class="run"
          data-tone={t}
          href={runHref(r)}
          style="animation-delay:{Math.min(0.3, i * 0.025).toFixed(3)}s"
        >
          <div class="rhead">
            <span class="rid">
              <b>#{r.runId}</b>
              {#if num(r.parentRunId) > 0}
                <span class="parent" title={`child of run ${r.parentRunId}`}>↳ {r.parentRunId}</span>
              {/if}
              <span class="rtype"
                >{r.workflowType || 'workflow'} ·
                <span class="tenant" style="color:{groupColorHsl(r.tenantId)}"
                  >{r.tenantId || '(default)'}</span
                ></span
              >
            </span>
            <span class="pill {t}"><span class="dot"></span>{label(r)}</span>
          </div>

          <div class="prog" data-tone={t}>
            <i class:active style="width:{active ? 60 : 100}%"></i>
          </div>

          <div class="rfoot">
            <span>seq <b>{fmtInt(num(r.curHistorySeq))}</b></span>
            <span class="grow"></span>
            <span>{fmtAgo(r.lastEventMs)}</span>
            <span class="chev">›</span>
          </div>
        </a>
      {/each}
    </div>

    <div class="card pagerwrap">
      <Pager
        page={pageIdx + 1}
        {hasNext}
        {hasPrev}
        loading={poll.loading}
        count={runs.length}
        onnext={next}
        onprev={prev}
      />
    </div>
  {/if}
</div>

<style>
  .run-count {
    font-family: var(--mono);
    font-size: 11px;
    color: var(--active);
    letter-spacing: 0.06em;
    text-transform: uppercase;
  }

  .filters {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    margin: 4px 0 16px;
  }
  .fchip {
    padding: 4px 12px;
    border-radius: 999px;
    border: 1px solid var(--line2);
    background: var(--panel2);
    color: var(--faint);
    font-family: var(--mono);
    font-size: 11px;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    transition: all 0.1s ease;
  }
  .fchip:hover {
    color: var(--dim);
    border-color: var(--accent);
  }
  .fchip.active {
    color: var(--ink);
    background: var(--accent-glow);
    border-color: var(--accent);
  }

  .reactor {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(360px, 1fr));
    gap: 12px;
  }
  .run {
    display: block;
    background: #0a0c12;
    border: 1px solid var(--line);
    border-radius: 12px;
    padding: 13px 15px;
    position: relative;
    overflow: hidden;
    animation: rise 0.6s both;
    transition:
      border-color 0.12s ease,
      transform 0.12s ease;
  }
  .run:hover {
    transform: translateY(-2px);
  }
  .run[data-tone='inflight'] {
    border-color: rgba(247, 183, 51, 0.28);
  }
  .run[data-tone='delayed'] {
    border-color: rgba(124, 132, 244, 0.3);
  }
  .run[data-tone='ready'] {
    border-color: rgba(54, 226, 180, 0.26);
  }
  .run[data-tone='dlq'] {
    border-color: rgba(251, 106, 134, 0.28);
  }

  .rhead {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    margin-bottom: 10px;
  }
  .rid {
    font-size: 13px;
    color: var(--ink);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    min-width: 0;
  }
  .rid b {
    color: var(--ink);
    font-weight: 600;
  }
  .parent {
    margin: 0 4px;
    font-size: 10.5px;
    color: var(--ghost);
  }
  .rtype {
    font-size: 11px;
    color: var(--dim);
  }
  .tenant {
    font-weight: 500;
  }

  .prog {
    position: relative;
    height: 9px;
    border-radius: 5px;
    background: rgba(255, 255, 255, 0.05);
    overflow: hidden;
  }
  .prog i {
    position: absolute;
    inset: 0 auto 0 0;
    border-radius: 5px;
    transition: width 0.5s ease;
  }
  .prog[data-tone='inflight'] i {
    background: linear-gradient(90deg, rgba(247, 183, 51, 0.5), var(--active));
    box-shadow: 0 0 12px rgba(247, 183, 51, 0.5);
  }
  .prog[data-tone='delayed'] i {
    background: linear-gradient(90deg, rgba(124, 132, 244, 0.4), var(--delayed));
    box-shadow: 0 0 12px rgba(124, 132, 244, 0.4);
  }
  .prog[data-tone='ready'] i {
    background: linear-gradient(90deg, rgba(54, 226, 180, 0.4), var(--served));
    box-shadow: 0 0 12px rgba(54, 226, 180, 0.4);
  }
  .prog[data-tone='dlq'] i {
    background: linear-gradient(90deg, rgba(251, 106, 134, 0.4), var(--fail));
  }
  .prog[data-tone='paused'] i {
    background: linear-gradient(90deg, rgba(91, 100, 114, 0.4), var(--pause));
  }
  .prog i.active {
    animation: pulsebar 1.2s infinite;
  }

  .rfoot {
    display: flex;
    align-items: center;
    gap: 10px;
    font-size: 10.5px;
    color: var(--faint);
    margin-top: 9px;
  }
  .rfoot b {
    color: var(--dim);
    font-weight: 500;
  }
  .rfoot .grow {
    flex: 1;
  }
  .chev {
    color: var(--ghost);
    font-size: 16px;
    line-height: 1;
  }
  .run:hover .chev {
    color: var(--served);
  }

  .pagerwrap {
    margin-top: 14px;
    overflow: hidden;
  }
</style>
