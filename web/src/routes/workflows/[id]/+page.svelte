<script lang="ts">
  import { page } from '$app/state';
  import { api, num, ApiError, type HistoryEvent } from '$lib/api';
  import { createPoll, createClock } from '$lib/poll.svelte';
  import { groupColorHsl } from '$lib/colors';
  import { fmtInt, fmtAgo } from '$lib/format';
  import { isRunning, statusLabel, statusTone, eventLabel, eventTone } from '$lib/workflow';
  import SignalForm from '$lib/components/SignalForm.svelte';
  import SwimlaneTimeline from '$lib/components/SwimlaneTimeline.svelte';

  const runId = $derived(decodeURIComponent(page.params.id ?? ''));

  // Run header + history. Poll while RUNNING; createPoll skips overlapping ticks.
  const run = createPoll(() => api.workflowRun(runId), 2500);
  const history = createPoll(() => api.workflowHistory(runId), 2500);
  // A 1s clock drives the live now-line + running-bar extension in the swimlane.
  const clock = createClock(1000);
  $effect(() => {
    run.start();
    history.start();
    clock.start();
    return () => {
      run.stop();
      history.stop();
      clock.stop();
    };
  });

  const r = $derived(run.data);
  const running = $derived(isRunning(r?.status));
  const tone = $derived(statusTone(r?.status));
  const events = $derived<HistoryEvent[]>(
    [...(history.data?.events ?? [])].sort((a, b) => num(a.eventId) - num(b.eventId))
  );

  // Raw event log toggle (the swimlane is primary; the log is for drill-down).
  let showLog = $state(false);

  // Cancel action.
  let cancelReason = $state('');
  let cancelBusy = $state(false);
  let cancelErr = $state('');
  let canceled = $state(false);

  async function cancel() {
    if (cancelBusy) return;
    cancelBusy = true;
    cancelErr = '';
    try {
      const resp = await api.cancelWorkflow(runId, cancelReason.trim());
      canceled = resp.canceled ?? true;
      setTimeout(() => {
        void run.refresh();
        void history.refresh();
      }, 400);
    } catch (e) {
      cancelErr = e instanceof ApiError ? `${e.status} ${e.body || e.message}` : String(e);
    } finally {
      cancelBusy = false;
    }
  }
  function afterSignal() {
    setTimeout(() => {
      void run.refresh();
      void history.refresh();
    }, 400);
  }
</script>

<svelte:head><title>Rota — run {runId}</title></svelte:head>

<div class="page">
  <nav class="crumb">
    <a href="/workflows" class="back">← workflows</a>
    <span class="slash">/</span>
    <span class="cur mono">run {runId}</span>
  </nav>

  {#if run.error && !run.data}
    <div class="err-banner">failed to load run — {run.error}</div>
  {:else}
    <!-- Run header: status as a big colored state -->
    <div class="panel runhdr delay1" data-tone={tone}>
      <div class="state">
        <div class="big-state c-{tone === 'inflight' ? 'inflight' : tone === 'ready' ? 'ready' : tone === 'dlq' ? 'dlq' : tone === 'delayed' ? 'delayed' : 'paused'}">
          {statusLabel(r?.status)}
        </div>
        {#if running}<span class="chip live">live</span>{/if}
        {#if r?.wfTaskPending}<span class="pill inflight"><span class="dot"></span>task pending</span>{/if}
      </div>

      <div class="hmeta">
        <div class="m">
          <span class="k">type</span>
          <span class="v serif">{r?.workflowType || '(workflow)'}</span>
        </div>
        <div class="m">
          <span class="k">tenant</span>
          <span class="v mono" style="color:{groupColorHsl(r?.tenantId)}">{r?.tenantId || '(default)'}</span>
        </div>
        <div class="m"><span class="k">run id</span><span class="v num">{r?.runId ?? runId}</span></div>
        <div class="m"><span class="k">epoch</span><span class="v num">{fmtInt(num(r?.runEpoch))}</span></div>
        <div class="m"><span class="k">seq</span><span class="v num">{fmtInt(num(r?.curHistorySeq))}</span></div>
        <div class="m">
          <span class="k">parent</span>
          {#if num(r?.parentRunId) > 0}
            <a class="v mono link" href={`/workflows/${r?.parentRunId}`}>↳ {r?.parentRunId}</a>
          {:else}
            <span class="v mono faint">—</span>
          {/if}
        </div>
        <div class="m sep"></div>
        <div class="m"><span class="k">started</span><span class="v ago">{fmtAgo(r?.startedMs, clock.now)}</span></div>
        <div class="m"><span class="k">last event</span><span class="v ago">{fmtAgo(r?.lastEventMs, clock.now)}</span></div>
      </div>
    </div>

    <!-- Swimlane timeline (primary) -->
    <h3 class="section-title">
      timeline
      {#if history.loading && !history.data}<span class="spin"></span>{/if}
      {#if history.stale && history.data}<span class="pill delayed"><span class="dot"></span>stale</span>{/if}
    </h3>

    {#if history.error && !history.data}
      <div class="err-banner">failed to load history — {history.error}</div>
    {:else}
      <SwimlaneTimeline {events} {running} now={clock.now} />
    {/if}

    <!-- Actions: signal + cancel -->
    <h3 class="section-title">actions</h3>
    <div class="card actions">
      <SignalForm {runId} disabled={!running} onsignaled={afterSignal} />
      <div class="cancel-row">
        <input
          class="in mono"
          type="text"
          placeholder="cancel reason (optional)"
          bind:value={cancelReason}
          disabled={!running || cancelBusy}
        />
        <button class="btn danger" disabled={!running || cancelBusy} onclick={cancel}>
          {#if cancelBusy}<span class="spin"></span>{/if}cancel run
        </button>
        {#if canceled}<span class="pill dlq"><span class="dot"></span>cancel requested</span>{/if}
      </div>
      {#if cancelErr}<div class="err-banner">cancel failed — {cancelErr}</div>{/if}
      {#if !running}
        <div class="closed-note faint">run is closed — signal &amp; cancel are disabled.</div>
      {/if}
    </div>

    <!-- Raw event log (collapsible drill-down) -->
    <h3 class="section-title">
      event log
      <button class="logtoggle" onclick={() => (showLog = !showLog)}>
        {showLog ? 'hide' : 'show'} · {events.length}
      </button>
    </h3>
    {#if showLog}
      {#if events.length === 0 && history.data}
        <div class="card empty">no history events yet.</div>
      {:else}
        <div class="card logwrap">
          <table class="tbl">
            <thead>
              <tr>
                <th class="r" style="width:60px">#</th>
                <th>type</th>
                <th>time</th>
              </tr>
            </thead>
            <tbody>
              {#each events as ev (ev.eventId)}
                <tr>
                  <td class="num eid">{ev.eventId}</td>
                  <td>
                    <span class="pill {eventTone(ev.eventType)}"><span class="dot"></span>{eventLabel(ev.eventType)}</span>
                  </td>
                  <td class="ago" title={fmtInt(num(ev.eventTimeMs))}>{fmtAgo(ev.eventTimeMs, clock.now)}</td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      {/if}
    {/if}
  {/if}
</div>

<style>
  .crumb {
    display: flex;
    align-items: center;
    gap: 9px;
    margin-bottom: 16px;
    font-size: 12px;
  }
  .back {
    color: var(--dim);
  }
  .back:hover {
    color: var(--served);
  }
  .slash {
    color: var(--ghost);
  }
  .cur {
    color: var(--ink);
    font-weight: 500;
  }

  .runhdr {
    padding: 18px 22px;
    margin-bottom: 6px;
    border-left: 3px solid var(--line2);
  }
  .runhdr[data-tone='inflight'] {
    border-left-color: var(--active);
  }
  .runhdr[data-tone='ready'] {
    border-left-color: var(--served);
  }
  .runhdr[data-tone='dlq'] {
    border-left-color: var(--fail);
  }
  .runhdr[data-tone='delayed'] {
    border-left-color: var(--delayed);
  }
  .runhdr[data-tone='paused'] {
    border-left-color: var(--pause);
  }

  .state {
    display: flex;
    align-items: center;
    gap: 14px;
    margin-bottom: 16px;
  }
  .big-state {
    font-family: var(--serif);
    font-size: 38px;
    line-height: 0.9;
    letter-spacing: 0.5px;
    text-transform: lowercase;
  }

  .hmeta {
    display: flex;
    flex-wrap: wrap;
    align-items: flex-end;
    gap: 26px;
  }
  .m {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }
  .m.sep {
    width: 1px;
    align-self: stretch;
    background: var(--line);
    gap: 0;
  }
  .k {
    font-size: 9.5px;
    text-transform: uppercase;
    letter-spacing: 0.18em;
    color: var(--faint);
  }
  .v {
    font-size: 15px;
    color: var(--ink);
  }
  .v.serif {
    font-family: var(--serif);
    font-size: 19px;
  }
  .v.ago {
    font-size: 13px;
    color: var(--dim);
  }
  .v.link {
    color: var(--delayed);
  }

  .actions {
    padding: 16px 18px;
    display: flex;
    flex-direction: column;
    gap: 14px;
  }
  .cancel-row {
    display: flex;
    align-items: center;
    gap: 12px;
    flex-wrap: wrap;
  }
  .in {
    background: var(--bg2);
    border: 1px solid var(--line);
    border-radius: var(--radius-sm);
    color: var(--ink);
    font-size: 12.5px;
    padding: 8px 11px;
    flex: 1;
    min-width: 200px;
    transition: border-color 0.1s ease;
  }
  .in:focus {
    outline: none;
    border-color: var(--accent);
  }
  .in:disabled {
    opacity: 0.5;
  }
  .in::placeholder {
    color: var(--ghost);
  }
  .closed-note {
    font-size: 11.5px;
  }

  .logtoggle {
    margin-left: auto;
    font-family: var(--mono);
    font-size: 10.5px;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--dim);
    background: var(--panel2);
    border: 1px solid var(--line2);
    border-radius: 999px;
    padding: 3px 11px;
    transition: all 0.1s ease;
  }
  .logtoggle:hover {
    color: var(--ink);
    border-color: var(--accent);
  }
  .logwrap {
    overflow: hidden;
  }
  .eid {
    color: var(--dim);
  }
  .ago {
    color: var(--faint);
    font-size: 11px;
    white-space: nowrap;
  }
</style>
