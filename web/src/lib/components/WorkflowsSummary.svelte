<script lang="ts">
  import { api, num, type WorkflowRun } from '$lib/api';
  import { createPoll } from '$lib/poll.svelte';
  import { groupColorHsl } from '$lib/colors';
  import { fmtInt, fmtAgo } from '$lib/format';
  import { statusTone, statusLabel, isRunning } from '$lib/workflow';

  // Compact workflows panel for the home view: a running count plus a few of the
  // most-recent active runs rendered as filling bars. Pipelines have no known
  // target, so an active run shows an animated bar and curHistorySeq as a
  // magnitude label rather than a faked percentage.
  const poll = createPoll(() => api.workflows({ pageSize: 24 }), 3000);
  $effect(() => {
    poll.start();
    return () => poll.stop();
  });

  const runs = $derived<WorkflowRun[]>(poll.data?.runs ?? []);
  const running = $derived(runs.filter((r) => isRunning(r.status)).length);

  // Prefer active runs first, then most-recently-touched; cap to a handful.
  const shown = $derived(
    [...runs]
      .sort((a, b) => {
        const ar = isRunning(a.status) ? 1 : 0;
        const br = isRunning(b.status) ? 1 : 0;
        if (ar !== br) return br - ar;
        return num(b.lastEventMs) - num(a.lastEventMs);
      })
      .slice(0, 5)
  );

  function href(r: WorkflowRun): string {
    return `/workflows/${encodeURIComponent(String(r.runId ?? ''))}`;
  }
</script>

<div class="panel delay2 wfpanel">
  <div class="phead">
    <div>
      <div class="ptitle">Workflows</div>
      <div class="psub">{running} running · {runs.length} recent</div>
    </div>
    {#if poll.data}<span class="chip live">live</span>{/if}
  </div>

  {#if poll.error && !poll.data}
    <div class="err-banner">failed to load workflows — {poll.error}</div>
  {:else if shown.length === 0 && poll.data}
    <div class="empty small">no workflow runs yet.</div>
  {:else}
    <div class="wf">
      {#each shown as r (r.runId)}
        {@const tone = statusTone(r.status)}
        {@const active = isRunning(r.status)}
        <a class="run" class:active href={href(r)}>
          <div class="rhead">
            <span class="rid"
              >#{r.runId}
              <span class="rtype"
                >{r.workflowType || 'workflow'} ·
                <span style="color:{groupColorHsl(r.tenantId)}">{r.tenantId || '(default)'}</span
                ></span
              ></span
            >
            <span class="pill {tone}"><span class="dot"></span>{statusLabel(r.status)}</span>
          </div>
          <div class="prog" data-tone={tone}>
            <i class:active style="width:{active ? 62 : 100}%"></i>
          </div>
          <div class="rfoot">
            <span>seq <b>{fmtInt(num(r.curHistorySeq))}</b></span>
            <span>{fmtAgo(r.lastEventMs)}</span>
          </div>
        </a>
      {/each}
    </div>
    <a class="allwf" href="/workflows">all workflows →</a>
  {/if}
</div>

<style>
  .wfpanel {
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
    font-size: 23px;
    letter-spacing: 0.3px;
  }
  .psub {
    font-size: 10px;
    letter-spacing: 0.16em;
    color: var(--faint);
    text-transform: uppercase;
    margin-top: 2px;
  }
  .empty.small {
    padding: 24px 8px;
    text-align: center;
    color: var(--faint);
    font-size: 12px;
  }

  .wf {
    display: flex;
    flex-direction: column;
    gap: 11px;
  }
  .run {
    display: block;
    background: #0a0c12;
    border: 1px solid var(--line);
    border-radius: 10px;
    padding: 12px 13px;
    position: relative;
    overflow: hidden;
    transition: border-color 0.12s ease;
  }
  .run:hover {
    border-color: var(--line2);
  }
  .run.active {
    border-color: rgba(247, 183, 51, 0.28);
  }
  .rhead {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    margin-bottom: 9px;
  }
  .rid {
    font-size: 13px;
    color: var(--ink);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .rtype {
    font-size: 11px;
    color: var(--dim);
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
  /* color the bar by status tone */
  .prog[data-tone='inflight'] i {
    background: linear-gradient(90deg, rgba(247, 183, 51, 0.5), var(--active));
    box-shadow: 0 0 12px rgba(247, 183, 51, 0.5);
  }
  .prog[data-tone='ready'] i {
    background: linear-gradient(90deg, rgba(54, 226, 180, 0.4), var(--served));
    box-shadow: 0 0 12px rgba(54, 226, 180, 0.4);
  }
  .prog[data-tone='delayed'] i {
    background: linear-gradient(90deg, rgba(124, 132, 244, 0.4), var(--delayed));
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
    justify-content: space-between;
    font-size: 10.5px;
    color: var(--faint);
    margin-top: 8px;
  }
  .rfoot b {
    color: var(--dim);
    font-weight: 500;
  }

  .allwf {
    display: inline-block;
    margin-top: 14px;
    font-size: 11px;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--dim);
  }
  .allwf:hover {
    color: var(--served);
  }
</style>
