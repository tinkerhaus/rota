<script lang="ts">
  import { api, ApiError } from '$lib/api';

  // Start-workflow form. POSTs {workflowType, tenantId, input} to /api/workflows
  // (leader-guarded). On success it notifies the parent so the list can refresh
  // and surfaces the new run id.
  interface Props {
    onstarted?: (runId: string) => void;
  }
  let { onstarted }: Props = $props();

  let workflowType = $state('');
  let tenantId = $state('');
  let input = $state('');
  let busy = $state(false);
  let error = $state('');
  let lastRun = $state('');

  const canSubmit = $derived(workflowType.trim().length > 0 && !busy);

  async function submit(e: SubmitEvent) {
    e.preventDefault();
    if (!canSubmit) return;
    busy = true;
    error = '';
    lastRun = '';
    try {
      const resp = await api.startWorkflow({
        workflowType: workflowType.trim(),
        tenantId: tenantId.trim(),
        input
      });
      const runId = String(resp.runId ?? '');
      lastRun = runId;
      input = '';
      onstarted?.(runId);
    } catch (err) {
      error = err instanceof ApiError ? `${err.status} ${err.body || err.message}` : String(err);
    } finally {
      busy = false;
    }
  }
</script>

<form class="card start" onsubmit={submit}>
  <div class="row">
    <label class="fld">
      <span class="flbl">workflow type</span>
      <input
        class="in mono"
        type="text"
        placeholder="e.g. order-fulfillment"
        bind:value={workflowType}
        required
      />
    </label>
    <label class="fld">
      <span class="flbl">tenant id</span>
      <input class="in mono" type="text" placeholder="(optional)" bind:value={tenantId} />
    </label>
  </div>
  <label class="fld">
    <span class="flbl">input</span>
    <textarea class="in mono ta" rows="2" placeholder="opaque input bytes (string)" bind:value={input}
    ></textarea>
  </label>
  <div class="actions">
    {#if lastRun}
      <span class="pill ready"><span class="dot"></span>started · run {lastRun}</span>
    {/if}
    {#if error}
      <span class="err-banner">start failed — {error}</span>
    {/if}
    <button class="btn primary" type="submit" disabled={!canSubmit}>
      {#if busy}<span class="spin"></span>{/if}start workflow
    </button>
  </div>
</form>

<style>
  .start {
    display: flex;
    flex-direction: column;
    gap: 12px;
    padding: 16px 18px;
  }
  .row {
    display: flex;
    flex-wrap: wrap;
    gap: 14px;
  }
  .fld {
    display: flex;
    flex-direction: column;
    gap: 5px;
    flex: 1;
    min-width: 200px;
  }
  .flbl {
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--fg-faint);
  }
  .in {
    background: var(--bg-2);
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
    color: var(--fg);
    font-size: 12.5px;
    padding: 7px 10px;
    width: 100%;
    transition: border-color 0.1s ease;
  }
  .in:focus {
    outline: none;
    border-color: var(--accent);
  }
  .in::placeholder {
    color: var(--fg-ghost);
  }
  .ta {
    resize: vertical;
    min-height: 38px;
    line-height: 1.4;
  }
  .actions {
    display: flex;
    align-items: center;
    gap: 12px;
  }
  .actions .err-banner {
    flex: 1;
  }
  .btn.primary {
    margin-left: auto;
    color: var(--fg);
    border-color: var(--accent);
    background: var(--accent-glow);
  }
  .btn.primary:hover:not(:disabled) {
    background: rgba(124, 140, 255, 0.32);
  }
</style>
