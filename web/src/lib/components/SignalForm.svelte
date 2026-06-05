<script lang="ts">
  import { api, ApiError } from '$lib/api';

  // Signal-delivery form. POSTs {signalName, payload} to /api/workflows/{id}/signal
  // (leader-guarded). Appends a SIGNAL_RECEIVED event to the run's history.
  interface Props {
    runId: string;
    disabled?: boolean;
    onsignaled?: () => void;
  }
  let { runId, disabled = false, onsignaled }: Props = $props();

  let signalName = $state('');
  let payload = $state('');
  let busy = $state(false);
  let error = $state('');
  let sent = $state(false);

  const canSubmit = $derived(signalName.trim().length > 0 && !busy && !disabled);

  async function submit(e: SubmitEvent) {
    e.preventDefault();
    if (!canSubmit) return;
    busy = true;
    error = '';
    sent = false;
    try {
      await api.signalWorkflow(runId, { signalName: signalName.trim(), payload });
      sent = true;
      payload = '';
      onsignaled?.();
      setTimeout(() => (sent = false), 2500);
    } catch (err) {
      error = err instanceof ApiError ? `${err.status} ${err.body || err.message}` : String(err);
    } finally {
      busy = false;
    }
  }
</script>

<form class="sig" onsubmit={submit}>
  <label class="fld">
    <span class="flbl">signal name</span>
    <input
      class="in mono"
      type="text"
      placeholder="e.g. approve"
      bind:value={signalName}
      {disabled}
      required
    />
  </label>
  <label class="fld grow">
    <span class="flbl">payload</span>
    <input class="in mono" type="text" placeholder="(optional)" bind:value={payload} {disabled} />
  </label>
  <button class="btn" type="submit" disabled={!canSubmit}>
    {#if busy}<span class="spin"></span>{/if}signal
  </button>
  {#if sent}
    <span class="pill ready"><span class="dot"></span>sent</span>
  {/if}
  {#if error}
    <span class="err-banner">{error}</span>
  {/if}
</form>

<style>
  .sig {
    display: flex;
    align-items: flex-end;
    flex-wrap: wrap;
    gap: 12px;
  }
  .fld {
    display: flex;
    flex-direction: column;
    gap: 5px;
    min-width: 160px;
  }
  .fld.grow {
    flex: 1;
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
  .in:disabled {
    opacity: 0.5;
  }
  .in::placeholder {
    color: var(--fg-ghost);
  }
</style>
