<script lang="ts">
  import { api, num, ApiError, type PolicyHealth, type PolicyValidation } from '$lib/api';
  import { createPoll } from '$lib/poll.svelte';
  import { fmtInt } from '$lib/format';

  // POLICY LAB — a CEL/WASM policy editor with a dry-run validator, sitting above
  // a prominent health banner (quarantine + faults from GetPolicyHealth). If the
  // gateway has no validate route yet, the editor stays usable and validation
  // degrades to the live health signal with a clear note.
  interface Props {
    lane: string;
  }
  let { lane }: Props = $props();

  // Live policy health — polled so quarantine/faults surface promptly.
  const health = createPoll<PolicyHealth>(() => api.policyHealth(lane), 4000);
  $effect(() => {
    health.start();
    return () => health.stop();
  });
  const h = $derived(health.data);

  const SAMPLE = `// CEL policy — gate which messages a group may lease.
// Available bindings: msg.headers, msg.attempt, group.id, group.weight, now
//
// Return true to allow the message to be served.
allow = msg.attempt < 5 && group.weight > 0
`;

  let source = $state(SAMPLE);
  let engine = $state<'cel' | 'wasm'>('cel');
  let validating = $state(false);
  // null = not run yet; otherwise the result of the last dry-run.
  let result = $state<PolicyValidation | null>(null);
  let resultErr = $state<string | undefined>(undefined);
  // True once we learn the validate route isn't wired on the gateway.
  let validateUnavailable = $state(false);

  async function validate() {
    validating = true;
    result = null;
    resultErr = undefined;
    try {
      const r = await api.validatePolicy(lane, source, engine);
      result = r;
      validateUnavailable = false;
    } catch (e) {
      if (e instanceof ApiError && [404, 405, 501].includes(e.status)) {
        // Route not implemented on this gateway. Fall back to the health probe
        // so the button still does something useful, and tell the operator why.
        validateUnavailable = true;
        await health.refresh();
        resultErr = undefined;
      } else if (e instanceof ApiError) {
        resultErr = `${e.status} ${e.body || e.message}`.trim();
      } else if (e instanceof Error) {
        resultErr = e.message;
      } else {
        resultErr = String(e);
      }
    } finally {
      validating = false;
    }
  }

  const valid = $derived(result ? (result.valid ?? result.ok ?? false) : undefined);
  const resultMsgs = $derived(
    result ? [...(result.errors ?? []), ...(result.diagnostics ?? [])] : []
  );
</script>

<div class="lab">
  <!-- Health banner — always visible, prominent when unhealthy. -->
  {#if health.error && !h}
    <div class="banner err-banner">failed to read policy health — {health.error}</div>
  {:else if h?.quarantined}
    <div class="banner crit">
      <span class="bdot"></span>
      <div class="btext">
        <strong>Policy QUARANTINED</strong> on lane <code class="mono">{lane || '(default)'}</code>
        — the evaluator was disabled after repeated faults; messages fall back to default scheduling.
      </div>
      <span class="bmeta mono">{h.engine || 'engine'} · v{fmtInt(num(h.version))}</span>
    </div>
  {:else if (h?.faults?.length ?? 0) > 0}
    <div class="banner warn">
      <span class="bdot"></span>
      <div class="btext">
        <strong>{h?.faults?.length} policy fault{(h?.faults?.length ?? 0) === 1 ? '' : 's'}</strong>
        observed on <code class="mono">{lane || '(default)'}</code> — not yet quarantined.
      </div>
      <span class="bmeta mono">{h?.engine || 'engine'} · v{fmtInt(num(h?.version))}</span>
    </div>
  {:else if h}
    <div class="banner ok">
      <span class="bdot"></span>
      <div class="btext">
        Policy healthy on <code class="mono">{lane || '(default)'}</code>.
      </div>
      <span class="bmeta mono">{h.engine || 'engine'} · v{fmtInt(num(h.version))}</span>
    </div>
  {/if}

  {#if (h?.faults?.length ?? 0) > 0}
    <ul class="faults card">
      {#each h?.faults ?? [] as f, i (i)}
        <li class="mono">{f}</li>
      {/each}
    </ul>
  {/if}

  <!-- Editor -->
  <div class="editor card">
    <div class="ed-head">
      <span class="elabel">policy source</span>
      <div class="engsel">
        <button class="seg" class:on={engine === 'cel'} onclick={() => (engine = 'cel')}>CEL</button>
        <button class="seg" class:on={engine === 'wasm'} onclick={() => (engine = 'wasm')}>WASM</button>
      </div>
      <span class="grow"></span>
      <button class="btn" disabled={validating || source.trim().length === 0} onclick={validate}>
        {#if validating}<span class="spin"></span>{/if}
        Validate (dry-run)
      </button>
    </div>
    <textarea
      class="src mono"
      bind:value={source}
      spellcheck="false"
      autocomplete="off"
      autocapitalize="off"
      placeholder="// write a {engine.toUpperCase()} policy…"
    ></textarea>
  </div>

  <!-- Validation result -->
  {#if validateUnavailable}
    <div class="result note">
      <span class="rdot note"></span>
      <div class="rtext">
        <strong>Dry-run endpoint not wired on this gateway.</strong>
        Validation falls back to the live policy-health probe above
        (<code class="mono">GET /api/policy/{lane || '(default)'}/health</code>).
        <span class="todo">TODO: add <code class="mono">POST /api/lanes/{'{lane}'}/policy/validate</code> → Control.ValidatePolicy.</span>
      </div>
    </div>
  {:else if resultErr}
    <div class="result bad">
      <span class="rdot bad"></span>
      <div class="rtext"><strong>Validation request failed.</strong> <code class="mono">{resultErr}</code></div>
    </div>
  {:else if result}
    <div class="result" class:good={valid} class:bad={!valid}>
      <span class="rdot" class:good={valid} class:bad={!valid}></span>
      <div class="rtext">
        <strong>{valid ? 'Policy is valid.' : 'Policy is invalid.'}</strong>
        {#if result.message}<span class="rmsg">{result.message}</span>{/if}
        {#if resultMsgs.length > 0}
          <ul class="diags mono">
            {#each resultMsgs as m, i (i)}<li>{m}</li>{/each}
          </ul>
        {/if}
      </div>
    </div>
  {/if}
</div>

<style>
  .lab {
    display: flex;
    flex-direction: column;
    gap: 14px;
  }

  /* Banner */
  .banner {
    display: flex;
    align-items: center;
    gap: 11px;
    padding: 11px 15px;
    border-radius: var(--radius-sm);
    border: 1px solid var(--border);
    font-size: 12.5px;
  }
  .banner .bdot {
    width: 9px;
    height: 9px;
    border-radius: 50%;
    flex: none;
  }
  .banner .btext {
    flex: 1;
    line-height: 1.5;
  }
  .banner .bmeta {
    font-size: 11px;
    color: var(--fg-faint);
    white-space: nowrap;
  }
  .banner code {
    color: var(--fg);
  }
  .banner.ok {
    background: var(--ready-bg);
    border-color: var(--ready-bd);
    color: var(--fg-dim);
  }
  .banner.ok .bdot {
    background: var(--ready);
    box-shadow: 0 0 7px var(--ready);
  }
  .banner.warn {
    background: var(--delayed-bg);
    border-color: var(--delayed-bd);
    color: #f5d79a;
  }
  .banner.warn .bdot {
    background: var(--delayed);
    box-shadow: 0 0 8px var(--delayed);
  }
  .banner.crit {
    background: var(--dlq-bg);
    border-color: var(--dlq-bd);
    color: #ffb3ae;
  }
  .banner.crit .bdot {
    background: var(--dlq);
    box-shadow: 0 0 9px var(--dlq);
    animation: pulse 1.5s ease-in-out infinite;
  }

  .faults {
    list-style: none;
    margin: 0;
    padding: 8px 0;
  }
  .faults li {
    padding: 6px 16px;
    font-size: 11.5px;
    color: var(--dlq);
    border-bottom: 1px solid var(--border);
  }
  .faults li:last-child {
    border-bottom: none;
  }

  /* Editor */
  .editor {
    overflow: hidden;
  }
  .ed-head {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 10px 14px;
    border-bottom: 1px solid var(--border);
    background: var(--bg-3);
  }
  .elabel {
    font-size: 10.5px;
    text-transform: uppercase;
    letter-spacing: 0.07em;
    color: var(--fg-faint);
  }
  .grow {
    flex: 1;
  }
  .engsel {
    display: inline-flex;
    border: 1px solid var(--border-strong);
    border-radius: var(--radius-sm);
    overflow: hidden;
  }
  .seg {
    padding: 3px 11px;
    background: var(--bg-1);
    border: none;
    color: var(--fg-faint);
    font-size: 11px;
    font-weight: 600;
  }
  .seg.on {
    background: var(--bg-hover);
    color: var(--accent);
  }
  .seg + .seg {
    border-left: 1px solid var(--border-strong);
  }
  .src {
    display: block;
    width: 100%;
    min-height: 220px;
    resize: vertical;
    padding: 14px 16px;
    background: var(--bg-1);
    color: var(--fg);
    border: none;
    outline: none;
    font-size: 12.5px;
    line-height: 1.6;
    tab-size: 2;
  }
  .src::placeholder {
    color: var(--fg-ghost);
  }

  /* Result */
  .result {
    display: flex;
    align-items: flex-start;
    gap: 11px;
    padding: 12px 15px;
    border-radius: var(--radius-sm);
    border: 1px solid var(--border);
    background: var(--bg-2);
    font-size: 12.5px;
  }
  .result .rdot {
    width: 9px;
    height: 9px;
    border-radius: 50%;
    margin-top: 4px;
    flex: none;
    background: var(--paused);
  }
  .result .rtext {
    line-height: 1.55;
  }
  .result.good {
    background: var(--ready-bg);
    border-color: var(--ready-bd);
  }
  .result.good .rdot,
  .rdot.good {
    background: var(--ready);
    box-shadow: 0 0 7px var(--ready);
  }
  .result.bad {
    background: var(--dlq-bg);
    border-color: var(--dlq-bd);
  }
  .result.bad .rdot,
  .rdot.bad {
    background: var(--dlq);
    box-shadow: 0 0 7px var(--dlq);
  }
  .result.note {
    background: var(--bg-3);
    border-color: var(--border-strong);
  }
  .rdot.note {
    background: var(--accent);
    box-shadow: 0 0 7px var(--accent-glow);
  }
  .rmsg {
    color: var(--fg-dim);
  }
  .diags {
    margin: 6px 0 0;
    padding-left: 18px;
    color: var(--fg-dim);
    font-size: 11.5px;
  }
  .diags li {
    margin: 2px 0;
  }
  .todo {
    display: block;
    margin-top: 5px;
    color: var(--fg-faint);
    font-size: 11.5px;
  }
  .todo code {
    color: var(--accent);
  }
</style>
