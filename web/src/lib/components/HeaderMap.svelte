<script lang="ts">
  // Compact key=value rendering for a proto map<string,string> (headers etc).
  interface Props {
    map?: Record<string, string>;
    tone?: 'plain' | 'fail';
    empty?: string;
  }
  let { map, tone = 'plain', empty = '—' }: Props = $props();
  const entries = $derived(Object.entries(map ?? {}));
</script>

{#if entries.length === 0}
  <span class="faint">{empty}</span>
{:else}
  <div class="hm" class:fail={tone === 'fail'}>
    {#each entries as [k, v] (k)}
      <span class="kvp" title={`${k}=${v}`}>
        <span class="k">{k}</span><span class="eq">=</span><span class="v">{v}</span>
      </span>
    {/each}
  </div>
{/if}

<style>
  .hm {
    display: flex;
    flex-wrap: wrap;
    gap: 4px 6px;
  }
  .kvp {
    display: inline-flex;
    align-items: baseline;
    max-width: 260px;
    padding: 1px 6px;
    border-radius: 4px;
    background: var(--bg-3);
    border: 1px solid var(--border);
    font-family: var(--mono);
    font-size: 11px;
    overflow: hidden;
  }
  .fail .kvp {
    background: var(--dlq-bg);
    border-color: var(--dlq-bd);
  }
  .k {
    color: var(--fg-dim);
  }
  .eq {
    color: var(--fg-ghost);
    margin: 0 1px;
  }
  .v {
    color: var(--fg);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
</style>
