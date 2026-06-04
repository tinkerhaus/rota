<script lang="ts">
  // Token-based pagination control. The parent owns the token stack; this just
  // renders prev/next + a page counter and emits intent via callbacks.
  interface Props {
    page: number; // 1-based, for display
    hasNext: boolean;
    hasPrev: boolean;
    loading?: boolean;
    count?: number; // rows on the current page
    onnext: () => void;
    onprev: () => void;
  }
  let { page, hasNext, hasPrev, loading = false, count, onnext, onprev }: Props = $props();
</script>

<div class="pager">
  {#if count !== undefined}
    <span class="faint mono">{count} on page</span>
  {/if}
  <div class="ctrls">
    <button class="btn sm" disabled={!hasPrev || loading} onclick={onprev}>&larr; prev</button>
    <span class="pg mono">page {page}</span>
    <button class="btn sm" disabled={!hasNext || loading} onclick={onnext}>next &rarr;</button>
  </div>
</div>

<style>
  .pager {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 14px;
    padding: 10px 14px;
    border-top: 1px solid var(--border);
    background: var(--bg-1);
    border-radius: 0 0 var(--radius) var(--radius);
  }
  .ctrls {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-left: auto;
  }
  .pg {
    font-size: 11.5px;
    color: var(--fg-dim);
    min-width: 56px;
    text-align: center;
  }
</style>
