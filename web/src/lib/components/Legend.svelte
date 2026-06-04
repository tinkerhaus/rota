<script lang="ts">
  // The shared color language, made explicit. Shown on detail views so the grid
  // is self-documenting.
  const items = [
    { c: 'ready', label: 'ready' },
    { c: 'inflight', label: 'inflight' },
    { c: 'delayed', label: 'delayed' },
    { c: 'dlq', label: 'dlq / dead' },
    { c: 'paused', label: 'paused' }
  ];
</script>

<div class="legend">
  {#each items as it (it.c)}
    <span class="item">
      <span class="swatch {it.c}"></span>{it.label}
    </span>
  {/each}
</div>

<style>
  .legend {
    display: flex;
    flex-wrap: wrap;
    gap: 14px;
    align-items: center;
  }
  .item {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-size: 11px;
    color: var(--fg-dim);
  }
  .swatch {
    width: 10px;
    height: 10px;
    border-radius: 3px;
  }
  .swatch.ready {
    background: var(--ready);
  }
  .swatch.inflight {
    background: var(--inflight);
  }
  .swatch.delayed {
    background: var(--delayed);
  }
  .swatch.dlq {
    background: var(--dlq);
  }
  .swatch.paused {
    background: var(--paused);
  }
</style>
