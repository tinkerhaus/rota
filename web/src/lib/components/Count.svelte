<script lang="ts">
  import { num } from '$lib/api';
  import { fmtInt } from '$lib/format';

  // A right-aligned monospace count, tinted by its semantic and ghosted at zero
  // so a busy grid reads at a glance (non-zero values pop, zeros recede).
  interface Props {
    value: string | number | undefined | null;
    kind?: 'ready' | 'inflight' | 'delayed' | 'dlq' | 'paused' | 'plain';
  }
  let { value, kind = 'plain' }: Props = $props();
  const n = $derived(num(value));
</script>

<span class="cnt num" class:zero={n === 0} class:c-ready={n > 0 && kind === 'ready'} class:c-inflight={n > 0 && kind === 'inflight'} class:c-delayed={n > 0 && kind === 'delayed'} class:c-dlq={n > 0 && kind === 'dlq'} class:c-paused={n > 0 && kind === 'paused'}>
  {fmtInt(n)}
</span>

<style>
  .cnt {
    font-size: 12.5px;
  }
  .zero {
    color: var(--fg-ghost);
  }
</style>
