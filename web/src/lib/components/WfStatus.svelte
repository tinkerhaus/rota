<script lang="ts">
  import type { WorkflowStatus } from '$lib/api';
  import { statusTone, statusLabel } from '$lib/workflow';

  // A status pill for a workflow run, tinted by the shared semantic language:
  //   RUNNING blue · COMPLETED green · FAILED/CANCELED red · CONTINUED grey.
  interface Props {
    status: WorkflowStatus | string | undefined;
  }
  let { status }: Props = $props();
  const tone = $derived(statusTone(status));
  const label = $derived(statusLabel(status));
</script>

<span class="pill {tone}"><span class="dot"></span>{label}</span>
