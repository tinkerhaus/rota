<script lang="ts">
  import '../app.css';
  import ClusterHeader from '$lib/components/ClusterHeader.svelte';

  let { children } = $props();
</script>

<ClusterHeader />

<main>
  {@render children?.()}
</main>

<style>
  main {
    display: block;
  }
</style>
