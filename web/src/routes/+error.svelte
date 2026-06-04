<script lang="ts">
  import { page } from '$app/state';
</script>

<div class="page err-page">
  <div class="card box">
    <div class="code mono">{page.status}</div>
    <div class="msg">{page.error?.message ?? 'something went wrong'}</div>
    <a href="/" class="btn">&larr; back to lanes</a>
  </div>
</div>

<style>
  .err-page {
    display: flex;
    justify-content: center;
    padding-top: 80px;
  }
  .box {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 14px;
    padding: 44px 56px;
    text-align: center;
  }
  .code {
    font-size: 44px;
    font-weight: 700;
    color: var(--dlq);
  }
  .msg {
    color: var(--fg-dim);
  }
</style>
