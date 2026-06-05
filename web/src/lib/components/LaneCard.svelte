<script lang="ts">
  import { num, type LaneStats } from '$lib/api';
  import { groupColorHsl } from '$lib/colors';
  import { fmtInt, fmtRate } from '$lib/format';

  // A single lane card for the home grid. Renders a live throughput sparkline
  // (rolling client-side history of the lane's lease rate), a mini served-order
  // ribbon sampled from the lane's fairness group colors, the depth / inflight /
  // dlq counts and the policy version chip.
  interface Props {
    lane: LaneStats;
    history: number[]; // rolling lease-rate samples, oldest → newest
    ribbon: string[]; // group ids sampled for the mini-ribbon
    index?: number;
  }
  let { lane, history, ribbon, index = 0 }: Props = $props();

  const name = $derived(lane.lane || '(default)');
  const href = $derived(`/lanes/${encodeURIComponent(lane.lane ?? '')}`);

  // Normalise the sparkline against its own rolling max so quiet lanes still
  // show shape. A floor keeps an all-zero lane from collapsing to nothing.
  const peak = $derived(Math.max(0.0001, ...history));
  const bars = $derived(history.map((v) => Math.max(0.06, v / peak)));

  const hot = $derived(num(lane.dlqDepth) > 0);
</script>

<a class="lane" href={href} style="animation-delay:{(0.28 + index * 0.06).toFixed(2)}s">
  <div class="lhead">
    <h4>{name}</h4>
    <span class="pol">policy v{fmtInt(num(lane.policyVersion))}</span>
  </div>

  <div class="spark" title="lease rate (last {history.length} samples)">
    {#each bars as b, i (i)}
      <i style="height:{(b * 100).toFixed(0)}%" class:tip={i === bars.length - 1}></i>
    {/each}
  </div>

  <div class="lmeta">
    <span><b class="c-ready">{fmtInt(num(lane.leasable))}</b> ready</span>
    <span><b class="c-inflight">{fmtInt(num(lane.inflight))}</b> inflight</span>
    <span class:hot><b class:c-dlq={hot}>{fmtInt(num(lane.dlqDepth))}</b> dlq</span>
  </div>

  <div class="lrates">
    <span>{fmtRate(lane.publishRate)} pub</span>
    <span class="midd">·</span>
    <span>{fmtRate(lane.leaseRate)} lease</span>
    <span class="grow"></span>
    <span class="grp">{fmtInt(num(lane.groupCount))} groups</span>
  </div>

  {#if ribbon.length > 0}
    <div class="mini-rib" title="recently served groups">
      {#each ribbon as gid, i (i)}
        <i style="background:{groupColorHsl(gid)}"></i>
      {/each}
    </div>
  {/if}
</a>

<style>
  .lane {
    display: block;
    background: linear-gradient(180deg, var(--panel), var(--bg2));
    border: 1px solid var(--line);
    border-radius: 12px;
    padding: 14px 15px;
    position: relative;
    overflow: hidden;
    animation: rise 0.7s both;
    transition:
      border-color 0.12s ease,
      transform 0.12s ease;
  }
  .lane:hover {
    border-color: var(--line2);
    transform: translateY(-2px);
  }
  .lhead {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    gap: 10px;
  }
  .lhead h4 {
    margin: 0;
    font-family: var(--serif);
    font-size: 19px;
    font-weight: 400;
    color: var(--ink);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .pol {
    font-size: 9px;
    letter-spacing: 0.13em;
    text-transform: uppercase;
    color: var(--faint);
    white-space: nowrap;
  }

  .spark {
    height: 34px;
    margin: 11px 0 9px;
    display: flex;
    align-items: flex-end;
    gap: 2px;
  }
  .spark i {
    flex: 1;
    background: linear-gradient(180deg, var(--served), rgba(54, 226, 180, 0.15));
    border-radius: 2px 2px 0 0;
    transition: height 0.3s ease;
    min-height: 2px;
  }
  .spark i.tip {
    box-shadow: 0 0 10px rgba(54, 226, 180, 0.6);
  }

  .lmeta {
    display: flex;
    justify-content: space-between;
    font-size: 10.5px;
    color: var(--dim);
  }
  .lmeta b {
    color: var(--ink);
    font-weight: 500;
  }
  .lmeta .hot {
    color: var(--fail);
  }

  .lrates {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 10px;
    color: var(--faint);
    margin-top: 7px;
  }
  .lrates .grow {
    flex: 1;
  }
  .lrates .midd {
    color: var(--ghost);
  }
  .lrates .grp {
    color: var(--dim);
  }

  .mini-rib {
    display: flex;
    gap: 2px;
    height: 8px;
    margin-top: 10px;
  }
  .mini-rib i {
    flex: 1;
    border-radius: 1px;
    opacity: 0.85;
  }
</style>
