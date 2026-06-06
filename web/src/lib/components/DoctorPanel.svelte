<script lang="ts">
  import {
    api,
    num,
    type ClusterInfo,
    type GroupStats,
    type HealthResponse,
    type LaneStats,
    type LeaseInfo,
    type WorkflowRun
  } from '$lib/api';
  import { fmtAgo, fmtCountdown, fmtInt } from '$lib/format';
  import { createPoll } from '$lib/poll.svelte';

  type Severity = 'FAIL' | 'WARN';
  type Status = 'OK' | Severity;

  interface Finding {
    severity: Severity;
    code: string;
    summary: string;
    lane?: string;
    groupId?: string;
    msgId?: string | number;
    leaseId?: string | number;
    runId?: string | number;
    tenantId?: string;
    deadlineMs?: number;
  }

  interface LaneDoctor {
    lane: string;
    pausedGroups: number;
    leases: number;
    expiredLeases: number;
    nearDeadlineLeases: number;
  }

  interface DoctorSnapshot {
    checkedAtMs: number;
    status: Status;
    cluster: {
      leaderId: string;
      peers: number;
      serving: boolean;
      hasQuorum: boolean;
    };
    lanes: number;
    inspectedLanes: number;
    workflows: {
      running: number;
      taskPending: number;
      possiblyStuck: number;
    };
    laneChecks: LaneDoctor[];
    findings: Finding[];
    findingTotal: number;
    failCount: number;
    warnCount: number;
  }

  const PAGE_SIZE = 80;
  const MAX_DEEP_LANES = 32;
  const MAX_FINDINGS = 8;
  const LEASE_WARN_MS = 5000;

  const doctor = createPoll(loadDoctor, 3500);
  $effect(() => {
    doctor.start();
    return () => doctor.stop();
  });

  const snapshot = $derived(doctor.data);
  const shownFindings = $derived(snapshot?.findings.slice(0, MAX_FINDINGS) ?? []);

  async function loadDoctor(): Promise<DoctorSnapshot> {
    const checkedAtMs = Date.now();
    const [cluster, health, stats, workflows] = await Promise.all([
      api.cluster(),
      api.health(),
      api.stats(),
      api.workflows({ status: 'WF_RUNNING', pageSize: 200 })
    ]);

    const findings: Finding[] = [];
    const lanes = [...(stats.lanes ?? [])].sort((a, b) =>
      (a.lane ?? '').localeCompare(b.lane ?? '')
    );
    const statsByLane = new Map(lanes.map((lane) => [lane.lane ?? '', lane]));

    collectClusterFindings(cluster, health, findings);
    collectLaneStatsFindings(lanes, findings);

    const deepLanes = lanes
      .slice()
      .sort((a, b) => laneInterest(b) - laneInterest(a) || (a.lane ?? '').localeCompare(b.lane ?? ''))
      .slice(0, MAX_DEEP_LANES);
    const laneChecks = await Promise.all(
      deepLanes.map((lane) => collectLaneDeepFindings(lane.lane ?? '', checkedAtMs, findings))
    );

    const workflowRuns = workflows.runs ?? [];
    const workflowReport = collectWorkflowFindings(workflowRuns, statsByLane, findings);
    const status = statusFor(findings);
    const failCount = findings.filter((finding) => finding.severity === 'FAIL').length;
    const warnCount = findings.filter((finding) => finding.severity === 'WARN').length;

    return {
      checkedAtMs,
      status,
      cluster: {
        leaderId: cluster.leaderId ?? '',
        peers: cluster.peers?.length ?? 0,
        serving: !!health.serving,
        hasQuorum: !!health.hasQuorum
      },
      lanes: lanes.length,
      inspectedLanes: deepLanes.length,
      workflows: workflowReport,
      laneChecks,
      findings,
      findingTotal: findings.length,
      failCount,
      warnCount
    };
  }

  function collectClusterFindings(
    cluster: ClusterInfo,
    health: HealthResponse,
    findings: Finding[]
  ) {
    if (!health.serving) {
      findings.push({
        severity: 'FAIL',
        code: 'not_serving',
        summary: 'node is not serving'
      });
    }
    if (!health.hasQuorum) {
      findings.push({
        severity: 'FAIL',
        code: 'no_quorum',
        summary: 'cluster does not currently have quorum'
      });
    }
    if (!cluster.leaderId) {
      findings.push({
        severity: 'WARN',
        code: 'leader_unknown',
        summary: 'leader is not known from this node'
      });
    }
  }

  function collectLaneStatsFindings(lanes: LaneStats[], findings: Finding[]) {
    for (const lane of lanes) {
      const dlqDepth = num(lane.dlqDepth);
      if (dlqDepth > 0) {
        findings.push({
          severity: 'WARN',
          code: 'dlq_depth',
          summary: `lane has ${fmtInt(dlqDepth)} dead letters`,
          lane: lane.lane
        });
      }
    }
  }

  async function collectLaneDeepFindings(
    lane: string,
    checkedAtMs: number,
    findings: Finding[]
  ): Promise<LaneDoctor> {
    const report: LaneDoctor = {
      lane,
      pausedGroups: 0,
      leases: 0,
      expiredLeases: 0,
      nearDeadlineLeases: 0
    };

    try {
      const [groups, leases] = await Promise.all([
        api.groups(lane, { pageSize: PAGE_SIZE }),
        api.leases(lane, { pageSize: PAGE_SIZE })
      ]);
      for (const group of groups.groups ?? []) {
        collectGroupFinding(lane, group, findings, report);
      }
      for (const lease of leases.leases ?? []) {
        collectLeaseFinding(lane, lease, checkedAtMs, findings, report);
      }
    } catch (e) {
      findings.push({
        severity: 'WARN',
        code: 'doctor_lane_read_failed',
        summary: e instanceof Error ? e.message : 'lane inspection failed',
        lane
      });
    }

    return report;
  }

  function collectGroupFinding(
    lane: string,
    group: GroupStats,
    findings: Finding[],
    report: LaneDoctor
  ) {
    if (!group.paused) return;
    report.pausedGroups += 1;
    const backlog = num(group.ready) + num(group.delayed) + num(group.inflight);
    if (backlog > 0) {
      findings.push({
        severity: 'WARN',
        code: 'paused_group_backlog',
        summary: 'paused group still has queued or in-flight work',
        lane,
        groupId: group.groupId
      });
    }
  }

  function collectLeaseFinding(
    lane: string,
    lease: LeaseInfo,
    checkedAtMs: number,
    findings: Finding[],
    report: LaneDoctor
  ) {
    report.leases += 1;
    const deadlineMs = num(lease.deadlineMs);
    if (!deadlineMs) return;
    if (deadlineMs <= checkedAtMs) {
      report.expiredLeases += 1;
      findings.push({
        severity: 'WARN',
        code: 'lease_past_deadline',
        summary: 'lease is past its visibility deadline and is still present',
        lane,
        groupId: lease.groupId,
        msgId: lease.msgId,
        leaseId: lease.leaseId,
        deadlineMs
      });
    } else if (deadlineMs <= checkedAtMs + LEASE_WARN_MS) {
      report.nearDeadlineLeases += 1;
      findings.push({
        severity: 'WARN',
        code: 'lease_near_deadline',
        summary: 'lease is close to its visibility deadline',
        lane,
        groupId: lease.groupId,
        msgId: lease.msgId,
        leaseId: lease.leaseId,
        deadlineMs
      });
    }
  }

  function collectWorkflowFindings(
    runs: WorkflowRun[],
    statsByLane: Map<string, LaneStats>,
    findings: Finding[]
  ): DoctorSnapshot['workflows'] {
    const report = { running: 0, taskPending: 0, possiblyStuck: 0 };
    for (const run of runs) {
      report.running += 1;
      if (!run.wfTaskPending) continue;
      report.taskPending += 1;
      const lane = `__wf/${run.workflowType ?? ''}`;
      const st = statsByLane.get(lane);
      if (!st || num(st.leasable) + num(st.delayed) + num(st.inflight) === 0) {
        report.possiblyStuck += 1;
        findings.push({
          severity: 'FAIL',
          code: 'workflow_task_missing',
          summary: 'workflow has a pending task but no visible workflow-task lane depth',
          lane,
          runId: run.runId,
          tenantId: run.tenantId
        });
      }
    }
    return report;
  }

  function laneInterest(lane: LaneStats): number {
    return (
      num(lane.dlqDepth) * 1000 +
      num(lane.inflight) * 50 +
      num(lane.leasable) * 10 +
      num(lane.delayed) * 5 +
      num(lane.oldestAgeMs) / 1000
    );
  }

  function statusFor(findings: Finding[]): Status {
    if (findings.some((finding) => finding.severity === 'FAIL')) return 'FAIL';
    if (findings.some((finding) => finding.severity === 'WARN')) return 'WARN';
    return 'OK';
  }

  function statusTone(status: Status): string {
    if (status === 'FAIL') return 'dlq';
    if (status === 'WARN') return 'inflight';
    return 'ready';
  }

  function severityTone(severity: Severity): string {
    return severity === 'FAIL' ? 'dlq' : 'inflight';
  }

  function meta(finding: Finding): string[] {
    const parts: string[] = [];
    if (finding.lane) parts.push(`lane ${finding.lane}`);
    if (finding.groupId) parts.push(`group ${finding.groupId}`);
    if (finding.tenantId) parts.push(`tenant ${finding.tenantId}`);
    if (finding.runId !== undefined) parts.push(`run #${finding.runId}`);
    if (finding.leaseId !== undefined) parts.push(`lease #${finding.leaseId}`);
    if (finding.msgId !== undefined) parts.push(`msg #${finding.msgId}`);
    if (finding.deadlineMs) {
      const deadline = fmtCountdown(finding.deadlineMs);
      parts.push(deadline.expired ? `deadline ${deadline.text}` : `deadline in ${deadline.text}`);
    }
    return parts;
  }

  function metaText(finding: Finding): string {
    return meta(finding).join(' · ');
  }
</script>

<div class="panel delay1 doctor">
  <div class="phead">
    <div>
      <div class="ptitle">Doctor</div>
      <div class="psub">
        {#if snapshot}
          {fmtAgo(snapshot.checkedAtMs)}
        {:else if doctor.loading}
          scanning
        {:else}
          idle
        {/if}
      </div>
    </div>
    {#if snapshot}
      <span class="pill {statusTone(snapshot.status)}"><span class="dot"></span>{snapshot.status}</span>
    {:else if doctor.loading}
      <span class="spin"></span>
    {/if}
  </div>

  {#if doctor.error && !snapshot}
    <div class="err-banner">doctor unavailable — {doctor.error}</div>
  {:else if snapshot}
    <div class="score">
      <div class="verdict {statusTone(snapshot.status)}">
        <span class="code">{snapshot.status}</span>
        <span>{snapshot.failCount} fail · {snapshot.warnCount} warn</span>
      </div>
      {#if doctor.stale}
        <span class="pill delayed"><span class="dot"></span>stale</span>
      {:else}
        <span class="chip live">live</span>
      {/if}
    </div>

    <div class="facts">
      <div>
        <span class="k">cluster</span>
        <span class="v">
          {#if snapshot.cluster.serving && snapshot.cluster.hasQuorum}
            serving
          {:else}
            degraded
          {/if}
        </span>
      </div>
      <div>
        <span class="k">leader</span>
        <span class="v">{snapshot.cluster.leaderId || 'unknown'}</span>
      </div>
      <div>
        <span class="k">lanes</span>
        <span class="v num">{fmtInt(snapshot.inspectedLanes)} / {fmtInt(snapshot.lanes)}</span>
      </div>
      <div>
        <span class="k">workflows</span>
        <span class="v num">{fmtInt(snapshot.workflows.running)}</span>
      </div>
    </div>

    {#if shownFindings.length === 0}
      <div class="empty small">all checks passing.</div>
    {:else}
      <div class="findings">
        {#each shownFindings as finding, i (`${finding.code}-${i}`)}
          {@const details = metaText(finding)}
          <div class="finding" data-severity={finding.severity}>
            <div class="fhead">
              <span class="pill {severityTone(finding.severity)}"><span class="dot"></span>{finding.severity}</span>
              <span class="fcode">{finding.code}</span>
            </div>
            <div class="fsummary">{finding.summary}</div>
            {#if details}
              <div class="fmeta">{details}</div>
            {/if}
          </div>
        {/each}
        {#if snapshot.findingTotal > shownFindings.length}
          <div class="more">+{fmtInt(snapshot.findingTotal - shownFindings.length)} more findings</div>
        {/if}
      </div>
    {/if}
  {/if}
</div>

<style>
  .doctor {
    padding: 16px 18px;
  }
  .phead {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 14px;
  }
  .ptitle {
    font-family: var(--serif);
    font-size: 23px;
    letter-spacing: 0.3px;
  }
  .psub {
    font-size: 10px;
    letter-spacing: 0.16em;
    color: var(--faint);
    text-transform: uppercase;
    margin-top: 2px;
  }

  .score {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    border: 1px solid var(--line);
    border-radius: var(--radius-sm);
    background: #0a0c12;
    padding: 11px 12px;
    margin-bottom: 12px;
  }
  .verdict {
    display: flex;
    flex-direction: column;
    gap: 1px;
    font-size: 10px;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: var(--faint);
  }
  .verdict .code {
    font-size: 18px;
    line-height: 1;
    color: var(--ink);
    letter-spacing: 0.08em;
  }
  .verdict.ready .code {
    color: var(--served);
  }
  .verdict.inflight .code {
    color: var(--active);
  }
  .verdict.dlq .code {
    color: var(--fail);
  }

  .facts {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 1px;
    overflow: hidden;
    border: 1px solid var(--line);
    border-radius: var(--radius-sm);
    margin-bottom: 12px;
    background: var(--line);
  }
  .facts div {
    min-width: 0;
    background: var(--panel2);
    padding: 9px 10px;
    display: flex;
    flex-direction: column;
    gap: 2px;
  }
  .facts .k {
    font-size: 9px;
    color: var(--faint);
    letter-spacing: 0.16em;
    text-transform: uppercase;
  }
  .facts .v {
    min-width: 0;
    color: var(--ink);
    font-size: 12px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .empty.small {
    padding: 18px 8px 6px;
    text-align: center;
    color: var(--faint);
    font-size: 12px;
  }
  .findings {
    display: flex;
    flex-direction: column;
    gap: 9px;
  }
  .finding {
    border: 1px solid var(--line);
    border-radius: var(--radius-sm);
    background: #0a0c12;
    padding: 10px 11px;
  }
  .finding[data-severity='FAIL'] {
    border-color: rgba(251, 106, 134, 0.28);
  }
  .finding[data-severity='WARN'] {
    border-color: rgba(247, 183, 51, 0.24);
  }
  .fhead {
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
    margin-bottom: 7px;
  }
  .fcode {
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    color: var(--faint);
    font-size: 10px;
    letter-spacing: 0.12em;
    text-transform: uppercase;
  }
  .fsummary {
    color: var(--ink);
    font-size: 12px;
    line-height: 1.35;
  }
  .fmeta {
    margin-top: 5px;
    color: var(--faint);
    font-size: 10.5px;
    line-height: 1.35;
    overflow-wrap: anywhere;
  }
  .more {
    color: var(--faint);
    font-size: 10px;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    text-align: center;
    padding: 3px 0 1px;
  }

  @media (max-width: 520px) {
    .facts {
      grid-template-columns: 1fr;
    }
  }
</style>
