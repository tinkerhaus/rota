package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
)

type doctorOptions struct {
	clientOptions
	lane           string
	pageSize       uint
	json           bool
	failOnWarn     bool
	leaseWarnAfter time.Duration
}

type doctorReport struct {
	CheckedAtMs uint64          `json:"checked_at_ms"`
	Status      string          `json:"status"`
	Cluster     doctorCluster   `json:"cluster"`
	Lanes       []doctorLane    `json:"lanes"`
	Workflows   doctorWorkflows `json:"workflows"`
	Findings    []doctorFinding `json:"findings"`
}

type doctorCluster struct {
	Serving      bool   `json:"serving"`
	HasQuorum    bool   `json:"has_quorum"`
	IsLeader     bool   `json:"is_leader"`
	LeaderID     string `json:"leader_id"`
	LeaderAddr   string `json:"leader_addr"`
	Term         uint64 `json:"term"`
	AppliedIndex uint64 `json:"applied_index"`
	PeerCount    int    `json:"peer_count"`
}

type doctorLane struct {
	Lane               string  `json:"lane"`
	Leasable           uint64  `json:"leasable"`
	Delayed            uint64  `json:"delayed"`
	Inflight           uint64  `json:"inflight"`
	DLQDepth           uint64  `json:"dlq_depth"`
	GroupCount         uint64  `json:"group_count"`
	PublishRate        float64 `json:"publish_rate"`
	LeaseRate          float64 `json:"lease_rate"`
	AckRate            float64 `json:"ack_rate"`
	OldestAgeMs        uint64  `json:"oldest_age_ms"`
	PausedGroups       int     `json:"paused_groups"`
	BackloggedGroups   int     `json:"backlogged_groups"`
	LeaseCount         int     `json:"lease_count"`
	ExpiredLeases      int     `json:"expired_leases"`
	NearDeadlineLeases int     `json:"near_deadline_leases"`
	DLQSample          int     `json:"dlq_sample"`
}

type doctorWorkflows struct {
	Running       int `json:"running"`
	TaskPending   int `json:"task_pending"`
	PossiblyStuck int `json:"possibly_stuck"`
}

type doctorFinding struct {
	Severity   string `json:"severity"`
	Code       string `json:"code"`
	Summary    string `json:"summary"`
	Lane       string `json:"lane,omitempty"`
	GroupID    string `json:"group_id,omitempty"`
	MsgID      uint64 `json:"msg_id,omitempty"`
	LeaseID    uint64 `json:"lease_id,omitempty"`
	RunID      uint64 `json:"run_id,omitempty"`
	DeadlineMs uint64 `json:"deadline_ms,omitempty"`
}

func cmdDoctor(args []string) error {
	opts := doctorOptions{pageSize: 200, leaseWarnAfter: 5 * time.Second}
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addClientFlags(fs, &opts.clientOptions)
	fs.StringVar(&opts.lane, "lane", "", "optional lane filter")
	fs.UintVar(&opts.pageSize, "page-size", opts.pageSize, "page size for introspection reads")
	fs.BoolVar(&opts.json, "json", false, "emit JSON")
	fs.BoolVar(&opts.failOnWarn, "fail-on-warn", false, "exit non-zero when status is WARN or FAIL")
	fs.DurationVar(&opts.leaseWarnAfter, "lease-warn", opts.leaseWarnAfter, "warn when leases expire within this duration")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := commandContext(opts.clientOptions)
	defer cancel()
	clients, err := dialClients(opts.clientOptions)
	if err != nil {
		return err
	}
	defer clients.close()

	report, err := collectDoctorReport(ctx, clients.control, clients.workflow, opts)
	if err != nil {
		return err
	}
	if err := writeDoctorReport(os.Stdout, report, opts.json); err != nil {
		return err
	}
	if opts.failOnWarn && report.Status != "OK" {
		return fmt.Errorf("doctor status %s", report.Status)
	}
	return nil
}

func collectDoctorReport(ctx context.Context, control rotav1.ControlClient, workflow rotav1.WorkflowClient, opts doctorOptions) (*doctorReport, error) {
	nowMs := uint64(time.Now().UnixMilli())
	pageSize := uint32(opts.pageSize)
	if pageSize == 0 {
		pageSize = 200
	}
	report := &doctorReport{CheckedAtMs: nowMs}

	health, err := control.Health(ctx, &rotav1.HealthRequest{})
	if err != nil {
		return nil, fmt.Errorf("health: %w", err)
	}
	cluster, err := control.DescribeCluster(ctx, &rotav1.DescribeClusterRequest{})
	if err != nil {
		return nil, fmt.Errorf("describe cluster: %w", err)
	}
	report.Cluster = doctorCluster{
		Serving:      health.GetServing(),
		HasQuorum:    health.GetHasQuorum(),
		IsLeader:     health.GetIsLeader(),
		LeaderID:     cluster.GetLeaderId(),
		LeaderAddr:   cluster.GetLeaderAddr(),
		Term:         cluster.GetTerm(),
		AppliedIndex: cluster.GetAppliedIndex(),
		PeerCount:    len(cluster.GetPeers()),
	}
	if !health.GetServing() {
		report.addFinding("FAIL", "not_serving", "node is not serving", "", "", 0, 0, 0, 0)
	}
	if !health.GetHasQuorum() {
		report.addFinding("FAIL", "no_quorum", "cluster does not currently have quorum", "", "", 0, 0, 0, 0)
	}
	if cluster.GetLeaderId() == "" {
		report.addFinding("WARN", "leader_unknown", "leader is not known from this node", "", "", 0, 0, 0, 0)
	}

	stats, err := control.GetStats(ctx, &rotav1.GetStatsRequest{Lane: opts.lane})
	if err != nil {
		return nil, fmt.Errorf("get stats: %w", err)
	}
	statsByLane := map[string]*rotav1.LaneStats{}
	for _, st := range stats.GetLanes() {
		statsByLane[st.GetLane()] = st
	}
	lanes := append([]*rotav1.LaneStats(nil), stats.GetLanes()...)
	sort.Slice(lanes, func(i, j int) bool { return lanes[i].GetLane() < lanes[j].GetLane() })
	warnWindowMs := uint64(opts.leaseWarnAfter.Milliseconds())

	for _, st := range lanes {
		lane := st.GetLane()
		groups, err := listAllGroups(ctx, control, lane, pageSize)
		if err != nil {
			return nil, fmt.Errorf("list groups %q: %w", lane, err)
		}
		leases, err := listAllLeases(ctx, control, lane, pageSize)
		if err != nil {
			return nil, fmt.Errorf("list leases %q: %w", lane, err)
		}
		dlqSample, err := listDeadLetterSample(ctx, control, lane, pageSize)
		if err != nil {
			return nil, fmt.Errorf("list dead letters %q: %w", lane, err)
		}
		ls := doctorLane{
			Lane:        lane,
			Leasable:    st.GetLeasable(),
			Delayed:     st.GetDelayed(),
			Inflight:    st.GetInflight(),
			DLQDepth:    st.GetDlqDepth(),
			GroupCount:  st.GetGroupCount(),
			PublishRate: st.GetPublishRate(),
			LeaseRate:   st.GetLeaseRate(),
			AckRate:     st.GetAckRate(),
			OldestAgeMs: st.GetOldestAgeMs(),
			LeaseCount:  len(leases),
			DLQSample:   len(dlqSample),
		}
		if st.GetDlqDepth() > 0 {
			report.addFinding("WARN", "dlq_depth", fmt.Sprintf("lane has %d dead letters", st.GetDlqDepth()), lane, "", 0, 0, 0, 0)
		}
		for _, g := range groups {
			backlog := g.GetReady() + g.GetDelayed() + g.GetInflight()
			if backlog > 0 {
				ls.BackloggedGroups++
			}
			if g.GetPaused() {
				ls.PausedGroups++
				if backlog > 0 {
					report.addFinding("WARN", "paused_group_backlog", "paused group still has queued or in-flight work", lane, g.GetGroupId(), 0, 0, 0, 0)
				}
			}
		}
		for _, lease := range leases {
			switch {
			case lease.GetDeadlineMs() <= nowMs:
				ls.ExpiredLeases++
				report.addFinding("WARN", "lease_past_deadline", "lease is past its visibility deadline and is still present", lane, lease.GetGroupId(), lease.GetMsgId(), lease.GetLeaseId(), 0, lease.GetDeadlineMs())
			case warnWindowMs > 0 && lease.GetDeadlineMs() <= nowMs+warnWindowMs:
				ls.NearDeadlineLeases++
				report.addFinding("WARN", "lease_near_deadline", "lease is close to its visibility deadline", lane, lease.GetGroupId(), lease.GetMsgId(), lease.GetLeaseId(), 0, lease.GetDeadlineMs())
			}
		}
		report.Lanes = append(report.Lanes, ls)
	}

	if workflow != nil {
		if err := report.collectWorkflowFindings(ctx, workflow, statsByLane, opts, pageSize); err != nil {
			return nil, err
		}
	}

	report.Status = reportStatus(report.Findings)
	return report, nil
}

func (r *doctorReport) collectWorkflowFindings(ctx context.Context, workflow rotav1.WorkflowClient, statsByLane map[string]*rotav1.LaneStats, opts doctorOptions, pageSize uint32) error {
	token := ""
	for {
		resp, err := workflow.ListWorkflowRuns(ctx, &rotav1.ListWorkflowRunsRequest{
			Status: rotav1.WorkflowStatus_WF_RUNNING, HasStatus: true, PageSize: pageSize, PageToken: token,
		})
		if err != nil {
			return fmt.Errorf("list workflow runs: %w", err)
		}
		for _, run := range resp.GetRuns() {
			wfLane := "__wf/" + run.GetWorkflowType()
			if opts.lane != "" && opts.lane != wfLane {
				continue
			}
			r.Workflows.Running++
			if !run.GetWfTaskPending() {
				continue
			}
			r.Workflows.TaskPending++
			st := statsByLane[wfLane]
			if st == nil || st.GetLeasable()+st.GetDelayed()+st.GetInflight() == 0 {
				r.Workflows.PossiblyStuck++
				r.addFinding("FAIL", "workflow_task_missing", "workflow has wf_task_pending=true but no visible workflow-task lane depth", wfLane, run.GetTenantId(), 0, 0, run.GetRunId(), 0)
			}
		}
		token = resp.GetNextPageToken()
		if token == "" {
			break
		}
	}
	return nil
}

func listAllGroups(ctx context.Context, control rotav1.ControlClient, lane string, pageSize uint32) ([]*rotav1.GroupStats, error) {
	var out []*rotav1.GroupStats
	token := ""
	for {
		resp, err := control.ListGroups(ctx, &rotav1.ListGroupsRequest{Lane: lane, PageSize: pageSize, PageToken: token})
		if err != nil {
			return nil, err
		}
		out = append(out, resp.GetGroups()...)
		token = resp.GetNextPageToken()
		if token == "" {
			return out, nil
		}
	}
}

func listAllLeases(ctx context.Context, control rotav1.ControlClient, lane string, pageSize uint32) ([]*rotav1.LeaseInfo, error) {
	var out []*rotav1.LeaseInfo
	token := ""
	for {
		resp, err := control.ListLeases(ctx, &rotav1.ListLeasesRequest{Lane: lane, PageSize: pageSize, PageToken: token})
		if err != nil {
			return nil, err
		}
		out = append(out, resp.GetLeases()...)
		token = resp.GetNextPageToken()
		if token == "" {
			return out, nil
		}
	}
}

func listDeadLetterSample(ctx context.Context, control rotav1.ControlClient, lane string, pageSize uint32) ([]*rotav1.DeadLetterInfo, error) {
	resp, err := control.ListDeadLetters(ctx, &rotav1.ListDeadLettersRequest{Lane: lane, PageSize: pageSize})
	if err != nil {
		return nil, err
	}
	return resp.GetDeadLetters(), nil
}

func (r *doctorReport) addFinding(severity, code, summary, lane, group string, msgID, leaseID, runID, deadlineMs uint64) {
	r.Findings = append(r.Findings, doctorFinding{
		Severity: severity, Code: code, Summary: summary, Lane: lane, GroupID: group,
		MsgID: msgID, LeaseID: leaseID, RunID: runID, DeadlineMs: deadlineMs,
	})
}

func reportStatus(findings []doctorFinding) string {
	status := "OK"
	for _, f := range findings {
		if f.Severity == "FAIL" {
			return "FAIL"
		}
		if f.Severity == "WARN" {
			status = "WARN"
		}
	}
	return status
}

func writeDoctorReport(w io.Writer, report *doctorReport, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	fmt.Fprintf(w, "Rota Doctor: %s\n", report.Status)
	fmt.Fprintf(w, "Cluster: serving=%t quorum=%t leader=%t leader_id=%s leader_addr=%s term=%d applied=%d peers=%d\n",
		report.Cluster.Serving, report.Cluster.HasQuorum, report.Cluster.IsLeader,
		emptyDash(report.Cluster.LeaderID), emptyDash(report.Cluster.LeaderAddr),
		report.Cluster.Term, report.Cluster.AppliedIndex, report.Cluster.PeerCount)
	fmt.Fprintf(w, "Workflows: running=%d task_pending=%d possibly_stuck=%d\n",
		report.Workflows.Running, report.Workflows.TaskPending, report.Workflows.PossiblyStuck)
	if len(report.Lanes) == 0 {
		fmt.Fprintln(w, "Lanes: none")
	} else {
		fmt.Fprintln(w, "Lanes:")
		for _, lane := range report.Lanes {
			fmt.Fprintf(w, "  %s ready=%d delayed=%d inflight=%d dlq=%d groups=%d leases=%d pub/s=%.2f lease/s=%.2f\n",
				lane.Lane, lane.Leasable, lane.Delayed, lane.Inflight, lane.DLQDepth,
				lane.GroupCount, lane.LeaseCount, lane.PublishRate, lane.LeaseRate)
			if lane.PausedGroups > 0 || lane.ExpiredLeases > 0 || lane.NearDeadlineLeases > 0 {
				fmt.Fprintf(w, "    paused_groups=%d expired_leases=%d near_deadline_leases=%d\n",
					lane.PausedGroups, lane.ExpiredLeases, lane.NearDeadlineLeases)
			}
		}
	}
	if len(report.Findings) == 0 {
		fmt.Fprintln(w, "Findings: none")
		return nil
	}
	fmt.Fprintln(w, "Findings:")
	for _, f := range report.Findings {
		parts := []string{fmt.Sprintf("[%s]", f.Severity), f.Code + ":", f.Summary}
		if f.Lane != "" {
			parts = append(parts, "lane="+f.Lane)
		}
		if f.GroupID != "" {
			parts = append(parts, "group="+f.GroupID)
		}
		if f.MsgID != 0 {
			parts = append(parts, fmt.Sprintf("msg=%d", f.MsgID))
		}
		if f.LeaseID != 0 {
			parts = append(parts, fmt.Sprintf("lease=%d", f.LeaseID))
		}
		if f.RunID != 0 {
			parts = append(parts, fmt.Sprintf("run=%d", f.RunID))
		}
		fmt.Fprintln(w, "  "+strings.Join(parts, " "))
	}
	return nil
}

func emptyDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
