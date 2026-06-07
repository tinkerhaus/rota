package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/node"
)

type soakOptions struct {
	clientOptions
	duration        time.Duration
	drain           time.Duration
	lanePrefix      string
	lanes           int
	groups          int
	publishers      int
	workers         int
	publishRate     int
	payloadBytes    int
	retryEvery      int64
	deadletterEvery int64
	workflowStorm   bool
	workflowRate    int
	chaosAfter      time.Duration
	chaosCommand    string
	json            bool
}

type soakReport struct {
	StartedAtMs      int64   `json:"started_at_ms"`
	DurationMs       int64   `json:"duration_ms"`
	DrainMs          int64   `json:"drain_ms"`
	Lanes            int     `json:"lanes"`
	Groups           int     `json:"groups"`
	Publishers       int     `json:"publishers"`
	Workers          int     `json:"workers"`
	PublishRate      int     `json:"publish_rate"`
	Published        int64   `json:"published"`
	Leased           int64   `json:"leased"`
	Acked            int64   `json:"acked"`
	Retried          int64   `json:"retried"`
	DeadLettered     int64   `json:"dead_lettered"`
	Errors           int64   `json:"errors"`
	WorkflowStarted  int64   `json:"workflow_started"`
	WorkflowTasks    int64   `json:"workflow_tasks"`
	ActivityTasks    int64   `json:"activity_tasks"`
	WorkflowCommands int64   `json:"workflow_commands"`
	ChaosRan         bool    `json:"chaos_ran"`
	ChaosExitCode    int     `json:"chaos_exit_code"`
	ChaosOutput      string  `json:"chaos_output,omitempty"`
	PublishPerSec    float64 `json:"publish_per_sec"`
	AckPerSec        float64 `json:"ack_per_sec"`
}

type soakCounters struct {
	published        atomic.Int64
	leased           atomic.Int64
	acked            atomic.Int64
	retried          atomic.Int64
	deadLettered     atomic.Int64
	errors           atomic.Int64
	workflowStarted  atomic.Int64
	workflowTasks    atomic.Int64
	activityTasks    atomic.Int64
	workflowCommands atomic.Int64
}

func cmdSoak(args []string) error {
	opts := soakOptions{
		duration: 30 * time.Second, drain: 5 * time.Second,
		lanePrefix: "soak", lanes: 4, groups: 32, publishers: 2, workers: 8,
		publishRate: 100, payloadBytes: 128, workflowRate: 10,
	}
	fs := flag.NewFlagSet("soak", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addClientFlags(fs, &opts.clientOptions)
	fs.DurationVar(&opts.duration, "duration", opts.duration, "active publish duration")
	fs.DurationVar(&opts.drain, "drain", opts.drain, "extra worker drain window after publishing stops")
	fs.StringVar(&opts.lanePrefix, "lane-prefix", opts.lanePrefix, "lane prefix used by generated load")
	fs.IntVar(&opts.lanes, "lanes", opts.lanes, "number of lanes")
	fs.IntVar(&opts.groups, "groups", opts.groups, "number of groups per lane")
	fs.IntVar(&opts.publishers, "publishers", opts.publishers, "publisher goroutines")
	fs.IntVar(&opts.workers, "workers", opts.workers, "worker streams")
	fs.IntVar(&opts.publishRate, "publish-rate", opts.publishRate, "target publishes per second across all publishers")
	fs.IntVar(&opts.payloadBytes, "payload-bytes", opts.payloadBytes, "payload size in bytes")
	fs.Int64Var(&opts.retryEvery, "retry-every", 0, "nack every Nth lease with RETRY once; 0 disables")
	fs.Int64Var(&opts.deadletterEvery, "deadletter-every", 0, "dead-letter every Nth lease; 0 disables")
	fs.BoolVar(&opts.workflowStorm, "workflow-storm", false, "also start/poll/complete simple workflows and activities")
	fs.IntVar(&opts.workflowRate, "workflow-rate", opts.workflowRate, "target workflow starts per second when --workflow-storm is enabled")
	fs.DurationVar(&opts.chaosAfter, "chaos-after", 0, "run --chaos-command after this much active time")
	fs.StringVar(&opts.chaosCommand, "chaos-command", "", "shell command to run during active load, e.g. kill a leader behind a stable endpoint")
	fs.BoolVar(&opts.json, "json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	report, err := runSoak(opts)
	if err != nil {
		return err
	}
	return writeSoakReport(os.Stdout, report, opts.json)
}

func runSoak(opts soakOptions) (*soakReport, error) {
	if opts.duration <= 0 {
		return nil, fmt.Errorf("--duration must be > 0")
	}
	if opts.drain < 0 {
		return nil, fmt.Errorf("--drain must be >= 0")
	}
	if opts.lanes < 1 || opts.groups < 1 || opts.publishers < 1 || opts.workers < 1 {
		return nil, fmt.Errorf("--lanes, --groups, --publishers, and --workers must be >= 1")
	}
	if opts.publishRate < 1 {
		return nil, fmt.Errorf("--publish-rate must be >= 1")
	}
	if opts.payloadBytes < 0 {
		return nil, fmt.Errorf("--payload-bytes must be >= 0")
	}

	clients, err := dialClients(opts.clientOptions)
	if err != nil {
		return nil, err
	}
	defer clients.close()

	started := time.Now()
	activeCtx, stopActive := context.WithTimeout(context.Background(), opts.duration)
	defer stopActive()
	runCtx, cancelRun := context.WithTimeout(context.Background(), opts.duration+opts.drain+5*time.Second)
	defer cancelRun()

	var counters soakCounters
	var wg sync.WaitGroup

	for i := 0; i < opts.workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runSoakWorker(runCtx, clients.broker, opts, workerID, &counters)
		}(i)
	}
	for i := 0; i < opts.publishers; i++ {
		wg.Add(1)
		go func(pubID int) {
			defer wg.Done()
			runSoakPublisher(activeCtx, clients.broker, opts, pubID, &counters)
		}(i)
	}

	if opts.workflowStorm {
		wg.Add(3)
		go func() {
			defer wg.Done()
			runSoakWorkflowStarter(activeCtx, clients.workflow, opts, &counters)
		}()
		go func() {
			defer wg.Done()
			runSoakWorkflowWorker(runCtx, clients.workflow, opts, &counters)
		}()
		go func() {
			defer wg.Done()
			runSoakActivityWorker(runCtx, clients.workflow, opts, &counters)
		}()
	}

	chaosRan, chaosExit, chaosOutput := runSoakChaos(activeCtx, opts)
	<-activeCtx.Done()
	if opts.drain > 0 {
		time.Sleep(opts.drain)
	}
	cancelRun()
	wg.Wait()

	elapsed := time.Since(started)
	published := counters.published.Load()
	acked := counters.acked.Load()
	return &soakReport{
		StartedAtMs: started.UnixMilli(), DurationMs: opts.duration.Milliseconds(), DrainMs: opts.drain.Milliseconds(),
		Lanes: opts.lanes, Groups: opts.groups, Publishers: opts.publishers, Workers: opts.workers, PublishRate: opts.publishRate,
		Published:        published,
		Leased:           counters.leased.Load(),
		Acked:            acked,
		Retried:          counters.retried.Load(),
		DeadLettered:     counters.deadLettered.Load(),
		Errors:           counters.errors.Load(),
		WorkflowStarted:  counters.workflowStarted.Load(),
		WorkflowTasks:    counters.workflowTasks.Load(),
		ActivityTasks:    counters.activityTasks.Load(),
		WorkflowCommands: counters.workflowCommands.Load(),
		ChaosRan:         chaosRan,
		ChaosExitCode:    chaosExit,
		ChaosOutput:      chaosOutput,
		PublishPerSec:    perSecond(float64(published), elapsed),
		AckPerSec:        perSecond(float64(acked), elapsed),
	}, nil
}

func runSoakPublisher(ctx context.Context, broker rotav1.BrokerClient, opts soakOptions, pubID int, counters *soakCounters) {
	perPublisher := opts.publishRate / opts.publishers
	if perPublisher < 1 {
		perPublisher = 1
	}
	ticker := time.NewTicker(time.Second / time.Duration(perPublisher))
	defer ticker.Stop()
	payload := []byte(strings.Repeat("x", opts.payloadBytes))
	var seq int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n := atomic.AddInt64(&seq, 1)
			lane := fmt.Sprintf("%s-%d", opts.lanePrefix, (int(n)+pubID)%opts.lanes)
			group := fmt.Sprintf("g-%d", (int(n)+pubID)%opts.groups)
			_, err := broker.Publish(ctx, &rotav1.PublishRequest{Message: &rotav1.MessageSpec{
				Lane: lane, GroupId: group, Payload: payload,
				Headers:     map[string]string{"soak": "true", "publisher": fmt.Sprint(pubID)},
				MaxAttempts: 3,
			}})
			if err != nil {
				if ctx.Err() == nil {
					counters.errors.Add(1)
				}
				continue
			}
			counters.published.Add(1)
		}
	}
}

func runSoakWorker(ctx context.Context, broker rotav1.BrokerClient, opts soakOptions, workerID int, counters *soakCounters) {
	lane := fmt.Sprintf("%s-%d", opts.lanePrefix, workerID%opts.lanes)
	for ctx.Err() == nil {
		stream, err := broker.Work(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			counters.errors.Add(1)
			sleepOrDone(ctx, 50*time.Millisecond)
			continue
		}
		err = stream.Send(&rotav1.WorkClientMsg{Msg: &rotav1.WorkClientMsg_LeaseRequest{
			LeaseRequest: &rotav1.LeaseRequest{Lane: lane, Credit: 1, ConsumerId: fmt.Sprintf("soak-%d", workerID)},
		}})
		if err != nil {
			if ctx.Err() == nil {
				counters.errors.Add(1)
			}
			_ = stream.CloseSend()
			continue
		}
		for ctx.Err() == nil {
			msg, err := stream.Recv()
			if err != nil {
				_ = stream.CloseSend()
				if ctx.Err() == nil {
					counters.errors.Add(1)
				}
				break
			}
			if msg.GetLease() == nil {
				continue
			}
			lease := msg.GetLease()
			n := counters.leased.Add(1)
			switch {
			case opts.deadletterEvery > 0 && n%opts.deadletterEvery == 0:
				if err := stream.Send(&rotav1.WorkClientMsg{Msg: &rotav1.WorkClientMsg_Nack{
					Nack: &rotav1.Nack{LeaseId: lease.GetLeaseId(), Mode: rotav1.NackMode_DEAD_LETTER, FailureMeta: map[string]string{"soak": "deadletter"}},
				}}); err != nil {
					if ctx.Err() == nil {
						counters.errors.Add(1)
					}
				} else {
					counters.deadLettered.Add(1)
				}
			case opts.retryEvery > 0 && n%opts.retryEvery == 0 && lease.GetAttempt() < 2:
				if err := stream.Send(&rotav1.WorkClientMsg{Msg: &rotav1.WorkClientMsg_Nack{
					Nack: &rotav1.Nack{LeaseId: lease.GetLeaseId(), Mode: rotav1.NackMode_RETRY},
				}}); err != nil {
					if ctx.Err() == nil {
						counters.errors.Add(1)
					}
				} else {
					counters.retried.Add(1)
				}
			default:
				if err := stream.Send(&rotav1.WorkClientMsg{Msg: &rotav1.WorkClientMsg_Ack{
					Ack: &rotav1.Ack{LeaseId: lease.GetLeaseId()},
				}}); err != nil {
					if ctx.Err() == nil {
						counters.errors.Add(1)
					}
				} else {
					counters.acked.Add(1)
				}
			}
		}
	}
}

func runSoakWorkflowStarter(ctx context.Context, workflow rotav1.WorkflowClient, opts soakOptions, counters *soakCounters) {
	rate := opts.workflowRate
	if rate < 1 {
		rate = 1
	}
	ticker := time.NewTicker(time.Second / time.Duration(rate))
	defer ticker.Stop()
	var seq int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n := atomic.AddInt64(&seq, 1)
			_, err := workflow.StartWorkflow(ctx, &rotav1.StartWorkflowRequest{
				WorkflowType: opts.lanePrefix + "-workflow",
				TenantId:     fmt.Sprintf("g-%d", int(n)%opts.groups),
				Input:        []byte("soak"),
			})
			if err != nil {
				if ctx.Err() == nil {
					counters.errors.Add(1)
				}
				continue
			}
			counters.workflowStarted.Add(1)
		}
	}
}

func runSoakWorkflowWorker(ctx context.Context, workflow rotav1.WorkflowClient, opts soakOptions, counters *soakCounters) {
	taskType := opts.lanePrefix + "-workflow"
	activityType := opts.lanePrefix + "-activity"
	for ctx.Err() == nil {
		task, err := workflow.PollWorkflowTask(ctx, &rotav1.PollTaskRequest{TaskType: taskType, ConsumerId: "soak-wf"})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			counters.errors.Add(1)
			sleepOrDone(ctx, 20*time.Millisecond)
			continue
		}
		if task.GetEmpty() {
			sleepOrDone(ctx, 20*time.Millisecond)
			continue
		}
		counters.workflowTasks.Add(1)
		var scheduled, completed bool
		for _, ev := range task.GetHistory() {
			switch ev.GetEventType() {
			case rotav1.HistoryEventType_HET_ACTIVITY_SCHEDULED:
				scheduled = true
			case rotav1.HistoryEventType_HET_ACTIVITY_COMPLETED:
				completed = true
			}
		}
		var cmds []*rotav1.WorkflowCommandProto
		switch {
		case !scheduled:
			cmds = []*rotav1.WorkflowCommandProto{{Kind: "schedule_activity", ActivityType: activityType, Input: []byte("activity")}}
		case completed:
			cmds = []*rotav1.WorkflowCommandProto{{Kind: "complete_workflow", Result: []byte("ok")}}
		default:
			cmds = []*rotav1.WorkflowCommandProto{}
		}
		if len(cmds) > 0 {
			counters.workflowCommands.Add(int64(len(cmds)))
		}
		_, err = workflow.RespondWorkflowTask(ctx, &rotav1.RespondWorkflowTaskRequest{
			RunId: task.GetRunId(), LeaseId: task.GetLeaseId(), RunEpoch: task.GetRunEpoch(),
			HistorySeq: task.GetHistorySeq(), PrefixChecksum: node.PrefixChecksumOf(task.GetHistory()),
			Commands: cmds,
		})
		if err != nil {
			if ctx.Err() == nil {
				counters.errors.Add(1)
			}
		}
	}
}

func runSoakActivityWorker(ctx context.Context, workflow rotav1.WorkflowClient, opts soakOptions, counters *soakCounters) {
	activityType := opts.lanePrefix + "-activity"
	for ctx.Err() == nil {
		task, err := workflow.PollActivityTask(ctx, &rotav1.PollTaskRequest{TaskType: activityType, ConsumerId: "soak-act"})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			counters.errors.Add(1)
			sleepOrDone(ctx, 20*time.Millisecond)
			continue
		}
		if task.GetEmpty() {
			sleepOrDone(ctx, 20*time.Millisecond)
			continue
		}
		counters.activityTasks.Add(1)
		if _, err := workflow.RespondActivityTask(ctx, &rotav1.RespondActivityTaskRequest{
			RunId: task.GetRunId(), LeaseId: task.GetLeaseId(), ScheduledEventId: task.GetScheduledEventId(),
			Success: true, Result: []byte("ok"),
		}); err != nil {
			if ctx.Err() == nil {
				counters.errors.Add(1)
			}
		}
	}
}

func runSoakChaos(ctx context.Context, opts soakOptions) (bool, int, string) {
	if opts.chaosCommand == "" || opts.chaosAfter <= 0 {
		return false, 0, ""
	}
	timer := time.NewTimer(opts.chaosAfter)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false, 0, ""
	case <-timer.C:
	}
	cmd := exec.CommandContext(context.Background(), "sh", "-c", opts.chaosCommand)
	out, err := cmd.CombinedOutput()
	output := strings.TrimSpace(string(out))
	if err == nil {
		return true, 0, output
	}
	if exit, ok := err.(*exec.ExitError); ok {
		return true, exit.ExitCode(), output
	}
	return true, -1, strings.TrimSpace(err.Error() + "\n" + output)
}

func sleepOrDone(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func writeSoakReport(w io.Writer, report *soakReport, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	fmt.Fprintf(w, "Rota soak duration=%s lanes=%d groups=%d publishers=%d workers=%d\n",
		time.Duration(report.DurationMs)*time.Millisecond, report.Lanes, report.Groups, report.Publishers, report.Workers)
	fmt.Fprintf(w, "publish: %d %.1f msg/s errors=%d\n", report.Published, report.PublishPerSec, report.Errors)
	fmt.Fprintf(w, "work:    leased=%d acked=%d retried=%d dead_lettered=%d %.1f ack/s\n",
		report.Leased, report.Acked, report.Retried, report.DeadLettered, report.AckPerSec)
	if report.WorkflowStarted > 0 || report.WorkflowTasks > 0 || report.ActivityTasks > 0 {
		fmt.Fprintf(w, "workflow: started=%d tasks=%d activities=%d commands=%d\n",
			report.WorkflowStarted, report.WorkflowTasks, report.ActivityTasks, report.WorkflowCommands)
	}
	if report.ChaosRan {
		fmt.Fprintf(w, "chaos: exit=%d output=%q\n", report.ChaosExitCode, report.ChaosOutput)
	}
	return nil
}
