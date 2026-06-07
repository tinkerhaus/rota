package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/node"
	"github.com/tinkerhaus/rota/internal/transport"
)

type benchSuiteOptions struct {
	profile          string
	json             bool
	quiet            bool
	progress         io.Writer
	tmpDir           string
	keepData         bool
	skipCluster      bool
	skipSoak         bool
	skipWorkflow     bool
	skipBackup       bool
	messages         int
	groups           int
	workers          int
	batchSize        int
	payloadBytes     int
	latencySamples   int
	soakDuration     time.Duration
	soakRate         int
	retryEvery       int64
	deadletterEvery  int64
	workflowDuration time.Duration
	workflowRate     int
	backupMessages   int
	timeout          time.Duration
}

type benchSuiteReport struct {
	StartedAtMs  int64                    `json:"started_at_ms"`
	FinishedAtMs int64                    `json:"finished_at_ms"`
	Profile      string                   `json:"profile"`
	RunDir       string                   `json:"run_dir"`
	Config       benchSuiteConfigReport   `json:"config"`
	SingleNode   *benchSuiteNodeReport    `json:"single_node,omitempty"`
	Soak         *soakReport              `json:"soak_pressure,omitempty"`
	Workflow     *benchWorkflowReport     `json:"workflow,omitempty"`
	Cluster      *benchSuiteClusterReport `json:"cluster,omitempty"`
	Backup       *benchBackupReport       `json:"backup,omitempty"`
}

type benchSuiteConfigReport struct {
	Messages        int   `json:"messages"`
	Groups          int   `json:"groups"`
	Workers         int   `json:"workers"`
	BatchSize       int   `json:"batch_size"`
	PayloadBytes    int   `json:"payload_bytes"`
	LatencySamples  int   `json:"latency_samples"`
	SoakMs          int64 `json:"soak_ms"`
	SoakRate        int   `json:"soak_rate"`
	RetryEvery      int64 `json:"retry_every"`
	DeadletterEvery int64 `json:"deadletter_every"`
	WorkflowMs      int64 `json:"workflow_ms"`
	WorkflowRate    int   `json:"workflow_rate"`
	BackupMessages  int   `json:"backup_messages"`
	TimeoutMs       int64 `json:"timeout_ms"`
}

type benchSuiteNodeReport struct {
	Throughput *benchReport        `json:"throughput,omitempty"`
	Latency    *benchLatencyReport `json:"latency,omitempty"`
}

type benchLatencyReport struct {
	Lane             string       `json:"lane"`
	Samples          int          `json:"samples"`
	PublishLatencyMs latencyStats `json:"publish_latency_ms"`
	LeaseWaitMs      latencyStats `json:"lease_wait_ms"`
}

type latencyStats struct {
	MinMs float64 `json:"min_ms"`
	P50Ms float64 `json:"p50_ms"`
	P95Ms float64 `json:"p95_ms"`
	P99Ms float64 `json:"p99_ms"`
	MaxMs float64 `json:"max_ms"`
	AvgMs float64 `json:"avg_ms"`
}

type benchSuiteClusterReport struct {
	Throughput *benchReport         `json:"throughput,omitempty"`
	Failover   *benchFailoverReport `json:"failover,omitempty"`
}

type benchFailoverReport struct {
	InitialLeader           string `json:"initial_leader"`
	NewLeader               string `json:"new_leader"`
	CommittedBeforeFailover int    `json:"committed_before_failover"`
	AckedBeforeFailover     int    `json:"acked_before_failover"`
	DrainedAfterFailover    int    `json:"drained_after_failover"`
	FailoverMs              int64  `json:"failover_ms"`
	PostFailoverDrainMs     int64  `json:"post_failover_drain_ms"`
	PostFailoverPublishMs   int64  `json:"post_failover_publish_ms"`
	PostFailoverLeaseMs     int64  `json:"post_failover_lease_ms"`
}

type benchWorkflowReport struct {
	DurationMs          int64   `json:"duration_ms"`
	Started             int64   `json:"started"`
	WorkflowTasks       int64   `json:"workflow_tasks"`
	ActivityTasks       int64   `json:"activity_tasks"`
	WorkflowCommands    int64   `json:"workflow_commands"`
	Errors              int64   `json:"errors"`
	StartsPerSec        float64 `json:"starts_per_sec"`
	ActivityTasksPerSec float64 `json:"activity_tasks_per_sec"`
}

type benchBackupReport struct {
	Messages     int                     `json:"messages"`
	Archive      string                  `json:"archive"`
	ArchiveBytes int64                   `json:"archive_bytes"`
	CreateMs     int64                   `json:"create_ms"`
	ValidateMs   int64                   `json:"validate_ms"`
	RestoreMs    int64                   `json:"restore_ms"`
	Validation   *backupValidationReport `json:"validation,omitempty"`
}

type benchNodeServer struct {
	grpcAddr  string
	node      *node.Node
	grpc      *grpc.Server
	closeOnce sync.Once
}

type benchCluster struct {
	nodes map[string]*benchNodeServer
}

func cmdBenchSuite(args []string) error {
	opts := defaultBenchSuiteOptions(benchSuiteProfileFromArgs(args))
	fs := flag.NewFlagSet("bench suite", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&opts.profile, "profile", opts.profile, "benchmark profile: smoke or standard")
	fs.IntVar(&opts.messages, "messages", opts.messages, "messages for each throughput benchmark")
	fs.IntVar(&opts.groups, "groups", opts.groups, "groups to spread benchmark messages across")
	fs.IntVar(&opts.workers, "workers", opts.workers, "concurrent Work streams for throughput benchmarks")
	fs.IntVar(&opts.batchSize, "batch-size", opts.batchSize, "PublishBatch chunk size")
	fs.IntVar(&opts.payloadBytes, "payload-bytes", opts.payloadBytes, "payload size in bytes")
	fs.IntVar(&opts.latencySamples, "latency-samples", opts.latencySamples, "publish/lease latency samples")
	fs.DurationVar(&opts.soakDuration, "soak-duration", opts.soakDuration, "retry/DLQ soak pressure duration")
	fs.IntVar(&opts.soakRate, "soak-rate", opts.soakRate, "target soak publishes per second")
	fs.Int64Var(&opts.retryEvery, "retry-every", opts.retryEvery, "nack every Nth soak lease with RETRY once; 0 disables")
	fs.Int64Var(&opts.deadletterEvery, "deadletter-every", opts.deadletterEvery, "dead-letter every Nth soak lease; 0 disables")
	fs.DurationVar(&opts.workflowDuration, "workflow-duration", opts.workflowDuration, "workflow storm measurement duration")
	fs.IntVar(&opts.workflowRate, "workflow-rate", opts.workflowRate, "target workflow starts per second")
	fs.IntVar(&opts.backupMessages, "backup-messages", opts.backupMessages, "messages to seed before backup timing")
	fs.DurationVar(&opts.timeout, "timeout", opts.timeout, "per-scenario timeout")
	fs.BoolVar(&opts.skipCluster, "skip-cluster", false, "skip 3-node cluster throughput and failover measurements")
	fs.BoolVar(&opts.skipSoak, "skip-soak", false, "skip retry/DLQ soak pressure measurement")
	fs.BoolVar(&opts.skipWorkflow, "skip-workflow", false, "skip workflow/activity storm measurement")
	fs.BoolVar(&opts.skipBackup, "skip-backup", false, "skip backup/validate/restore timing")
	fs.StringVar(&opts.tmpDir, "tmp-dir", "", "directory for benchmark data; a run subdirectory is created inside it")
	fs.BoolVar(&opts.keepData, "keep-data", false, "keep benchmark data directory after the suite finishes")
	fs.BoolVar(&opts.json, "json", false, "emit JSON")
	fs.BoolVar(&opts.quiet, "quiet", false, "suppress progress output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !opts.quiet {
		opts.progress = os.Stderr
	}
	report, err := runBenchSuite(opts)
	if err != nil {
		return err
	}
	return writeBenchSuiteReport(os.Stdout, report, opts.json)
}

func defaultBenchSuiteOptions(profile string) benchSuiteOptions {
	if profile == "" {
		profile = "standard"
	}
	opts := benchSuiteOptions{
		profile:          profile,
		messages:         5000,
		groups:           100,
		workers:          8,
		batchSize:        250,
		payloadBytes:     128,
		latencySamples:   200,
		soakDuration:     5 * time.Second,
		soakRate:         200,
		retryEvery:       20,
		deadletterEvery:  50,
		workflowDuration: 5 * time.Second,
		workflowRate:     100,
		backupMessages:   1000,
		timeout:          60 * time.Second,
	}
	if profile == "smoke" {
		opts.messages = 100
		opts.groups = 4
		opts.workers = 2
		opts.batchSize = 25
		opts.latencySamples = 20
		opts.soakDuration = time.Second
		opts.soakRate = 40
		opts.retryEvery = 5
		opts.deadletterEvery = 11
		opts.workflowDuration = time.Second
		opts.workflowRate = 20
		opts.backupMessages = 100
		opts.timeout = 30 * time.Second
	}
	return opts
}

func benchSuiteProfileFromArgs(args []string) string {
	for i, arg := range args {
		if arg == "--profile" && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(arg, "--profile=") {
			return strings.TrimPrefix(arg, "--profile=")
		}
	}
	return "standard"
}

func runBenchSuite(opts benchSuiteOptions) (*benchSuiteReport, error) {
	if err := validateBenchSuiteOptions(opts); err != nil {
		return nil, err
	}
	root, cleanup, err := prepareBenchSuiteRoot(opts)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	started := time.Now()
	progress := opts.progress
	benchSuiteProgress(progress, "starting profile=%s run_dir=%s", opts.profile, root)
	report := &benchSuiteReport{
		StartedAtMs: started.UnixMilli(),
		Profile:     opts.profile,
		RunDir:      root,
		Config: benchSuiteConfigReport{
			Messages: opts.messages, Groups: opts.groups, Workers: opts.workers,
			BatchSize: opts.batchSize, PayloadBytes: opts.payloadBytes,
			LatencySamples: opts.latencySamples, SoakMs: opts.soakDuration.Milliseconds(),
			SoakRate: opts.soakRate, RetryEvery: opts.retryEvery, DeadletterEvery: opts.deadletterEvery,
			WorkflowMs:   opts.workflowDuration.Milliseconds(),
			WorkflowRate: opts.workflowRate, BackupMessages: opts.backupMessages,
			TimeoutMs: opts.timeout.Milliseconds(),
		},
	}

	stepStart := time.Now()
	benchSuiteProgress(progress, "starting single-node broker")
	single, err := startBenchSingleNode(filepath.Join(root, "single"))
	if err != nil {
		return nil, err
	}
	defer single.close()
	benchSuiteProgress(progress, "single-node broker ready addr=%s elapsed=%s", single.grpcAddr, benchSuiteDuration(stepStart))

	stepStart = time.Now()
	benchSuiteProgress(progress, "running single-node throughput messages=%d groups=%d workers=%d batch=%d",
		opts.messages, opts.groups, opts.workers, opts.batchSize)
	throughput, err := runBench(benchOptions{
		clientOptions: clientOptions{grpcAddr: single.grpcAddr, timeout: opts.timeout},
		lane:          "bench-suite-single",
		messages:      opts.messages,
		groups:        opts.groups,
		workers:       opts.workers,
		batchSize:     opts.batchSize,
		payloadBytes:  opts.payloadBytes,
	})
	if err != nil {
		return nil, fmt.Errorf("single-node throughput: %w", err)
	}
	benchSuiteProgress(progress, "single-node throughput done acked=%d total=%s rate=%.1f msg/s",
		throughput.Acked, time.Duration(throughput.TotalMs)*time.Millisecond, throughput.EndToEndPerSec)

	stepStart = time.Now()
	benchSuiteProgress(progress, "running publish/lease latency samples=%d", opts.latencySamples)
	latency, err := runBenchLatency(context.Background(), single.grpcAddr, benchLatencyOptions{
		lane:         "bench-suite-latency",
		samples:      opts.latencySamples,
		groups:       opts.groups,
		payloadBytes: opts.payloadBytes,
		timeout:      opts.timeout,
	})
	if err != nil {
		return nil, fmt.Errorf("single-node latency: %w", err)
	}
	benchSuiteProgress(progress, "latency done elapsed=%s publish_p95=%.3fms lease_p95=%.3fms",
		benchSuiteDuration(stepStart), latency.PublishLatencyMs.P95Ms, latency.LeaseWaitMs.P95Ms)
	report.SingleNode = &benchSuiteNodeReport{Throughput: throughput, Latency: latency}

	if !opts.skipSoak {
		stepStart = time.Now()
		benchSuiteProgress(progress, "running retry/DLQ soak duration=%s rate=%d/s retry_every=%d deadletter_every=%d",
			opts.soakDuration, opts.soakRate, opts.retryEvery, opts.deadletterEvery)
		soak, err := runBenchSoak(single.grpcAddr, opts)
		if err != nil {
			return nil, fmt.Errorf("soak pressure: %w", err)
		}
		benchSuiteProgress(progress, "soak done elapsed=%s published=%d acked=%d retried=%d dead_lettered=%d errors=%d",
			benchSuiteDuration(stepStart), soak.Published, soak.Acked, soak.Retried, soak.DeadLettered, soak.Errors)
		report.Soak = soak
	} else {
		benchSuiteProgress(progress, "skipping retry/DLQ soak")
	}

	if !opts.skipWorkflow {
		stepStart = time.Now()
		benchSuiteProgress(progress, "running workflow storm duration=%s rate=%d/s", opts.workflowDuration, opts.workflowRate)
		workflow, err := runBenchWorkflow(single.grpcAddr, opts)
		if err != nil {
			return nil, fmt.Errorf("workflow storm: %w", err)
		}
		benchSuiteProgress(progress, "workflow storm done elapsed=%s started=%d workflow_tasks=%d activity_tasks=%d errors=%d",
			benchSuiteDuration(stepStart), workflow.Started, workflow.WorkflowTasks, workflow.ActivityTasks, workflow.Errors)
		report.Workflow = workflow
	} else {
		benchSuiteProgress(progress, "skipping workflow storm")
	}

	if !opts.skipCluster {
		stepStart = time.Now()
		benchSuiteProgress(progress, "starting 3-node cluster")
		cluster, err := startBenchCluster(filepath.Join(root, "cluster"))
		if err != nil {
			return nil, err
		}
		defer cluster.close()
		benchSuiteProgress(progress, "3-node cluster ready elapsed=%s", benchSuiteDuration(stepStart))
		stepStart = time.Now()
		benchSuiteProgress(progress, "running cluster throughput and leader failover")
		clusterReport, err := runBenchCluster(cluster, opts)
		if err != nil {
			return nil, err
		}
		benchSuiteProgress(progress, "cluster done elapsed=%s acked=%d failover=%s->%s failover_ms=%d drained_after=%d",
			benchSuiteDuration(stepStart), clusterReport.Throughput.Acked, clusterReport.Failover.InitialLeader,
			clusterReport.Failover.NewLeader, clusterReport.Failover.FailoverMs, clusterReport.Failover.DrainedAfterFailover)
		report.Cluster = clusterReport
	} else {
		benchSuiteProgress(progress, "skipping 3-node cluster")
	}

	if !opts.skipBackup {
		stepStart = time.Now()
		benchSuiteProgress(progress, "running backup create/validate/restore messages=%d", opts.backupMessages)
		backup, err := runBenchBackup(filepath.Join(root, "backup"), opts)
		if err != nil {
			return nil, fmt.Errorf("backup: %w", err)
		}
		benchSuiteProgress(progress, "backup done elapsed=%s archive_bytes=%d validation_ok=%t",
			benchSuiteDuration(stepStart), backup.ArchiveBytes, backup.Validation != nil && backup.Validation.OK)
		report.Backup = backup
	} else {
		benchSuiteProgress(progress, "skipping backup")
	}

	report.FinishedAtMs = time.Now().UnixMilli()
	benchSuiteProgress(progress, "complete elapsed=%s", benchSuiteDuration(started))
	return report, nil
}

func benchSuiteProgress(w io.Writer, format string, args ...interface{}) {
	if w == nil {
		return
	}
	fmt.Fprintf(w, "bench suite: "+format+"\n", args...)
}

func benchSuiteDuration(start time.Time) time.Duration {
	return time.Since(start).Round(time.Millisecond)
}

func validateBenchSuiteOptions(opts benchSuiteOptions) error {
	switch opts.profile {
	case "smoke", "standard":
	default:
		return fmt.Errorf("--profile must be smoke or standard")
	}
	if opts.messages < 1 || opts.groups < 1 || opts.workers < 1 || opts.batchSize < 1 {
		return fmt.Errorf("--messages, --groups, --workers, and --batch-size must be >= 1")
	}
	if opts.payloadBytes < 0 || opts.latencySamples < 1 || opts.workflowRate < 1 || opts.backupMessages < 1 {
		return fmt.Errorf("--payload-bytes must be >= 0 and --latency-samples, --workflow-rate, --backup-messages must be >= 1")
	}
	if opts.soakRate < 1 || opts.retryEvery < 0 || opts.deadletterEvery < 0 {
		return fmt.Errorf("--soak-rate must be >= 1 and retry/DLQ intervals must be >= 0")
	}
	if opts.soakDuration <= 0 || opts.workflowDuration <= 0 || opts.timeout <= 0 {
		return fmt.Errorf("--soak-duration, --workflow-duration, and --timeout must be > 0")
	}
	if !opts.skipWorkflow && opts.timeout <= opts.workflowDuration {
		return fmt.Errorf("--timeout must be greater than --workflow-duration")
	}
	return nil
}

func prepareBenchSuiteRoot(opts benchSuiteOptions) (string, func(), error) {
	if opts.tmpDir == "" {
		root, err := os.MkdirTemp("", "rota-bench-suite-*")
		if err != nil {
			return "", nil, err
		}
		return root, cleanupBenchSuiteRoot(root, opts.keepData), nil
	}
	if err := os.MkdirAll(opts.tmpDir, 0o755); err != nil {
		return "", nil, err
	}
	root := filepath.Join(opts.tmpDir, fmt.Sprintf("run-%d", time.Now().UnixNano()))
	if err := os.MkdirAll(root, 0o755); err != nil {
		return "", nil, err
	}
	return root, cleanupBenchSuiteRoot(root, opts.keepData), nil
}

func cleanupBenchSuiteRoot(root string, keep bool) func() {
	if keep {
		return func() { fmt.Fprintf(os.Stderr, "bench suite data kept at %s\n", root) }
	}
	return func() { _ = os.RemoveAll(root) }
}

type benchLatencyOptions struct {
	lane         string
	samples      int
	groups       int
	payloadBytes int
	timeout      time.Duration
}

func runBenchLatency(parent context.Context, addr string, opts benchLatencyOptions) (*benchLatencyReport, error) {
	clients, err := dialClients(clientOptions{grpcAddr: addr, timeout: opts.timeout})
	if err != nil {
		return nil, err
	}
	defer clients.close()
	ctx, cancel := context.WithTimeout(parent, opts.timeout)
	defer cancel()

	payload := []byte(strings.Repeat("x", opts.payloadBytes))
	publishTimes := make([]time.Duration, 0, opts.samples)
	leaseTimes := make([]time.Duration, 0, opts.samples)
	stream, err := clients.broker.Work(ctx)
	if err != nil {
		return nil, err
	}
	defer stream.CloseSend()
	if err := stream.Send(&rotav1.WorkClientMsg{Msg: &rotav1.WorkClientMsg_LeaseRequest{
		LeaseRequest: &rotav1.LeaseRequest{Lane: opts.lane, Credit: 1, ConsumerId: "bench-suite-latency"},
	}}); err != nil {
		return nil, err
	}
	for i := 0; i < opts.samples; i++ {
		start := time.Now()
		if _, err := clients.broker.Publish(ctx, &rotav1.PublishRequest{Message: &rotav1.MessageSpec{
			Lane: opts.lane, GroupId: fmt.Sprintf("g-%d", i%opts.groups), Payload: payload,
			Headers: map[string]string{"bench-suite": "latency"},
		}}); err != nil {
			return nil, err
		}
		publishTimes = append(publishTimes, time.Since(start))

		leaseStart := time.Now()
		lease, err := recvBenchLatencyLease(stream)
		if err != nil {
			return nil, err
		}
		leaseTimes = append(leaseTimes, time.Since(leaseStart))
		if err := stream.Send(&rotav1.WorkClientMsg{Msg: &rotav1.WorkClientMsg_Ack{
			Ack: &rotav1.Ack{LeaseId: lease.GetLeaseId()},
		}}); err != nil {
			return nil, err
		}
	}
	return &benchLatencyReport{
		Lane: opts.lane, Samples: opts.samples,
		PublishLatencyMs: latencyStatsFromDurations(publishTimes),
		LeaseWaitMs:      latencyStatsFromDurations(leaseTimes),
	}, nil
}

func recvBenchLatencyLease(stream rotav1.Broker_WorkClient) (*rotav1.LeasedMessage, error) {
	for {
		msg, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		if se := msg.GetError(); se != nil {
			return nil, fmt.Errorf("work stream error %s: %s", se.GetCode(), se.GetDetail())
		}
		if lease := msg.GetLease(); lease != nil {
			return lease, nil
		}
	}
}

func latencyStatsFromDurations(values []time.Duration) latencyStats {
	if len(values) == 0 {
		return latencyStats{}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	var total time.Duration
	for _, v := range values {
		total += v
	}
	return latencyStats{
		MinMs: durationMillis(values[0]),
		P50Ms: durationMillis(percentileDuration(values, 0.50)),
		P95Ms: durationMillis(percentileDuration(values, 0.95)),
		P99Ms: durationMillis(percentileDuration(values, 0.99)),
		MaxMs: durationMillis(values[len(values)-1]),
		AvgMs: durationMillis(total / time.Duration(len(values))),
	}
}

func percentileDuration(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(values)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

func durationMillis(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

func runBenchWorkflow(addr string, opts benchSuiteOptions) (*benchWorkflowReport, error) {
	clients, err := dialClients(clientOptions{grpcAddr: addr, timeout: opts.timeout})
	if err != nil {
		return nil, err
	}
	defer clients.close()

	start := time.Now()
	drain := opts.workflowDuration / 2
	if drain < 500*time.Millisecond {
		drain = 500 * time.Millisecond
	}
	if drain > 3*time.Second {
		drain = 3 * time.Second
	}
	activeCtx, stopActive := context.WithTimeout(context.Background(), opts.workflowDuration)
	defer stopActive()
	runCtx, cancelRun := context.WithTimeout(context.Background(), opts.workflowDuration+drain+2*time.Second)
	defer cancelRun()

	soakOpts := soakOptions{
		lanePrefix:   "bench-suite",
		groups:       opts.groups,
		workflowRate: opts.workflowRate,
	}
	var counters soakCounters
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		runSoakWorkflowStarter(activeCtx, clients.workflow, soakOpts, &counters)
	}()
	go func() {
		defer wg.Done()
		runSoakWorkflowWorker(runCtx, clients.workflow, soakOpts, &counters)
	}()
	go func() {
		defer wg.Done()
		runSoakActivityWorker(runCtx, clients.workflow, soakOpts, &counters)
	}()

	<-activeCtx.Done()
	sleepOrDone(runCtx, drain)
	cancelRun()
	wg.Wait()

	elapsed := time.Since(start)
	started := counters.workflowStarted.Load()
	activities := counters.activityTasks.Load()
	return &benchWorkflowReport{
		DurationMs: elapsed.Milliseconds(),
		Started:    started, WorkflowTasks: counters.workflowTasks.Load(),
		ActivityTasks: activities, WorkflowCommands: counters.workflowCommands.Load(),
		Errors:              counters.errors.Load(),
		StartsPerSec:        perSecond(float64(started), elapsed),
		ActivityTasksPerSec: perSecond(float64(activities), elapsed),
	}, nil
}

func runBenchSoak(addr string, opts benchSuiteOptions) (*soakReport, error) {
	drain := opts.soakDuration / 2
	if drain < 500*time.Millisecond {
		drain = 500 * time.Millisecond
	}
	if drain > 2*time.Second {
		drain = 2 * time.Second
	}
	lanes := opts.groups
	if lanes > 8 {
		lanes = 8
	}
	publishers := opts.workers / 2
	if publishers < 1 {
		publishers = 1
	}
	if publishers > 4 {
		publishers = 4
	}
	return runSoak(soakOptions{
		clientOptions:   clientOptions{grpcAddr: addr, timeout: opts.timeout},
		duration:        opts.soakDuration,
		drain:           drain,
		lanePrefix:      "bench-suite-soak",
		lanes:           lanes,
		groups:          opts.groups,
		publishers:      publishers,
		workers:         opts.workers,
		publishRate:     opts.soakRate,
		payloadBytes:    opts.payloadBytes,
		retryEvery:      opts.retryEvery,
		deadletterEvery: opts.deadletterEvery,
	})
}

func runBenchCluster(cluster *benchCluster, opts benchSuiteOptions) (*benchSuiteClusterReport, error) {
	leaderID, leader, err := cluster.leader(opts.timeout)
	if err != nil {
		return nil, err
	}
	throughput, err := runBench(benchOptions{
		clientOptions: clientOptions{grpcAddr: leader.grpcAddr, timeout: opts.timeout},
		lane:          "bench-suite-cluster",
		messages:      opts.messages,
		groups:        opts.groups,
		workers:       opts.workers,
		batchSize:     opts.batchSize,
		payloadBytes:  opts.payloadBytes,
	})
	if err != nil {
		return nil, fmt.Errorf("cluster throughput on leader %s: %w", leaderID, err)
	}
	failover, err := runBenchFailover(cluster, leaderID, opts.timeout)
	if err != nil {
		return nil, err
	}
	return &benchSuiteClusterReport{Throughput: throughput, Failover: failover}, nil
}

func runBenchFailover(cluster *benchCluster, leaderID string, timeout time.Duration) (*benchFailoverReport, error) {
	leader := cluster.nodes[leaderID]
	if leader == nil {
		return nil, fmt.Errorf("leader %s disappeared before failover", leaderID)
	}
	const failoverMessages = 32
	const ackBeforeFailover = 8
	const lane = "bench-suite-failover"
	for i := 0; i < failoverMessages; i++ {
		if _, err := leader.node.Publish(node.PublishReq{
			Lane: lane, GroupID: fmt.Sprintf("g-%d", i%4), Payload: []byte("before-failover"),
		}); err != nil {
			return nil, fmt.Errorf("pre-failover publish: %w", err)
		}
	}
	for i := 0; i < ackBeforeFailover; i++ {
		lease, ok, err := leader.node.LeaseOne(lane, "bench-suite-failover-before")
		if err != nil {
			return nil, fmt.Errorf("pre-failover lease: %w", err)
		}
		if !ok {
			return nil, fmt.Errorf("pre-failover lease returned no work after %d/%d acks", i, ackBeforeFailover)
		}
		if err := leader.node.Ack(lease.LeaseID); err != nil {
			return nil, fmt.Errorf("pre-failover ack: %w", err)
		}
	}

	start := time.Now()
	leader.close()
	delete(cluster.nodes, leaderID)
	newLeaderID, newLeader, err := cluster.leader(timeout)
	if err != nil {
		return nil, fmt.Errorf("leader failover: %w", err)
	}
	failoverElapsed := time.Since(start)
	if err := newLeader.node.Barrier(timeout); err != nil {
		return nil, fmt.Errorf("post-failover barrier: %w", err)
	}

	drainStart := time.Now()
	drained, err := drainFailoverBacklog(newLeader.node, lane, failoverMessages-ackBeforeFailover, timeout)
	if err != nil {
		return nil, err
	}
	drainElapsed := time.Since(drainStart)

	publishStart := time.Now()
	if _, err := newLeader.node.Publish(node.PublishReq{
		Lane: lane, GroupID: "g", Payload: []byte("after-failover"),
	}); err != nil {
		return nil, fmt.Errorf("post-failover publish: %w", err)
	}
	publishElapsed := time.Since(publishStart)

	leaseStart := time.Now()
	lease, ok, err := newLeader.node.LeaseOne(lane, "bench-suite-failover-after")
	if err != nil {
		return nil, fmt.Errorf("post-failover lease: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("post-failover lease returned no work")
	}
	if err := newLeader.node.Ack(lease.LeaseID); err != nil {
		return nil, fmt.Errorf("post-failover ack: %w", err)
	}
	leaseElapsed := time.Since(leaseStart)

	return &benchFailoverReport{
		InitialLeader: leaderID, NewLeader: newLeaderID,
		CommittedBeforeFailover: failoverMessages,
		AckedBeforeFailover:     ackBeforeFailover,
		DrainedAfterFailover:    drained,
		FailoverMs:              failoverElapsed.Milliseconds(),
		PostFailoverDrainMs:     drainElapsed.Milliseconds(),
		PostFailoverPublishMs:   publishElapsed.Milliseconds(),
		PostFailoverLeaseMs:     leaseElapsed.Milliseconds(),
	}, nil
}

func drainFailoverBacklog(n *node.Node, lane string, want int, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	got := 0
	for got < want && time.Now().Before(deadline) {
		lease, ok, err := n.LeaseOne(lane, "bench-suite-failover-drain")
		if err != nil {
			return got, fmt.Errorf("post-failover drain lease: %w", err)
		}
		if !ok {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if err := n.Ack(lease.LeaseID); err != nil {
			return got, fmt.Errorf("post-failover drain ack: %w", err)
		}
		got++
	}
	if got != want {
		return got, fmt.Errorf("post-failover drained %d/%d committed messages", got, want)
	}
	return got, nil
}

func runBenchBackup(root string, opts benchSuiteOptions) (*benchBackupReport, error) {
	dataDir := filepath.Join(root, "data")
	n, err := node.Open(node.Config{DataDir: dataDir, NodeID: "backup", RaftLogLevel: "OFF"})
	if err != nil {
		return nil, err
	}
	if err := n.WaitLeader(opts.timeout); err != nil {
		_ = n.Close()
		return nil, err
	}
	for i := 0; i < opts.backupMessages; i++ {
		if _, err := n.Publish(node.PublishReq{
			Lane: "bench-suite-backup", GroupID: fmt.Sprintf("g-%d", i%opts.groups), Payload: []byte("backup"),
		}); err != nil {
			_ = n.Close()
			return nil, err
		}
	}
	if err := n.Close(); err != nil {
		return nil, err
	}

	archive := filepath.Join(root, "backup.tar.gz")
	createStart := time.Now()
	if err := createBackupArchive(dataDir, archive); err != nil {
		return nil, err
	}
	createElapsed := time.Since(createStart)
	stat, err := os.Stat(archive)
	if err != nil {
		return nil, err
	}

	validateStart := time.Now()
	validation, cleanup, err := validateBackupArchive(archive, "", false)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		return nil, err
	}
	validateElapsed := time.Since(validateStart)

	restoreDir := filepath.Join(root, "restore")
	restoreStart := time.Now()
	if err := restoreBackupArchive(archive, restoreDir, false); err != nil {
		return nil, err
	}
	restoreElapsed := time.Since(restoreStart)

	return &benchBackupReport{
		Messages: opts.backupMessages, Archive: archive, ArchiveBytes: stat.Size(),
		CreateMs: createElapsed.Milliseconds(), ValidateMs: validateElapsed.Milliseconds(),
		RestoreMs: restoreElapsed.Milliseconds(), Validation: validation,
	}, nil
}

func startBenchSingleNode(dataDir string) (*benchNodeServer, error) {
	n, err := node.Open(node.Config{DataDir: dataDir, NodeID: "single", RaftLogLevel: "OFF"})
	if err != nil {
		return nil, err
	}
	if err := n.WaitLeader(10 * time.Second); err != nil {
		_ = n.Close()
		return nil, err
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = n.Close()
		return nil, err
	}
	srv := newBenchGRPCServer(n)
	go func() { _ = srv.Serve(lis) }()
	return &benchNodeServer{grpcAddr: lis.Addr().String(), node: n, grpc: srv}, nil
}

func startBenchCluster(root string) (*benchCluster, error) {
	ids := []string{"n1", "n2", "n3"}
	raftAddrs := map[string]string{}
	grpcListeners := map[string]net.Listener{}
	grpcAddrs := map[string]string{}
	for _, id := range ids {
		addr, err := freeBenchAddr()
		if err != nil {
			return nil, err
		}
		raftAddrs[id] = addr
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			for _, open := range grpcListeners {
				_ = open.Close()
			}
			return nil, err
		}
		grpcListeners[id] = lis
		grpcAddrs[id] = lis.Addr().String()
	}
	peers := make([]node.Peer, 0, len(ids))
	for _, id := range ids {
		peers = append(peers, node.Peer{ID: id, Addr: raftAddrs[id]})
	}
	cluster := &benchCluster{nodes: map[string]*benchNodeServer{}}
	for _, id := range ids {
		cfg := node.Config{
			DataDir:      filepath.Join(root, id),
			NodeID:       id,
			RaftBind:     raftAddrs[id],
			Bootstrap:    id == "n1",
			InitialPeers: nil,
			RaftLogLevel: "OFF",
			GRPCAddrs:    grpcAddrs,
			VisibilityMs: 60_000,
		}
		if id == "n1" {
			cfg.InitialPeers = peers
		}
		n, err := node.Open(cfg)
		if err != nil {
			cluster.close()
			for _, open := range grpcListeners {
				_ = open.Close()
			}
			return nil, err
		}
		srv := newBenchGRPCServer(n)
		go func(lis net.Listener) { _ = srv.Serve(lis) }(grpcListeners[id])
		cluster.nodes[id] = &benchNodeServer{
			grpcAddr: grpcAddrs[id], node: n, grpc: srv,
		}
	}
	if err := cluster.nodes["n1"].node.WaitClusterLeader(20 * time.Second); err != nil {
		cluster.close()
		return nil, err
	}
	if _, _, err := cluster.leader(15 * time.Second); err != nil {
		cluster.close()
		return nil, err
	}
	return cluster, nil
}

func newBenchGRPCServer(n *node.Node) *grpc.Server {
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(transport.AuthUnaryInterceptor(n), transport.LeaderGuardInterceptor(n)),
		grpc.ChainStreamInterceptor(transport.AuthStreamInterceptor(n)),
	)
	rotav1.RegisterBrokerServer(srv, transport.NewBroker(n))
	rotav1.RegisterControlServer(srv, transport.NewControl(n))
	rotav1.RegisterWorkflowServer(srv, transport.NewWorkflow(n))
	return srv
}

func (s *benchNodeServer) close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		if s.grpc != nil {
			done := make(chan struct{})
			go func() {
				s.grpc.GracefulStop()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				s.grpc.Stop()
			}
		}
		if s.node != nil {
			_ = s.node.Close()
		}
	})
}

func (c *benchCluster) close() {
	if c == nil {
		return
	}
	for _, n := range c.nodes {
		n.close()
	}
}

func (c *benchCluster) leader(timeout time.Duration) (string, *benchNodeServer, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for id, n := range c.nodes {
			if n.node.IsLeader() {
				return id, n, nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "", nil, fmt.Errorf("no leader elected within %s", timeout)
}

func freeBenchAddr() (string, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		return "", err
	}
	return addr, nil
}

func writeBenchSuiteReport(w io.Writer, report *benchSuiteReport, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	fmt.Fprintf(w, "Rota bench suite profile=%s run_dir=%s\n", report.Profile, report.RunDir)
	if report.SingleNode != nil {
		t := report.SingleNode.Throughput
		fmt.Fprintf(w, "single-node throughput: total=%dms %.1f msg/s publish=%.1f msg/s drain=%.1f msg/s acked=%d\n",
			t.TotalMs, t.EndToEndPerSec, t.PublishPerSec, t.DrainPerSec, t.Acked)
		l := report.SingleNode.Latency
		fmt.Fprintf(w, "single-node latency: publish p50=%.3fms p95=%.3fms p99=%.3fms; lease p50=%.3fms p95=%.3fms p99=%.3fms\n",
			l.PublishLatencyMs.P50Ms, l.PublishLatencyMs.P95Ms, l.PublishLatencyMs.P99Ms,
			l.LeaseWaitMs.P50Ms, l.LeaseWaitMs.P95Ms, l.LeaseWaitMs.P99Ms)
	}
	if report.Soak != nil {
		s := report.Soak
		fmt.Fprintf(w, "soak pressure: published=%d leased=%d acked=%d retried=%d dead_lettered=%d errors=%d %.1f pub/s %.1f ack/s\n",
			s.Published, s.Leased, s.Acked, s.Retried, s.DeadLettered, s.Errors, s.PublishPerSec, s.AckPerSec)
	}
	if report.Workflow != nil {
		wf := report.Workflow
		fmt.Fprintf(w, "workflow storm: started=%d workflow_tasks=%d activity_tasks=%d commands=%d errors=%d %.1f starts/s %.1f activities/s\n",
			wf.Started, wf.WorkflowTasks, wf.ActivityTasks, wf.WorkflowCommands, wf.Errors, wf.StartsPerSec, wf.ActivityTasksPerSec)
	}
	if report.Cluster != nil {
		t := report.Cluster.Throughput
		f := report.Cluster.Failover
		fmt.Fprintf(w, "cluster throughput: total=%dms %.1f msg/s acked=%d\n", t.TotalMs, t.EndToEndPerSec, t.Acked)
		fmt.Fprintf(w, "cluster failover: %s -> %s in %dms; committed=%d acked_before=%d drained_after=%d drain=%dms publish=%dms lease=%dms\n",
			f.InitialLeader, f.NewLeader, f.FailoverMs, f.CommittedBeforeFailover, f.AckedBeforeFailover,
			f.DrainedAfterFailover, f.PostFailoverDrainMs, f.PostFailoverPublishMs, f.PostFailoverLeaseMs)
	}
	if report.Backup != nil {
		b := report.Backup
		ok := false
		messages := 0
		if b.Validation != nil {
			ok = b.Validation.OK
			messages = b.Validation.Messages
		}
		fmt.Fprintf(w, "backup: messages=%d archive=%d bytes create=%dms validate=%dms restore=%dms validation_ok=%t validation_messages=%d\n",
			b.Messages, b.ArchiveBytes, b.CreateMs, b.ValidateMs, b.RestoreMs, ok, messages)
	}
	return nil
}
