package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
)

type benchOptions struct {
	clientOptions
	lane         string
	messages     int
	groups       int
	workers      int
	batchSize    int
	payloadBytes int
	json         bool
}

type benchReport struct {
	Lane            string  `json:"lane"`
	Messages        int     `json:"messages"`
	Groups          int     `json:"groups"`
	Workers         int     `json:"workers"`
	PublishMs       int64   `json:"publish_ms"`
	DrainMs         int64   `json:"drain_ms"`
	TotalMs         int64   `json:"total_ms"`
	PublishPerSec   float64 `json:"publish_per_sec"`
	DrainPerSec     float64 `json:"drain_per_sec"`
	EndToEndPerSec  float64 `json:"end_to_end_per_sec"`
	Acked           int64   `json:"acked"`
	PublishFailures int     `json:"publish_failures"`
}

func cmdBench(args []string) error {
	opts := benchOptions{
		lane: "bench", messages: 1000, groups: 10, workers: 4,
		batchSize: 100, payloadBytes: 128,
	}
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addClientFlags(fs, &opts.clientOptions)
	fs.StringVar(&opts.lane, "lane", opts.lane, "lane used for the benchmark workload")
	fs.IntVar(&opts.messages, "messages", opts.messages, "number of messages to publish and drain")
	fs.IntVar(&opts.groups, "groups", opts.groups, "number of groups to spread messages across")
	fs.IntVar(&opts.workers, "workers", opts.workers, "concurrent Work streams draining the lane")
	fs.IntVar(&opts.batchSize, "batch-size", opts.batchSize, "PublishBatch chunk size")
	fs.IntVar(&opts.payloadBytes, "payload-bytes", opts.payloadBytes, "payload size in bytes")
	fs.BoolVar(&opts.json, "json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	report, err := runBench(opts)
	if err != nil {
		return err
	}
	return writeBenchReport(os.Stdout, report, opts.json)
}

func runBench(opts benchOptions) (*benchReport, error) {
	if opts.messages < 1 {
		return nil, fmt.Errorf("--messages must be >= 1")
	}
	if opts.groups < 1 {
		return nil, fmt.Errorf("--groups must be >= 1")
	}
	if opts.workers < 1 {
		return nil, fmt.Errorf("--workers must be >= 1")
	}
	if opts.batchSize < 1 {
		return nil, fmt.Errorf("--batch-size must be >= 1")
	}
	if opts.payloadBytes < 0 {
		return nil, fmt.Errorf("--payload-bytes must be >= 0")
	}

	ctx, cancel := commandContext(opts.clientOptions)
	defer cancel()
	clients, err := dialClients(opts.clientOptions)
	if err != nil {
		return nil, err
	}
	defer clients.close()

	start := time.Now()
	publishStart := time.Now()
	failures, err := publishBenchMessages(ctx, clients.broker, opts)
	if err != nil {
		return nil, err
	}
	publishElapsed := time.Since(publishStart)

	drainStart := time.Now()
	acked, err := drainBenchMessages(ctx, clients.broker, opts)
	if err != nil {
		return nil, err
	}
	drainElapsed := time.Since(drainStart)
	totalElapsed := time.Since(start)

	return &benchReport{
		Lane: opts.lane, Messages: opts.messages, Groups: opts.groups, Workers: opts.workers,
		PublishMs: publishElapsed.Milliseconds(), DrainMs: drainElapsed.Milliseconds(), TotalMs: totalElapsed.Milliseconds(),
		PublishPerSec:   perSecond(float64(opts.messages-failures), publishElapsed),
		DrainPerSec:     perSecond(float64(acked), drainElapsed),
		EndToEndPerSec:  perSecond(float64(acked), totalElapsed),
		Acked:           acked,
		PublishFailures: failures,
	}, nil
}

func publishBenchMessages(ctx context.Context, broker rotav1.BrokerClient, opts benchOptions) (int, error) {
	payload := []byte(strings.Repeat("x", opts.payloadBytes))
	failures := 0
	for start := 0; start < opts.messages; start += opts.batchSize {
		end := start + opts.batchSize
		if end > opts.messages {
			end = opts.messages
		}
		req := &rotav1.PublishBatchRequest{Atomic: true}
		for i := start; i < end; i++ {
			req.Messages = append(req.Messages, &rotav1.MessageSpec{
				Lane: opts.lane, GroupId: fmt.Sprintf("g-%d", i%opts.groups), Payload: payload,
				Headers: map[string]string{"bench": "true"},
			})
		}
		resp, err := broker.PublishBatch(ctx, req)
		if err != nil {
			return failures, err
		}
		for _, item := range resp.GetResults() {
			if item.GetCode() != rotav1.ErrorCode_OK {
				failures++
			}
		}
	}
	return failures, nil
}

func drainBenchMessages(parent context.Context, broker rotav1.BrokerClient, opts benchOptions) (int64, error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	var acked atomic.Int64
	errCh := make(chan error, opts.workers)
	var wg sync.WaitGroup
	for i := 0; i < opts.workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			errCh <- runBenchWorker(ctx, cancel, broker, opts, workerID, &acked)
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-parent.Done():
		cancel()
		<-done
	}

	close(errCh)
	for err := range errCh {
		if err != nil {
			return acked.Load(), err
		}
	}
	if got := acked.Load(); got < int64(opts.messages) {
		return got, fmt.Errorf("drained %d/%d messages before timeout", got, opts.messages)
	}
	return acked.Load(), nil
}

func runBenchWorker(ctx context.Context, cancel context.CancelFunc, broker rotav1.BrokerClient, opts benchOptions, workerID int, acked *atomic.Int64) error {
	stream, err := broker.Work(ctx)
	if err != nil {
		return err
	}
	defer stream.CloseSend()
	if err := stream.Send(&rotav1.WorkClientMsg{Msg: &rotav1.WorkClientMsg_LeaseRequest{
		LeaseRequest: &rotav1.LeaseRequest{
			Lane: opts.lane, Credit: 1, ConsumerId: fmt.Sprintf("bench-%d", workerID),
		},
	}}); err != nil {
		return err
	}
	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF || ctx.Err() != nil {
				return nil
			}
			return err
		}
		if se := msg.GetError(); se != nil {
			if se.GetCode() == rotav1.ErrorCode_NOT_LEADER {
				return fmt.Errorf("not leader: retry against %s", se.GetLeaderAddr())
			}
			continue
		}
		lease := msg.GetLease()
		if lease == nil {
			continue
		}
		if err := stream.Send(&rotav1.WorkClientMsg{Msg: &rotav1.WorkClientMsg_Ack{
			Ack: &rotav1.Ack{LeaseId: lease.GetLeaseId()},
		}}); err != nil {
			return err
		}
		if acked.Add(1) >= int64(opts.messages) {
			cancel()
			return nil
		}
	}
}

func perSecond(count float64, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	return count / elapsed.Seconds()
}

func writeBenchReport(w io.Writer, report *benchReport, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	fmt.Fprintf(w, "Rota bench lane=%s messages=%d groups=%d workers=%d\n", report.Lane, report.Messages, report.Groups, report.Workers)
	fmt.Fprintf(w, "publish: %dms %.1f msg/s failures=%d\n", report.PublishMs, report.PublishPerSec, report.PublishFailures)
	fmt.Fprintf(w, "drain:   %dms %.1f msg/s acked=%d\n", report.DrainMs, report.DrainPerSec, report.Acked)
	fmt.Fprintf(w, "total:   %dms %.1f msg/s\n", report.TotalMs, report.EndToEndPerSec)
	return nil
}
