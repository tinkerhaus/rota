package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
)

func cmdDLQ(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: rota dlq redrive [flags]")
	}
	switch args[0] {
	case "redrive":
		return cmdDLQRedrive(args[1:])
	default:
		return fmt.Errorf("unknown dlq command %q", args[0])
	}
}

func cmdDLQRedrive(args []string) error {
	var opts clientOptions
	var lane, group string
	var msgID uint64
	fs := flag.NewFlagSet("dlq redrive", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addClientFlags(fs, &opts)
	fs.StringVar(&lane, "lane", "", "lane containing the dead letter")
	fs.StringVar(&group, "group", "", "group containing the dead letter")
	fs.Uint64Var(&msgID, "msg-id", 0, "dead-letter message id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := requireFlag("lane", lane); err != nil {
		return err
	}
	if err := requireFlag("group", group); err != nil {
		return err
	}
	if msgID == 0 {
		return fmt.Errorf("--msg-id is required")
	}
	return withClients(opts, func(ctx context.Context, clients *rotaClients) error {
		resp, err := clients.control.RedriveDeadLetter(ctx, &rotav1.RedriveDeadLetterRequest{Lane: lane, GroupId: group, MsgId: msgID})
		if err != nil {
			return err
		}
		if !resp.GetOk() {
			fmt.Fprintf(os.Stdout, "dead letter not found lane=%s group=%s msg=%d\n", lane, group, msgID)
			return nil
		}
		fmt.Fprintf(os.Stdout, "redriven lane=%s group=%s old_msg=%d new_msg=%d\n", lane, group, msgID, resp.GetNewMsgId())
		return nil
	})
}

func cmdLane(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: rota lane <pause|resume|config> [flags]")
	}
	switch args[0] {
	case "pause":
		return cmdLanePause(args[1:])
	case "resume":
		return cmdLaneResume(args[1:])
	case "config":
		return cmdLaneConfig(args[1:])
	default:
		return fmt.Errorf("unknown lane command %q", args[0])
	}
}

func cmdLanePause(args []string) error {
	var opts clientOptions
	var lane string
	var dur time.Duration
	fs := flag.NewFlagSet("lane pause", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addClientFlags(fs, &opts)
	fs.StringVar(&lane, "lane", "", "lane to pause")
	fs.DurationVar(&dur, "duration", 0, "pause duration; 0 means until resumed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := requireFlag("lane", lane); err != nil {
		return err
	}
	return withClients(opts, func(ctx context.Context, clients *rotaClients) error {
		_, err := clients.control.PauseLane(ctx, &rotav1.PauseLaneRequest{Lane: lane, Duration: durationpb.New(dur)})
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "paused lane=%s duration=%s\n", lane, dur)
		return nil
	})
}

func cmdLaneResume(args []string) error {
	var opts clientOptions
	var lane string
	fs := flag.NewFlagSet("lane resume", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addClientFlags(fs, &opts)
	fs.StringVar(&lane, "lane", "", "lane to resume")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := requireFlag("lane", lane); err != nil {
		return err
	}
	return withClients(opts, func(ctx context.Context, clients *rotaClients) error {
		_, err := clients.control.ResumeLane(ctx, &rotav1.LaneRef{Lane: lane})
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "resumed lane=%s\n", lane)
		return nil
	})
}

func cmdLaneConfig(args []string) error {
	var opts clientOptions
	var lane string
	var rate float64
	var burst uint
	fs := flag.NewFlagSet("lane config", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addClientFlags(fs, &opts)
	fs.StringVar(&lane, "lane", "", "lane to configure")
	fs.Float64Var(&rate, "rate", 0, "dequeue rate per second; 0 disables rate limiting")
	fs.UintVar(&burst, "burst", 1, "rate-limit burst")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := requireFlag("lane", lane); err != nil {
		return err
	}
	return withClients(opts, func(ctx context.Context, clients *rotaClients) error {
		cfg, err := clients.control.SetLaneConfig(ctx, &rotav1.SetLaneConfigRequest{Lane: lane, RatePerSec: rate, Burst: uint32(burst)})
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "configured lane=%s rate=%.2f burst=%d paused=%t\n", cfg.GetLane(), cfg.GetRatePerSec(), cfg.GetBurst(), cfg.GetPaused())
		return nil
	})
}

func cmdGroup(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: rota group <pause|resume|cancel|purge|reap> [flags]")
	}
	switch args[0] {
	case "pause", "resume", "cancel", "purge", "reap":
		return cmdGroupOp(args[0], args[1:])
	default:
		return fmt.Errorf("unknown group command %q", args[0])
	}
}

func cmdGroupOp(op string, args []string) error {
	var opts clientOptions
	var lane, group string
	fs := flag.NewFlagSet("group "+op, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addClientFlags(fs, &opts)
	fs.StringVar(&lane, "lane", "", "lane containing the group")
	fs.StringVar(&group, "group", "", "group id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := requireFlag("lane", lane); err != nil {
		return err
	}
	if err := requireFlag("group", group); err != nil {
		return err
	}
	return withClients(opts, func(ctx context.Context, clients *rotaClients) error {
		ref := &rotav1.GroupRef{Lane: lane, GroupId: group}
		switch op {
		case "pause":
			_, err := clients.control.PauseGroup(ctx, ref)
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stdout, "paused group lane=%s group=%s\n", lane, group)
		case "resume":
			_, err := clients.control.ResumeGroup(ctx, ref)
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stdout, "resumed group lane=%s group=%s\n", lane, group)
		case "cancel":
			res, err := clients.control.CancelGroup(ctx, ref)
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stdout, "canceled group lane=%s group=%s affected=%d\n", lane, group, res.GetAffectedMessages())
		case "purge":
			res, err := clients.control.PurgeGroup(ctx, ref)
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stdout, "purged group lane=%s group=%s affected=%d\n", lane, group, res.GetAffectedMessages())
		case "reap":
			res, err := clients.control.ReapGroup(ctx, ref)
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stdout, "reaped group lane=%s group=%s affected=%d\n", lane, group, res.GetAffectedMessages())
		}
		return nil
	})
}

func cmdWorkflow(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: rota workflow cancel [flags]")
	}
	switch args[0] {
	case "cancel":
		return cmdWorkflowCancel(args[1:])
	default:
		return fmt.Errorf("unknown workflow command %q", args[0])
	}
}

func cmdWorkflowCancel(args []string) error {
	var opts clientOptions
	var runID uint64
	var reason string
	fs := flag.NewFlagSet("workflow cancel", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addClientFlags(fs, &opts)
	fs.Uint64Var(&runID, "run-id", 0, "workflow run id")
	fs.StringVar(&reason, "reason", "operator_cancel", "cancel reason")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if runID == 0 {
		return fmt.Errorf("--run-id is required")
	}
	return withClients(opts, func(ctx context.Context, clients *rotaClients) error {
		resp, err := clients.workflow.CancelWorkflow(ctx, &rotav1.CancelWorkflowRequest{RunId: runID, Reason: []byte(reason)})
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "workflow run=%d canceled=%t\n", runID, resp.GetCanceled())
		return nil
	})
}

func cmdLeases(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: rota leases <list|force-expire> [flags]")
	}
	switch args[0] {
	case "list":
		return cmdLeasesList(args[1:])
	case "force-expire":
		return cmdLeasesForceExpire(args[1:])
	default:
		return fmt.Errorf("unknown leases command %q", args[0])
	}
}

func cmdLeasesList(args []string) error {
	var opts clientOptions
	var lane string
	var pageSize uint
	var asJSON bool
	fs := flag.NewFlagSet("leases list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addClientFlags(fs, &opts)
	fs.StringVar(&lane, "lane", "", "lane to inspect")
	fs.UintVar(&pageSize, "page-size", 200, "page size")
	fs.BoolVar(&asJSON, "json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := requireFlag("lane", lane); err != nil {
		return err
	}
	return withClients(opts, func(ctx context.Context, clients *rotaClients) error {
		leases, err := listAllLeases(ctx, clients.control, lane, uint32(pageSize))
		if err != nil {
			return err
		}
		return writeLeases(os.Stdout, leases, asJSON)
	})
}

func cmdLeasesForceExpire(args []string) error {
	var opts clientOptions
	var leaseID uint64
	var delay time.Duration
	fs := flag.NewFlagSet("leases force-expire", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addClientFlags(fs, &opts)
	fs.Uint64Var(&leaseID, "lease-id", 0, "lease id to requeue")
	fs.DurationVar(&delay, "delay", 0, "optional redelivery delay")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if leaseID == 0 {
		return fmt.Errorf("--lease-id is required")
	}
	return withClients(opts, func(ctx context.Context, clients *rotaClients) error {
		stream, err := clients.broker.Work(ctx)
		if err != nil {
			return err
		}
		if err := stream.Send(&rotav1.WorkClientMsg{Msg: &rotav1.WorkClientMsg_LeaseRequest{
			LeaseRequest: &rotav1.LeaseRequest{Credit: 0, ConsumerId: "rota-operator"},
		}}); err != nil {
			return err
		}
		nack := &rotav1.Nack{LeaseId: leaseID, Mode: rotav1.NackMode_REQUEUE_NO_PENALTY}
		if delay > 0 {
			nack.Delay = durationpb.New(delay)
		}
		nack.FailureMeta = map[string]string{"operator_action": "force_expire"}
		if err := stream.Send(&rotav1.WorkClientMsg{Msg: &rotav1.WorkClientMsg_Nack{Nack: nack}}); err != nil {
			return err
		}
		if err := stream.CloseSend(); err != nil {
			return err
		}
		for {
			msg, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					fmt.Fprintf(os.Stdout, "force-expired lease=%d delay=%s\n", leaseID, delay)
					return nil
				}
				return err
			}
			if se := msg.GetError(); se != nil && se.GetCode() == rotav1.ErrorCode_NOT_LEADER {
				return fmt.Errorf("not leader: retry against %s", se.GetLeaderAddr())
			}
		}
	})
}

func cmdMessages(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: rota messages peek [flags]")
	}
	switch args[0] {
	case "peek":
		return cmdMessagesPeek(args[1:])
	default:
		return fmt.Errorf("unknown messages command %q", args[0])
	}
}

func cmdMessagesPeek(args []string) error {
	var opts clientOptions
	var lane, group string
	var limit uint
	var asJSON bool
	fs := flag.NewFlagSet("messages peek", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addClientFlags(fs, &opts)
	fs.StringVar(&lane, "lane", "", "lane containing the group")
	fs.StringVar(&group, "group", "", "group id")
	fs.UintVar(&limit, "limit", 20, "number of messages to inspect")
	fs.BoolVar(&asJSON, "json", false, "emit JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := requireFlag("lane", lane); err != nil {
		return err
	}
	if err := requireFlag("group", group); err != nil {
		return err
	}
	return withClients(opts, func(ctx context.Context, clients *rotaClients) error {
		resp, err := clients.control.PeekMessages(ctx, &rotav1.PeekMessagesRequest{Lane: lane, GroupId: group, Limit: uint32(limit)})
		if err != nil {
			return err
		}
		return writeMessages(os.Stdout, resp.GetMessages(), asJSON)
	})
}

func writeLeases(w io.Writer, leases []*rotav1.LeaseInfo, asJSON bool) error {
	if asJSON {
		return json.NewEncoder(w).Encode(leases)
	}
	if len(leases) == 0 {
		fmt.Fprintln(w, "leases: none")
		return nil
	}
	nowMs := uint64(time.Now().UnixMilli())
	for _, l := range leases {
		remaining := int64(l.GetDeadlineMs()) - int64(nowMs)
		fmt.Fprintf(w, "lease=%d lane=%s group=%s msg=%d consumer=%s attempt=%d deadline_ms=%d remaining_ms=%d\n",
			l.GetLeaseId(), l.GetLane(), l.GetGroupId(), l.GetMsgId(), l.GetConsumerId(), l.GetAttempt(), l.GetDeadlineMs(), remaining)
	}
	return nil
}

func writeMessages(w io.Writer, msgs []*rotav1.MessagePeek, asJSON bool) error {
	if asJSON {
		return json.NewEncoder(w).Encode(msgs)
	}
	if len(msgs) == 0 {
		fmt.Fprintln(w, "messages: none")
		return nil
	}
	for _, m := range msgs {
		fmt.Fprintf(w, "msg=%d state=%s attempt=%d enqueue_ms=%d not_before_ms=%d\n",
			m.GetMsgId(), m.GetState().String(), m.GetAttempt(), m.GetEnqueueMs(), m.GetNotBeforeMs())
	}
	return nil
}
