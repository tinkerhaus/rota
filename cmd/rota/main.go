// Command rota is the single-binary broker. Phase 0 supports two subcommands:
//
//	rota serve   run a single-node broker and serve the gRPC Broker API
//	rota demo    self-contained demo proving cross-group DRR fairness
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/node"
	"github.com/tinkerhaus/rota/internal/transport"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: rota <serve|demo> [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = cmdServe(os.Args[2:])
	case "demo":
		err = cmdDemo(os.Args[2:])
	default:
		fmt.Fprintln(os.Stderr, "unknown command:", os.Args[1])
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func cmdServe(args []string) error {
	dataDir := "./data"
	addr := "127.0.0.1:7100"
	id := "node1"
	for i := 0; i < len(args)-1; i += 2 {
		switch args[i] {
		case "--data":
			dataDir = args[i+1]
		case "--grpc":
			addr = args[i+1]
		case "--id":
			id = args[i+1]
		}
	}

	n, err := node.Open(node.Config{DataDir: dataDir, NodeID: id})
	if err != nil {
		return err
	}
	defer n.Close()
	if err := n.WaitLeader(10 * time.Second); err != nil {
		return err
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := grpc.NewServer()
	rotav1.RegisterBrokerServer(srv, transport.NewBroker(n))

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		srv.GracefulStop()
	}()

	fmt.Printf("rota: serving Broker on %s (data=%s, id=%s)\n", addr, dataDir, id)
	return srv.Serve(lis)
}

func cmdDemo(args []string) error {
	const (
		lane  = "demo"
		nA    = 500
		nB    = 20
		total = nA + nB
	)

	dir, err := os.MkdirTemp("", "rota-demo-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	n, err := node.Open(node.Config{DataDir: dir, NodeID: "demo"})
	if err != nil {
		return err
	}
	defer n.Close()
	if err := n.WaitLeader(10 * time.Second); err != nil {
		return err
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	srv := grpc.NewServer()
	rotav1.RegisterBrokerServer(srv, transport.NewBroker(n))
	go srv.Serve(lis)
	defer srv.GracefulStop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := rotav1.NewBrokerClient(conn)
	ctx := context.Background()

	fmt.Printf("rota demo: publishing %d messages to group A and %d to group B in lane %q\n", nA, nB, lane)
	pub := func(group string, count int) error {
		for i := 0; i < count; i++ {
			_, err := client.Publish(ctx, &rotav1.PublishRequest{Message: &rotav1.MessageSpec{
				Lane:    lane,
				GroupId: group,
				Payload: []byte(fmt.Sprintf("%s-%d", group, i)),
			}})
			if err != nil {
				return err
			}
		}
		return nil
	}
	if err := pub("A", nA); err != nil {
		return err
	}
	if err := pub("B", nB); err != nil {
		return err
	}

	stream, err := client.Work(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&rotav1.WorkClientMsg{Msg: &rotav1.WorkClientMsg_LeaseRequest{
		LeaseRequest: &rotav1.LeaseRequest{Lane: lane, Credit: 1, ConsumerId: "demo"},
	}}); err != nil {
		return err
	}

	order := make([]string, 0, total)
	for len(order) < total {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		lease := msg.GetLease()
		if lease == nil {
			continue
		}
		order = append(order, lease.GetGroupId())
		if err := stream.Send(&rotav1.WorkClientMsg{Msg: &rotav1.WorkClientMsg_Ack{
			Ack: &rotav1.Ack{LeaseId: lease.GetLeaseId()},
		}}); err != nil {
			return err
		}
	}

	// Report the interleaving.
	first := 40
	if first > len(order) {
		first = len(order)
	}
	fmt.Printf("first %d served: ", first)
	for _, g := range order[:first] {
		fmt.Print(g, " ")
	}
	fmt.Println()

	lastB := -1
	for i, g := range order {
		if g == "B" {
			lastB = i
		}
	}
	fmt.Printf("group B's %d messages were all delivered within the first %d of %d total deliveries.\n", nB, lastB+1, total)
	if lastB+1 <= 3*nB {
		fmt.Println("PASS: the 500-message group did not head-of-line-block the 20-message group.")
	} else {
		fmt.Println("WARN: group B finished later than expected for a 1:1 interleave.")
	}
	return nil
}
