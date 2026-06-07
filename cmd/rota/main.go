// Command rota is the single-binary broker and operator CLI.
//
//	rota serve   run a single-node broker and serve the gRPC Broker API
//	rota demo    self-contained demo proving cross-group DRR fairness
package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/httpapi"
	"github.com/tinkerhaus/rota/internal/node"
	"github.com/tinkerhaus/rota/internal/transport"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, rootUsage())
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = cmdServe(os.Args[2:])
	case "demo":
		err = cmdDemo(os.Args[2:])
	case "doctor":
		err = cmdDoctor(os.Args[2:])
	case "dlq":
		err = cmdDLQ(os.Args[2:])
	case "lane":
		err = cmdLane(os.Args[2:])
	case "group":
		err = cmdGroup(os.Args[2:])
	case "workflow":
		err = cmdWorkflow(os.Args[2:])
	case "leases":
		err = cmdLeases(os.Args[2:])
	case "messages":
		err = cmdMessages(os.Args[2:])
	case "bench":
		err = cmdBench(os.Args[2:])
	case "soak":
		err = cmdSoak(os.Args[2:])
	case "backup":
		err = cmdBackup(os.Args[2:])
	case "auth":
		err = cmdAuth(os.Args[2:])
	default:
		fmt.Fprintln(os.Stderr, "unknown command:", os.Args[1])
		fmt.Fprintln(os.Stderr, rootUsage())
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func rootUsage() string {
	return `usage: rota <command> [flags]

commands:
  serve       run a Rota broker node
  demo        run a self-contained fairness demo
  doctor      inspect cluster health, lanes, leases, DLQ, and workflow task liveness
  dlq         redrive dead letters
  lane        pause/resume/configure a lane
  group       pause/resume/cancel/purge/reap a group
  workflow    cancel a workflow run
  leases      list in-flight leases
  messages    peek messages in a group
  bench       publish and drain a load-test workload
  soak        run a long-lived load/chaos exercise
  backup      create, restore, or validate an offline data-directory archive
  auth        manage replicated principals, tokens, and grants`
}

func cmdServe(args []string) error {
	cfg := node.Config{DataDir: "./data", NodeID: "node1"}
	addr := "127.0.0.1:7100"
	metricsAddr := "127.0.0.1:7101"
	webDir := "web/dist"
	var peers, grpcPeers string
	var bootstrapAdminToken, bootstrapAdminTokenFile, bootstrapAdminEnv, bootstrapAdminPrincipal string
	var tlsCert, tlsKey, clientCA string
	bootstrapAdminPrincipal = node.BootstrapAdminPrincipal
	for i := 0; i < len(args)-1; i += 2 {
		switch args[i] {
		case "--data":
			cfg.DataDir = args[i+1]
		case "--grpc":
			addr = args[i+1]
		case "--metrics":
			metricsAddr = args[i+1]
		case "--web":
			webDir = args[i+1]
		case "--id":
			cfg.NodeID = args[i+1]
		case "--raft":
			cfg.RaftBind = args[i+1]
		case "--bootstrap":
			cfg.Bootstrap = args[i+1] == "true"
		case "--peers":
			peers = args[i+1] // id1=raftaddr1,id2=raftaddr2,...
		case "--grpc-peers":
			grpcPeers = args[i+1] // id1=grpcaddr1,id2=grpcaddr2,...
		case "--bootstrap-admin-token":
			bootstrapAdminToken = args[i+1]
		case "--bootstrap-admin-token-file":
			bootstrapAdminTokenFile = args[i+1]
		case "--bootstrap-admin-env":
			bootstrapAdminEnv = args[i+1]
		case "--bootstrap-admin-principal":
			bootstrapAdminPrincipal = args[i+1]
		case "--tls-cert":
			tlsCert = args[i+1]
		case "--tls-key":
			tlsKey = args[i+1]
		case "--client-ca":
			clientCA = args[i+1]
		case "--visibility":
			if secs, err := strconv.Atoi(args[i+1]); err == nil {
				cfg.VisibilityMs = uint64(secs) * 1000
			}
		}
	}
	cfg.InitialPeers = parsePeers(peers)
	// Build the node-id -> gRPC-addr map for NOT_LEADER redirects (include self).
	cfg.GRPCAddrs = map[string]string{cfg.NodeID: addr}
	for _, p := range parsePeers(grpcPeers) {
		cfg.GRPCAddrs[p.ID] = p.Addr
	}

	n, err := node.Open(cfg)
	if err != nil {
		return err
	}
	defer n.Close()
	if err := n.WaitClusterLeader(15 * time.Second); err != nil {
		return err
	}
	adminToken, err := bootstrapToken(bootstrapAdminToken, bootstrapAdminTokenFile, bootstrapAdminEnv)
	if err != nil {
		return err
	}
	if err := n.BootstrapAuthAdmin(bootstrapAdminPrincipal, adminToken); err != nil {
		return err
	}
	if adminToken != "" {
		if err := waitAuthEnabled(n, 15*time.Second); err != nil {
			return err
		}
	}
	registerNodeHealthMetrics(n)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	var grpcOpts []grpc.ServerOption
	if creds, err := serverTransportCredentials(tlsCert, tlsKey, clientCA); err != nil {
		return err
	} else if creds != nil {
		grpcOpts = append(grpcOpts, grpc.Creds(creds))
	}
	grpcOpts = append(grpcOpts,
		grpc.ChainUnaryInterceptor(transport.AuthUnaryInterceptor(n), transport.LeaderGuardInterceptor(n)),
		grpc.ChainStreamInterceptor(transport.AuthStreamInterceptor(n)),
	)
	srv := grpc.NewServer(grpcOpts...)
	rotav1.RegisterBrokerServer(srv, transport.NewBroker(n))
	control := transport.NewControl(n)
	rotav1.RegisterControlServer(srv, control)
	wf := transport.NewWorkflow(n)
	rotav1.RegisterWorkflowServer(srv, wf)

	// Standard gRPC health + server reflection: let grpcurl/k8s probes and the
	// dashboard discover and health-check the services without out-of-band specs.
	hsrv := health.NewServer()
	hsrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	hsrv.SetServingStatus("rota.v1.Broker", healthpb.HealthCheckResponse_SERVING)
	hsrv.SetServingStatus("rota.v1.Control", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, hsrv)
	reflection.Register(srv)

	// Metrics + health + the dashboard HTTP/JSON gateway + static SPA, all on the
	// side HTTP port. /metrics and /healthz are registered explicitly; everything
	// else (the /api/* routes and the SPA catch-all) is handled by httpapi.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if serving, _, _ := n.Health(); serving {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	mux.Handle("/", httpapi.Handler(n, control, wf, webDir))
	metricsSrv := &http.Server{Addr: metricsAddr, Handler: mux}
	go func() { _ = metricsSrv.ListenAndServe() }()

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		_ = metricsSrv.Close()
		srv.GracefulStop()
	}()

	fmt.Printf("rota: Broker+Control on %s, metrics+dashboard on %s (data=%s, id=%s, web=%s)\n", addr, metricsAddr, cfg.DataDir, cfg.NodeID, webDir)
	return srv.Serve(lis)
}

// parsePeers parses "id1=addr1,id2=addr2" into node.Peer entries.
func parsePeers(s string) []node.Peer {
	if s == "" {
		return nil
	}
	var out []node.Peer
	for _, part := range strings.Split(s, ",") {
		if kv := strings.SplitN(part, "=", 2); len(kv) == 2 {
			out = append(out, node.Peer{ID: kv[0], Addr: kv[1]})
		}
	}
	return out
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
