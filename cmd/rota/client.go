package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
)

const defaultGRPCAddr = "127.0.0.1:7100"

type clientOptions struct {
	grpcAddr      string
	timeout       time.Duration
	token         string
	tlsCA         string
	tlsServerName string
}

func addClientFlags(fs *flag.FlagSet, opts *clientOptions) {
	fs.StringVar(&opts.grpcAddr, "grpc", defaultGRPCAddr, "Rota gRPC address")
	fs.DurationVar(&opts.timeout, "timeout", 5*time.Second, "RPC timeout")
	fs.StringVar(&opts.token, "token", "", "Rota auth token")
	fs.StringVar(&opts.tlsCA, "tls-ca", "", "CA file for TLS server verification")
	fs.StringVar(&opts.tlsServerName, "tls-server-name", "", "TLS server name override")
}

type rotaClients struct {
	conn     *grpc.ClientConn
	control  rotav1.ControlClient
	workflow rotav1.WorkflowClient
	broker   rotav1.BrokerClient
}

func dialClients(opts clientOptions) (*rotaClients, error) {
	if opts.grpcAddr == "" {
		return nil, fmt.Errorf("--grpc is required")
	}
	creds, err := clientTransportCredentials(opts)
	if err != nil {
		return nil, err
	}
	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}
	if opts.token != "" {
		dialOpts = append(dialOpts,
			grpc.WithUnaryInterceptor(authUnaryClientInterceptor(opts.token)),
			grpc.WithStreamInterceptor(authStreamClientInterceptor(opts.token)),
		)
	}
	conn, err := grpc.NewClient(opts.grpcAddr, dialOpts...)
	if err != nil {
		return nil, err
	}
	return &rotaClients{
		conn:     conn,
		control:  rotav1.NewControlClient(conn),
		workflow: rotav1.NewWorkflowClient(conn),
		broker:   rotav1.NewBrokerClient(conn),
	}, nil
}

func (c *rotaClients) close() {
	if c != nil && c.conn != nil {
		_ = c.conn.Close()
	}
}

func commandContext(opts clientOptions) (context.Context, context.CancelFunc) {
	timeout := opts.timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return context.WithTimeout(context.Background(), timeout)
}

func withClients(opts clientOptions, fn func(context.Context, *rotaClients) error) error {
	ctx, cancel := commandContext(opts)
	defer cancel()
	clients, err := dialClients(opts)
	if err != nil {
		return err
	}
	defer clients.close()
	return fn(ctx, clients)
}

func requireFlag(name, value string) error {
	if value == "" {
		return fmt.Errorf("--%s is required", name)
	}
	return nil
}

func clientTransportCredentials(opts clientOptions) (credentials.TransportCredentials, error) {
	if opts.tlsCA != "" {
		return credentials.NewClientTLSFromFile(opts.tlsCA, opts.tlsServerName)
	}
	if opts.tlsServerName != "" {
		return credentials.NewClientTLSFromCert(nil, opts.tlsServerName), nil
	}
	return insecure.NewCredentials(), nil
}

func authUnaryClientInterceptor(token string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, callOpts ...grpc.CallOption) error {
		return invoker(withAuthToken(ctx, token), method, req, reply, cc, callOpts...)
	}
}

func authStreamClientInterceptor(token string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, callOpts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(withAuthToken(ctx, token), desc, cc, method, callOpts...)
	}
}

func withAuthToken(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token, "x-rota-token", token)
}
