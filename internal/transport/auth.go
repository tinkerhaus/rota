package transport

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// AuthUnaryInterceptor enforces a static bearer/API token for Rota service
// methods. Non-Rota services such as grpc.health.v1 and reflection remain open so
// probes and tooling can still discover a locked-down node.
func AuthUnaryInterceptor(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if err := requireAuth(ctx, info.FullMethod, token); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// AuthStreamInterceptor is the streaming twin of AuthUnaryInterceptor.
func AuthStreamInterceptor(token string) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := requireAuth(stream.Context(), info.FullMethod, token); err != nil {
			return err
		}
		return handler(srv, stream)
	}
}

func requireAuth(ctx context.Context, method, token string) error {
	if token == "" || !strings.HasPrefix(method, "/rota.v1.") {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing auth metadata")
	}
	for _, got := range md.Get("x-rota-token") {
		if got == token {
			return nil
		}
	}
	for _, got := range md.Get("authorization") {
		if strings.TrimSpace(got) == "Bearer "+token {
			return nil
		}
	}
	return status.Error(codes.Unauthenticated, "invalid rota auth token")
}
