package transport

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestRequireAuth(t *testing.T) {
	const token = "secret"
	tests := []struct {
		name    string
		method  string
		md      metadata.MD
		wantErr bool
	}{
		{name: "non rota method allowed", method: "/grpc.health.v1.Health/Check", wantErr: false},
		{name: "missing metadata rejected", method: "/rota.v1.Control/GetStats", wantErr: true},
		{name: "x rota token accepted", method: "/rota.v1.Control/GetStats", md: metadata.Pairs("x-rota-token", token), wantErr: false},
		{name: "bearer accepted", method: "/rota.v1.Control/GetStats", md: metadata.Pairs("authorization", "Bearer "+token), wantErr: false},
		{name: "invalid rejected", method: "/rota.v1.Control/GetStats", md: metadata.Pairs("authorization", "Bearer wrong"), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.md != nil {
				ctx = metadata.NewIncomingContext(ctx, tt.md)
			}
			err := requireAuth(ctx, tt.method, token)
			if tt.wantErr {
				if status.Code(err) != codes.Unauthenticated {
					t.Fatalf("err = %v, want unauthenticated", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
		})
	}
}
