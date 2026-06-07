package transport

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/node"
)

func TestRequireAuthReplicatedGrants(t *testing.T) {
	n, err := node.Open(node.Config{DataDir: t.TempDir(), NodeID: "auth-test"})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if err := n.WaitLeader(5 * time.Second); err != nil {
		t.Fatal(err)
	}

	if err := requireAuth(context.Background(), n, rotav1.Control_GetStats_FullMethodName, one(rotav1.AuthAction_AUTH_READ, "", "")); err != nil {
		t.Fatalf("auth disabled err = %v, want nil", err)
	}

	_, token, err := n.CreateAuthPrincipal("reader", nil, []*rotav1.AuthGrant{{
		LanePattern: ".*", GroupPattern: ".*", Actions: []rotav1.AuthAction{rotav1.AuthAction_AUTH_READ},
	}})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		method string
		token  string
		checks []node.AuthCheck
		code   codes.Code
	}{
		{name: "non rota method allowed", method: "/grpc.health.v1.Health/Check", checks: one(rotav1.AuthAction_AUTH_ADMIN, "", ""), code: codes.OK},
		{name: "missing token rejected", method: rotav1.Control_GetStats_FullMethodName, checks: one(rotav1.AuthAction_AUTH_READ, "", ""), code: codes.Unauthenticated},
		{name: "read token can read", method: rotav1.Control_GetStats_FullMethodName, token: token, checks: one(rotav1.AuthAction_AUTH_READ, "", ""), code: codes.OK},
		{name: "read token cannot publish", method: rotav1.Broker_Publish_FullMethodName, token: token, checks: one(rotav1.AuthAction_AUTH_PUBLISH, "jobs", "a"), code: codes.PermissionDenied},
		{name: "invalid token rejected", method: rotav1.Control_GetStats_FullMethodName, token: "wrong", checks: one(rotav1.AuthAction_AUTH_READ, "", ""), code: codes.Unauthenticated},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.token != "" {
				ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer "+tt.token))
			}
			err := requireAuth(ctx, n, tt.method, tt.checks)
			if tt.code == codes.OK {
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
				return
			}
			if status.Code(err) != tt.code {
				t.Fatalf("err = %v, want %s", err, tt.code)
			}
		})
	}
}

func TestWorkStreamChecksLaneAndGroup(t *testing.T) {
	n, err := node.Open(node.Config{DataDir: t.TempDir(), NodeID: "auth-work-test"})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if err := n.WaitLeader(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	_, token, err := n.CreateAuthPrincipal("worker", nil, []*rotav1.AuthGrant{{
		LanePattern: "^jobs$", GroupPattern: "^tenant-a$", Actions: []rotav1.AuthAction{rotav1.AuthAction_AUTH_CONSUME},
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-rota-token", token))

	msg := &rotav1.WorkClientMsg{Msg: &rotav1.WorkClientMsg_LeaseRequest{
		LeaseRequest: &rotav1.LeaseRequest{Lane: "jobs", GroupAllow: []string{"tenant-a"}, Credit: 1},
	}}
	checks, requiresPrior := checksForStreamMessage(rotav1.Broker_Work_FullMethodName, msg)
	if requiresPrior {
		t.Fatal("lease request should not require prior stream auth")
	}
	if err := requireAuth(ctx, n, rotav1.Broker_Work_FullMethodName, checks); err != nil {
		t.Fatalf("authorized group err = %v, want nil", err)
	}

	msg.GetLeaseRequest().GroupAllow = []string{"tenant-b"}
	checks, _ = checksForStreamMessage(rotav1.Broker_Work_FullMethodName, msg)
	if err := requireAuth(ctx, n, rotav1.Broker_Work_FullMethodName, checks); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("wrong group err = %v, want permission denied", err)
	}
}
