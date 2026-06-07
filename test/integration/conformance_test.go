package integration

import (
	"testing"
	"time"

	"github.com/tinkerhaus/rota/internal/fsm"
	"github.com/tinkerhaus/rota/internal/node"
)

// TestBrokerConformance is the compact behavior contract for Rota's core broker
// semantics. Keep this file high-level and cross-feature; feature-specific
// regression tests can stay near the bug they protect.
func TestBrokerConformance(t *testing.T) {
	t.Run("publish lease ack removes work", func(t *testing.T) {
		n := openNode(t, 60_000)
		if _, err := n.Publish(node.PublishReq{Lane: "conf-ack", GroupID: "g", Payload: []byte("x")}); err != nil {
			t.Fatal(err)
		}
		lr, ok := leaseOne(t, n, "conf-ack")
		if !ok {
			t.Fatal("expected a lease")
		}
		if string(lr.Payload) != "x" || lr.GroupID != "g" {
			t.Fatalf("lease = %+v, want group g payload x", lr)
		}
		if err := n.Ack(lr.LeaseID); err != nil {
			t.Fatal(err)
		}
		if _, ok := leaseOne(t, n, "conf-ack"); ok {
			t.Fatal("acked message was leased again")
		}
	})

	t.Run("retry dead letters at max attempts", func(t *testing.T) {
		n := openNode(t, 60_000)
		if _, err := n.Publish(node.PublishReq{Lane: "conf-dlq", GroupID: "g", Payload: []byte("x"), MaxAttempts: 1}); err != nil {
			t.Fatal(err)
		}
		lr, ok := leaseOne(t, n, "conf-dlq")
		if !ok {
			t.Fatal("expected a lease")
		}
		dead, err := n.Nack(lr.LeaseID, fsm.NackRetry, 0, map[string]string{"why": "conformance"})
		if err != nil {
			t.Fatal(err)
		}
		if !dead {
			t.Fatal("retry at max attempts should dead-letter")
		}
		if count, _ := n.DLQCount("conf-dlq"); count != 1 {
			t.Fatalf("DLQ count = %d, want 1", count)
		}
	})

	t.Run("ttl expires undelivered messages", func(t *testing.T) {
		n := openNode(t, 60_000)
		if _, err := n.Publish(node.PublishReq{Lane: "conf-ttl", GroupID: "g", Payload: []byte("x"), TtlMs: 80}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(350 * time.Millisecond)
		if _, ok := leaseOne(t, n, "conf-ttl"); ok {
			t.Fatal("expired message should not be leasable")
		}
		if count, _ := n.DLQCount("conf-ttl"); count != 1 {
			t.Fatalf("DLQ count = %d, want 1", count)
		}
	})

	t.Run("atomic batch dedup collapses duplicates", func(t *testing.T) {
		n := openNode(t, 60_000)
		res, err := n.PublishBatch([]node.PublishReq{
			{Lane: "conf-batch", GroupID: "g", Payload: []byte("one"), DedupKey: "k"},
			{Lane: "conf-batch", GroupID: "g", Payload: []byte("two"), DedupKey: "k"},
		}, true)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 2 || !res[0].OK || !res[1].OK || !res[1].Duplicate || res[0].MsgID != res[1].MsgID {
			t.Fatalf("batch results = %+v, want second item duplicate of first", res)
		}
		if _, ok := leaseOne(t, n, "conf-batch"); !ok {
			t.Fatal("expected one leased message")
		}
		if _, ok := leaseOne(t, n, "conf-batch"); ok {
			t.Fatal("duplicate batch item created a second message")
		}
	})

	t.Run("complete by token failure delay controls retry time", func(t *testing.T) {
		n := openNode(t, 60_000)
		if _, err := n.Publish(node.PublishReq{Lane: "conf-token", GroupID: "g", Payload: []byte("x"), IssueToken: true}); err != nil {
			t.Fatal(err)
		}
		lr, ok := leaseOne(t, n, "conf-token")
		if !ok || len(lr.ExternalToken) == 0 {
			t.Fatalf("expected token lease, ok=%t token_len=%d", ok, len(lr.ExternalToken))
		}
		if _, _, err := n.Complete(lr.ExternalToken, false, nil, 150); err != nil {
			t.Fatal(err)
		}
		if _, ok := leaseOne(t, n, "conf-token"); ok {
			t.Fatal("retry should not be immediately leasable")
		}
		time.Sleep(350 * time.Millisecond)
		if _, ok := leaseOne(t, n, "conf-token"); !ok {
			t.Fatal("retry was not leasable after caller-supplied delay")
		}
	})

	t.Run("stats expose group filter oldest age and ack rate", func(t *testing.T) {
		n := openNode(t, 60_000)
		if _, err := n.Publish(node.PublishReq{Lane: "conf-stats", GroupID: "a", Payload: []byte("a")}); err != nil {
			t.Fatal(err)
		}
		if _, err := n.Publish(node.PublishReq{Lane: "conf-stats", GroupID: "b", Payload: []byte("b")}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
		stats, err := n.Stats("conf-stats", "a")
		if err != nil {
			t.Fatal(err)
		}
		if len(stats) != 1 || stats[0].Leasable != 1 || stats[0].GroupCount != 1 {
			t.Fatalf("filtered stats = %+v, want one leasable message in one group", stats)
		}
		if stats[0].OldestAgeMs == 0 {
			t.Fatalf("oldest age = %d, want > 0", stats[0].OldestAgeMs)
		}
		lr, ok := leaseOne(t, n, "conf-stats")
		if !ok {
			t.Fatal("expected a lease")
		}
		if err := n.Ack(lr.LeaseID); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1200 * time.Millisecond)
		stats, err = n.Stats("conf-stats", "")
		if err != nil {
			t.Fatal(err)
		}
		if len(stats) != 1 || stats[0].AckRate <= 0 {
			t.Fatalf("stats = %+v, want positive ack rate", stats)
		}
	})
}
