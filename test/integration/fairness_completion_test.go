package integration

import (
	"testing"

	"github.com/tinkerhaus/rota/internal/policy"
)

// grantsUnderHeldSlow leases `leases` times from a two-group lane (equal weight),
// acking "fast" immediately (it drains, low in-flight) and HOLDING "slow" (never
// acked, so its in-flight grows). Returns how many grants each group received.
func grantsUnderHeldSlow(t *testing.T, kind policy.Kind, leases int) (fast, slow int) {
	t.Helper()
	n := openNode(t, 60_000)
	const lane = "ca"
	if err := n.SetPolicy(lane, policy.Binding{Kind: kind}); err != nil {
		t.Fatalf("set policy %s: %v", kind, err)
	}
	pubN(t, n, lane, "fast", leases+100, 1)
	pubN(t, n, lane, "slow", leases+100, 1)
	for i := 0; i < leases; i++ {
		lr, ok := leaseOne(t, n, lane)
		if !ok {
			break
		}
		if lr.GroupID == "fast" {
			_ = n.Ack(lr.LeaseID) // drains => low in-flight
			fast++
		} else {
			slow++ // held: never ack => 'slow' accumulates in-flight
		}
	}
	return fast, slow
}

// completion_aware must SUSTAINABLY throttle a tenant that hoards in-flight work —
// its per-serve cost grows with in-flight, so it is served progressively less. (The
// old additive version only produced a one-time offset and stayed ~50/50.)
func TestCompletionAwareThrottlesHoarder(t *testing.T) {
	fast, slow := grantsUnderHeldSlow(t, policy.KindCompletionAware, 150)
	t.Logf("completion_aware: fast=%d slow=%d", fast, slow)
	if fast < slow*3 {
		t.Fatalf("completion_aware should throttle the hoarder (want fast >> slow): fast=%d slow=%d", fast, slow)
	}
}

// Control: plain WFQ is lease-fair — it ignores in-flight, so a tenant holding
// leases still gets ~its fair share of grants (this is the behaviour completion_aware
// deliberately departs from).
func TestWFQIgnoresHoarding(t *testing.T) {
	fast, slow := grantsUnderHeldSlow(t, policy.KindWFQ, 150)
	t.Logf("WFQ: fast=%d slow=%d", fast, slow)
	if slow < fast/2 {
		t.Fatalf("WFQ should stay roughly lease-fair despite held leases: fast=%d slow=%d", fast, slow)
	}
}
