package fsm

import (
	"hash/fnv"
)

// Retry/backoff tuning. These are constants (not per-node config) so the FSM
// computes identical results on every replica. Per-lane overrides arrive with
// the control plane in a later phase, stored in the replicated FSM.
const (
	defaultMaxAttempts = 3
	backoffBaseMs      = 500
	backoffMaxMs       = 30_000
	backoffMaxShift    = 16
	penaliseExpiry     = false // a pure visibility timeout does NOT burn retry budget
)

// backoffMs returns the delay before the next retry of (msgID, attempt). The
// jitter is DETERMINISTIC (seeded by msgID+attempt), never an RNG, so the next
// ready_at computed inside Apply is identical on all nodes.
func backoffMs(msgID uint64, attempt uint32) uint64 {
	shift := attempt
	if shift > backoffMaxShift {
		shift = backoffMaxShift
	}
	base := uint64(backoffBaseMs) << shift
	if base > backoffMaxMs {
		base = backoffMaxMs
	}
	// Deterministic jitter in [0, base/4].
	h := fnv.New64a()
	var buf [12]byte
	for i := 0; i < 8; i++ {
		buf[i] = byte(msgID >> (8 * i))
	}
	for i := 0; i < 4; i++ {
		buf[8+i] = byte(attempt >> (8 * i))
	}
	_, _ = h.Write(buf[:])
	jitter := h.Sum64() % (base/4 + 1)
	d := base + jitter
	if d > backoffMaxMs {
		d = backoffMaxMs
	}
	return d
}
