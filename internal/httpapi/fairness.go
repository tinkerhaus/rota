package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
)

// ─── Fairness Observatory routes ────────────────────────────────────────────────
//
//	GET /api/lanes/{lane}/fairness          -> snapshot JSON (GetLaneFairness)
//	GET /api/lanes/{lane}/fairness/stream   -> text/event-stream (coalesced ~300ms)
//	GET /api/policy/{lane}/health           -> policy health JSON (GetPolicyHealth)

// coalesceInterval is the SSE render cadence: the projection is nudged on every
// lease, but we batch those nudges and emit at most one `served` frame per tick.
const coalesceInterval = 300 * time.Millisecond

// keepaliveInterval bounds how long the stream stays silent; an SSE comment line
// keeps proxies and the browser EventSource from timing the connection out.
const keepaliveInterval = 15 * time.Second

// fairness serves the point-in-time per-group fairness snapshot.
func (s *server) fairness(w http.ResponseWriter, r *http.Request, lane string) {
	if !get(w, r) {
		return
	}
	resp, err := s.control.GetLaneFairness(r.Context(), &rotav1.LaneRef{Lane: lane})
	writeProto(w, resp, err)
}

// policyHealth serves the lane's scheduling-policy health.
func (s *server) policyHealth(w http.ResponseWriter, r *http.Request, lane string) {
	if !get(w, r) {
		return
	}
	resp, err := s.control.GetPolicyHealth(r.Context(), &rotav1.LaneRef{Lane: lane})
	writeProto(w, resp, err)
}

// ribbonFrame is the SSE `served` event payload (the dashboard fairness ribbon).
type ribbonFrame struct {
	Ribbon      []string              `json:"ribbon"`
	Shares      map[string]shareEntry `json:"shares"`
	TotalServed uint64                `json:"totalServed"`
}

type shareEntry struct {
	Expected float64 `json:"expected"`
	Actual   float64 `json:"actual"`
	Served   uint64  `json:"served"`
}

// fairnessStream is the SSE endpoint. It subscribes to the leader's projection,
// coalesces nudges on a ~300ms ticker, and emits a `served` event with the recent
// ribbon + per-group shares. A keepalive comment is sent on idle so the stream
// stays open. The writer is context-cancel-aware: when the client disconnects
// (r.Context() done) or write/flush is impossible, the loop exits and the
// subscription is torn down. The hot lease path is untouched — it only does a
// non-blocking nudge into a buffered channel.
func (s *server) fairnessStream(w http.ResponseWriter, r *http.Request, lane string) {
	if !get(w, r) {
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // disable proxy buffering (nginx)
	w.WriteHeader(http.StatusOK)

	nudges, unsub := s.n.SubscribeFairness(lane)
	defer unsub()

	ctx := r.Context()
	ticker := time.NewTicker(coalesceInterval)
	defer ticker.Stop()
	keepalive := time.NewTicker(keepaliveInterval)
	defer keepalive.Stop()

	// Emit one frame immediately so a freshly-connected client sees current state
	// without waiting a full tick (and so a test can read at least one event fast).
	if !s.emitServed(w, flusher, lane) {
		return
	}

	dirty := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-nudges:
			dirty = true // coalesce: defer the actual emit to the next tick
		case <-ticker.C:
			if !dirty {
				continue
			}
			dirty = false
			if !s.emitServed(w, flusher, lane) {
				return
			}
		case <-keepalive.C:
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// emitServed writes one `served` SSE event for the lane. Returns false if the
// write failed (client gone) so the caller can stop the stream.
func (s *server) emitServed(w http.ResponseWriter, flusher http.Flusher, lane string) bool {
	snap := s.n.FairnessSnapshot(lane)

	// Join the rolling counts with expected shares from GetLaneFairness so the
	// frame carries both halves of the fairness signal in one event.
	shares := map[string]shareEntry{}
	if lf, err := s.control.GetLaneFairness(context.Background(), &rotav1.LaneRef{Lane: lane}); err == nil {
		for _, g := range lf.GetGroups() {
			shares[g.GetGroupId()] = shareEntry{
				Expected: g.GetExpectedShare(),
				Actual:   g.GetActualShare(),
				Served:   g.GetServed(),
			}
		}
	}
	// Ensure every group that appears in the ribbon/counts has a share entry, even
	// if it had no GroupMeta row (defensive — keeps the frame self-consistent).
	for g, c := range snap.Served {
		if _, ok := shares[g]; !ok {
			var actual float64
			if snap.Total > 0 {
				actual = float64(c) / float64(snap.Total)
			}
			shares[g] = shareEntry{Actual: actual, Served: c}
		}
	}

	frame := ribbonFrame{Ribbon: snap.Ribbon, Shares: shares, TotalServed: snap.Total}
	if frame.Ribbon == nil {
		frame.Ribbon = []string{}
	}
	payload, err := json.Marshal(frame)
	if err != nil {
		return false
	}
	if _, err := fmt.Fprintf(w, "event: served\ndata: %s\n\n", payload); err != nil {
		return false
	}
	flusher.Flush()
	return true
}
