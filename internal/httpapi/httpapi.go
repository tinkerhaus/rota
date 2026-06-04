// Package httpapi is the in-process HTTP/JSON gateway for the operator dashboard.
// It mounts a small set of REST routes onto the existing metrics mux and serves
// the single-page app statically. Responses are proto messages marshaled with
// protojson, so field names are stable camelCase and match the wire schema.
//
// Reads go straight to the node's follower-servable methods. The one mutating
// route (DLQ redrive) calls the node directly; if this node is not the raft
// leader it returns 409 Conflict with a redirect hint (the leader's gRPC addr),
// mirroring the gRPC leader guard rather than silently failing on a follower.
package httpapi

import (
	"net/http"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/node"
	"github.com/tinkerhaus/rota/internal/transport"
)

// marshaler emits stable camelCase JSON, includes zero-valued fields (so the SPA
// sees a consistent shape), and pretty-prints for human-friendly curling.
var marshaler = protojson.MarshalOptions{EmitUnpopulated: true, UseProtoNames: false, Indent: "  "}

// Handler builds the dashboard HTTP gateway. Control is the in-process Control
// service (the same one served over gRPC) so HTTP and gRPC share one code path.
// staticDir is served for any unmatched route (the SPA); a missing dir simply
// 404s, so the Go binary never depends on a frontend build existing.
func Handler(n *node.Node, control *transport.ControlService, staticDir string) http.Handler {
	mux := http.NewServeMux()
	api := &server{n: n, control: control}

	mux.HandleFunc("/api/cluster", api.cluster)
	mux.HandleFunc("/api/health", api.health)
	mux.HandleFunc("/api/stats", api.stats)
	// Lane sub-resources share one prefix handler that dispatches on the tail.
	mux.HandleFunc("/api/lanes/", api.lanes)
	// Policy sub-resources: /api/policy/{lane}/health.
	mux.HandleFunc("/api/policy/", api.policy)

	// Static SPA for everything else.
	fs := http.FileServer(http.Dir(staticDir))
	mux.Handle("/", spaFallback(staticDir, fs))
	return mux
}

type server struct {
	n       *node.Node
	control *transport.ControlService
}

// ─── Top-level read routes ──────────────────────────────────────────────────────

func (s *server) cluster(w http.ResponseWriter, r *http.Request) {
	if !get(w, r) {
		return
	}
	resp, err := s.control.DescribeCluster(r.Context(), &rotav1.DescribeClusterRequest{})
	writeProto(w, resp, err)
}

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	if !get(w, r) {
		return
	}
	resp, err := s.control.Health(r.Context(), &rotav1.HealthRequest{})
	writeProto(w, resp, err)
}

func (s *server) stats(w http.ResponseWriter, r *http.Request) {
	if !get(w, r) {
		return
	}
	resp, err := s.control.GetStats(r.Context(), &rotav1.GetStatsRequest{Lane: r.URL.Query().Get("lane")})
	writeProto(w, resp, err)
}

// ─── Lane sub-resource dispatch ─────────────────────────────────────────────────
//
// Routes under /api/lanes/{lane}/...:
//
//	GET  groups, dlq, leases
//	GET  groups/{group}/messages
//	POST dlq/redrive
func (s *server) lanes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/api/lanes/")
	parts := strings.Split(rest, "/")
	if len(parts) < 2 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	lane := parts[0]
	switch {
	case len(parts) == 2 && parts[1] == "groups":
		s.listGroups(w, r, lane)
	case len(parts) == 2 && parts[1] == "dlq":
		s.listDLQ(w, r, lane)
	case len(parts) == 2 && parts[1] == "leases":
		s.listLeases(w, r, lane)
	case len(parts) == 2 && parts[1] == "fairness":
		s.fairness(w, r, lane)
	case len(parts) == 3 && parts[1] == "fairness" && parts[2] == "stream":
		s.fairnessStream(w, r, lane)
	case len(parts) == 4 && parts[1] == "groups" && parts[3] == "messages":
		s.peekMessages(w, r, lane, parts[2])
	case len(parts) == 3 && parts[1] == "dlq" && parts[2] == "redrive":
		s.redrive(w, r, lane)
	default:
		http.NotFound(w, r)
	}
}

// policy dispatches /api/policy/{lane}/health.
func (s *server) policy(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/api/policy/")
	parts := strings.Split(rest, "/")
	if len(parts) == 2 && parts[0] != "" && parts[1] == "health" {
		s.policyHealth(w, r, parts[0])
		return
	}
	http.NotFound(w, r)
}

func (s *server) listGroups(w http.ResponseWriter, r *http.Request, lane string) {
	if !get(w, r) {
		return
	}
	q := r.URL.Query()
	resp, err := s.control.ListGroups(r.Context(), &rotav1.ListGroupsRequest{
		Lane: lane, PageSize: parseU32(q.Get("page_size")), PageToken: q.Get("page_token"),
	})
	writeProto(w, resp, err)
}

func (s *server) listDLQ(w http.ResponseWriter, r *http.Request, lane string) {
	if !get(w, r) {
		return
	}
	q := r.URL.Query()
	resp, err := s.control.ListDeadLetters(r.Context(), &rotav1.ListDeadLettersRequest{
		Lane: lane, PageSize: parseU32(q.Get("page_size")), PageToken: q.Get("page_token"),
	})
	writeProto(w, resp, err)
}

func (s *server) listLeases(w http.ResponseWriter, r *http.Request, lane string) {
	if !get(w, r) {
		return
	}
	q := r.URL.Query()
	resp, err := s.control.ListLeases(r.Context(), &rotav1.ListLeasesRequest{
		Lane: lane, PageSize: parseU32(q.Get("page_size")), PageToken: q.Get("page_token"),
	})
	writeProto(w, resp, err)
}

func (s *server) peekMessages(w http.ResponseWriter, r *http.Request, lane, group string) {
	if !get(w, r) {
		return
	}
	resp, err := s.control.PeekMessages(r.Context(), &rotav1.PeekMessagesRequest{
		Lane: lane, GroupId: group, Limit: parseU32(r.URL.Query().Get("limit")),
	})
	writeProto(w, resp, err)
}

// redrive is the one mutating route. It must run on the leader; on a follower we
// return 409 with the leader's gRPC address so the caller can redirect, instead
// of failing opaquely deep in raft.
func (s *server) redrive(w http.ResponseWriter, r *http.Request, lane string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.n.IsLeader() {
		addr, id := s.n.LeaderHint()
		w.Header().Set("X-Rota-Leader-Addr", addr)
		w.Header().Set("X-Rota-Leader-Id", id)
		http.Error(w, "not leader: redirect to "+addr, http.StatusConflict)
		return
	}
	var body struct {
		GroupID string `json:"group_id"`
		MsgID   uint64 `json:"msg_id"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	resp, err := s.control.RedriveDeadLetter(r.Context(), &rotav1.RedriveDeadLetterRequest{
		Lane: lane, GroupId: body.GroupID, MsgId: body.MsgID,
	})
	writeProto(w, resp, err)
}

// ─── Helpers ─────────────────────────────────────────────────────────────────────

func get(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return false
	}
	return true
}

func parseU32(s string) uint32 {
	if s == "" {
		return 0
	}
	v, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0
	}
	return uint32(v)
}

func writeProto(w http.ResponseWriter, m proto.Message, err error) {
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	out, merr := marshaler.Marshal(m)
	if merr != nil {
		http.Error(w, merr.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(out)
}
