package httpapi

import (
	"net/http"
	"strconv"
	"strings"

	rotav1 "github.com/tinkerhaus/rota/gen/rota/v1"
	"github.com/tinkerhaus/rota/internal/node"
)

// workflows dispatches the durable-execution dashboard routes:
//
//	GET  /api/workflows?status=&page_token=     list runs
//	POST /api/workflows {workflowType,tenantId,input}   start
//	GET  /api/workflows/{id}                     run detail
//	GET  /api/workflows/{id}/history             event history
//	POST /api/workflows/{id}/signal {name,payload}
//	POST /api/workflows/{id}/cancel {reason}
func (s *server) workflows(w http.ResponseWriter, r *http.Request) {
	rest := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/workflows"), "/")
	if rest == "" {
		switch r.Method {
		case http.MethodGet:
			s.listWorkflowRuns(w, r)
		case http.MethodPost:
			s.startWorkflow(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	parts := strings.Split(rest, "/")
	id, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	ref := &rotav1.WorkflowRunRef{RunId: id}
	switch {
	case len(parts) == 1 && r.Method == http.MethodGet:
		if !s.authorizeWorkflowRun(w, r, rotav1.AuthAction_AUTH_READ, id) {
			return
		}
		resp, err := s.wf.GetWorkflowRun(r.Context(), ref)
		writeProto(w, resp, err)
	case len(parts) == 2 && parts[1] == "history" && r.Method == http.MethodGet:
		if !s.authorizeWorkflowRun(w, r, rotav1.AuthAction_AUTH_READ, id) {
			return
		}
		resp, err := s.wf.GetWorkflowHistory(r.Context(), ref)
		writeProto(w, resp, err)
	case len(parts) == 2 && parts[1] == "signal" && r.Method == http.MethodPost:
		s.signalWorkflow(w, r, id)
	case len(parts) == 2 && parts[1] == "cancel" && r.Method == http.MethodPost:
		s.cancelWorkflow(w, r, id)
	default:
		http.NotFound(w, r)
	}
}

func (s *server) listWorkflowRuns(w http.ResponseWriter, r *http.Request) {
	if !s.authorize(w, r, []node.AuthCheck{{Action: rotav1.AuthAction_AUTH_READ}}) {
		return
	}
	req := &rotav1.ListWorkflowRunsRequest{
		PageSize:  parseU32(r.URL.Query().Get("page_size")),
		PageToken: r.URL.Query().Get("page_token"),
	}
	if st := r.URL.Query().Get("status"); st != "" {
		if v, ok := rotav1.WorkflowStatus_value[st]; ok {
			req.Status = rotav1.WorkflowStatus(v)
			req.HasStatus = true
		}
	}
	resp, err := s.wf.ListWorkflowRuns(r.Context(), req)
	writeProto(w, resp, err)
}

func (s *server) startWorkflow(w http.ResponseWriter, r *http.Request) {
	if !s.requireLeader(w) {
		return
	}
	var body struct {
		WorkflowType string `json:"workflowType"`
		TenantID     string `json:"tenantId"`
		Input        string `json:"input"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	if !s.authorize(w, r, []node.AuthCheck{{Action: rotav1.AuthAction_AUTH_WORKFLOW, Lane: body.WorkflowType, Group: body.TenantID}}) {
		return
	}
	resp, err := s.wf.StartWorkflow(r.Context(), &rotav1.StartWorkflowRequest{
		WorkflowType: body.WorkflowType, TenantId: body.TenantID, Input: []byte(body.Input),
	})
	writeProto(w, resp, err)
}

func (s *server) signalWorkflow(w http.ResponseWriter, r *http.Request, id uint64) {
	if !s.requireLeader(w) {
		return
	}
	var body struct {
		SignalName string `json:"signalName"`
		Payload    string `json:"payload"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	if !s.authorizeWorkflowRun(w, r, rotav1.AuthAction_AUTH_WORKFLOW, id) {
		return
	}
	resp, err := s.wf.SignalWorkflow(r.Context(), &rotav1.SignalWorkflowRequest{
		RunId: id, SignalName: body.SignalName, Payload: []byte(body.Payload),
	})
	writeProto(w, resp, err)
}

func (s *server) cancelWorkflow(w http.ResponseWriter, r *http.Request, id uint64) {
	if !s.requireLeader(w) {
		return
	}
	if !s.authorizeWorkflowRun(w, r, rotav1.AuthAction_AUTH_WORKFLOW, id) {
		return
	}
	resp, err := s.wf.CancelWorkflow(r.Context(), &rotav1.CancelWorkflowRequest{
		RunId: id, Reason: []byte(r.URL.Query().Get("reason")),
	})
	writeProto(w, resp, err)
}

func (s *server) authorizeWorkflowRun(w http.ResponseWriter, r *http.Request, action rotav1.AuthAction, id uint64) bool {
	run, ok := s.n.GetRun(id)
	if !ok {
		return s.authorize(w, r, []node.AuthCheck{{Action: action}})
	}
	return s.authorize(w, r, []node.AuthCheck{{Action: action, Lane: run.GetWorkflowType(), Group: run.GetTenantId()}})
}
