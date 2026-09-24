package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"abi/internal/actions"
	"abi/internal/predict"
)

// Actions are attached to the Server with AttachActions (+ AttachPredict for
// the model-health surface). While either is nil their endpoints 503 with a
// clear message, mirroring how the realtime endpoints gate on AttachLive.

// AttachActions activates the approval-queue endpoints (/api/v1/actions/*).
func (s *Server) AttachActions(svc *actions.Service) { s.actions = svc }

// AttachPredict activates the model-health surface (/api/v1/model-drift).
func (s *Server) AttachPredict(p *predict.Service) { s.predict = p }

// DemoActor is the fixed demo identity used for dashboard approvals. Phase 4
// has no real auth (explicit scope note in docs/phase4.md); every decision
// still records WHO decided and WHY in the immutable audit log, so the
// governance story does not depend on a login system.
const DemoActor = "analyst@abi.demo"

// DecidedRequest is the body of approve/reject: a mandatory reason plus an
// optional actor.
type DecidedRequest struct {
	Reason string `json:"reason"`
	Actor  string `json:"actor"`
}

func actionID(r *http.Request) (int64, error) {
	return strconv.ParseInt(r.PathValue("id"), 10, 64)
}

// handleListActions is GET /api/v1/actions?status=&limit= — the approval
// queue (and, with no status filter, the whole recent ledger).
func (s *Server) handleListActions(w http.ResponseWriter, r *http.Request) {
	if s.actions == nil {
		writeError(w, http.StatusServiceUnavailable, "actions not attached", s.now)
		return
	}
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	rows, err := s.actions.List(r.Context(), r.URL.Query().Get("status"), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error(), s.now)
		return
	}
	writeJSON(w, http.StatusOK, envelope(rows, time.Now(), s.now))
}

// handleApproveAction is POST /api/v1/actions/{id}/approve — the human
// approval that authorizes execution (recorded on the audit log first).
func (s *Server) handleApproveAction(w http.ResponseWriter, r *http.Request) {
	s.decideAction(w, r, false)
}

// handleRejectAction is POST /api/v1/actions/{id}/reject — a refused
// proposal records its reason and never executes.
func (s *Server) handleRejectAction(w http.ResponseWriter, r *http.Request) {
	s.decideAction(w, r, true)
}

func (s *Server) decideAction(w http.ResponseWriter, r *http.Request, reject bool) {
	if s.actions == nil {
		writeError(w, http.StatusServiceUnavailable, "actions not attached", s.now)
		return
	}
	id, err := actionID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid action id", s.now)
		return
	}
	var req DecidedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "body must be JSON {reason, actor}", s.now)
		return
	}
	// Mandatory reason: an approval without a reason is not an auditable
	// decision (Phase 4 governance contract).
	if req.Reason == "" {
		writeError(w, http.StatusBadRequest, "reason is required", s.now)
		return
	}
	actor := req.Actor
	if actor == "" {
		actor = DemoActor
	}
	if reject {
		err = s.actions.Reject(r.Context(), id, req.Reason, actor)
	} else {
		err = s.actions.Approve(r.Context(), id, req.Reason, actor)
	}
	if err != nil {
		switch {
		case errors.Is(err, actions.ErrInvalidState):
			writeError(w, http.StatusConflict, err.Error(), s.now)
		case errors.Is(err, actions.ErrUnauthorized):
			writeError(w, http.StatusForbidden, err.Error(), s.now)
		default:
			writeError(w, http.StatusInternalServerError, err.Error(), s.now)
		}
		return
	}
	verb := "rejected"
	if !reject {
		verb = "approved"
	}
	writeJSON(w, http.StatusOK, envelope(map[string]any{
		"id":     id,
		"status": verb,
		"actor":  actor,
	}, time.Now(), s.now))
}

// handleTraceAction is GET /api/v1/actions/{id}/trace — the full evidence
// trail: the proposal's trigger payload + every audit transition.
func (s *Server) handleTraceAction(w http.ResponseWriter, r *http.Request) {
	if s.actions == nil {
		writeError(w, http.StatusServiceUnavailable, "actions not attached", s.now)
		return
	}
	id, err := actionID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid action id", s.now)
		return
	}
	row, audit, err := s.actions.Trace(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error(), s.now)
		return
	}
	writeJSON(w, http.StatusOK, envelope(map[string]any{
		"action": row,
		"audit":  audit,
	}, time.Now(), s.now))
}

// handleModelDrift is GET /api/v1/model-drift?model=&limit= — the model
// health panel's read surface over gold.model_drift (PSI bands + the honest
// distinction between distribution drift and real decay, on the kind field).
func (s *Server) handleModelDrift(w http.ResponseWriter, r *http.Request) {
	if s.predict == nil {
		writeError(w, http.StatusServiceUnavailable, "model surface not attached", s.now)
		return
	}
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	rows, err := s.predict.ListDrift(r.Context(), r.URL.Query().Get("model"), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error(), s.now)
		return
	}
	writeJSON(w, http.StatusOK, envelope(rows, time.Now(), s.now))
}