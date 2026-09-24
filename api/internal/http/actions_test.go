package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"abi/internal/actions"
)

// HTTP-level tests for the approval-queue handlers (audit H7): the mandatory
// reason, actor defaulting, and the mapping of service errors to 400/403/404/409/500.
// They run against a fake service so they need no database.

type fakeActions struct {
	approveErr, rejectErr, retryErr, traceErr error
	calls                                     []string
}

func (f *fakeActions) List(_ context.Context, status string, limit int) ([]actions.ActionRow, error) {
	f.calls = append(f.calls, fmt.Sprintf("list:%s:%d", status, limit))
	return []actions.ActionRow{{ID: 1, Action: "hold_order_for_review", Status: "pending"}}, nil
}
func (f *fakeActions) Approve(_ context.Context, id int64, reason, actor string) error {
	f.calls = append(f.calls, fmt.Sprintf("approve:%d:%s:%s", id, reason, actor))
	return f.approveErr
}
func (f *fakeActions) Reject(_ context.Context, id int64, reason, actor string) error {
	f.calls = append(f.calls, fmt.Sprintf("reject:%d:%s:%s", id, reason, actor))
	return f.rejectErr
}
func (f *fakeActions) Retry(_ context.Context, id int64, actor string) error {
	f.calls = append(f.calls, fmt.Sprintf("retry:%d:%s", id, actor))
	return f.retryErr
}
func (f *fakeActions) Trace(_ context.Context, id int64) (actions.ActionRow, []actions.AuditRow, error) {
	if f.traceErr != nil {
		return actions.ActionRow{}, nil, f.traceErr
	}
	return actions.ActionRow{ID: id}, []actions.AuditRow{{ID: 1, ActionID: id, Transition: "proposed"}}, nil
}

func post(t *testing.T, srv http.Handler, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return rec
}

func serverWith(t *testing.T, f *fakeActions) *Server {
	t.Helper()
	srv := testServer(t)
	srv.AttachActions(f)
	return srv
}

func TestApproveAndRejectRequireAReason(t *testing.T) {
	f := &fakeActions{}
	srv := serverWith(t, f)
	for _, verb := range []string{"approve", "reject"} {
		for _, body := range []string{`{"actor":"a"}`, `{"reason":"","actor":"a"}`} {
			if rec := post(t, srv, "/api/v1/actions/7/"+verb, body); rec.Code != http.StatusBadRequest {
				t.Errorf("%s %s: status %d, want 400 (an approval without a reason is not an auditable decision)", verb, body, rec.Code)
			}
		}
	}
	if len(f.calls) != 0 {
		t.Errorf("service was called despite the missing reason: %v", f.calls)
	}
}

func TestApproveDefaultsTheDemoActorAndPassesTheReason(t *testing.T) {
	f := &fakeActions{}
	srv := serverWith(t, f)
	if rec := post(t, srv, "/api/v1/actions/7/approve", `{"reason":"looks fine"}`); rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if want := "approve:7:looks fine:" + DemoActor; len(f.calls) != 1 || f.calls[0] != want {
		t.Errorf("calls = %v, want [%s]", f.calls, want)
	}
	f.calls = nil
	post(t, srv, "/api/v1/actions/8/reject", `{"reason":"no","actor":"bob@abi"}`)
	if want := "reject:8:no:bob@abi"; len(f.calls) != 1 || f.calls[0] != want {
		t.Errorf("calls = %v, want [%s]", f.calls, want)
	}
}

func TestServiceErrorsMapToTheRightStatus(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"already decided", fmt.Errorf("%w: id 7", actions.ErrInvalidState), http.StatusConflict},
		{"execution without authority", fmt.Errorf("%w: action_id 7", actions.ErrUnauthorized), http.StatusForbidden},
		{"executor / storage failure", errors.New("boom"), http.StatusInternalServerError},
	}
	for _, c := range cases {
		f := &fakeActions{approveErr: c.err, retryErr: c.err}
		srv := serverWith(t, f)
		if rec := post(t, srv, "/api/v1/actions/7/approve", `{"reason":"r"}`); rec.Code != c.want {
			t.Errorf("approve/%s: status %d, want %d", c.name, rec.Code, c.want)
		}
		if rec := post(t, srv, "/api/v1/actions/7/retry", `{}`); rec.Code != c.want {
			t.Errorf("retry/%s: status %d, want %d", c.name, rec.Code, c.want)
		}
	}
}

func TestRetryNeedsNoReasonButRecordsTheActor(t *testing.T) {
	f := &fakeActions{}
	srv := serverWith(t, f)
	if rec := post(t, srv, "/api/v1/actions/9/retry", ``); rec.Code != http.StatusOK {
		t.Fatalf("retry with an empty body: status %d", rec.Code)
	}
	if want := "retry:9:" + DemoActor; len(f.calls) != 1 || f.calls[0] != want {
		t.Errorf("calls = %v, want [%s]", f.calls, want)
	}
}

func TestBadIdsAndBodiesAre400AndMissingTraceIs404(t *testing.T) {
	f := &fakeActions{traceErr: errors.New("action 5 not found")}
	srv := serverWith(t, f)
	if rec := post(t, srv, "/api/v1/actions/abc/approve", `{"reason":"r"}`); rec.Code != http.StatusBadRequest {
		t.Errorf("non-numeric id: status %d, want 400", rec.Code)
	}
	if rec := post(t, srv, "/api/v1/actions/7/approve", `not json`); rec.Code != http.StatusBadRequest {
		t.Errorf("malformed body: status %d, want 400", rec.Code)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/actions/5/trace", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("trace of a missing action: status %d, want 404", rec.Code)
	}
}

func TestActionsEndpointsAre503WhenNotAttached(t *testing.T) {
	srv := testServer(t) // no AttachActions
	if rec := post(t, srv, "/api/v1/actions/7/approve", `{"reason":"r"}`); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status %d, want 503", rec.Code)
	}
}
