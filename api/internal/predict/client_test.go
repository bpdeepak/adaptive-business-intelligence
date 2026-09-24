package predict

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// The sidecar refuses an incomplete feature vector with a 400; the client must
// surface that as a RejectedError (caller error), not a generic gateway failure.
func TestClientSurfacesSidecar400AsRejectedError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"missing features for fraud_risk: velocity_24h"}`))
	}))
	defer srv.Close()

	_, err := NewScoreClient(srv.URL).Score(context.Background(), "fraud_risk", map[string]float64{"order_value": 1})
	var rejected *RejectedError
	if !errors.As(err, &rejected) {
		t.Fatalf("err = %v, want *RejectedError", err)
	}
	if rejected.Message != "missing features for fraud_risk: velocity_24h" {
		t.Fatalf("message = %q", rejected.Message)
	}
}

func TestClientKeepsOther5xxAsPlainError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"boom"}`))
	}))
	defer srv.Close()
	_, err := NewScoreClient(srv.URL).Score(context.Background(), "m", map[string]float64{"a": 1})
	var rejected *RejectedError
	if err == nil || errors.As(err, &rejected) {
		t.Fatalf("a 500 must stay a gateway-class error, got %v", err)
	}
}
