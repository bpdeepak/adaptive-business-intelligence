package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"abi/internal/model"
	"abi/internal/realtime"
	"abi/internal/store"
)

// sseServer builds a Server with a live feed attached.
func sseServer(t *testing.T) (*Server, *realtime.Broadcaster) {
	t.Helper()
	fake := &store.FakeStore{
		OpenAnomaliesFn: func(context.Context, string) ([]model.Anomaly, error) {
			return []model.Anomaly{{
				ID: 1, Metric: "revenue", Detector: "statistical", BucketStart: "2026-01-01T00:00:00Z",
				Observed: 9000, Expected: 4000, ZScore: 5.2, Severity: "severe",
				Status: "open", DetectedAt: "2026-01-01T00:05:00Z",
			}}, nil
		},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := New(fake, logger)
	bc := realtime.NewBroadcaster()
	srv.AttachLive(realtime.NewLiveFeed(bc, 2880, "replay"))
	return srv, bc
}

func TestStreamUnavailableWithoutLive(t *testing.T) {
	srv := testServer(t) // never attached
	req := httptest.NewRequest(http.MethodGet, "/api/v1/stream/metrics", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestSSEStreamsUpdates(t *testing.T) {
	srv, bc := sseServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stream/metrics", nil)
	ctx, cancel := context.WithCancel(req.Context())
	req = req.WithContext(ctx)
	defer cancel()

	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		srv.ServeHTTP(rec, req)
		close(done)
	}()

	// The handler subscribes before we publish; wait a moment for it to attach,
	// then push a MetricsUpdate.
	time.Sleep(100 * time.Millisecond)
	bc.Publish(model.MetricsUpdate{
		Current:         model.RealtimeBucket{BucketStart: "2026-01-01T00:01:00Z", Revenue: 321.5, Orders: 3},
		SpeedMultiplier: 2880,
		Status:          "replay",
	})

	deadline := time.After(3 * time.Second)
	for {
		body := rec.Body.String()
		if strings.Contains(body, "event: metrics") && strings.Contains(body, "321.5") {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("SSE frame never arrived; body so far: %s", body)
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Parse the data line as a MetricsUpdate to confirm the payload shape.
	dataLine := ""
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if strings.HasPrefix(line, "data: ") {
			dataLine = strings.TrimPrefix(line, "data: ")
		}
	}
	if dataLine == "" {
		t.Fatal("no data line found")
	}
	var got model.MetricsUpdate
	if err := json.Unmarshal([]byte(dataLine), &got); err != nil {
		t.Fatalf("bad SSE data JSON: %v", err)
	}
	if got.Current.Revenue != 321.5 || got.SpeedMultiplier != 2880 || got.Status != "replay" {
		t.Fatalf("unexpected SSE payload: %+v", got)
	}
	if !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/event-stream") {
		t.Fatalf("content-type = %q", rec.Header().Get("Content-Type"))
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return after cancellation")
	}
}

func TestRealtimeMetricsEndpoint(t *testing.T) {
	fake := &store.FakeStore{
		RecentMetricsFn: func(_ context.Context, n int) ([]model.RealtimeBucket, error) {
			return []model.RealtimeBucket{
				{BucketStart: "2026-01-01T00:00:00Z", Revenue: 100.5, Orders: 2, ActiveSessions: 5},
				{BucketStart: "2026-01-01T00:01:00Z", Revenue: 250, Orders: 4, ActiveSessions: 7},
			}, nil
		},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := New(fake, logger)

	rec, env := doReq(t, srv, http.MethodGet, "/api/v1/realtime/metrics?limit=3")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	data, ok := env.Data.(map[string]any)
	if !ok {
		t.Fatalf("data = %v", env.Data)
	}
	raw, err := json.Marshal(data["buckets"])
	if err != nil {
		t.Fatal(err)
	}
	var buckets []model.RealtimeBucket
	if err := json.Unmarshal(raw, &buckets); err != nil {
		t.Fatal(err)
	}
	if len(buckets) != 2 || buckets[0].Revenue != 100.5 || buckets[1].Orders != 4 {
		t.Fatalf("unexpected buckets payload: %+v", buckets)
	}
}

func TestAnomaliesEndpoint(t *testing.T) {
	srv, _ := sseServer(t)
	rec, env := doReq(t, srv, http.MethodGet, "/api/v1/anomalies")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	raw, err := json.Marshal(env.Data)
	if err != nil {
		t.Fatal(err)
	}
	var anoms []model.Anomaly
	if err := json.Unmarshal(raw, &anoms); err != nil {
		t.Fatal(err)
	}
	if len(anoms) != 1 || anoms[0].ID != 1 || anoms[0].Metric != "revenue" {
		t.Fatalf("unexpected anomalies payload: %+v", anoms)
	}
}

func TestDismissAnomaly(t *testing.T) {
	srv, _ := sseServer(t)
	rec, env := doReq(t, srv, http.MethodPost, "/api/v1/anomalies/7/dismiss")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	data, ok := env.Data.(map[string]any)
	if !ok {
		t.Fatalf("data = %v", env.Data)
	}
	if data["status"] != "dismissed" {
		t.Fatalf("unexpected dismiss payload: %v", data)
	}

	// Non-numeric id → 400.
	rec2, _ := doReq(t, srv, http.MethodPost, "/api/v1/anomalies/abc/dismiss")
	if rec2.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec2.Code)
	}
}
