//go:build integration

package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"abi/internal/realtime"
)

// The banner hysteresis (audit C6): a model rate anomaly is persisted on its first
// breached window but only *surfaced* once the breach has persisted. The SSE frame
// honoured that; the REST path (GET /api/v1/anomalies, which the dashboard calls
// on every page load) returned every open row, so a one-window blip reached the
// banner after a reload. Also pins that a model anomaly (z_score IS NULL by
// construction) can be read at all - the query scanned z_score into a float64.
func TestOpenAnomaliesReturnsOnlySurfacedAndToleratesNullZScore(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	if err := realtime.EnsureSchema(ctx, s.pool); err != nil {
		t.Fatalf("ensure realtime schema: %v", err)
	}
	metric := fmt.Sprintf("it_rate_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = s.pool.Exec(ctx, `DELETE FROM gold.anomalies WHERE metric LIKE $1`, metric+"%")
	})
	ins := func(suffix, detector string, z any, surfaced bool) {
		t.Helper()
		if _, err := s.pool.Exec(ctx, `
INSERT INTO gold.anomalies (metric, detector, bucket_start, observed, expected, z_score, severity, status, surfaced)
VALUES ($1, $2, '2014-01-01T00:00:00Z', 0.5, 0.02, $3, 'severe', 'open', $4)`,
			metric+suffix, detector, z, surfaced); err != nil {
			t.Fatalf("insert %s: %v", suffix, err)
		}
	}
	ins("_blip", "model", nil, false)      // first window: recorded, not surfaced
	ins("_sustained", "model", nil, true)  // breach persisted: surfaced, NULL z_score
	ins("_stat", "statistical", 6.2, true) // statistical anomalies surface as they fire

	got, err := s.OpenAnomalies(ctx, "")
	if err != nil {
		t.Fatalf("OpenAnomalies failed (a NULL z_score must not break the read): %v", err)
	}
	seen := map[string]bool{}
	for _, a := range got {
		seen[a.Metric] = true
	}
	if seen[metric+"_blip"] {
		t.Error("an unsurfaced first-window anomaly leaked through the REST path")
	}
	if !seen[metric+"_sustained"] || !seen[metric+"_stat"] {
		t.Errorf("surfaced anomalies missing from the open list: %v", seen)
	}
	for _, a := range got {
		if a.Metric == metric+"_sustained" && a.ZScore != 0 {
			t.Errorf("model anomaly z_score = %v, want 0 (NULL)", a.ZScore)
		}
	}
}
