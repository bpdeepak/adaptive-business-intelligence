package scorewriter

import (
	"testing"
	"time"

	"abi/internal/model"
)

func rtWith(base float64) *RateTracker {
	return NewRateTracker("fraud_risk", ModelConfig{RecommendedThreshold: 0.795, BaselineRate: base})
}

// observeN feeds `n` predictions of the given value at 10s spacing starting at t.
func observeN(rt *RateTracker, t time.Time, n int, value float64) int {
	fired := 0
	for i := 0; i < n; i++ {
		if rt.Observe(t.Add(time.Duration(i)*10*time.Second), value) != nil {
			fired++
		}
	}
	return fired
}

func TestNoTriggerBelowBaseline(t *testing.T) {
	// Baseline 0.5 → trigger needs rate > 1.5. A 40%-positive stream never
	// crosses it in any trailing window, however it slides.
	rt := rtWith(0.5)
	start := time.Date(2023, 6, 15, 10, 0, 0, 0, time.UTC)
	fired := 0
	for i := 0; i < 300; i++ {
		v := 0.0
		if i%10 < 4 {
			v = 0.9 // 40% at-risk, below the 3x baseline
		}
		if rt.Observe(start.Add(time.Duration(i)*10*time.Second), v) != nil {
			fired++
		}
	}
	if fired != 0 {
		t.Errorf("fired %d anomalies below the 3x baseline, want 0", fired)
	}
}

func TestNoTriggerBelowMinSamples(t *testing.T) {
	rt := rtWith(0.01)
	start := time.Date(2023, 6, 15, 10, 0, 0, 0, time.UTC)
	// 3 predictions (< MinRateSamples), all at-risk → no anomaly: a fraction
	// of a handful of rows is not a rate (3A contract).
	fired := observeN(rt, start, 3, 0.9)
	if fired != 0 {
		t.Errorf("window of %d (< %d) fired, want 0", 3, MinRateSamples)
	}
}

func TestLullWindowBelowMinSamplesNeverFires(t *testing.T) {
	// Phase 3 review pin: at MinRateSamples=5, quiet-replay windows holding
	// only 1–10 observations crossed the 3× line on small-sample lulls (309
	// spurious rows across the ~180 simulated days). The 5 → 20 bump keeps a
	// sparse window silent even when it is *all* positive, while a genuinely
	// busy window (≥ MinRateSamples samples, one 5-minute bucket) still fires
	// exactly once.
	rt := rtWith(0.01)
	start := time.Date(2023, 6, 15, 10, 0, 0, 0, time.UTC)
	if fired := observeN(rt, start, MinRateSamples-1, 0.9); fired != 0 {
		t.Fatalf("sparse window of %d all-positive fired %d times, want 0 "+
			"(lull noise must stay silent)", MinRateSamples-1, fired)
	}
	rt2 := rtWith(0.01)
	if fired := observeN(rt2, start, MinRateSamples, 0.9); fired != 1 {
		t.Fatalf("busy window of %d all-positive fired %d, want exactly 1",
			MinRateSamples, fired)
	}
}

func TestTriggerAtMinSamples(t *testing.T) {
	rt := rtWith(0.01)
	start := time.Date(2023, 6, 15, 10, 0, 0, 0, time.UTC)
	fired := observeN(rt, start, MinRateSamples, 0.9)
	if fired != 1 {
		t.Errorf("fired = %d, want exactly 1 at MinRateSamples all-positive", fired)
	}
}

func TestOneAnomalyPerBucket(t *testing.T) {
	rt := rtWith(0.01)
	start := time.Date(2023, 6, 15, 10, 0, 0, 0, time.UTC)
	// 100 all-positive samples at 1s spacing: one 5-minute bucket, so the
	// one-per-bucket dedupe caps this at a single anomaly.
	fired := 0
	for i := 0; i < 100; i++ {
		if rt.Observe(start.Add(time.Duration(i)*time.Second), 0.9) != nil {
			fired++
		}
	}
	if fired != 1 {
		t.Errorf("fired = %d for same-bucket stream, want 1", fired)
	}
	// Crossing into a new 5-min bucket with a fresh all-positive window fires
	// again (the ratio is still > 3x baseline).
	next := start.Add(time.Duration(RateWindowSecs+10) * time.Second)
	for i := 0; i < MinRateSamples; i++ {
		if rt.Observe(next.Add(time.Duration(i)*time.Second), 0.9) != nil {
			fired++
		}
	}
	if fired != 2 {
		t.Errorf("after new bucket fired = %d, want 2", fired)
	}
}

func TestSeverityEscalation(t *testing.T) {
	// Baseline 0.02 → 3x = 0.06 (elevated), 5x = 0.10 (severe).
	rt := rtWith(0.02)
	start := time.Date(2023, 6, 15, 10, 0, 0, 0, time.UTC)
	// Mix: 11 positives out of 100 samples → 0.11 > 0.10 → severe.
	var lastSeverity string
	for i := 0; i < 100; i++ {
		v := 0.1 // below threshold
		if i < 11 {
			v = 0.9
		}
		if anom := rt.Observe(start.Add(time.Duration(i)*time.Second), v); anom != nil {
			lastSeverity = anom.Severity
		}
	}
	if lastSeverity != "severe" {
		t.Errorf("severity = %q, want severe (rate ~0.11 > 5x0.02)", lastSeverity)
	}
}

func TestRateAnomalyFields(t *testing.T) {
	rt := rtWith(0.01)
	start := time.Date(2023, 6, 15, 10, 0, 0, 0, time.UTC)
	var anom *model.Anomaly
	for i := 0; i < MinRateSamples; i++ {
		anom = rt.Observe(start.Add(time.Duration(i)*time.Second), 0.9)
	}
	if anom == nil {
		t.Fatal("expected an anomaly exactly at MinRateSamples all-positive")
	}
	if anom.Detector != "model" {
		t.Errorf("detector = %q, want model", anom.Detector)
	}
	if anom.Metric != "fraud_risk_rate" {
		t.Errorf("metric = %q", anom.Metric)
	}
	if anom.ZScore != 0 {
		t.Errorf("z_score = %v, want 0 (rate-based)", anom.ZScore)
	}
	if anom.Observed != 1.0 {
		t.Errorf("observed = %v, want 1.0", anom.Observed)
	}
	if anom.Expected != 0.01 {
		t.Errorf("expected = %v, want 0.01", anom.Expected)
	}
}

func TestRateTrackerSnapshotRestore(t *testing.T) {
	rt := rtWith(0.01)
	start := time.Date(2023, 6, 15, 10, 0, 0, 0, time.UTC)
	observeN(rt, start, MinRateSamples, 0.9)
	lastTrigger := rt.lastTrigger
	if lastTrigger == 0 {
		t.Fatal("expected a trigger bucket to be set")
	}

	trace := rt.Snapshot()
	rt2 := rtWith(0.01)
	rt2.Restore(trace)
	// Same-bucket window re-observed must NOT re-fire (dedupe restored).
	if anom := rt2.Observe(start.Add(time.Duration(MinRateSamples+1)*time.Second), 0.9); anom != nil {
		t.Error("restored tracker re-fired in the same bucket")
	}
}