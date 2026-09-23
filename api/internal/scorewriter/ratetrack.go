package scorewriter

import (
	"math"
	"time"

	"abi/internal/model"
)

const (
	// RateWindowSecs is the anomaly evaluation window (event-time): the
	// proportion of at-risk predictions in the trailing 5 minutes.
	RateWindowSecs = 5 * 60
	// MinRateSamples is the minimum window population before a rate anomaly can
	// fire — a cold window is statistically meaningless (3A contract: no
	// single-prediction anomalies, ever). Raised 5 → 20 on the Phase 3 review:
	// at 5, quiet-replay windows holding only 1–10 observations crossed the 3×
	// line on small-sample lulls; 20 keeps a busy-window breach (the signal)
	// while silencing lull noise.
	MinRateSamples = 20
	// RateAnomalyMultiplier is the trigger: windowed at-risk rate > 3x baseline.
	RateAnomalyMultiplier = 3.0
	// SevereRateMultiplier escalates severity to "severe".
	SevereRateMultiplier = 5.0
)

// rateObs is one scored prediction inside the trailing window.
type rateObs struct {
	T   float64 `json:"t"` // event-time epoch seconds
	Pos bool    `json:"p"` // prediction >= recommended threshold
}

// RateTrace is the persisted form of one tracker.
type RateTrace struct {
	Obs         []rateObs `json:"obs"`
	LastTrigger int64     `json:"last_trigger,omitempty"`
}

// RateTracker watches the at-risk rate (score >= recommended threshold) of one
// model over a trailing 5-minute event-time window. When the windowed rate
// exceeds 3x the batch baseline it emits one model-driven anomaly — a whole
// window proportion, never a single prediction (3A contract). Only the current
// ~5 minutes of observations are retained, so the window lives entirely in
// memory and the persisted snapshot stays small.
type RateTracker struct {
	model       string
	baseRate    float64
	threshold   float64
	obs         []rateObs
	lastTrigger int64
}

// NewRateTracker builds a tracker for one model over its baseline/threshold.
func NewRateTracker(model string, cfg ModelConfig) *RateTracker {
	return &RateTracker{
		model:     model,
		baseRate:  cfg.BaselineRate,
		threshold: cfg.RecommendedThreshold,
		obs:       []rateObs{},
	}
}

// Restore replaces the tracker's window with a persisted trace.
func (rt *RateTracker) Restore(trace *RateTrace) {
	if trace == nil {
		return
	}
	rt.obs = trace.Obs
	rt.lastTrigger = trace.LastTrigger
}

// Snapshot returns the current window + last trigger for persistence.
func (rt *RateTracker) Snapshot() *RateTrace {
	return &RateTrace{Obs: rt.obs, LastTrigger: rt.lastTrigger}
}

// Observe folds one scored prediction at event time `t`. It returns a non-nil
// anomaly exactly when the trailing 5-minute at-risk rate fires for a fresh
// window bucket (one anomaly per 5-minute bucket, not per prediction). The
// returned anomaly is fully formed except for the persistence id.
func (rt *RateTracker) Observe(t time.Time, prediction float64) *model.Anomaly {
	secs := float64(t.Unix())
	rt.obs = append(rt.obs, rateObs{T: secs, Pos: prediction >= rt.threshold})
	cutoff := secs - RateWindowSecs
	first := 0
	for first < len(rt.obs) && rt.obs[first].T < cutoff {
		first++
	}
	rt.obs = rt.obs[first:]

	if len(rt.obs) < MinRateSamples {
		return nil
	}
	var positive int
	for _, o := range rt.obs {
		if o.Pos {
			positive++
		}
	}
	rate := float64(positive) / float64(len(rt.obs))
	if rate <= RateAnomalyMultiplier*rt.baseRate {
		return nil
	}
	bucket := int64(secs) / RateWindowSecs
	if bucket == rt.lastTrigger {
		return nil // one anomaly per 5-minute bucket
	}
	rt.lastTrigger = bucket

	severity := "elevated"
	if rate > SevereRateMultiplier*rt.baseRate {
		severity = "severe"
	}
	return &model.Anomaly{
		Metric:      rt.model + "_rate",
		Detector:    "model",
		BucketStart: time.Unix(bucket*RateWindowSecs, 0).UTC().Format(time.RFC3339),
		Observed:    round2(rate),
		Expected:    rt.baseRate,
		ZScore:      0, // rate-based anomalies carry no z-score (stays NULL in the DB)
		Severity:    severity,
		Status:      "open",
		DetectedAt:  time.Now().UTC().Format(time.RFC3339),
	}
}

func round2(v float64) float64 {
	if math.IsInf(v, 0) || math.IsNaN(v) {
		return 0
	}
	return math.Round(v*1000) / 1000
}