package events

import (
	"sort"
	"time"
)

// payloads.go — the ONE place event payload shapes are built and declared.
//
// Every producer (the score-writer, the realtime aggregator, the drift poller)
// and the governance reconciler (which re-derives events from persisted rows)
// build their events through the constructors below, so a live event and a
// reconciled one can never differ in shape. DeclaredFields is the matching
// contract the playbook compiler validates rule conditions against; the tests
// in payloads_test.go pin that contract to what the constructors really emit in
// both directions (a declared field nobody emits would make a rule that boots
// cleanly and silently never fires).

// PositiveRateFloor floors a model's batch positive-rate baseline so a tiny
// (or zero) registry value cannot make the rate trackers hypersensitive.
const PositiveRateFloor = 0.01

// BaselineRate applies PositiveRateFloor. Shared by the score-writer (live) and
// the reconciler so the registry.positive_rate field means the same thing on
// both paths.
func BaselineRate(positiveRate float64) float64 {
	if positiveRate < PositiveRateFloor {
		return PositiveRateFloor
	}
	return positiveRate
}

// Scored describes one persisted prediction. Threshold and PositiveRate are
// pointers on purpose: nil means "the registry did not give us one", and the
// constructor then OMITS the field, so a rule comparing against it fails closed
// (never fires) instead of comparing against a fabricated 0.
type Scored struct {
	Model        string
	Version      string
	Entity       string
	Score        float64
	Confidence   float64
	PredictionID int64
	Threshold    *float64
	PositiveRate *float64
	// Rank is the entity's position in a batch-scored population (1 = highest
	// score). Only batch producers set it (churn); 0 means "not ranked" and the
	// field is omitted, so a rule on it fails closed for unranked events.
	Rank       int
	Reconciled bool // set only by the reconciler; rides in the trigger evidence
}

// NewScored builds an order_scored / session_scored / churn_scored event.
func NewScored(t Type, s Scored, at time.Time) Event {
	pred := map[string]any{
		"model":         s.Model,
		"model_version": s.Version,
		"entity_id":     s.Entity,
		"score":         s.Score,
		"confidence":    s.Confidence,
		// The persisted prediction row id: the per-trigger-instance
		// discriminator of the playbook dedup key.
		"id": s.PredictionID,
	}
	if s.Rank > 0 {
		pred["rank"] = s.Rank
	}
	payload := map[string]any{"prediction": pred}
	reg := map[string]any{}
	if s.Threshold != nil {
		pred["threshold"] = *s.Threshold
		reg["recommended_threshold"] = *s.Threshold
	}
	if s.PositiveRate != nil {
		reg["positive_rate"] = *s.PositiveRate
	}
	if len(reg) > 0 {
		payload["registry"] = reg
	}
	if s.Reconciled {
		payload["reconciled"] = true
	}
	return Event{Type: t, At: at, Payload: payload}
}

// Anomaly describes one persisted gold.anomalies row.
type Anomaly struct {
	Metric      string
	Detector    string // "statistical" | "model"
	BucketStart string
	Observed    float64
	Expected    float64
	ZScore      float64 // statistical detector only
	Severity    string
	Status      string
	Surfaced    bool
}

// NewAnomalyDetected builds an anomaly_detected event. z_score is emitted only
// for the statistical detector: rate-based model anomalies have no z-score by
// construction (the DB column is NULL), so a rule on anomaly.z_score fails
// closed for them rather than reading a fabricated 0.
func NewAnomalyDetected(a Anomaly, at time.Time) Event {
	an := map[string]any{
		"metric":       a.Metric,
		"detector":     a.Detector,
		"bucket_start": a.BucketStart,
		"observed":     a.Observed,
		"expected":     a.Expected,
		"severity":     a.Severity,
		"status":       a.Status,
		"surfaced":     a.Surfaced,
	}
	if a.Detector == "statistical" {
		an["z_score"] = a.ZScore
	}
	return Event{Type: TypeAnomalyDetected, At: at, Payload: map[string]any{"anomaly": an}}
}

// Forecast describes one persisted next-week demand forecast (orders) for a
// category, with the trailing 4-week realized mean it is compared against.
type Forecast struct {
	Category      string
	Week          string // the forecast week (YYYY-MM-DD, week start)
	PointEstimate float64
	RecentAvg     float64
	Reconciled    bool
}

// NewForecastUpdated builds a forecast_updated event (producer: the Phase 5
// forecast worker; rebuilt by the governance reconciler from the persisted row).
func NewForecastUpdated(f Forecast, at time.Time) Event {
	payload := map[string]any{
		"forecast": map[string]any{
			"category":       f.Category,
			"week":           f.Week,
			"point_estimate": f.PointEstimate,
			"recent_avg":     f.RecentAvg,
		},
	}
	if f.Reconciled {
		payload["reconciled"] = true
	}
	return Event{Type: TypeForecastUpdated, At: at, Payload: payload}
}

// DriftRun accumulates the gold.model_drift rows of one (model, computed_at)
// measurement run: worst status wins, max PSI is reported. The drift poller and
// the reconciler both fold rows through this one type, so their events (and
// therefore dedup keys) agree exactly.
type DriftRun struct {
	Model      string
	Version    string
	ComputedAt string // RFC3339 UTC, second precision

	worst    int
	status   string
	maxPSI   float64
	features []map[string]any
}

// Add folds one gold.model_drift row into the run.
func (r *DriftRun) Add(feature string, psi float64, status, kind string) {
	if r.status == "" {
		r.status = "ok" // a run of only healthy findings must still report "ok"
	}
	r.features = append(r.features, map[string]any{
		"feature": feature, "psi": psi, "status": status, "kind": kind,
	})
	if s := DriftRank(status); s > r.worst {
		r.worst, r.status = s, status
	}
	if psi > r.maxPSI {
		r.maxPSI = psi
	}
}

// DriftRank orders drift statuses for worst-takes-all merging.
func DriftRank(status string) int {
	switch status {
	case "critical":
		return 2
	case "warning":
		return 1
	default:
		return 0
	}
}

// Event builds the drift_computed event for the run.
func (r *DriftRun) Event(reconciled bool, at time.Time) Event {
	status := r.status
	if status == "" {
		status = "ok"
	}
	payload := map[string]any{
		"drift": map[string]any{
			"model":         r.Model,
			"model_version": r.Version,
			"status":        status,
			"psi":           r.maxPSI,
			"feature_count": len(r.features),
			"features":      r.features,
			"computed_at":   r.ComputedAt,
		},
	}
	if reconciled {
		payload["reconciled"] = true
	}
	return Event{Type: TypeDriftComputed, At: at, Payload: payload}
}

// DeclaredFields returns the dotted payload paths a playbook condition may
// reference for an event type, and whether the type has a declared contract at
// all. The playbook compiler rejects any other path at boot.
func DeclaredFields(t Type) (map[string]bool, bool) {
	f, ok := declared[t]
	if !ok {
		return nil, false
	}
	out := make(map[string]bool, len(f))
	for k := range f {
		out[k] = true
	}
	return out, true
}

// DeclaredTypes lists the event types with a declared contract (stable order).
func DeclaredTypes() []Type {
	out := make([]Type, 0, len(declared))
	for t := range declared {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

var scoredFields = map[string]bool{
	"prediction.model":               true,
	"prediction.model_version":       true,
	"prediction.entity_id":           true,
	"prediction.score":               true,
	"prediction.confidence":          true,
	"prediction.threshold":           true,
	"prediction.id":                  true,
	"registry.recommended_threshold": true,
	"registry.positive_rate":         true,
}

// churnFields adds the batch rank (1 = highest risk in the scored population), so
// retention policy can work down from the top instead of a hard cutoff alone.
var churnFields = func() map[string]bool {
	m := map[string]bool{"prediction.rank": true}
	for k := range scoredFields {
		m[k] = true
	}
	return m
}()

var declared = map[Type]map[string]bool{
	TypeOrderScored:   scoredFields,
	TypeSessionScored: scoredFields,
	TypeChurnScored:   churnFields, // producer: the Phase 5 churn scorer (uses NewScored)
	TypeDriftComputed: {
		"drift.model":         true,
		"drift.model_version": true,
		"drift.status":        true,
		"drift.psi":           true,
		"drift.feature_count": true,
		"drift.features":      true,
		"drift.computed_at":   true,
	},
	TypeAnomalyDetected: {
		"anomaly.metric":       true,
		"anomaly.detector":     true,
		"anomaly.bucket_start": true,
		"anomaly.observed":     true,
		"anomaly.expected":     true,
		"anomaly.z_score":      true, // statistical detector only
		"anomaly.severity":     true,
		"anomaly.status":       true,
		"anomaly.surfaced":     true,
	},
	// Producer: the Phase 5 forecast worker (NewForecastUpdated).
	TypeForecastUpdated: {
		"forecast.category":       true,
		"forecast.week":           true,
		"forecast.point_estimate": true,
		"forecast.recent_avg":     true,
	},
}
