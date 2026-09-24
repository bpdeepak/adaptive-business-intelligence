package events

import (
	"sort"
	"testing"
	"time"
)

// leafPaths flattens a payload into dotted paths; non-map values (numbers,
// strings, bools, slices) are leaves.
func leafPaths(prefix string, v any, out map[string]bool) {
	m, ok := v.(map[string]any)
	if !ok {
		out[prefix] = true
		return
	}
	for k, child := range m {
		p := k
		if prefix != "" {
			p = prefix + "." + k
		}
		leafPaths(p, child, out)
	}
}

func pathsOf(e Event) map[string]bool {
	out := map[string]bool{}
	leafPaths("", e.Payload, out)
	delete(out, "reconciled") // recovery marker, not a rule-visible field
	return out
}

func f64(v float64) *float64 { return &v }

// sampleEvents builds every shape each constructor can emit, per event type.
func sampleEvents() map[Type][]Event {
	now := time.Now().UTC()
	full := Scored{Model: "m", Version: "v", Entity: "e", Score: .9, Confidence: .9,
		PredictionID: 7, Threshold: f64(.8), PositiveRate: f64(.02)}
	rec := full
	rec.Reconciled = true

	run := &DriftRun{Model: "m", Version: "v", ComputedAt: "2026-01-01T00:00:00Z"}
	run.Add("f", 0.3, "critical", "psi")

	anomStat := Anomaly{Metric: "revenue", Detector: "statistical", BucketStart: "b",
		Observed: 1, Expected: 2, ZScore: 4, Severity: "severe", Status: "open", Surfaced: true}
	anomModel := anomStat
	anomModel.Detector = "model"

	return map[Type][]Event{
		TypeOrderScored:     {NewScored(TypeOrderScored, full, now), NewScored(TypeOrderScored, rec, now)},
		TypeSessionScored:   {NewScored(TypeSessionScored, full, now)},
		TypeChurnScored:     {NewScored(TypeChurnScored, full, now)},
		TypeDriftComputed:   {run.Event(false, now), run.Event(true, now)},
		TypeAnomalyDetected: {NewAnomalyDetected(anomStat, now), NewAnomalyDetected(anomModel, now)},
	}
}

// TestDeclaredFieldsMatchWhatProducersEmit pins the playbook payload schema to
// the real constructors in BOTH directions. A field the schema declares but no
// producer emits is exactly the silent failure boot validation exists to
// prevent (a rule that compiles, boots, and never fires); a field a producer
// emits but the schema omits could never be used by a rule.
func TestDeclaredFieldsMatchWhatProducersEmit(t *testing.T) {
	samples := sampleEvents()
	for _, typ := range DeclaredTypes() {
		if typ == TypeForecastUpdated {
			continue // dormant: no producer yet (Phase 5 forecast worker)
		}
		declaredSet, _ := DeclaredFields(typ)
		evs, ok := samples[typ]
		if !ok {
			t.Fatalf("no sample producer output for declared type %s", typ)
		}
		union := map[string]bool{}
		for _, e := range evs {
			for p := range pathsOf(e) {
				union[p] = true
				if !declaredSet[p] {
					t.Errorf("%s: producers emit %q but the schema does not declare it", typ, p)
				}
			}
		}
		var missing []string
		for p := range declaredSet {
			if !union[p] {
				missing = append(missing, p)
			}
		}
		sort.Strings(missing)
		if len(missing) > 0 {
			t.Errorf("%s: schema declares fields no producer ever emits: %v", typ, missing)
		}
	}
}

func TestScoredOmitsThresholdWhenRegistryHasNone(t *testing.T) {
	e := NewScored(TypeOrderScored, Scored{Model: "fraud_risk", Entity: "o", Score: 0.99, PredictionID: 1}, time.Now())
	paths := pathsOf(e)
	for _, p := range []string{"prediction.threshold", "registry.recommended_threshold", "registry.positive_rate"} {
		if paths[p] {
			t.Errorf("%s must be omitted (not defaulted to 0) when the registry gave no value", p)
		}
	}
	if !paths["prediction.id"] {
		t.Error("prediction.id must always be present: it scopes the dedup key")
	}
}

func TestModelAnomalyCarriesNoZScore(t *testing.T) {
	e := NewAnomalyDetected(Anomaly{Detector: "model", ZScore: 0}, time.Now())
	if pathsOf(e)["anomaly.z_score"] {
		t.Error("rate-based model anomalies have no z-score; a rule on it must fail closed")
	}
}

func TestDriftRunWorstStatusWinsAndHealthyRunIsOK(t *testing.T) {
	r := &DriftRun{Model: "m", ComputedAt: "t"}
	r.Add("a", 0.01, "ok", "psi")
	if got := r.Event(false, time.Now()).Payload["drift"].(map[string]any)["status"]; got != "ok" {
		t.Fatalf("healthy run status = %v, want ok", got)
	}
	r.Add("b", 0.15, "warning", "psi")
	r.Add("c", 0.5, "critical", "psi")
	r.Add("d", 0.0, "ok", "psi")
	d := r.Event(false, time.Now()).Payload["drift"].(map[string]any)
	if d["status"] != "critical" || d["feature_count"] != 4 || d["psi"] != 0.5 {
		t.Fatalf("merged drift = %+v", d)
	}
}
