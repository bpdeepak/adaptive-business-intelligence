package realtime

import (
	"math"
	"sort"
)

// Severity levels for detected anomalies.
const (
	SeverityWarning = "warning"
	SeveritySevere  = "severe"
)

// Severity escalation is driven by a "sigma floor" applied inside the z-score
// computation (not clamped post-hoc): the sample sigma floored to the larger of
// an absolute floor and a relative fraction of the baseline magnitude. A quiet,
// zero-variance baseline (repeated identical closed buckets) then yields
// large-but-finite z-scores for real divergence instead of ±Inf, which the old
// code turned into a false "severe" on literally every quiet stretch.
const (
	sigmaFloorAbs = 1e-2 // absolute floor in the metric's own units
	sigmaFloorRel = 5e-2 // relative floor: 5% of the baseline magnitude
)

// MetricState is a serialisable snapshot of one metric's accumulator.
type MetricState struct {
	Metric string  `json:"metric"`
	N      int64   `json:"n"`
	Mean   float64 `json:"mean"`
	M2     float64 `json:"m2"`
}

// Welford is an online accumulator of mean/variance via Welford's algorithm. It
// classifies each new closed bucket against the distribution seen so far
// without keeping the series in memory.
type Welford struct {
	n        int
	mean, m2 float64
}

// Add folds one observation into the running statistics.
func (w *Welford) Add(x float64) {
	w.n++
	if w.n == 1 {
		w.mean = x
		return
	}
	delta := x - w.mean
	w.mean += delta / float64(w.n)
	w.m2 += delta * (x - w.mean)
}

// N returns the number of observations folded in.
func (w *Welford) N() int { return w.n }

// Mean returns the running mean.
func (w *Welford) Mean() float64 { return w.mean }

// Sigma returns the sample standard deviation and whether it is computable
// (n >= 2). The z-score floor lives in Z, never here, so the accumulator's raw
// statistics stay exact online (mean/variance) for persistence round-trips.
func (w *Welford) Sigma() (float64, bool) {
	if w.n < 2 {
		return 0, false
	}
	// Sample variance: m2/(n-1). Welford's dais (not the test) wants sample.
	return math.Sqrt(w.m2 / float64(w.n-1)), true
}

// Z returns the z-score of observed against the running baseline. ok is false
// until a stable baseline exists (n >= 2). The baseline's sample sigma is
// floored (sigmaFloorAbs/Rel) inside the score so a zero-variance baseline
// yields a large-but-finite score for real divergence instead of ±Inf for
// literally any divergence (which old code escalated to a false "severe" on
// every quiet stretch). The floor is applied pre-scoring, never post-hoc.
func (w *Welford) Z(x float64) (float64, bool) {
	if w.n < 2 {
		return 0, false
	}
	s, _ := w.Sigma()
	floor := math.Max(sigmaFloorAbs, math.Abs(w.mean)*sigmaFloorRel)
	if s < floor {
		s = floor
	}
	return (x - w.mean) / s, true
}

// Snapshot captures the persistent state of this accumulator. The metric is
// carried on the state so it round-trips through storage on its own.
func (w *Welford) Snapshot(metric string) MetricState {
	return MetricState{
		Metric: metric,
		N:      int64(w.n),
		Mean:   w.mean,
		M2:     w.m2,
	}
}

// Result of one detector evaluation.
type Result struct {
	Triggered bool
	Z         float64
	Expected  float64
	Count     int
	Severity  string
}

// Detector flags each new closed bucket that deviates from its online baseline
// by more than Threshold standard deviations, escalating severity with the
// magnitude of the deviation.
type Detector struct {
	Warmup    int
	Threshold float64
	SevereZ   float64

	stats map[string]*Welford
}

// NewDetector builds a detector with sensible defaults.
func NewDetector() *Detector {
	return &Detector{
		Warmup:    20,
		Threshold: 3.0,
		SevereZ:   5.0,
		stats:     make(map[string]*Welford),
	}
}

// Evaluate folds a closed bucket's observed value into the running baseline and
// reports whether it deviates. During warm-up (fewer than Warmup observations)
// the detector accumulates but never flags; the observed value is only ever
// classified against the baseline *seen before* that observation so a spike
// cannot dilute itself.
func (d *Detector) Evaluate(metric string, observed float64) Result {
	w := d.stats[metric]
	if w == nil {
		w = &Welford{}
		d.stats[metric] = w
	}
	res := Result{Expected: w.Mean(), Count: w.N()}
	if z, ok := w.Z(observed); ok && w.N() >= d.Warmup {
		res.Z = z
		if math.Abs(z) >= d.Threshold && math.Abs(z) >= d.SevereZ {
			res.Triggered = true
			res.Severity = SeveritySevere
		} else if math.Abs(z) >= d.Threshold {
			res.Triggered = true
			res.Severity = SeverityWarning
		}
	}
	w.Add(observed)
	return res
}

// Snapshot serialises every metric's accumulator, sorted by metric for
// determinism. Safe on a zero-value Detector.
func (d *Detector) Snapshot() []MetricState {
	if d.stats == nil {
		return nil
	}
	names := make([]string, 0, len(d.stats))
	for m := range d.stats {
		names = append(names, m)
	}
	sort.Strings(names)
	out := make([]MetricState, 0, len(names))
	for _, m := range names {
		out = append(out, d.stats[m].Snapshot(m))
	}
	return out
}

// Restore seeds the accumulators from a saved snapshot. Safe on a zero-value
// Detector (nil stats map) so a caller that never ran Evaluate can still resume
// from persisted state — same nil-safe contract as Evaluate.
func (d *Detector) Restore(states []MetricState) {
	if d.stats == nil {
		d.stats = make(map[string]*Welford)
	}
	for _, st := range states {
		w := d.stats[st.Metric]
		if w == nil {
			w = &Welford{}
			d.stats[st.Metric] = w
		}
		w.n = int(st.N)
		w.mean = st.Mean
		w.m2 = st.M2
	}
}
