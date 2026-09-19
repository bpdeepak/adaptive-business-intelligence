package realtime

import "math"

// Severity levels for detected anomalies.
const (
	SeverityWarning = "warning"
	SeveritySevere  = "severe"
)

// Welford is an online mean/variance accumulator (Welford's algorithm). It lets
// the aggregator classify each new closed bucket against the distribution seen
// so far without keeping the whole series in memory.
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

// N returns the number of observations fed so far.
func (w *Welford) N() int { return w.n }

// Mean returns the running mean (0 when nothing observed yet).
func (w *Welford) Mean() float64 { return w.mean }

// Sigma returns the sample standard deviation and whether it is computable
// (requires at least 2 observations).
func (w *Welford) Sigma() (float64, bool) {
	if w.n < 2 {
		return 0, false
	}
	return math.Sqrt(w.m2 / float64(w.n-1)), true
}

// Z returns the z-score of x against the running distribution. ok is false
// until a stable baseline exists (n >= 2). A zero-variance baseline yields
// ±Inf for any difference from the mean (a constant series never shifts).
func (w *Welford) Z(x float64) (float64, bool) {
	if w.n < 2 {
		return 0, false
	}
	s := math.Sqrt(w.m2 / float64(w.n-1))
	if s == 0 {
		if x == w.mean {
			return 0, false
		}
		if x > w.mean {
			return math.Inf(1), true
		}
		return math.Inf(-1), true
	}
	return (x - w.mean) / s, true
}

// Detector evaluates a metric's newest closed bucket against its online
// baseline. It never flags the warm-up window, so the first loop doesn't spam
// anomalies before a baseline exists.
type Detector struct {
	// Warmup is the minimum number of closed buckets required before any
	// anomaly can be raised.
	Warmup int
	// Threshold is the |z| above which an anomaly is raised.
	Threshold float64
	// SevereZ is the |z| at which severity escalates to "severe".
	SevereZ float64

	stats map[string]*Welford
}

// NewDetector builds a detector with sensible defaults.
func NewDetector() *Detector {
	return &Detector{
		Warmup:    20, // 20 minutes of baseline before flagging
		Threshold: 3.0,
		SevereZ:   5.0,
		stats:     make(map[string]*Welford),
	}
}

// Result is whether an observation triggered, and with what severity.
type Result struct {
	Triggered bool
	Z         float64
	Expected  float64
	Severity  string
	Count     int // baseline count used
}

// Evaluate scores `observed` for `metric` against the baseline accumulated so
// far (the observation is NOT allowed to dilute its own outlier score — z is
// computed before it is folded in). The observation is then added to the
// baseline so the detector stays adaptive to trend shifts; a sustained shift
// will keep firing until the baseline catches up.
func (d *Detector) Evaluate(metric string, observed float64) Result {
	w := d.stats[metric]
	if w == nil {
		w = &Welford{}
		d.stats[metric] = w
	}
	expected := w.Mean()
	z, zok := w.Z(observed) // pre-add: baseline cannot dilute this observation
	w.Add(observed)

	if w.N() <= d.Warmup {
		return Result{Expected: expected, Count: w.N()}
	}
	if !zok {
		return Result{Expected: expected, Count: w.N()}
	}
	abs := math.Abs(z)
	if abs < d.Threshold {
		return Result{Expected: expected, Count: w.N()}
	}
	r := Result{
		Triggered: true,
		Z:         z,
		Expected:  expected,
		Count:     w.N(),
		Severity:  SeverityWarning,
	}
	if abs >= d.SevereZ {
		r.Severity = SeveritySevere
	}
	return r
}
