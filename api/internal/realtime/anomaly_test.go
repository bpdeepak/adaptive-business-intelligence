package realtime

import (
	"math"
	"testing"
)

func TestWelfordBasics(t *testing.T) {
	var w Welford
	for i := 1; i <= 10; i++ {
		w.Add(float64(i))
	}
	if w.N() != 10 {
		t.Fatalf("n = %d, want 10", w.N())
	}
	if math.Abs(w.Mean()-5.5) > 1e-9 {
		t.Fatalf("mean = %v, want 5.5", w.Mean())
	}
	s, ok := w.Sigma()
	if !ok {
		t.Fatal("sigma should be computable with n >= 2")
	}
	// Sample variance of 1..10 is 55/9 ≈ 6.1111 → sigma ≈ 2.449 / wait:
	// variance of population is 8.25; sample variance = sum((x-mean)^2)/(n-1).
	// sum = 82.5 → sample var 9.1667 → sigma ≈ 3.02765.
	want := math.Sqrt(82.5 / 9)
	if math.Abs(s-want) > 1e-9 {
		t.Fatalf("sigma = %v, want %v", s, want)
	}
}

func TestDetectorWarmupNeverFlags(t *testing.T) {
	d := NewDetector()
	d.Warmup = 10
	for i := 0; i < 10; i++ {
		r := d.Evaluate("revenue", 1000)
		if r.Triggered {
			t.Fatalf("warmup observation %d must not trigger", i)
		}
	}
}

func TestDetectorFlagsSpike(t *testing.T) {
	d := NewDetector()
	d.Warmup = 20
	d.Threshold = 3.0
	for i := 0; i < 20; i++ {
		d.Evaluate("orders", 100+float64(i%3)) // tight baseline ~100-102
	}
	// 20% spike over ~101 baseline with tiny sigma → must fire.
	r := d.Evaluate("orders", 101*1.3)
	if !r.Triggered {
		t.Fatalf("expected spike to trigger, got %+v", r)
	}
	if math.Abs(r.Z) < 3.0 {
		t.Fatalf("z = %v, expected |z| >= 3", r.Z)
	}
	if r.Severity != SeverityWarning && r.Severity != SeveritySevere {
		t.Fatalf("severity = %q", r.Severity)
	}
}

func TestDetectorEscalatesSeverity(t *testing.T) {
	d := NewDetector()
	d.Warmup = 20
	for i := 0; i < 20; i++ {
		d.Evaluate("revenue", 1000)
	}
	r := d.Evaluate("revenue", 1000*10) // 10x spike → huge z
	if !r.Triggered {
		t.Fatal("expected trigger")
	}
	if r.Severity != SeveritySevere {
		t.Fatalf("expected severe for 10x spike, got %q", r.Severity)
	}
}

// TestZeroVarianceBaselineSigmaFloor pins the fix for false alarms on quiet
// stretches: a perfectly flat (zero-variance) baseline must score against the
// in-mean sigma floor — max(sigmaFloorAbs, |mean|*sigmaFloorRel) = 50 for a
// 1000-revenue baseline — never an infinite z. Under the old post-hoc clamp a
// +1 deviation on flat traffic produced ±Inf → ±1e15 → a spurious "severe";
// the floor makes it score 0.02 and stay silent, while real divergence still
// escalates with a large-but-finite z.
func TestZeroVarianceBaselineSigmaFloor(t *testing.T) {
	d := NewDetector()
	d.Warmup = 5
	for i := 0; i < 5; i++ {
		d.Evaluate("revenue", 1000) // perfectly flat baseline: sigma == 0
	}

	small := d.Evaluate("revenue", 1001)
	if small.Triggered {
		t.Fatalf("tiny deviation on flat baseline must not trigger, got %+v", small)
	}
	if math.IsInf(small.Z, 0) || math.IsNaN(small.Z) {
		t.Fatalf("z must stay finite on a zero-variance baseline, got %v", small.Z)
	}
	if want := 1.0 / 50.0; math.Abs(small.Z-want) > 1e-9 {
		t.Fatalf("z = %v, want %v (1 unit diverged / 50-unit sigma floor)", small.Z, want)
	}

	big := d.Evaluate("revenue", 1600)
	if !big.Triggered {
		t.Fatalf("real divergence on flat baseline must trigger, got %+v", big)
	}
	if big.Severity != SeveritySevere {
		t.Fatalf("~12σ divergence should be severe, got %q", big.Severity)
	}
	if math.IsInf(big.Z, 0) || math.IsNaN(big.Z) {
		t.Fatalf("z must stay finite even when triggering, got %v", big.Z)
	}
	if big.Z <= 10 {
		t.Fatalf("z = %v, want > 10 for a 600-unit divergence over the 50-unit floor", big.Z)
	}
}

// TestZeroMeanAbsoluteSigmaFloor exercises the absolute half of the floor: with
// a zero-mean flat baseline the relative term contributes nothing, so the
// sigmaFloorAbs (1e-2) keeps z finite and meaningful. 0.05 units then scores
// exactly 5σ and escalates to severe without any ±Inf clamp.
func TestZeroMeanAbsoluteSigmaFloor(t *testing.T) {
	d := NewDetector()
	d.Warmup = 4
	for i := 0; i < 4; i++ {
		d.Evaluate("orders", 0) // zero-mean flat baseline
	}
	r := d.Evaluate("orders", 0.05)
	if math.IsInf(r.Z, 0) || math.IsNaN(r.Z) {
		t.Fatalf("z must stay finite on a zero-mean baseline, got %v", r.Z)
	}
	if want := 0.05 / sigmaFloorAbs; math.Abs(r.Z-want) > 1e-9 {
		t.Fatalf("z = %v, want %v (0.05 / absolute floor 1e-2)", r.Z, want)
	}
	if !r.Triggered || r.Severity != SeveritySevere {
		t.Fatalf("5σ divergence with absolute floor should be severe, got %+v", r)
	}
}
