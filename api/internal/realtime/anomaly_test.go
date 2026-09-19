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
