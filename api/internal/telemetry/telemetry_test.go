package telemetry

import (
	"strings"
	"testing"
)

func TestRegistryRenderOrderAndFormat(t *testing.T) {
	r := NewRegistry()
	r.Counter("abi_producer_events_total", "Records acknowledged by the broker.").Add(42)
	r.GaugeLabel("abi_consumer_lag", "Unread records for a group/topic/partition.",
		map[string]string{"group": "rt", "topic": "ecommerce.orders", "partition": "0"}).Set(7)
	r.Counter("abi_producer_events_total", "ignored re-registration").Add(3)

	out := string(r.Render())

	for _, want := range []string{
		"# HELP abi_producer_events_total Records acknowledged by the broker.",
		"# TYPE abi_producer_events_total counter",
		"abi_producer_events_total 45",
		"# TYPE abi_consumer_lag gauge",
		`abi_consumer_lag{group="rt",partition="0",topic="ecommerce.orders"} 7`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("render missing %q\n%s", want, out)
		}
	}
	// Deterministic: two renders are identical.
	if second := string(r.Render()); second != out {
		t.Errorf("render non-deterministic")
	}
}

func TestSeriesIdentityAndNilSafety(t *testing.T) {
	r := NewRegistry()
	a := r.Counter("x", "help")
	b := r.Counter("x", "different help ignored")
	if a != b {
		t.Fatalf("same identity must reuse one series")
	}
	// First registration fixes the help text.
	if !strings.Contains(string(r.Render()), "# HELP x help") {
		t.Fatal("help text should come from first registration")
	}

	// Nil registry is a safe no-op.
	var nilReg *Registry
	c := nilReg.Counter("nope", "help")
	c.Inc()
	c.Set(3)
	if c.Value() != 0 {
		t.Fatalf("nil registry must stay at zero, got %v", c.Value())
	}
}
