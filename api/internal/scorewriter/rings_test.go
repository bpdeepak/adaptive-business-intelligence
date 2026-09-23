package scorewriter

import (
	"testing"
	"time"

	"abi/internal/stream"
)

func secs(s string) float64 {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return float64(t.Unix())
}

func TestVelocityWindowBoundary(t *testing.T) {
	r := NewRings()
	// Order at t0, then one at t0+24h exactly — the batch searchsorted
	// side="left" at t-24h INCLUDES an order exactly at the boundary.
	t0 := secs("2023-06-15T10:00:00Z")
	od1 := stream.OrderPlaced{
		OrderID: "o1", CustomerID: "c1", PurchaseTime: time.Unix(int64(t0), 0).UTC(), PaymentValue: 1,
	}
	r.RecordOrder(od1, testRefs(), "catA")

	at24h := time.Unix(int64(t0+24*3600), 0).UTC()
	v, age := r.Velocity("c1", float64(at24h.Unix()))
	if v != 1 {
		t.Errorf("velocity at exactly t-24h = %d, want 1 (inclusive)", v)
	}
	if age != 1 {
		t.Errorf("age at exactly t-24h = %d, want 1", age)
	}
}

func TestVelocityTrimsPast24h(t *testing.T) {
	r := NewRings()
	t0 := secs("2023-06-15T10:00:00Z")
	od1 := stream.OrderPlaced{
		OrderID: "o1", CustomerID: "c1", PurchaseTime: time.Unix(int64(t0), 0).UTC(), PaymentValue: 1,
	}
	r.RecordOrder(od1, testRefs(), "catA")

	at25h := time.Unix(int64(t0+25*3600), 0).UTC()
	v, _ := r.Velocity("c1", float64(at25h.Unix()))
	if v != 0 {
		t.Errorf("velocity at t-25h = %d, want 0 (window is [t-24h, t))", v)
	}
}

func TestBenchmarkWindowBoundaryAndTrim(t *testing.T) {
	r := NewRings()
	ref := testRefs()
	t0 := secs("2023-01-01T00:00:00Z")
	mk := func(id string, at float64, val float64) stream.OrderPlaced {
		return stream.OrderPlaced{
			OrderID: id, CustomerID: "c_" + id, PurchaseTime: time.Unix(int64(at), 0).UTC(),
			PaymentValue: val, Items: []stream.OrderItem{{ProductID: "p1", Price: val, Freight: 0}},
		}
	}
	// 90 days back: exactly at the boundary is included.
	o90 := mk("a", t0-90*86400, 100)
	r.RecordOrder(o90, ref, "catA")
	// 91 days back: trimmed out.
	o91 := mk("b", t0-91*86400, 50)
	r.RecordOrder(o91, ref, "catA")

	count, sum := r.Benchmark("catA", t0)
	if count != 1 || sum != 100 {
		t.Errorf("benchmark = (%d, %v), want (1, 100)", count, sum)
	}

	// A later order at t0+10d: o90 is now 100d back → out; nothing else.
	count2, sum2 := r.Benchmark("catA", t0+10*86400)
	if count2 != 0 || sum2 != 0 {
		t.Errorf("benchmark at +10d = (%d, %v), want (0, 0)", count2, sum2)
	}
}

func TestRingsSnapshotRestore(t *testing.T) {
	r := NewRings()
	at := secs("2023-06-15T10:00:00Z")
	od := stream.OrderPlaced{
		OrderID: "o1", CustomerID: "c1", PurchaseTime: time.Unix(int64(at), 0).UTC(),
		PaymentValue: 42, Items: []stream.OrderItem{{ProductID: "p1", Price: 42, Freight: 0}},
	}
	ref := testRefs()
	r.RecordOrder(od, ref, "catA")

	snap := r.Snapshot()
	r2 := NewRings()
	r2.Restore(snap)

	v, age := r2.Velocity("c1", at+3600)
	if v != 1 || age != 0 {
		t.Errorf("restored velocity/age = (%d, %d), want (1, 0)", v, age)
	}
	count, sum := r2.Benchmark("catA", at+100)
	if count != 1 || sum != 42 {
		t.Errorf("restored benchmark = (%d, %v), want (1, 42)", count, sum)
	}
}

func TestFloorDays(t *testing.T) {
	cases := []struct {
		secs float64
		want int
	}{
		{0, 0},
		{86399, 0},
		{86400, 1},
		{2 * 86400, 2},
		{25 * 3600, 1},
	}
	for _, c := range cases {
		if got := floorDays(c.secs); got != c.want {
			t.Errorf("floorDays(%v) = %d, want %d", c.secs, got, c.want)
		}
	}
}