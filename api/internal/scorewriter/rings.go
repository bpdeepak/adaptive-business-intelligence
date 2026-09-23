package scorewriter

import (
	"sync"

	"abi/internal/stream"
)

const (
	// VelocityWindowSecs is the trailing-24h event-time window for
	// velocity_24h (ml/build_fraud_features.py: trailing_velocity).
	VelocityWindowSecs = 24 * 3600
	// BenchmarkWindowSecs is the trailing-90-day window for the category
	// price benchmark (ml/build_fraud_features.py: trailing_benchmark).
	BenchmarkWindowSecs = 90 * 86400
	// DaysToSecs converts whole days to seconds for account_age_days.
	DaysToSecs = 86400
)

// customerState tracks one customer's processed orders for velocity_24h and
// account_age_days. `Times` stays trimmed to the 24h window; `First` is the
// epoch second of the first processed order.
type customerState struct {
	First float64   `json:"f"`
	Times []float64 `json:"t"`
	Head  int       `json:"h"`
}

// catState tracks one category's processed orders for the price benchmark.
type catState struct {
	Times  []float64 `json:"t"`
	Values []float64 `json:"v"`
	Head   int       `json:"h"`
}

// Rings accumulate the event-time fraud features. It mirrors the batch
// vectorized windows with a strictly-before semantics: feature values are read
// BEFORE the order is recorded, so an order never counts itself. The service
// guarantees (via its reorder gate) that orders are processed in event-time
// order, which is what makes per-category benchmark sums identical to the
// batch's strictly-before scan.
//
// The rings are deliberately a single shared structure across the partition
// set — not per-partition — so velocity (same customer → same partition) and
// the benchmark (any customer in a category → any partition) both see the full
// event ordering.
type Rings struct {
	mu   sync.Mutex
	cust map[string]*customerState
	cat  map[string]*catState

	restored bool
}

// NewRings builds an empty ring set.
func NewRings() *Rings {
	return &Rings{
		cust: map[string]*customerState{},
		cat:  map[string]*catState{},
	}
}

// Restore replaces the current state with a snapshot (from gold.scorewriter_state).
func (r *Rings) Restore(state *RingsSnapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cust = state.Customers
	r.cat = state.Categories
	if r.cust == nil {
		r.cust = map[string]*customerState{}
	}
	if r.cat == nil {
		r.cat = map[string]*catState{}
	}
	r.restored = true
}

// Snapshot copies the current state for persistence.
func (r *Rings) Snapshot() *RingsSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := &RingsSnapshot{
		Customers:   make(map[string]*customerState, len(r.cust)),
		Categories:  make(map[string]*catState, len(r.cat)),
		Restored:    r.restored,
	}
	for k, v := range r.cust {
		cp := *v
		cp.Times = make([]float64, len(v.Times)-v.Head)
		copy(cp.Times, v.Times[v.Head:])
		cp.Head = 0
		out.Customers[k] = &cp
	}
	for k, v := range r.cat {
		cp := *v
		cp.Times = make([]float64, len(v.Times)-v.Head)
		cp.Values = make([]float64, len(v.Values)-v.Head)
		copy(cp.Times, v.Times[v.Head:])
		copy(cp.Values, v.Values[v.Head:])
		cp.Head = 0
		out.Categories[k] = &cp
	}
	return out
}

// RingsSnapshot is the persisted form of the rings (JSON-friendly).
type RingsSnapshot struct {
	Customers  map[string]*customerState `json:"customers"`
	Categories map[string]*catState      `json:"categories"`
	Restored   bool                      `json:"restored,omitempty"`
}

// Velocity returns the count of the customer's prior orders with event time
// >= t-24h (batch: searchsorted(gts, t-24h, side="left") → count, self
// excluded) and the account age in whole days since first seen.
func (r *Rings) Velocity(customer string, t float64) (velocity, ageDays int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cs := r.cust[customer]
	if cs == nil {
		return 0, 0
	}
	cs.trim(lowBound(t, VelocityWindowSecs))
	return cs.countSince(lowBound(t, VelocityWindowSecs)), floorDays(t - cs.First)
}

// Benchmark returns (count, sum) of the category's prior orders with event
// time in [t-90d, t). An empty result means "no history" — the assembler falls
// back to the order's own value so the ratio is 1.0.
func (r *Rings) Benchmark(category string, t float64) (count int, sum float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cs := r.cat[category]
	if cs == nil {
		return 0, 0
	}
	lb := lowBound(t, BenchmarkWindowSecs)
	cs.trim(lb)
	for i := cs.Head; i < len(cs.Times); i++ {
		if cs.Times[i] >= lb {
			sum += cs.Values[i]
			count++
		}
	}
	return count, sum
}

// RecordOrder folds an order into the rings AFTER its features were read: the
// category benchmark uses the order's primary category, velocity/age use the
// customer. Order identity is the caller's responsibility (dedupe before
// RecordOrder); recording is idempotent per order id by construction of the
// caller's dedupe.
func (r *Rings) RecordOrder(od stream.OrderPlaced, ref *References, primaryCat string) {
	t := float64(od.PurchaseTime.Unix())
	r.mu.Lock()
	defer r.mu.Unlock()

	cs := r.cust[od.CustomerID]
	if cs == nil {
		cs = &customerState{First: t}
		r.cust[od.CustomerID] = cs
	}
	cs.Times = append(cs.Times, t)

	cat := primaryCat
	if _, ok := ref.CategoryRank[cat]; !ok {
		cat = "unknown" // rank misses also flowed as 'unknown' at training
	}
	k := r.cat[cat]
	if k == nil {
		k = &catState{}
		r.cat[cat] = k
	}
	k.Times = append(k.Times, t)
	k.Values = append(k.Values, od.PaymentValue)
}

func (c *customerState) trim(bound float64) {
	for c.Head < len(c.Times) && c.Times[c.Head] < bound {
		c.Head++
	}
}

// countSince counts entries at or after `bound`. Called after trim so all
// live entries qualify (they are all strictly before t by processing order).
func (c *customerState) countSince(bound float64) int {
	n := 0
	for i := c.Head; i < len(c.Times); i++ {
		if c.Times[i] >= bound {
			n++
		}
	}
	return n
}

func (c *catState) trim(bound float64) {
	for c.Head < len(c.Times) && c.Times[c.Head] < bound {
		c.Head++
	}
}

// lowBound returns t - window (the inclusive lower edge of the trailing window:
// searchsorted side="left" keeps entries exactly at the boundary).
func lowBound(t, window float64) float64 { return t - window }

// floorDays truncates a seconds span to whole days (batch: // 86400 on the
// timedelta). Inputs are positive; Go's int() truncation toward zero then
// equals floor.
func floorDays(seconds float64) int {
	return int(seconds / DaysToSecs)
}