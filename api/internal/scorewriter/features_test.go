package scorewriter

import (
	"encoding/json"
	"testing"
	"time"

	"abi/internal/stream"
)

// testRefs builds a small reference set with two categories, like the real
// registry rank (descending revenue, code 0 = top).
func testRefs() *References {
	return &References{
		ProductCategory: map[string]string{"p1": "catA", "p2": "catB", "p3": "catA"},
		CategoryRank:    map[string]int{"catA": 0, "catB": 1},
		Models: map[string]ModelConfig{
			"fraud_risk": {RecommendedThreshold: 0.795, BaselineRate: 0.01},
			"bot_score":  {RecommendedThreshold: 0.5, BaselineRate: 0.02},
		},
	}
}

func TestFraudFeatureNamesFromEmbeddedSpec(t *testing.T) {
	names, err := FraudFeatureNames()
	if err != nil {
		t.Fatalf("FraudFeatureNames: %v", err)
	}
	if len(names) != 19 {
		t.Fatalf("spec has %d features, want 19", len(names))
	}
	want := []string{
		"order_value", "item_count", "freight_share", "payment_count",
		"installments_max", "categories_count", "price_vs_benchmark",
		"velocity_24h", "account_age_days", "order_hour", "is_weekend",
		"is_lost", "pay_boleto", "pay_credit_card", "pay_debit_card",
		"pay_not_defined", "pay_unknown", "pay_voucher", "category_code",
	}
	if len(names) != len(want) {
		t.Fatalf("length mismatch")
	}
	for i := range want {
		if names[i] != want[i] {
			t.Errorf("feature[%d] = %q, want %q", i, names[i], want[i])
		}
	}
	// Spec must be parseable independently of the Go types (single source).
	var raw struct {
		Model   string `json:"model"`
		Version int    `json:"version"`
	}
	if err := json.Unmarshal(fraudSpecJSON, &raw); err != nil {
		t.Fatalf("embedded spec unparseable: %v", err)
	}
	if raw.Model != "fraud_risk" {
		t.Errorf("spec model = %q", raw.Model)
	}
}

func TestAssembleFreshOrder(t *testing.T) {
	a := NewFraudAssembler(testRefs())
	at := time.Date(2023, 6, 15, 14, 30, 0, 0, time.UTC) // Thursday, 14h
	od := stream.OrderPlaced{
		OrderID: "o1", CustomerID: "c1", Status: "delivered",
		PurchaseTime: at, PaymentValue: 100, IsLost: false,
		Items: []stream.OrderItem{
			{ProductID: "p1", Price: 50, Freight: 5},
			{ProductID: "p2", Price: 45, Freight: 0},
		},
		Payments: []stream.OrderPayment{{Type: "credit_card", Installments: 3, Value: 100}},
	}
	f := a.Assemble(od, NewRings())

	checks := map[string]float64{
		"order_value":        100,
		"item_count":         2,
		"freight_share":      5.0 / 100.0,
		"payment_count":      1,
		"installments_max":   3,
		"categories_count":   2,
		"price_vs_benchmark": 1.0, // no history → own value ratio
		"velocity_24h":       0,
		"account_age_days":   0,
		"order_hour":         14,
		"is_weekend":         0,
		"is_lost":            0,
		"pay_credit_card":    1,
		"pay_boleto":         0,
		"category_code":      0, // p1 (top price) → catA = rank 0
	}
	for name, want := range checks {
		if got := f[name]; got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}

func TestAssembleRingFeaturesAndRecordAdvance(t *testing.T) {
	a := NewFraudAssembler(testRefs())
	r := NewRings()
	base := time.Date(2023, 6, 15, 10, 0, 0, 0, time.UTC)

	// Order 1: customer first order, category catA, value 100.
	o1 := stream.OrderPlaced{
		OrderID: "o1", CustomerID: "c1", PurchaseTime: base, PaymentValue: 100,
		Items: []stream.OrderItem{{ProductID: "p1", Price: 100, Freight: 0}},
		Payments: []stream.OrderPayment{{Type: "boleto", Installments: 1, Value: 100}},
	}
	f1 := a.Assemble(o1, r)
	if f1["velocity_24h"] != 0 || f1["account_age_days"] != 0 || f1["price_vs_benchmark"] != 1.0 {
		t.Fatalf("fresh order: v=%v age=%v bench=%v", f1["velocity_24h"], f1["account_age_days"], f1["price_vs_benchmark"])
	}
	r.RecordOrder(o1, a.ref, a.primaryCategory(o1.Items))

	// Order 2: same customer +1h, different product (catB), value 200.
	o2 := stream.OrderPlaced{
		OrderID: "o2", CustomerID: "c1", PurchaseTime: base.Add(time.Hour), PaymentValue: 200,
		Items: []stream.OrderItem{{ProductID: "p2", Price: 200, Freight: 0}},
		Payments: []stream.OrderPayment{{Type: "credit_card", Installments: 6, Value: 200}},
	}
	f2 := a.Assemble(o2, r)
	if f2["velocity_24h"] != 1 {
		t.Errorf("velocity = %v, want 1", f2["velocity_24h"])
	}
	if f2["account_age_days"] != 0 {
		t.Errorf("age = %v, want 0", f2["account_age_days"])
	}
	if f2["price_vs_benchmark"] != 1.0 { // catB has no history
		t.Errorf("catB benchmark = %v, want 1.0", f2["price_vs_benchmark"])
	}
	if f2["category_code"] != 1 {
		t.Errorf("catB code = %v, want 1", f2["category_code"])
	}
	r.RecordOrder(o2, a.ref, a.primaryCategory(o2.Items))

	// Order 3: +2h, catA again; its benchmark is catA's mean (100 → one sample).
	o3 := stream.OrderPlaced{
		OrderID: "o3", CustomerID: "c1", PurchaseTime: base.Add(2 * time.Hour), PaymentValue: 50,
		Items: []stream.OrderItem{{ProductID: "p3", Price: 50, Freight: 0}},
		Payments: []stream.OrderPayment{{Type: "voucher", Installments: 1, Value: 50}},
	}
	f3 := a.Assemble(o3, r)
	if f3["price_vs_benchmark"] != 50.0/100.0 {
		t.Errorf("catA benchmark ratio = %v, want 0.5", f3["price_vs_benchmark"])
	}
	if f3["velocity_24h"] != 2 {
		t.Errorf("velocity = %v, want 2", f3["velocity_24h"])
	}
	if f3["category_code"] != 0 {
		t.Errorf("catA code = %v, want 0", f3["category_code"])
	}
	r.RecordOrder(o3, a.ref, a.primaryCategory(o3.Items))

	// Order 4: +25h — the velocity window is [t-24h, t) = [t0+1h, t0+25h), so
	// o2 (+1h, boundary-inclusive) and o3 (+2h) still count; only o1 falls out.
	o4 := stream.OrderPlaced{
		OrderID: "o4", CustomerID: "c1", PurchaseTime: base.Add(25 * time.Hour), PaymentValue: 90,
		Items: []stream.OrderItem{{ProductID: "p1", Price: 90, Freight: 0}},
		Payments: []stream.OrderPayment{{Type: "boleto", Installments: 1, Value: 90}},
	}
	f4 := a.Assemble(o4, r)
	if f4["velocity_24h"] != 2 {
		t.Errorf("velocity after 25h = %v, want 2 (o2 at boundary + o3 in window)", f4["velocity_24h"])
	}
	if f4["account_age_days"] != 1 {
		t.Errorf("age after 25h = %v, want 1", f4["account_age_days"])
	}
	// catA now has o1=100 and o3=50 → mean 75; benchmark ratio = 90/75.
	if want := 90.0 / 75.0; f4["price_vs_benchmark"] != want {
		t.Errorf("catA benchmark ratio = %v, want %v", f4["price_vs_benchmark"], want)
	}
}

func TestAssembleNoPaymentOrder(t *testing.T) {
	a := NewFraudAssembler(testRefs())
	od := stream.OrderPlaced{
		OrderID: "o9", CustomerID: "c9",
		PurchaseTime: time.Date(2023, 6, 15, 9, 0, 0, 0, time.UTC),
		PaymentValue: 5, Items: []stream.OrderItem{{ProductID: "p1", Price: 5, Freight: 0}},
	}
	f := a.Assemble(od, NewRings())
	if f["pay_unknown"] != 1 {
		t.Errorf("pay_unknown = %v, want 1", f["pay_unknown"])
	}
	if f["pay_credit_card"] != 0 || f["pay_boleto"] != 0 {
		t.Errorf("other pay flags must be 0")
	}
	if _, ok := f["category_code"]; !ok {
		t.Error("category_code missing on no-payment order")
	}
	if f["installments_max"] != 1 {
		t.Errorf("installments_max = %v, want 1 (fillna)", f["installments_max"])
	}
}

func TestAssembleWireOrderNormalized(t *testing.T) {
	// All 19 manifest names must be present in every assembled vector so the
	// sidecar receives a complete row (no reliance on its zero-fill).
	a := NewFraudAssembler(testRefs())
	names, _ := FraudFeatureNames()
	od := stream.OrderPlaced{
		OrderID: "o9", CustomerID: "c9",
		PurchaseTime: time.Date(2023, 6, 15, 9, 0, 0, 0, time.UTC),
		PaymentValue: 5, IsLost: true,
		Items: []stream.OrderItem{{ProductID: "p1", Price: 5, Freight: 0}},
	}
	f := a.Assemble(od, NewRings())
	for _, n := range names {
		if _, ok := f[n]; !ok {
			t.Errorf("assembled vector missing %q", n)
		}
	}
}

func TestBotFeaturesMapping(t *testing.T) {
	se := stream.SessionEnd{
		SessionID: "s1", IsConverting: 1, ClickCount: 7,
		DurationSeconds: 120.5, ClickIntervalCV: 0.3,
		HasSearch: 1, HasProductPage: 1, HasCartPage: 1, HasCheckoutPage: 0,
		PageTypesDistinct: 4, CartAdded: 1, CartValue: 99.9, HourOfDay: 20, IsWeekend: 1,
	}
	f := BotFeatures(se)
	checks := map[string]float64{
		"is_converting": 1, "click_count": 7, "duration_seconds": 120.5,
		"click_interval_cv": 0.3, "has_search": 1, "has_product_page": 1,
		"has_cart_page": 1, "has_checkout_page": 0, "page_types_distinct": 4,
		"cart_added": 1, "cart_value": 99.9, "hour_of_day": 20, "is_weekend": 1,
	}
	if len(f) != len(checks) {
		t.Fatalf("bot features = %d, want 13", len(f))
	}
	for k, want := range checks {
		if f[k] != want {
			t.Errorf("%s = %v, want %v", k, f[k], want)
		}
	}
}
// TestBotFeaturesMatchSharedSpec pins the hand-written BotFeatures mapping to the
// spec file the Python trainer is also asserted against
// (ml/tests/test_bot_feature_spec.py), so the two sides cannot drift apart
// silently — the sidecar now refuses a vector with a missing feature.
func TestBotFeaturesMatchSharedSpec(t *testing.T) {
	var spec FeatureSpec
	if err := json.Unmarshal(botSpecJSON, &spec); err != nil {
		t.Fatalf("parse embedded bot spec: %v", err)
	}
	got := BotFeatures(stream.SessionEnd{})
	want := map[string]bool{}
	for _, f := range spec.Features {
		want[f.Name] = true
		if _, ok := got[f.Name]; !ok {
			t.Errorf("BotFeatures does not emit spec feature %q", f.Name)
		}
	}
	for name := range got {
		if !want[name] {
			t.Errorf("BotFeatures emits %q which is not in bot_feature_spec.json", name)
		}
	}
}

// The three assembler skews the train/serve parity test (parity_integration_test.go)
// found against the batch feature table. Each is pinned here without a database.

// Batch reads order_hour / is_weekend from the UTC timestamp. An event's
// time.Time keeps whatever zone its producer marshalled (pgx hands back the
// machine's local zone), so reading .Hour() directly skewed both features on
// any non-UTC host.
func TestAssembleReadsCalendarFieldsInUTC(t *testing.T) {
	a := NewFraudAssembler(testRefs())
	ist := time.FixedZone("IST", 5*3600+1800)
	// Monday 01:00 IST == Sunday 19:30 UTC.
	at := time.Date(2017, 1, 16, 1, 0, 0, 0, ist)
	f := a.Assemble(stream.OrderPlaced{OrderID: "o", CustomerID: "c", PurchaseTime: at, PaymentValue: 10,
		Items: []stream.OrderItem{{ProductID: "p1", Price: 10}}}, NewRings())
	if f["order_hour"] != 19 {
		t.Errorf("order_hour = %v, want 19 (UTC), not the event's local hour", f["order_hour"])
	}
	if f["is_weekend"] != 1 {
		t.Errorf("is_weekend = %v, want 1 (Sunday in UTC)", f["is_weekend"])
	}
}

// Batch: an order with no item rows has category_primary 'unknown' and
// categories_count 1 (fillna), not "" / 0.
func TestAssembleOrderWithNoItemsMatchesBatchFillna(t *testing.T) {
	a := NewFraudAssembler(testRefs())
	od := stream.OrderPlaced{OrderID: "o", CustomerID: "c", PurchaseTime: time.Now(), PaymentValue: 10}
	if got := a.primaryCategory(od.Items); got != "unknown" {
		t.Errorf("primaryCategory(no items) = %q, want unknown", got)
	}
	f := a.Assemble(od, NewRings())
	if f["categories_count"] != 1 {
		t.Errorf("categories_count = %v, want 1", f["categories_count"])
	}
	if f["item_count"] != 0 {
		t.Errorf("item_count = %v, want 0 (that one really is zero in batch)", f["item_count"])
	}
}

// Olist gives every order its own customer_id; batch velocity/age group by the
// person (customer_unique_id). Keyed by the event's customer_id a repeat buyer
// is never seen twice.
func TestVelocityAndAgeAreKeyedByPersonNotOrderCustomerID(t *testing.T) {
	refs := testRefs()
	refs.CustomerKey = map[string]string{"cust-order-1": "person", "cust-order-2": "person"}
	a := NewFraudAssembler(refs)
	r := NewRings()
	item := []stream.OrderItem{{ProductID: "p1", Price: 10}}
	t0 := time.Date(2017, 3, 1, 10, 0, 0, 0, time.UTC)

	first := stream.OrderPlaced{OrderID: "o1", CustomerID: "cust-order-1", PurchaseTime: t0, PaymentValue: 10, Items: item}
	a.Assemble(first, r)
	r.RecordOrder(first, refs, a.primaryCategory(first.Items))

	second := stream.OrderPlaced{OrderID: "o2", CustomerID: "cust-order-2", PurchaseTime: t0.Add(3 * time.Hour), PaymentValue: 10, Items: item}
	f := a.Assemble(second, r)
	if f["velocity_24h"] != 1 {
		t.Errorf("velocity_24h = %v, want 1: same person, 3h apart, different per-order customer_id", f["velocity_24h"])
	}

	third := stream.OrderPlaced{OrderID: "o3", CustomerID: "cust-order-2", PurchaseTime: t0.Add(72 * time.Hour), PaymentValue: 10, Items: item}
	r.RecordOrder(second, refs, a.primaryCategory(second.Items))
	if f := a.Assemble(third, r); f["account_age_days"] != 3 {
		t.Errorf("account_age_days = %v, want 3 (days since the person's first order)", f["account_age_days"])
	}
}

// An order with two equally priced items in different categories: the primary
// category is the FIRST such item (batch: ORDER BY price DESC, order_item_id). The
// producer loads items in (order_id, order_item_id) order, so first-listed wins.
func TestPrimaryCategoryTieBreaksToTheFirstListedItem(t *testing.T) {
	a := NewFraudAssembler(testRefs())
	catAfirst := []stream.OrderItem{{ProductID: "p1", Price: 10}, {ProductID: "p2", Price: 10}}
	catBfirst := []stream.OrderItem{{ProductID: "p2", Price: 10}, {ProductID: "p1", Price: 10}}
	if got := a.primaryCategory(catAfirst); got != "catA" {
		t.Errorf("primaryCategory = %q, want catA (first of two equal prices)", got)
	}
	if got := a.primaryCategory(catBfirst); got != "catB" {
		t.Errorf("primaryCategory = %q, want catB (first of two equal prices)", got)
	}
	// A strictly higher price still wins regardless of position.
	if got := a.primaryCategory([]stream.OrderItem{{ProductID: "p1", Price: 5}, {ProductID: "p2", Price: 10}}); got != "catB" {
		t.Errorf("primaryCategory = %q, want catB (highest price)", got)
	}
}
