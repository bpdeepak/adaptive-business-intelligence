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