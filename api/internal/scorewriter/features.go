// Package scorewriter implements the Phase 3 stream-driven scoring layer
// (3A): it consumes order.placed and session.end events, assembles the exact
// feature vectors the batch-trained models saw (fraud per order, bot per
// session), scores them through the sidecar, persists explainable predictions
// and — unlike the Phase 1 statistical detector — triggers rate-based model
// anomalies (proportion of at-risk rows in a 5-minute window > 3x baseline).
//
// The fraud feature names/formulas are pinned by the embedded spec
// (fraud_feature_spec.json), the single contract shared with the Python
// trainer; a Go text test asserts the assembler emits exactly those names and
// the Python test suite asserts the trainer builds the same ones.
package scorewriter

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"abi/internal/stream"
)

//go:embed fraud_feature_spec.json
var fraudSpecJSON []byte

// FeatureSpec is the embedded fraud feature contract (names in manifest order).
type FeatureSpec struct {
	Model    string       `json:"model"`
	Version  int          `json:"version"`
	Features []FeatureDef `json:"features"`
}

// FeatureDef documents one feature of the fraud vector.
type FeatureDef struct {
	Name      string `json:"name"`
	Source    string `json:"source"`
	Transform string `json:"transform"`
	Units     string `json:"units"`
}

// FraudFeatureNames returns the manifest-order feature names from the spec.
func FraudFeatureNames() ([]string, error) {
	var spec FeatureSpec
	if err := json.Unmarshal(fraudSpecJSON, &spec); err != nil {
		return nil, fmt.Errorf("parse embedded fraud feature spec: %w", err)
	}
	names := make([]string, len(spec.Features))
	for i, f := range spec.Features {
		names[i] = f.Name
	}
	return names, nil
}

// The distinct primary payment types fixed by training (get_dummies over the
// historical values, plus the "unknown" fill). The pay_* one-hots index into
// this set; the spec pins them in this exact order.
var knownPaymentTypes = []string{"boleto", "credit_card", "debit_card", "not_defined", "voucher"}

// ModelConfig is the per-model serving contract resolved from the active
// registry row's metrics at boot — the same artifacts the model was validated
// with, so the stream never invents a threshold or baseline.
type ModelConfig struct {
	RecommendedThreshold float64
	// BaselineRate is the batch test-split positive rate, floored at 1% so a
	// cold window with a handful of predictions cannot fire a rate anomaly.
	BaselineRate float64
}

// References are the training-time artifacts the assembler needs but never
// re-derives: product→category (gold.dim_products), the category revenue-rank
// codes (registry metrics.category_rank), and each model's threshold/baseline.
type References struct {
	ProductCategory map[string]string
	CategoryRank    map[string]int
	Models          map[string]ModelConfig
}

// LoadReferences snapshots the reference tables at boot, mirroring how the
// batch feature builder resolves product categories (LEFT JOIN dim_products
// with 'unknown' fallback) and how the forecast family resolves the rank.
func LoadReferences(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) (*References, error) {
	ref := &References{
		ProductCategory: map[string]string{},
		CategoryRank:    map[string]int{},
		Models:          map[string]ModelConfig{},
	}

	prodRows, err := pool.Query(ctx, `SELECT product_id, product_category FROM gold.dim_products`)
	if err != nil {
		return nil, fmt.Errorf("load product categories: %w", err)
	}
	defer prodRows.Close()
	for prodRows.Next() {
		var pid, cat string
		if err := prodRows.Scan(&pid, &cat); err != nil {
			return nil, fmt.Errorf("scan product category: %w", err)
		}
		ref.ProductCategory[pid] = cat
	}
	if err := prodRows.Err(); err != nil {
		return nil, err
	}

	// category_rank lives in the ACTIVE fraud row's metrics; the forecast
	// family keeps its own copy, but the fraud assembler only needs ours.
	var metrics []byte
	if err := pool.QueryRow(ctx, `
SELECT metrics FROM gold.model_registry
WHERE model_name = 'fraud_risk' AND status = 'active'
ORDER BY created_at DESC LIMIT 1`).Scan(&metrics); err != nil {
		return nil, fmt.Errorf("load fraud category_rank: %w", err)
	}
	var m struct {
		CategoryRank map[string]int `json:"category_rank"`
	}
	if err := json.Unmarshal(metrics, &m); err != nil {
		return nil, fmt.Errorf("parse fraud registry metrics: %w", err)
	}
	ref.CategoryRank = m.CategoryRank

	// Per-model thresholds + baselines from the active rows.
	modelRows, err := pool.Query(ctx, `
SELECT model_name, metrics FROM gold.model_registry
WHERE model_name IN ('fraud_risk', 'bot_score') AND status = 'active'`)
	if err != nil {
		return nil, fmt.Errorf("load model configs: %w", err)
	}
	defer modelRows.Close()
	for modelRows.Next() {
		var name string
		var raw []byte
		if err := modelRows.Scan(&name, &raw); err != nil {
			return nil, fmt.Errorf("scan model config: %w", err)
		}
		var mm struct {
			RecommendedThreshold float64 `json:"recommended_threshold"`
			PositiveRate         float64 `json:"positive_rate"`
		}
		if err := json.Unmarshal(raw, &mm); err != nil {
			return nil, fmt.Errorf("parse registry metrics for %s: %w", name, err)
		}
		base := mm.PositiveRate
		if base < 0.01 {
			base = 0.01
		}
		ref.Models[name] = ModelConfig{
			RecommendedThreshold: mm.RecommendedThreshold,
			BaselineRate:         base,
		}
	}
	if err := modelRows.Err(); err != nil {
		return nil, err
	}

	log.Info("scorewriter references loaded",
		"products", len(ref.ProductCategory),
		"categories", len(ref.CategoryRank),
		"fraud_threshold", ref.Models["fraud_risk"].RecommendedThreshold,
		"fraud_baseline", ref.Models["fraud_risk"].BaselineRate,
		"bot_threshold", ref.Models["bot_score"].RecommendedThreshold,
		"bot_baseline", ref.Models["bot_score"].BaselineRate)
	return ref, nil
}

// FraudAssembler builds the fraud feature vector from an order event plus the
// event-time rings. All formulas mirror ml/build_fraud_features.py exactly.
// The caller MUST hold a stable event-time processing order across categories
// (the service's reorder gate guarantees it).
type FraudAssembler struct {
	ref *References
}

// NewFraudAssembler constructs an assembler over the boot-loaded references.
func NewFraudAssembler(ref *References) *FraudAssembler {
	return &FraudAssembler{ref: ref}
}

// classify sums item price/freight totals.
func orderTotals(items []stream.OrderItem) (gross, freight float64) {
	for _, it := range items {
		gross += it.Price
		freight += it.Freight
	}
	return gross, freight
}

// primaryPayment returns the payment row with the maximum value, matching the
// batch's `select distinct on (order_id) ... order by payment_value desc`
// (the producer loads payments value-desc, so the first is the max).
func primaryPayment(payments []stream.OrderPayment) *stream.OrderPayment {
	if len(payments) == 0 {
		return nil
	}
	max := payments[0]
	for _, p := range payments[1:] {
		if p.Value > max.Value {
			max = p
		}
	}
	return &max
}

// primaryCategory returns the category of the top-price item (batch: rn = 1
// over `order by price desc`).
func (a *FraudAssembler) primaryCategory(items []stream.OrderItem) string {
	if len(items) == 0 {
		return ""
	}
	best := items[0]
	for _, it := range items[1:] {
		if it.Price > best.Price {
			best = it
		}
	}
	if cat, ok := a.ref.ProductCategory[best.ProductID]; ok {
		return cat
	}
	return "unknown"
}

// categoryCount is the distinct product categories across the order items
// (batch: count(distinct coalesce(dim_products.product_category,'unknown'))).
func (a *FraudAssembler) categoryCount(items []stream.OrderItem) int {
	if len(items) == 0 {
		return 0
	}
	seen := make(map[string]struct{}, 1)
	for _, it := range items {
		cat, ok := a.ref.ProductCategory[it.ProductID]
		if !ok {
			cat = "unknown"
		}
		seen[cat] = struct{}{}
	}
	return len(seen)
}

// categoryCode maps the primary category through the training-time rank.
func (a *FraudAssembler) categoryCode(cat string) float64 {
	if code, ok := a.ref.CategoryRank[cat]; ok {
		return float64(code)
	}
	return float64(len(a.ref.CategoryRank)) // fillna(len(rank)) == unknown bucket
}

// Assemble computes the 19-feature fraud vector for one order. ring-limited
// features (velocity, account age, benchmark) are evaluated against the rings
// BEFORE the order is recorded; the caller then records it (Rings.RecordOrder).
func (a *FraudAssembler) Assemble(od stream.OrderPlaced, r *Rings) map[string]float64 {
	t := float64(od.PurchaseTime.Unix())
	velocity, ageDays := r.Velocity(od.CustomerID, t)
	cat := a.primaryCategory(od.Items)
	benchCount, benchSum := r.Benchmark(cat, t)

	gross, freight := orderTotals(od.Items)
	freightShare := 0.0
	if gross+freight > 0 {
		freightShare = freight / (gross + freight)
	}

	installments := 1
	if len(od.Payments) > 0 {
		installments = 0
		for _, p := range od.Payments {
			if p.Installments > installments {
				installments = p.Installments
			}
		}
		if installments <= 0 {
			installments = 1 // all-null installments → batch fillna(1)
		}
	}

	f := make(map[string]float64, 18)
	f["order_value"] = od.PaymentValue
	f["item_count"] = float64(len(od.Items))
	f["freight_share"] = freightShare
	f["payment_count"] = float64(len(od.Payments))
	f["installments_max"] = float64(installments)
	f["categories_count"] = float64(a.categoryCount(od.Items))

	bench := od.PaymentValue // "no history → own value" gives ratio 1.0
	if benchCount > 0 {
		bench = benchSum / float64(benchCount)
	}
	if bench > 0 {
		f["price_vs_benchmark"] = od.PaymentValue / bench
	} else {
		f["price_vs_benchmark"] = 1.0
	}

	f["velocity_24h"] = float64(velocity)
	f["account_age_days"] = float64(ageDays)
	f["order_hour"] = float64(od.PurchaseTime.Hour())
	f["is_weekend"] = boolToFloat(od.PurchaseTime.Weekday() == time.Saturday || od.PurchaseTime.Weekday() == time.Sunday)
	f["is_lost"] = boolToFloat(od.IsLost)

	// pay_* one-hots from the primary payment type; no rows → 'unknown'. The
	// vector always carries all six flags so the assembled map is a complete
	// manifest row (serve.py fills any missing with 0.0, but the assembler
	// should not rely on that).
	primary := ""
	pp := primaryPayment(od.Payments)
	if pp != nil {
		primary = pp.Type
	}
	for _, t := range knownPaymentTypes {
		f["pay_"+t] = boolToFloat(primary == t)
	}
	if primary == "" || !matchesKnown(primary) {
		f["pay_unknown"] = 1
	}
	f["category_code"] = a.categoryCode(cat)
	return f
}

func matchesKnown(t string) bool {
	for _, k := range knownPaymentTypes {
		if t == k {
			return true
		}
	}
	return false
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// BotFeatures maps a session.end payload onto the bot_score manifest feature
// names. The producer pre-aggregates every statistic (see SessionEnd), so this
// is a pure field mapping — the score-writer never re-derives session math.
// The names mirror ml/replay_session_corpus.py's build_session_row output.
func BotFeatures(se stream.SessionEnd) map[string]float64 {
	return map[string]float64{
		"is_converting":       float64(se.IsConverting),
		"click_count":         float64(se.ClickCount),
		"duration_seconds":    se.DurationSeconds,
		"click_interval_cv":   se.ClickIntervalCV,
		"has_search":          float64(se.HasSearch),
		"has_product_page":    float64(se.HasProductPage),
		"has_cart_page":       float64(se.HasCartPage),
		"has_checkout_page":   float64(se.HasCheckoutPage),
		"page_types_distinct": float64(se.PageTypesDistinct),
		"cart_added":          float64(se.CartAdded),
		"cart_value":          se.CartValue,
		"hour_of_day":         float64(se.HourOfDay),
		"is_weekend":          float64(se.IsWeekend),
	}
}

// sortedKeys is a tiny helper for deterministic logging/maps in tests.
func sortedKeys(m map[string]float64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}