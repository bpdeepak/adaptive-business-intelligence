// Package metrics is the machine-readable semantic/metric dictionary of the
// platform. It is served at GET /api/v1/metrics and is the object an agent
// (or a human) fetches before interpreting numbers, so answers can cite the
// canonical definition instead of guessing.
package metrics

// Metric is one canonical business metric.
type Metric struct {
	Name        string `json:"name"`
	Label       string `json:"label"`
	Description string `json:"description"`
	// Source is the required provenance tag: "batch" (complete historical
	// aggregates over gold.daily_*) or "live_replay" (compressed 1-minute
	// streaming buckets over gold.realtime_metrics). Values of the two sources
	// are never additive; agents must handle the split explicitly.
	Source      string   `json:"source"`
	Population  string   `json:"population"`
	Grain       string   `json:"grain"`
	DateField   string   `json:"date_field,omitempty"`
	Aggregation string   `json:"aggregation"`
	Unit        string   `json:"unit"`
	SourceTable string   `json:"source_table"`
	Notes       []string `json:"notes,omitempty"`
	Tags        []string `json:"tags,omitempty"`
}

// Catalog is the full dictionary with dataset-level notes.
type Catalog struct {
	Version string   `json:"version"`
	Metrics []Metric `json:"metrics"`
	Notes   []string `json:"notes,omitempty"`
}

// Source provenance tags (mirrors model.SourceBatch/SourceLiveReplay without
// importing the model package into the definitions).
const (
	SourceBatch      = "batch"
	SourceLiveReplay = "live_replay"
	// SourceModel marks a model output: a scored/forecast value in
	// gold.predictions produced by a versioned model in gold.model_registry.
	// It is never additive with the batch or live_replay sources.
	SourceModel = "model"
	// SourceGovernance marks approval-queue state in gold.action_queue: what a
	// playbook PROPOSED and what a human decided. These are counts of decisions,
	// not business figures — never additive with any other source.
	SourceGovernance = "governance"
	// SourceMonitoring marks model-health findings in gold.model_drift: per-
	// feature PSI and forecast/churn decay measurements. They describe model
	// inputs and error trends, not business value.
	SourceMonitoring = "monitoring"
)

// Version is bumped whenever a definition changes. Keep it in sync with the
// `Metric definitions` table in docs/phase0.md (section 4) and docs/phase1.md.
const Version = "1.4.0"

// Default returns the Phase 0 catalog.
func Default() Catalog {
	return Catalog{
		Version: Version,
		Metrics: []Metric{
			{
				Name:        "revenue",
				Label:       "Revenue",
				Source:      SourceBatch,
				Description: "Total captured payments from non-lost orders.",
				Population:  "orders with order_status NOT IN ('canceled','unavailable') AND payment_value_total > 0",
				Grain:       "order",
				DateField:   "order_purchase_date",
				Aggregation: "SUM(payment_value_total)",
				Unit:        "BRL",
				SourceTable: "gold.daily_revenue (+ gold.fct_orders)",
				Tags:        []string{"kpi", "money"},
				Notes: []string{
					"Attributed to the order purchase date (when the customer decided to buy), not approval or delivery time.",
					"Payment-based: sums payment_value from the installments ledger, which is the money actually captured.",
					"Excludes canceled and unavailable orders (money never expected or already refunded).",
				},
			},
			{
				Name:        "orders",
				Label:       "Orders",
				Source:      SourceBatch,
				Description: "Distinct orders.",
				Population:  "summary endpoint: paying, non-lost population (same as revenue). daily_orders: all orders.",
				Grain:       "order",
				DateField:   "order_purchase_date",
				Aggregation: "COUNT(DISTINCT order_id)",
				Unit:        "count",
				SourceTable: "gold.daily_orders; gold.daily_revenue (summary count)",
				Tags:        []string{"kpi", "count"},
				Notes: []string{
					"summary.orders counts the paying, non-lost population (98,206); the raw order count is 99,441. The difference is lost orders (canceled/unavailable). Both are intentional.",
					"daily_orders also exposes delivered_orders and lost_orders as separate splits over ALL orders.",
				},
			},
			{
				Name:        "aov",
				Label:       "Average order value",
				Source:      SourceBatch,
				Description: "Revenue per order of the same population.",
				Population:  "paying, non-lost orders with payment_value_total > 0",
				Grain:       "day",
				DateField:   "order_purchase_date",
				Aggregation: "SUM(payment_value_total) / COUNT(DISTINCT order_id), rounded to 2 decimals",
				Unit:        "BRL",
				SourceTable: "gold.daily_aov",
				Tags:        []string{"kpi", "money"},
				Notes: []string{
					"Aligned to the revenue population so AOV never mixes lost orders into the denominator.",
					"At day grain it lives in gold.daily_aov; the summary endpoint recomputes it over the selected window.",
				},
			},
			{
				Name:        "active_customers",
				Label:       "Active customers",
				Source:      SourceBatch,
				Description: "Distinct business customers with at least one non-lost order.",
				Population:  "customer_unique_id with an order in order_status NOT IN ('canceled','unavailable')",
				Grain:       "customer",
				Aggregation: "COUNT(DISTINCT customer_unique_id)",
				Unit:        "count",
				SourceTable: "gold.fct_orders (+ gold.dim_customers)",
				Tags:        []string{"kpi", "count"},
				Notes: []string{
					"Uses customer_unique_id (the business person), not customer_id (one per order account).",
				},
			},
			{
				Name:        "delivered_orders",
				Label:       "Delivered orders",
				Source:      SourceBatch,
				Description: "Orders whose final status is 'delivered'.",
				Population:  "all orders",
				Grain:       "order",
				DateField:   "order_purchase_date",
				Aggregation: "COUNT(DISTINCT order_id) FILTER (WHERE is_delivered)",
				Unit:        "count",
				SourceTable: "gold.daily_orders",
				Tags:        []string{"count"},
			},
			{
				Name:        "lost_orders",
				Label:       "Lost orders",
				Source:      SourceBatch,
				Description: "Orders whose final status is 'canceled' or 'unavailable'.",
				Population:  "all orders",
				Grain:       "order",
				DateField:   "order_purchase_date",
				Aggregation: "COUNT(DISTINCT order_id) FILTER (WHERE is_lost)",
				Unit:        "count",
				SourceTable: "gold.daily_orders",
				Tags:        []string{"count"},
				Notes: []string{
					"Lost orders are excluded from revenue, AOV and active_customers.",
				},
			},
			{
				Name:        "top_categories",
				Label:       "Top categories",
				Source:      SourceBatch,
				Description: "Product categories ranked by revenue or order count.",
				Population:  "paying, non-lost orders with payment_value_total > 0",
				Grain:       "product category (per order line item)",
				DateField:   "order_purchase_date",
				Aggregation: "SUM(payment_value_total) / COUNT(DISTINCT order_id) per category",
				Unit:        "BRL (or count)",
				SourceTable: "gold.top_categories",
				Tags:        []string{"dimension", "category"},
				Notes: []string{
					"An order's payment_value_total is attributed to EVERY category present in that order's line items.",
					"Because one order can span several categories, summing top_categories revenue double-counts those orders and will exceed the revenue metric. Never compare the sum of category revenue to the revenue metric directly.",
					"Categories with no Portuguese name, or no English translation, are grouped under 'unknown'.",
				},
			},
			{
				Name:        "review_score",
				Label:       "Review score",
				Source:      SourceBatch,
				Description: "Customer satisfaction score per order (1–5).",
				Population:  "orders with at least one review",
				Grain:       "order",
				Aggregation: "AVG(review_score) over the order's reviews",
				Unit:        "score 1–5",
				SourceTable: "gold.fct_orders.review_score_avg",
				Tags:        []string{"satisfaction"},
				Notes: []string{
					"Multiple reviews per order are averaged; exact duplicate review rows were removed in silver (stg_order_reviews).",
					"Not yet exposed on an endpoint — available in the gold model for Phase 2+ questions.",
				},
			},
			{
				Name:        "revenue_realtime",
				Label:       "Revenue — live (1-min bucket)",
				Source:      SourceLiveReplay,
				Description: "Revenue in the current 1-minute streaming bucket; identical population to `revenue` but bucketed by event time.",
				Population:  "orders with order_status NOT IN ('canceled','unavailable') AND payment_value_total > 0 (replayed)",
				Grain:       "1-minute bucket (bucket_start)",
				DateField:   "bucket_start",
				Aggregation: "SUM(payment_value_total) per bucket",
				Unit:        "BRL",
				SourceTable: "gold.realtime_metrics",
				Tags:        []string{"kpi", "money", "realtime"},
				Notes: []string{
					"Same revenue filter as the batch metric — but computed on replayed order events, bucketed by the event's occurred_at minute.",
					"Live means compressed replay: the whole dataset is replayed ~1 day per 30 seconds (SPEED_MULTIPLIER), so a \"minute\" of data is a fraction of a wall second. Do NOT add realtime values to batch totals.",
					"Buckets are upserted every 5 seconds; a bucket may still grow within its minute window.",
				},
			},
			{
				Name:        "orders_realtime",
				Label:       "Orders — live (1-min bucket)",
				Source:      SourceLiveReplay,
				Description: "Distinct non-lost orders in the current 1-minute streaming bucket.",
				Population:  "orders with order_status NOT IN ('canceled','unavailable') AND payment_value_total > 0 (replayed)",
				Grain:       "1-minute bucket (bucket_start)",
				DateField:   "bucket_start",
				Aggregation: "COUNT(DISTINCT order_id) per bucket",
				Unit:        "count",
				SourceTable: "gold.realtime_metrics",
				Tags:        []string{"kpi", "count", "realtime"},
				Notes: []string{
					"Aligned to the revenue_realtime population; lost orders are excluded by construction.",
					"Anomaly detection runs on this metric after each closed bucket.",
				},
			},
			{
				Name:        "active_sessions",
				Label:       "Active sessions — live (1-min bucket)",
				Source:      SourceLiveReplay,
				Description: "Distinct browsing sessions with at least one click event in the bucket.",
				Population:  "sessions with ≥1 page.view/cart.abandoned event (replayed, includes synthetic bot traffic)",
				Grain:       "1-minute bucket (bucket_start)",
				DateField:   "bucket_start",
				Aggregation: "COUNT(DISTINCT session_id) per bucket",
				Unit:        "count",
				SourceTable: "gold.realtime_metrics",
				Tags:        []string{"kpi", "traffic", "realtime"},
				Notes: []string{
					"Counts ALL sessions (human + synthetic bots). Bot ground truth lives in the restricted ecommerce.internal.training_labels topic / bronze.training_ground_truth, never in the clickstream.",
					"Only for estimating concurrent/converting traffic pressure; not comparable to customers.",
				},
			},
			{
				Name:        "forecast_revenue",
				Label:       "Forecast revenue — category × week",
				Source:      SourceModel,
				Description: "4-week-ahead revenue forecast per product category, from the trained LightGBM model.",
				Population:  "all categories with >=16 weeks of demand history",
				Grain:       "product category × ISO week (week_start)",
				DateField:   "week_start",
				Aggregation: "LightGBM regression on lag/rolling features; rolling-origin backtest reported in gold.model_registry",
				Unit:        "BRL",
				SourceTable: "gold.predictions (model_name='forecast_category_weekly_revenue')",
				Tags:        []string{"model", "forecast", "money"},
				Notes: []string{
					"Revenue here is allocated across an order's line items by price share, so it is mutually exclusive across categories (unlike top_categories).",
					"Model outputs are NOT comparable to batch/realtime actuals as if they were observed revenue; always cite the model version and horizon.",
					"Explanation payload carries the model's global SHAP ranking.",
				},
			},
			{
				Name:        "churn_risk",
				Label:       "Customer churn risk",
				Source:      SourceModel,
				Description: "Probability that a repeat customer will not purchase again within 90 days, scored as-of their penultimate order.",
				Population:  "customers with >=2 orders",
				Grain:       "customer (customer_unique_id)",
				Aggregation: "XGBoost binary classifier; time-split backtest (latest 20% of as-of dates held out)",
				Unit:        "probability 0–1",
				SourceTable: "gold.predictions (model_name='churn_risk')",
				Tags:        []string{"model", "risk", "customer"},
				Notes: []string{
					"Churn label: no purchase within 90 days following the as-of order; the penultimate-order design means no right-censoring.",
					"Every served/persisted prediction carries per-row SHAP contributions in explanation.shap.",
					"Use the lift-at-5% metric in gold.model_registry to size retention campaigns.",
				},
			},
			{
				Name:        "fraud_risk",
				Label:       "Transaction fraud risk",
				Source:      SourceModel,
				Description: "Probability that an order is fraudulent, from features available at order time.",
				Population:  "all orders",
				Grain:       "order (order_id)",
				Aggregation: "XGBoost binary classifier on synthetically injected fraud patterns; time-split backtest",
				Unit:        "probability 0–1",
				SourceTable: "gold.predictions (model_name='fraud_risk')",
				Tags:        []string{"model", "risk", "order"},
				Notes: []string{
					"Labels are SYNTHETIC (the Olist source is clean): velocity >=3 orders/24h, price >=8x the trailing-90d category average, excessive installments, plus a 0.4% noise floor.",
					"Features are strictly as-of the order timestamp (trailing windows), so the backtest has no future leakage.",
					"Per-row SHAP contributions are stored with every prediction.",
				},
			},
			{
				Name:        "bot_score",
				Label:       "Synthetic-bot session score",
				Source:      SourceModel,
				Description: "Probability that a browsing session is automated, from session-shape features only.",
				Population:  "all sessions (converting + abandoned)",
				Grain:       "session (session_id)",
				Aggregation: "LightGBM binary classifier; time-split backtest by session date",
				Unit:        "probability 0–1",
				SourceTable: "gold.predictions (model_name='bot_score'); features in gold.session_features",
				Tags:        []string{"model", "traffic", "risk"},
				Notes: []string{
					"Trained on gold.session_features, the durable corpus built by ml/replay_session_corpus.py that mirrors the replay's session synthesis (labels never appear in the clickstream).",
					"Signals: page-to-page timing (bots fire 3–12 clicks in <1s), funnel shape (bots skip search), and inter-click variance.",
					"Persisted test predictions are a sample; live scoring is available through POST /api/v1/score.",
				},
			},
			{
				Name:        "governance_pending_actions",
				Label:       "Pending governed actions",
				Source:      SourceGovernance,
				Description: "Playbook proposals awaiting a human decision on the approval queue. Every row is a concrete 'do something' suggestion (hold an order, retrain a model, draft an order) derived from a domain event; nothing executes until a human approves it (or an allow-listed auto tier records an explicit auto-approval).",
				Population:  "proposals in gold.action_queue",
				Grain:       "action (id)",
				Aggregation: "count of gold.action_queue rows with status='pending'",
				Unit:        "actions",
				SourceTable: "gold.action_queue",
				Tags:        []string{"governance", "phase4"},
				Notes: []string{
					"Proposals are deduplicated: one row per (playbook rule, trigger scope); a replayed event can never flood the queue.",
					"Every transition (proposed → approved/rejected → executed → outcome) is an immutable row in gold.action_audit_log tying actor, reason, and payload to one action id.",
				},
			},
			{
				Name:        "governance_decided_actions",
				Label:       "Decided governed actions",
				Source:      SourceGovernance,
				Description: "Playbook proposals that have been decided (approved, rejected, or auto-approved), including their execution outcome. Counts of decisions — not business figures — useful for reviewing how often the platform proposed versus what humans actually allowed.",
				Population:  "proposals in gold.action_queue",
				Grain:       "action (id)",
				Aggregation: "count of gold.action_queue rows with status != 'pending'",
				Unit:        "actions",
				SourceTable: "gold.action_queue + gold.action_audit_log",
				Tags:        []string{"governance", "phase4"},
				Notes: []string{
					"The immutable audit log (gold.action_audit_log) is the source of truth for the decision trail; the queue row is the current state, the audit rows are the history.",
				},
			},
			{
				Name:        "model_drift_psi",
				Label:       "Per-feature model input drift (PSI)",
				Source:      SourceMonitoring,
				Description: "Population Stability Index of one model feature: how far the recent input distribution has shifted from the distribution the model was trained on. Bands (ok <0.10, warning 0.10–0.20, critical >0.20) drive the retrain-on-critical-drift playbook proposal. NaN PSI maps to ok; infinite to critical.",
				Population:  "one (active model, feature, run) row in gold.model_drift",
				Grain:       "model feature (computed_at)",
				Aggregation: "10-bin PSI of recent samples vs the training-matrix histogram stored in the registry drift_baseline",
				Unit:        "PSI (dimensionless)",
				SourceTable: "gold.model_drift (kind='psi')",
				Tags:        []string{"monitoring", "phase4", "drift"},
				Notes: []string{
					"Stream models (fraud_risk, bot_score) sample the feature vectors the Go score-writer persists in gold.predictions.features; batch models (churn_risk, forecasts) sample their feature marts.",
					"Baselines exclude features that are not re-measurable in a comparable window (category_code is not a mart column; year is constant within any rolling window) — measuring what can't be measured comparably would produce pure noise.",
					"Findings are advisory: the playbook PROPOSES a retrain; a human approves or rejects it with a recorded reason.",
				},
			},
			{
				Name:        "model_forecast_decay",
				Label:       "Forecast accuracy decay",
				Source:      SourceMonitoring,
				Description: "Current WAPE of the newest rolling-origin backtest window for a forecast model versus the baseline WMAPE it registered at training time. Crosses the 1.5x deterioration bound when the model's recent error meaningfully exceeds its own trained behavior.",
				Population:  "one (active forecast model, run) row in gold.model_drift",
				Grain:       "model (computed_at)",
				Aggregation: "WAPE over the most recent backtest window vs registry metrics.wmape; status from the 1.5x deterioration boundary",
				Unit:        "ratio (current/baseline WAPE)",
				SourceTable: "gold.model_drift (kind='forecast_decay')",
				Tags:        []string{"monitoring", "phase4", "drift"},
				Notes: []string{
					"Same advisory posture as model_drift_psi: a decay finding can raise the retrain-on-critical-drift proposal, but never silently acts.",
				},
			},
		},
		Notes: []string{
			"Dataset: Olist Brazilian E-Commerce, 2016-09-04 → 2018-09-03 (purchase dates).",
			"Timestamps are loaded as naive CSV text and cast to timestamptz using the session timezone (default UTC); compare dates only, not intraday times.",
			"The source contains 3 customers with empty city/state ('unidentified'); they are flagged is_identified=false but not excluded.",
			"Geo data (bronze.geolocation, 1,000,163 rows) is landed but not staged in Phase 0.",
			"Errata: summary's aov/counts describe the paying, non-lost population; daily_orders.total describes all orders. Always check the population field of a metric before combining metrics.",
			"Realtime metrics (Phase 1) describe a compressed REPLAY of the same orders; they are bounded to their own gold.realtime_metrics table and are never added to the batch gold.daily_* totals.",
			"Every metric carries a required `source` field: \"batch\" vs \"live_replay\". Never add or compare figures across the two sources; treat the split as part of the contract.",
			"Phase 2 adds a third source, \"model\": forecast/risk scores in gold.predictions produced by versioned models in gold.model_registry. Model outputs are never additive with batch or live_replay figures; always cite the model version and its backtest metrics.",
			"Phase 4 adds \"governance\" (approval-queue state: counts of proposals and decisions, not business figures) and \"monitoring\" (model health: per-feature PSI and forecast decay). Both are observability sources — maps of the system — and are never additive with batch, live_replay, or model figures.",
		},
	}
}
