// Package metrics is the machine-readable semantic/metric dictionary of the
// platform. It is served at GET /api/v1/metrics and is the object an agent
// (or a human) fetches before interpreting numbers, so answers can cite the
// canonical definition instead of guessing.
package metrics

// Metric is one canonical business metric.
type Metric struct {
	Name        string   `json:"name"`
	Label       string   `json:"label"`
	Description string   `json:"description"`
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

// Version is bumped whenever a definition changes. Keep it in sync with the
// `Metric definitions` table in docs/phase0.md (section 4).
const Version = "1.0.0"

// Default returns the Phase 0 catalog.
func Default() Catalog {
	return Catalog{
		Version: Version,
		Metrics: []Metric{
			{
				Name:        "revenue",
				Label:       "Revenue",
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
		},
		Notes: []string{
			"Dataset: Olist Brazilian E-Commerce, 2016-09-04 → 2018-09-03 (purchase dates).",
			"Timestamps are loaded as naive CSV text and cast to timestamptz using the session timezone (default UTC); compare dates only, not intraday times.",
			"The source contains 3 customers with empty city/state ('unidentified'); they are flagged is_identified=false but not excluded.",
			"Geo data (bronze.geolocation, 1,000,163 rows) is landed but not staged in Phase 0.",
			"Errata: summary's aov/counts describe the paying, non-lost population; daily_orders.total describes all orders. Always check the population field of a metric before combining metrics.",
		},
	}
}