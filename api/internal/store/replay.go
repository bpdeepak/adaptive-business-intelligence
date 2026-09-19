package store

import (
	"context"
	"fmt"
	"time"

	"abi/internal/model"
)

// ReplayOrders loads the full historical order sequence from the gold layer —
// the single source the Phase 1 producer replays. Reading gold (not bronze)
// reuses the typed/cleaned facts the same way the REST API does.
func (s *PostgresStore) ReplayOrders(ctx context.Context) ([]model.ReplayOrder, error) {
	rows, err := s.pool.Query(ctx, `
SELECT o.order_id, o.customer_id, o.order_status, o.order_purchase_timestamp,
       o.payment_value_total::float8,
       (o.order_status IN ('canceled','unavailable')) AS is_lost
FROM gold.fct_orders o
ORDER BY o.order_purchase_timestamp, o.order_id`)
	if err != nil {
		return nil, fmt.Errorf("replay orders: %w", err)
	}
	defer rows.Close()

	type orderBase struct {
		orderID, customerID, status string
		purchaseAt                  time.Time
		paymentValue                float64
		isLost                      bool
	}
	var bases []orderBase
	for rows.Next() {
		var b orderBase
		if err := rows.Scan(&b.orderID, &b.customerID, &b.status, &b.purchaseAt, &b.paymentValue, &b.isLost); err != nil {
			return nil, fmt.Errorf("replay orders scan: %w", err)
		}
		bases = append(bases, b)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	items, err := s.ReplayOrderItems(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]model.ReplayOrder, 0, len(bases))
	for _, b := range bases {
		out = append(out, model.ReplayOrder{
			OrderID:      b.orderID,
			CustomerID:   b.customerID,
			Status:       b.status,
			PurchaseAt:   b.purchaseAt,
			PaymentValue: b.paymentValue,
			IsLost:       b.isLost,
			Items:        items[b.orderID],
		})
	}
	return out, nil
}

// ReplayOrderItems loads per-item detail for every historical order from
// gold.fct_order_items, keyed by order_id.
func (s *PostgresStore) ReplayOrderItems(ctx context.Context) (map[string][]model.ReplayItem, error) {
	rows, err := s.pool.Query(ctx, `
SELECT order_id, product_id, seller_id, price::float8, freight_value::float8
FROM gold.fct_order_items
ORDER BY order_id, order_item_id`)
	if err != nil {
		return nil, fmt.Errorf("replay order items: %w", err)
	}
	defer rows.Close()

	out := make(map[string][]model.ReplayItem)
	for rows.Next() {
		var orderID string
		var it model.ReplayItem
		if err := rows.Scan(&orderID, &it.ProductID, &it.SellerID, &it.Price, &it.Freight); err != nil {
			return nil, fmt.Errorf("replay order items scan: %w", err)
		}
		out[orderID] = append(out[orderID], it)
	}
	return out, rows.Err()
}

// ReplayProducts returns the distinct products sold, ordered — used by the
// price-change generator.
func (s *PostgresStore) ReplayProducts(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT product_id FROM gold.fct_order_items ORDER BY product_id`)
	if err != nil {
		return nil, fmt.Errorf("replay products: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// InitialPrices loads the last known historical price per product, used to seed
// the price-change random walk.
func (s *PostgresStore) InitialPrices(ctx context.Context) (map[string]float64, error) {
	rows, err := s.pool.Query(ctx, `
SELECT DISTINCT ON (i.product_id) i.product_id, i.price::float8
FROM gold.fct_order_items i
JOIN gold.fct_orders o ON o.order_id = i.order_id
ORDER BY i.product_id, o.order_purchase_timestamp DESC`)
	if err != nil {
		return nil, fmt.Errorf("initial prices: %w", err)
	}
	defer rows.Close()

	out := make(map[string]float64)
	for rows.Next() {
		var id string
		var price float64
		if err := rows.Scan(&id, &price); err != nil {
			return nil, err
		}
		out[id] = price
	}
	return out, rows.Err()
}
