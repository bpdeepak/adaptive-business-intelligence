package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"abi/internal/model"
)

// LoadReplayOrders loads the full historical order sequence from the gold layer
// (plus payments from silver; items in (order_id, order_item_id) order so the
// assembler's first-of-equal-price tie-break matches the batch builder's) — the single source the Phase 1 producer replays.
// The producer and the score-writer's train/serve parity test both call this, so
// the parity test exercises the exact rows the live stream is built from.
//
// (It replaced two drifted copies: the producer's private loader and an unused
// PostgresStore.ReplayOrders that silently lacked payments.)
func LoadReplayOrders(ctx context.Context, pool *pgxpool.Pool) ([]model.ReplayOrder, error) {
	rows, err := pool.Query(ctx, `
SELECT o.order_id, o.customer_id, o.order_status, o.order_purchase_timestamp,
       o.payment_value_total::float8,
       (o.order_status IN ('canceled','unavailable')) AS is_lost
FROM gold.fct_orders o
ORDER BY o.order_purchase_timestamp, o.order_id`)
	if err != nil {
		return nil, fmt.Errorf("load replay orders: %w", err)
	}
	defer rows.Close()

	type base struct {
		orderID, customerID, status string
		at                          time.Time
		value                       float64
		lost                        bool
	}
	var bs []base
	for rows.Next() {
		var b base
		if err := rows.Scan(&b.orderID, &b.customerID, &b.status, &b.at, &b.value, &b.lost); err != nil {
			return nil, fmt.Errorf("scan replay order: %w", err)
		}
		bs = append(bs, b)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	itemRows, err := pool.Query(ctx, `
SELECT order_id, product_id, seller_id, price::float8, freight_value::float8
FROM gold.fct_order_items
ORDER BY order_id, order_item_id`)
	if err != nil {
		return nil, fmt.Errorf("load replay items: %w", err)
	}
	defer itemRows.Close()
	items := make(map[string][]model.ReplayItem)
	for itemRows.Next() {
		var orderID, productID, sellerID string
		var price, freight float64
		if err := itemRows.Scan(&orderID, &productID, &sellerID, &price, &freight); err != nil {
			return nil, fmt.Errorf("scan replay item: %w", err)
		}
		items[orderID] = append(items[orderID], model.ReplayItem{
			ProductID: productID, SellerID: sellerID, Price: price, Freight: freight,
		})
	}
	if err := itemRows.Err(); err != nil {
		return nil, err
	}

	payRows, err := pool.Query(ctx, `
SELECT order_id, payment_type,
       COALESCE(payment_installments, 0)::int,
       payment_value::float8
FROM silver.stg_order_payments
ORDER BY order_id, payment_value DESC, payment_installments DESC, payment_type`)
	if err != nil {
		return nil, fmt.Errorf("load replay payments: %w", err)
	}
	defer payRows.Close()
	payments := make(map[string][]model.ReplayPayment)
	for payRows.Next() {
		var orderID, pType string
		var installments int
		var value float64
		if err := payRows.Scan(&orderID, &pType, &installments, &value); err != nil {
			return nil, fmt.Errorf("scan replay payment: %w", err)
		}
		payments[orderID] = append(payments[orderID], model.ReplayPayment{
			Type: pType, Installments: installments, Value: value,
		})
	}
	if err := payRows.Err(); err != nil {
		return nil, err
	}

	out := make([]model.ReplayOrder, 0, len(bs))
	for _, b := range bs {
		out = append(out, model.ReplayOrder{
			OrderID: b.orderID, CustomerID: b.customerID, Status: b.status,
			PurchaseAt: b.at, PaymentValue: b.value, IsLost: b.lost,
			Items: items[b.orderID], Payments: payments[b.orderID],
		})
	}
	return out, nil
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
