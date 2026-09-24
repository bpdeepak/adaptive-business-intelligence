// Package monitor bridges the Phase 4 ML monitoring pipeline (ml/monitor,
// Python) into the Go governance reactor. The Python drift/decay jobs write
// plain rows to gold.model_drift; this package polls those rows on a timer
// and emits one DriftComputed event per (model, computed_at) run — the event
// the playbook engine turns into a governed retrain proposal. There is no new
// internal HTTP endpoint between Python and Go (F5 from the Phase 4 review):
// the DB table IS the contract.
package monitor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"abi/internal/events"
)

// EnsureSchema idempotently creates the monitor's own state table (the parity
// pattern used by the realtime + predict layers).
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	ddl := []string{
		`CREATE TABLE IF NOT EXISTS gold.monitor_state (
			key   text PRIMARY KEY,
			value jsonb NOT NULL DEFAULT '{}'::jsonb,
			updated_at timestamptz NOT NULL DEFAULT now()
		)`,
	}
	for _, q := range ddl {
		if _, err := pool.Exec(ctx, q); err != nil {
			return fmt.Errorf("ensure monitor schema: %w", err)
		}
	}
	return nil
}

// Poller watches gold.model_drift for rows written since the last poll and
// publishes TypeDriftComputed events. The watermark (max row id seen) is
// persisted in gold.monitor_state, so a restart resumes where it left off —
// no event is lost and none is replayed twice.
type Poller struct {
	pool *pgxpool.Pool
	bus  *events.Bus
	log  func(msg string, args ...any)
}

// NewPoller builds a poller for one bus.
func NewPoller(pool *pgxpool.Pool, bus *events.Bus, log func(string, ...any)) *Poller {
	return &Poller{pool: pool, bus: bus, log: log}
}

// Run ticks until ctx is cancelled, polling for new drift rows each tick.
func (p *Poller) Run(ctx context.Context, every time.Duration) error {
	if err := p.poll(ctx); err != nil && p.log != nil {
		p.log("monitor: initial drift poll failed", "error", err)
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := p.poll(ctx); err != nil && p.log != nil {
				p.log("monitor: drift poll failed", "error", err)
			}
		}
	}
}

// poll publishes one event per (model, computed_at) touched by rows newer than
// the watermark, then advances the watermark.
func (p *Poller) poll(ctx context.Context) error {
	watermark, err := p.readWatermark(ctx)
	if err != nil {
		return err
	}

	rows, err := p.pool.Query(ctx, `
SELECT id, model_name, model_version, feature, psi, status, kind,
       to_char(computed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
FROM gold.model_drift WHERE id > $1 ORDER BY id`, watermark)
	if err != nil {
		return fmt.Errorf("query model_drift: %w", err)
	}
	defer rows.Close()

	runs := map[string]*events.DriftRun{} // key: model|computedAt
	maxID := watermark
	for rows.Next() {
		var id int64
		var model, version, feature, status, kind, computedAt string
		var psi float64
		if err := rows.Scan(&id, &model, &version, &feature, &psi, &status, &kind, &computedAt); err != nil {
			return fmt.Errorf("scan model_drift: %w", err)
		}
		if id > maxID {
			maxID = id
		}
		key := model + "|" + computedAt
		r, ok := runs[key]
		if !ok {
			r = &events.DriftRun{Model: model, Version: version, ComputedAt: computedAt}
			runs[key] = r
		}
		r.Add(feature, psi, status, kind)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, r := range runs {
		ev := r.Event(false, time.Now().UTC())
		p.bus.Publish(ev)
		if p.log != nil {
			drift, _ := ev.Payload["drift"].(map[string]any)
			p.log("monitor: drift event", "model", r.Model, "status", drift["status"], "computed_at", r.ComputedAt)
		}
	}

	if maxID > watermark {
		if err := p.writeWatermark(ctx, maxID); err != nil {
			return err
		}
	}
	return nil
}

func (p *Poller) readWatermark(ctx context.Context) (int64, error) {
	var id int64
	err := p.pool.QueryRow(ctx, `
SELECT COALESCE((value->>'drift_watermark')::bigint, 0)
FROM gold.monitor_state WHERE key = 'drift'`).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		// Fresh state: never polled before — start from the beginning. The
		// first poll writes the row, so this is the bootstrap path.
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read monitor watermark: %w", err)
	}
	return id, nil
}

func (p *Poller) writeWatermark(ctx context.Context, id int64) error {
	_, err := p.pool.Exec(ctx, `
INSERT INTO gold.monitor_state (key, value, updated_at)
VALUES ('drift', $1::jsonb, now())
ON CONFLICT (key) DO UPDATE SET value = $1::jsonb, updated_at = now()`,
		fmt.Sprintf(`{"drift_watermark": %d}`, id))
	if err != nil {
		return fmt.Errorf("write monitor watermark: %w", err)
	}
	return nil
}