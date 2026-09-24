package actions

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"abi/internal/events"
)

// Context carries what an executor needs: the DB pool, the rule's static
// params, and the domain event that triggered the proposal (the evidence).
// Executors are deliberately small, idempotent SQL effects over ABI's own
// tables.
type Context struct {
	Pool   *pgxpool.Pool
	Event  events.Event
	Params map[string]any
}

// Result is the structured execution outcome persisted on the audit log.
type Result struct {
	OK     bool `json:"ok"`
	Detail any  `json:"detail,omitempty"`
}

// Executor is one named action implementation. It must be safe to retry:
// executing the same action twice must not double-effect (upserts + natural
// keys below).
type Executor func(ctx context.Context, ac Context) (Result, error)

// Service is the action registry + approval store. Execution is gated by the
// governance invariant in execute(): an approved or auto_approved transition
// must already exist on the audit log, or the executor refuses to run.
type Service struct {
	pool *pgxpool.Pool
	log  *slog.Logger
	reg  map[string]Executor
}

// New builds a Service with the default executor set.
func New(pool *pgxpool.Pool, log *slog.Logger) *Service {
	s := &Service{pool: pool, log: log, reg: map[string]Executor{}}
	for name, fn := range defaultExecutors {
		s.Register(name, fn)
	}
	return s
}

// Register adds or replaces one named executor.
func (s *Service) Register(name string, fn Executor) { s.reg[name] = fn }

// Has reports whether an executor exists for name (unknown actions fail closed
// at proposal time — a playbook cannot reference an action that does not exist).
func (s *Service) Has(name string) bool { _, ok := s.reg[name]; return ok }

// ErrUnknownAction is returned when a playbook references an unregistered action.
var ErrUnknownAction = errors.New("unknown action")

// ErrUnauthorized guards the governance invariant: no executor runs without a
// prior approved (or allow-list auto_approved) transition on the audit log.
var ErrUnauthorized = errors.New("execution without prior approval transition")

// ErrInvalidState is returned for approve/reject/execute on a row that is not
// in the expected state (already decided, or missing).
var ErrInvalidState = errors.New("action not in the expected state")

// Field resolves a dotted path inside the event payload (e.g.
// "prediction.entity_id"), returning (value, true) when present.
func Field(payload map[string]any, dotted string) (any, bool) {
	seg := splitPath(dotted)
	var cur any = payload
	for _, s := range seg {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		v, ok := m[s]
		if !ok {
			return nil, false
		}
		cur = v
	}
	return cur, true
}

func splitPath(dotted string) []string {
	out := []string{}
	start := 0
	for i := 0; i <= len(dotted); i++ {
		if i == len(dotted) || dotted[i] == '.' {
			if i > start {
				out = append(out, dotted[start:i])
			}
			start = i + 1
		}
	}
	return out
}

// StringField returns a dotted-path payload field as a string.
func StringField(payload map[string]any, dotted string) string {
	v, ok := Field(payload, dotted)
	if !ok {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// ---------------------------------------------------------------------------
// Default executors (all simulated — they mutate only ABI's own tables).
// ---------------------------------------------------------------------------

var defaultExecutors = map[string]Executor{
	// Sets the held_for_review flag + reason on the order's flag row. The
	// immutable gold.fct_orders row is never touched. Idempotent: re-setting
	// the same flag is a no-op upsert.
	"hold_order_for_review": func(ctx context.Context, ac Context) (Result, error) {
		orderID := StringField(ac.Event.Payload, "prediction.entity_id")
		if orderID == "" {
			return Result{}, fmt.Errorf("hold_order_for_review: no prediction.entity_id in event")
		}
		reason := StringField(ac.Event.Payload, "prediction.model") + " >= threshold"
		if r, ok := ac.Params["reason"].(string); ok && r != "" {
			reason = r
		}
		_, err := ac.Pool.Exec(ctx, `
INSERT INTO gold.order_flags (order_id, held_for_review, reason, set_at)
VALUES ($1, true, $2, now())
ON CONFLICT (order_id) DO UPDATE SET
    held_for_review = true,
    reason = EXCLUDED.reason,
    set_at = now(),
    released_at = NULL`, orderID, reason)
		if err != nil {
			return Result{}, fmt.Errorf("hold_order_for_review: %w", err)
		}
		return Result{OK: true, Detail: map[string]any{"order_id": orderID, "reason": reason}}, nil
	},

	// Mirrors hold: clears the flag. Itself always governance-gated (releasing
	// a hold is a business decision, never allow-listed).
	"release_order": func(ctx context.Context, ac Context) (Result, error) {
		orderID := StringField(ac.Event.Payload, "prediction.entity_id")
		if orderID == "" {
			return Result{}, fmt.Errorf("release_order: no prediction.entity_id in event")
		}
		_, err := ac.Pool.Exec(ctx, `
UPDATE gold.order_flags
SET held_for_review = false, released_at = now()
WHERE order_id = $1`, orderID)
		if err != nil {
			return Result{}, fmt.Errorf("release_order: %w", err)
		}
		return Result{OK: true, Detail: map[string]any{"order_id": orderID, "released": true}}, nil
	},

	// Drafts a purchase order referencing the triggering forecast. One draft
	// per (category, forecast_week) — the natural idempotency key.
	"draft_purchase_order": func(ctx context.Context, ac Context) (Result, error) {
		category := StringField(ac.Event.Payload, "forecast.category")
		week := StringField(ac.Event.Payload, "forecast.week")
		if category == "" || week == "" {
			return Result{}, fmt.Errorf("draft_purchase_order: forecast.category/week missing")
		}
		qty := 0
		if q, ok := numVal(ac.Params["quantity"]); ok {
			qty = int(q)
		}
		var id int64
		err := ac.Pool.QueryRow(ctx, `
INSERT INTO gold.purchase_orders (category, forecast_week, quantity, status, reference)
VALUES ($1, $2, $3, 'draft', $4::jsonb)
ON CONFLICT (category, forecast_week) DO NOTHING
RETURNING id`, category, week, qty, `{}`).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return Result{OK: true, Detail: map[string]any{"idempotent": true, "category": category, "forecast_week": week}}, nil
		}
		if err != nil {
			return Result{}, fmt.Errorf("draft_purchase_order: %w", err)
		}
		return Result{OK: true, Detail: map[string]any{"id": id, "category": category, "forecast_week": week, "quantity": qty}}, nil
	},

	// Informational only — no real email exists. Idempotence guard: same
	// customer + kind within 24h logs once.
	"log_retention_email": func(ctx context.Context, ac Context) (Result, error) {
		return insertRetention(ctx, ac, "email", "sent")
	},
	"propose_retention_offer": func(ctx context.Context, ac Context) (Result, error) {
		return insertRetention(ctx, ac, "offer", "proposed")
	},

	// Schedules a retrain through the pending-work queue a Python worker
	// (ml/monitor/retrain_worker.py) consumes; the worker registers a
	// 'candidate' version and writes the outcome transition. One request per
	// action id (unique action_id).
	"retrain_model": func(ctx context.Context, ac Context) (Result, error) {
		model := StringField(ac.Event.Payload, "drift.model")
		if model == "" {
			model = StringField(ac.Event.Payload, "model")
		}
		if model == "" {
			if m, ok := ac.Params["model"].(string); ok {
				model = m
			}
		}
		if model == "" {
			return Result{}, fmt.Errorf("retrain_model: no model in event or params")
		}
		actionID, _ := ac.Params["__action_id"].(int64)
		var id int64
		err := ac.Pool.QueryRow(ctx, `
INSERT INTO gold.retrain_requests (action_id, model, status)
VALUES ($1, $2, 'pending')
ON CONFLICT (action_id) DO NOTHING
RETURNING id`, actionID, model).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return Result{OK: true, Detail: map[string]any{"idempotent": true, "model": model}}, nil
		}
		if err != nil {
			return Result{}, fmt.Errorf("retrain_model: %w", err)
		}
		return Result{OK: true, Detail: map[string]any{"request_id": id, "model": model, "status": "pending"}}, nil
	},

	// Generic low-cost informational log — the allow-list auto action.
	"log_event_note": func(ctx context.Context, ac Context) (Result, error) {
		kind := "info"
		if k, ok := ac.Params["kind"].(string); ok && k != "" {
			kind = k
		}
		entity := StringField(ac.Event.Payload, "prediction.entity_id")
		if entity == "" {
			entity = StringField(ac.Event.Payload, "anomaly.metric")
		}
		_, err := ac.Pool.Exec(ctx, `
INSERT INTO gold.event_log (kind, entity, detail)
VALUES ($1, $2, $3)`, kind, entity, fmt.Sprintf("event=%s payload=%v", ac.Event.Type, ac.Event.Payload))
		if err != nil {
			return Result{}, fmt.Errorf("log_event_note: %w", err)
		}
		return Result{OK: true, Detail: map[string]any{"kind": kind, "entity": entity}}, nil
	},
}

func insertRetention(ctx context.Context, ac Context, kind, status string) (Result, error) {
	payload, _ := ac.Event.Payload["prediction"].(map[string]any)
	cust, _ := payload["entity_id"].(string)
	if cust == "" {
		return Result{}, fmt.Errorf("retention action: no prediction.entity_id in event")
	}
	score, _ := numVal(payload["score"])
	msg := fmt.Sprintf("templated %s for customer %s (churn score %.3f)", kind, cust, score)
	var id int64
	err := ac.Pool.QueryRow(ctx, `
INSERT INTO gold.retention_actions (customer_id, churn_score, kind, status, message)
SELECT $1, $2, $3, $4, $5
WHERE NOT EXISTS (
    SELECT 1 FROM gold.retention_actions
    WHERE customer_id = $1 AND kind = $3 AND created_at > now() - interval '24 hours')
RETURNING id`, cust, score, kind, status, msg).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return Result{OK: true, Detail: map[string]any{"idempotent": true, "kind": kind, "customer_id": cust}}, nil
	}
	if err != nil {
		return Result{}, fmt.Errorf("insertRetention: %w", err)
	}
	return Result{OK: true, Detail: map[string]any{"id": id, "kind": kind, "customer_id": cust, "status": status}}, nil
}

func numVal(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case int32:
		return float64(t), true
	}
	return 0, false
}