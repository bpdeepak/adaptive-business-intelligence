package actions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"abi/internal/events"
)

// ActionStatus values on gold.action_queue.
const (
	StatusPending  = "pending"
	StatusApproved = "approved"
	StatusRejected = "rejected"
	StatusExecuted = "executed"
	StatusFailed   = "failed"
)

// Risk tiers. Default posture: everything requires approval; 'auto' is an
// explicit, reviewable allow-list (a config decision, not a code path).
const (
	RiskAuto             = "auto"
	RiskApprovalRequired = "approval_required"
)

// Proposal is one playbook match that should become a pending queue row.
type Proposal struct {
	Action   string
	Entity   string
	RiskTier string
	Rule     string
	DedupKey string
	Params   map[string]any
	Trigger  map[string]any // evidence trail (event payload + condition)
}

// ActionRow is one queue row as the API and dashboard see it.
type ActionRow struct {
	ID         int64           `json:"id"`
	Action     string          `json:"action"`
	Entity     string          `json:"entity,omitempty"`
	RiskTier   string          `json:"risk_tier"`
	Status     string          `json:"status"`
	Rule       string          `json:"rule"`
	Trigger    json.RawMessage `json:"trigger,omitempty"`
	Outcome    json.RawMessage `json:"outcome,omitempty"`
	CreatedAt  string          `json:"created_at"`
	DecidedAt  *string         `json:"decided_at,omitempty"`
	ExecutedAt *string         `json:"executed_at,omitempty"`
}

// AuditRow is one immutable transition on an action's audit log.
type AuditRow struct {
	ID         int64           `json:"id"`
	ActionID   int64           `json:"action_id"`
	Transition string          `json:"transition"`
	Actor      string          `json:"actor"`
	Reason     string          `json:"reason,omitempty"`
	Detail     json.RawMessage `json:"detail,omitempty"`
	At         string          `json:"at"`
}

// Propose creates a pending queue row + its 'proposed' audit transition.
//
// A proposal is a no-op (id 0, nil error) when:
//   - its dedup key already exists — replayed events, retried proposals and
//     at-least-once redelivery cannot flood the queue; or
//   - an OPEN (pending) proposal already exists for the same rule + entity — a
//     re-scored or redelivered entity must not stack a second identical
//     question in front of the reviewer. Once a human decides the open one, a
//     later, genuinely new trigger on that entity proposes again (an entity
//     flagged, reviewed and released is never permanently vaccinated).
//
// The check and the insert run in one transaction under a per-(rule, entity)
// advisory lock, so the live bus and the reconciler cannot both slip through.
// The queue row and its 'proposed' audit row commit together.
func (s *Service) Propose(ctx context.Context, p Proposal) (int64, error) {
	if !s.Has(p.Action) {
		return 0, fmt.Errorf("%w: %s", ErrUnknownAction, p.Action)
	}
	if p.RiskTier == "" {
		p.RiskTier = RiskApprovalRequired
	}
	if p.RiskTier != RiskAuto && p.RiskTier != RiskApprovalRequired {
		return 0, fmt.Errorf("invalid risk_tier %q", p.RiskTier)
	}
	if p.RiskTier == RiskAuto && !AutoAllowed(p.Action) {
		return 0, fmt.Errorf("%w: %s", ErrAutoNotAllowed, p.Action)
	}
	params := p.Params
	if params == nil {
		params = map[string]any{}
	}
	trigger := p.Trigger
	if trigger == nil {
		trigger = map[string]any{}
	}
	paramsJSON, _ := json.Marshal(params)
	trigJSON, _ := json.Marshal(trigger)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin propose: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if p.Entity != "" {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`,
			"propose|"+p.Rule+"|"+p.Entity); err != nil {
			return 0, fmt.Errorf("lock proposal scope: %w", err)
		}
		var open bool
		if err := tx.QueryRow(ctx, `
SELECT EXISTS (SELECT 1 FROM gold.action_queue
               WHERE rule = $1 AND entity = $2 AND status = 'pending')`,
			p.Rule, p.Entity).Scan(&open); err != nil {
			return 0, fmt.Errorf("check open proposal: %w", err)
		}
		if open {
			s.log.Info("actions: proposal suppressed, an open one exists for this rule+entity",
				"rule", p.Rule, "entity", p.Entity, "dedup_key", p.DedupKey)
			return 0, nil
		}
	}

	var id int64
	err = tx.QueryRow(ctx, `
INSERT INTO gold.action_queue
    (action, entity, risk_tier, status, params, trigger, rule, dedup_key)
VALUES ($1, $2, $3, 'pending', $4::jsonb, $5::jsonb, $6, $7)
ON CONFLICT (dedup_key) DO NOTHING
RETURNING id`, p.Action, p.Entity, p.RiskTier, string(paramsJSON), string(trigJSON),
		p.Rule, p.DedupKey).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		// Duplicate trigger — nothing new to propose or audit.
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("insert action_queue: %w", err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO gold.action_audit_log (action_id, transition, actor, detail)
VALUES ($1, 'proposed', 'playbook', $2::jsonb)`, id, string(trigJSON)); err != nil {
		return 0, fmt.Errorf("audit proposed: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit proposal: %w", err)
	}
	return id, nil
}

// decide moves a pending action to `to` and records the matching audit
// transition in ONE transaction: the queue status and the authority row can never
// disagree (a status of 'approved' with no approval row could never execute and
// could not be re-decided).
func (s *Service) decide(ctx context.Context, id int64, to, reason, actor string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin %s: %w", to, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var status string
	err = tx.QueryRow(ctx, `
UPDATE gold.action_queue SET status = $2, decided_at = now()
WHERE id = $1 AND status = 'pending'
RETURNING status`, id, to).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: id %d", ErrInvalidState, id)
	}
	if err != nil {
		return fmt.Errorf("%s action: %w", to, err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO gold.action_audit_log (action_id, transition, actor, reason)
VALUES ($1, $2, $3, $4)`, id, to, actor, reason); err != nil {
		return fmt.Errorf("audit %s: %w", to, err)
	}
	return tx.Commit(ctx)
}

// Approve marks the action approved (with a human reason + actor) then executes
// it. The 'approved' audit row is the authority the executor guard requires.
func (s *Service) Approve(ctx context.Context, id int64, reason, actor string) error {
	if actor == "" {
		actor = "dashboard"
	}
	if err := s.decide(ctx, id, "approved", reason, actor); err != nil {
		return err
	}
	return s.execute(ctx, id, actor)
}

// Reject marks the action rejected; nothing executes.
func (s *Service) Reject(ctx context.Context, id int64, reason, actor string) error {
	if actor == "" {
		actor = "dashboard"
	}
	return s.decide(ctx, id, "rejected", reason, actor)
}

// AutoApproveAndExecute is the single allow-list path: an auto-tier proposal
// records an explicit 'auto_approved' transition (the code-level allow-list
// evidence) before executing — identical governance shape to a human approval.
func (s *Service) AutoApproveAndExecute(ctx context.Context, id int64) error {
	var riskTier, action, status string
	err := s.pool.QueryRow(ctx, `
SELECT risk_tier, action, status FROM gold.action_queue WHERE id = $1`, id).
		Scan(&riskTier, &action, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: id %d", ErrInvalidState, id)
	}
	if err != nil {
		return err
	}
	if riskTier != RiskAuto {
		return fmt.Errorf("%w: action %d is %s, not auto-tier", ErrUnauthorized, id, riskTier)
	}
	// The tier on the row was set by the proposing rule; the allow-list is the
	// independent authority. A row whose action is not on it never auto-executes.
	if !AutoAllowed(action) {
		return fmt.Errorf("%w: action %d (%s)", ErrAutoNotAllowed, id, action)
	}
	// Status is checked BEFORE the authority row is written: a decided or
	// executed row must not gain a misleading auto_approved transition.
	if status != StatusPending {
		return fmt.Errorf("%w: id %d status=%s", ErrInvalidState, id, status)
	}
	if _, err := s.pool.Exec(ctx, `
INSERT INTO gold.action_audit_log (action_id, transition, actor, reason, detail)
VALUES ($1, 'auto_approved', 'playbook-engine', 'allow-list auto tier', '"auto"'::jsonb)`, id); err != nil {
		return fmt.Errorf("audit auto_approved: %w", err)
	}
	return s.execute(ctx, id, "playbook-engine")
}

// Retry re-runs a failed execution. The original human 'approved' (or
// allow-list 'auto_approved') transition remains the authority — retrying a
// transient executor failure after a human already decided requires no new
// approval — and the retry adds a 'retrying' audit row so the trail shows the
// failure was noticed and re-attempted. Execute() still enforces the same
// governance invariant on every attempt.
//
// The failed -> approved re-arm and its audit row are one transaction and the
// re-arm is conditional on status = 'failed', so two concurrent retries cannot
// both proceed: the loser gets ErrInvalidState and the executor runs once.
func (s *Service) Retry(ctx context.Context, id int64, actor string) error {
	if actor == "" {
		actor = "system"
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin retry: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// 'approved' is the execution-authority state; the original approval
	// transition is still on the log, so the guard in execute() passes. A failed
	// retry lands back on 'failed' with the fresh error in the outcome.
	tag, err := tx.Exec(ctx, `
UPDATE gold.action_queue SET status = 'approved'
WHERE id = $1 AND status = 'failed'`, id)
	if err != nil {
		return fmt.Errorf("re-arm failed action: %w", err)
	}
	if tag.RowsAffected() == 0 {
		var status string
		err := tx.QueryRow(ctx, `SELECT status FROM gold.action_queue WHERE id = $1`, id).Scan(&status)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: id %d", ErrInvalidState, id)
		}
		return fmt.Errorf("%w: id %d status=%s (retry requires a failed execution)", ErrInvalidState, id, status)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO gold.action_audit_log (action_id, transition, actor, reason)
VALUES ($1, 'retrying', $2, 'user-initiated retry of a failed execution')`, id, actor); err != nil {
		return fmt.Errorf("audit retrying: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit retry: %w", err)
	}
	return s.execute(ctx, id, actor)
}

// execute runs one action's executor — but ONLY when the governance invariant
// holds: an approved or auto_approved transition already exists on the audit
// log for this action. This is the single most important guard in the phase:
// no action outside the allow-list ever executes without a prior approved row.
func (s *Service) execute(ctx context.Context, id int64, actor string) error {
	var action, riskTier, status, model, entity string
	var paramsJSON, trigJSON []byte
	err := s.pool.QueryRow(ctx, `
SELECT action, COALESCE(risk_tier,''), status, COALESCE(params->>'model',''),
       COALESCE(entity,''), params, trigger
FROM gold.action_queue WHERE id = $1`, id).
		Scan(&action, &riskTier, &status, &model, &entity, &paramsJSON, &trigJSON)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: id %d", ErrInvalidState, id)
	}
	if err != nil {
		return err
	}
	if status != StatusPending && status != StatusApproved {
		return fmt.Errorf("%w: id %d status=%s", ErrInvalidState, id, status)
	}
	_ = riskTier

	var authed bool
	if err := s.pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM gold.action_audit_log
    WHERE action_id = $1 AND transition IN ('approved', 'auto_approved')
)`, id).Scan(&authed); err != nil {
		return err
	}
	if !authed {
		s.log.Warn("actions: refused unauthorized execution",
			"action_id", id, "action", action, "status", status)
		return fmt.Errorf("%w: action_id %d", ErrUnauthorized, id)
	}

	fn, ok := s.reg[action]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownAction, action)
	}

	// The executor acts on the exact proposal a human (or the allow-list)
	// approved: the rule params + the triggering event both come from the
	// persisted queue row — never from live bus state.
	var ruleParams map[string]any
	if len(paramsJSON) > 0 {
		_ = json.Unmarshal(paramsJSON, &ruleParams)
	}
	params := map[string]any{}
	for k, v := range ruleParams {
		params[k] = v
	}
	params["__action_id"] = id
	params["model"] = model
	params["entity"] = entity
	event := EventFromTrigger(trigJSON)

	res, err := fn(ctx, Context{Pool: s.pool, Event: event, Params: params})
	detailJSON, _ := json.Marshal(map[string]any{"result": res, "error": errOrNil(err)})
	if err != nil {
		if rerr := s.recordOutcome(ctx, id, StatusFailed, actor, string(detailJSON)); rerr != nil {
			s.log.Error("actions: executor failed AND the failure could not be recorded",
				"action_id", id, "action", action, "exec_error", err, "record_error", rerr)
			return fmt.Errorf("execute %s: %w (and recording the failure failed: %v)", action, err, rerr)
		}
		return fmt.Errorf("execute %s: %w", action, err)
	}
	if rerr := s.recordOutcome(ctx, id, StatusExecuted, actor, string(detailJSON)); rerr != nil {
		// The effect is applied but its outcome is not on the record. Say so loudly
		// (log + error to the caller) rather than reporting success; the row stays
		// 'approved', visible in the queue, and the executor is idempotent by contract.
		s.log.Error("actions: executor succeeded but the outcome could not be recorded",
			"action_id", id, "action", action, "record_error", rerr)
		return fmt.Errorf("execute %s: effect applied but outcome not recorded: %w", action, rerr)
	}
	return nil
}

// recordOutcome writes the terminal status ('executed' | 'failed') and its audit
// row in one transaction, so the queue and the log cannot disagree about how an
// execution ended.
func (s *Service) recordOutcome(ctx context.Context, id int64, status, actor, detail string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
UPDATE gold.action_queue SET status = $2, executed_at = now(), outcome = $3::jsonb
WHERE id = $1`, id, status, detail); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO gold.action_audit_log (action_id, transition, actor, detail)
VALUES ($1, $2, $3, $4::jsonb)`, id, status, actor, detail); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func errOrNil(err error) any {
	if err == nil {
		return nil
	}
	return err.Error()
}

// EventFromTrigger reconstructs the triggering event from the persisted
// evidence captured at proposal time (payload under "payload", event type +
// time at the top level). Unknown/missing evidence degrades to an empty event
// rather than failing the whole execution.
func EventFromTrigger(trig []byte) events.Event {
	var raw struct {
		Type    string         `json:"event_type"`
		At      string         `json:"event_at"`
		Payload map[string]any `json:"payload"`
	}
	if len(trig) > 0 {
		_ = json.Unmarshal(trig, &raw)
	}
	e := events.Event{Type: events.Type(raw.Type), Payload: raw.Payload}
	if t, err := time.Parse(time.RFC3339, raw.At); err == nil {
		e.At = t
	}
	return e
}

// Stats is the governance backlog at a glance, for the operational gauges.
type Stats struct {
	Pending    int64 // proposals awaiting a human decision
	Failed     int64 // executions that failed and have not been retried successfully
	Approved   int64 // approved but not yet executed/failed (stuck rows show up here)
	Reconciled int64 // proposals created by the reconciliation backstop, not the live bus
}

// Stats counts queue rows by state in one query.
func (s *Service) Stats(ctx context.Context) (Stats, error) {
	var st Stats
	err := s.pool.QueryRow(ctx, `
SELECT count(*) FILTER (WHERE status = 'pending'),
       count(*) FILTER (WHERE status = 'failed'),
       count(*) FILTER (WHERE status = 'approved'),
       count(*) FILTER (WHERE trigger #>> '{payload,reconciled}' = 'true')
FROM gold.action_queue`).Scan(&st.Pending, &st.Failed, &st.Approved, &st.Reconciled)
	return st, err
}

// actionCols is the SELECT column list shared by List and Trace.
const actionCols = `
    id, action, COALESCE(entity,''), risk_tier, status, rule,
    trigger, outcome,
    to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
    CASE WHEN decided_at IS NULL THEN NULL ELSE
         to_char(decided_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END,
    CASE WHEN executed_at IS NULL THEN NULL ELSE
         to_char(executed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') END`

// actionRowScan folds one actionCols row into an ActionRow.
func actionRowScan(r *ActionRow, entity, trig, outcome []byte, decided, executed *string) {
	r.Entity = string(entity)
	r.Trigger = json.RawMessage(trig)
	r.Outcome = json.RawMessage(outcome)
	r.DecidedAt, r.ExecutedAt = decided, executed
}

// List returns queue rows filtered by status (default: everything, newest
// first), capped at limit.
func (s *Service) List(ctx context.Context, status string, limit int) ([]ActionRow, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := "SELECT" + actionCols + " FROM gold.action_queue"
	args := []any{}
	if status != "" {
		query += " WHERE status = $1"
		args = append(args, status)
	}
	args = append(args, limit)
	query += fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT $%d", len(args))
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list actions: %w", err)
	}
	defer rows.Close()

	out := []ActionRow{}
	for rows.Next() {
		var r ActionRow
		var entity, trig, outcome []byte
		var decided, executed *string
		if err := rows.Scan(&r.ID, &r.Action, &entity, &r.RiskTier, &r.Status, &r.Rule,
			&trig, &outcome, &r.CreatedAt, &decided, &executed); err != nil {
			return nil, fmt.Errorf("scan action row: %w", err)
		}
		actionRowScan(&r, entity, trig, outcome, decided, executed)
		out = append(out, r)
	}
	return out, rows.Err()
}

// Trace returns the full evidence trail of one action: the queue row plus
// every audit transition. This is the "show your work" surface (the same
// spirit as the Phase 3 agent's tool_trace).
func (s *Service) Trace(ctx context.Context, id int64) (ActionRow, []AuditRow, error) {
	var r ActionRow
	var entity, trig, outcome []byte
	var decided, executed *string
	err := s.pool.QueryRow(ctx, "SELECT"+actionCols+" FROM gold.action_queue WHERE id = $1", id).
		Scan(&r.ID, &r.Action, &entity, &r.RiskTier, &r.Status, &r.Rule,
			&trig, &outcome, &r.CreatedAt, &decided, &executed)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, nil, fmt.Errorf("action %d not found", id)
	}
	if err != nil {
		return r, nil, err
	}
	actionRowScan(&r, entity, trig, outcome, decided, executed)

	aRows, err := s.pool.Query(ctx, `
SELECT id, action_id, transition, actor, COALESCE(reason,''), detail,
       to_char(at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
FROM gold.action_audit_log WHERE action_id = $1 ORDER BY id`, id)
	if err != nil {
		return r, nil, err
	}
	defer aRows.Close()
	audits := []AuditRow{}
	for aRows.Next() {
		var a AuditRow
		var detail []byte
		if err := aRows.Scan(&a.ID, &a.ActionID, &a.Transition, &a.Actor, &a.Reason,
			&detail, &a.At); err != nil {
			return r, nil, err
		}
		a.Detail = json.RawMessage(detail)
		audits = append(audits, a)
	}
	return r, audits, aRows.Err()
}
