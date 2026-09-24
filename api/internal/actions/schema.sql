-- Phase 4 governance tables: the approval queue, the audit log, and the
-- simulated-effect tables.
--
-- SINGLE SOURCE OF TRUTH (same pattern as api/internal/predict/schema.sql).
-- Both sides load this exact file:
--   * Go     — api/internal/actions/schema.go embeds it (//go:embed schema.sql)
--   * Python — ml/common.py reads it (GOVERNANCE_SQL); the retrain worker and
--              its tests bootstrap/verify the tables from it.
-- Everything is idempotent (IF NOT EXISTS), so whichever side runs first creates
-- the tables. Edit HERE ONLY; nothing else in the repo may re-declare them.
--
-- Scope honesty: every Phase 4 action mutates ABI's own tables and is logged as
-- if executed; there is no real store behind them (ABI is a portfolio demo).

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.action_queue (
    id           bigserial PRIMARY KEY,
    action       text NOT NULL,
    entity       text,
    risk_tier    text NOT NULL,           -- 'auto' | 'approval_required'
    status       text NOT NULL DEFAULT 'pending', -- pending|approved|rejected|executed|failed
    params       jsonb NOT NULL DEFAULT '{}'::jsonb,
    trigger      jsonb NOT NULL DEFAULT '{}'::jsonb, -- evidence trail (event + rule + condition)
    rule         text NOT NULL,
    dedup_key    text NOT NULL,           -- playbook rule + trigger scope; UNIQUE below
    created_at   timestamptz NOT NULL DEFAULT now(),
    decided_at   timestamptz,
    executed_at  timestamptz,
    outcome      jsonb
);
ALTER TABLE gold.action_queue ADD COLUMN IF NOT EXISTS dedup_key text;
CREATE UNIQUE INDEX IF NOT EXISTS uq_action_queue_dedup ON gold.action_queue (dedup_key);
CREATE INDEX IF NOT EXISTS idx_action_queue_status ON gold.action_queue (status, created_at);

CREATE TABLE IF NOT EXISTS gold.action_audit_log (
    id          bigserial PRIMARY KEY,
    action_id   bigint NOT NULL REFERENCES gold.action_queue (id),
    transition  text NOT NULL,   -- proposed|auto_approved|approved|rejected|executed|failed|retrying|outcome
    actor       text NOT NULL DEFAULT 'system',
    reason      text,
    detail      jsonb NOT NULL DEFAULT '{}'::jsonb,
    at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_action_audit_action ON gold.action_audit_log (action_id, at);

-- The audit log is append-only, enforced by the database and not merely by the
-- application never issuing UPDATE/DELETE. Any UPDATE, DELETE or TRUNCATE is
-- refused unless the session has explicitly opted in with
--     SET LOCAL abi.audit_maintenance = 'on';
-- (inside a transaction). That switch exists for test cleanup and operator
-- maintenance and is visible in the statement log; it is NOT a security boundary
-- against a privileged database user, who could also drop the trigger — it stops
-- accidental and application-level mutation and makes deliberate mutation an
-- explicit act.
CREATE OR REPLACE FUNCTION gold.action_audit_log_append_only() RETURNS trigger AS $fn$
BEGIN
    IF current_setting('abi.audit_maintenance', true) = 'on' THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        IF TG_OP = 'UPDATE' THEN RETURN NEW; END IF;
        RETURN NULL;
    END IF;
    RAISE EXCEPTION 'gold.action_audit_log is append-only: % refused', TG_OP
        USING ERRCODE = 'restrict_violation';
END;
$fn$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_action_audit_log_append_only ON gold.action_audit_log;
CREATE TRIGGER trg_action_audit_log_append_only
    BEFORE UPDATE OR DELETE ON gold.action_audit_log
    FOR EACH ROW EXECUTE FUNCTION gold.action_audit_log_append_only();

DROP TRIGGER IF EXISTS trg_action_audit_log_no_truncate ON gold.action_audit_log;
CREATE TRIGGER trg_action_audit_log_no_truncate
    BEFORE TRUNCATE ON gold.action_audit_log
    FOR EACH STATEMENT EXECUTE FUNCTION gold.action_audit_log_append_only();

CREATE TABLE IF NOT EXISTS gold.order_flags (
    order_id        text PRIMARY KEY,
    held_for_review boolean NOT NULL DEFAULT true,
    reason          text,
    set_at          timestamptz NOT NULL DEFAULT now(),
    released_at     timestamptz
);

CREATE TABLE IF NOT EXISTS gold.purchase_orders (
    id            bigserial PRIMARY KEY,
    category      text NOT NULL,
    forecast_week text NOT NULL,
    quantity      int,
    status        text NOT NULL DEFAULT 'draft',
    reference     jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (category, forecast_week)   -- idempotency key: one draft PO per forecast week+category
);

CREATE TABLE IF NOT EXISTS gold.retention_actions (
    id            bigserial PRIMARY KEY,
    customer_id   text,
    churn_score   double precision,
    kind          text NOT NULL,         -- 'email' | 'offer'
    status        text NOT NULL DEFAULT 'logged', -- logged|proposed|rejected (nothing is ever actually sent)
    message       text,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold.retrain_requests (
    id           bigserial PRIMARY KEY,
    action_id    bigint NOT NULL UNIQUE,  -- one request per governance action
    model        text NOT NULL,
    status       text NOT NULL DEFAULT 'pending', -- pending|running|done|failed
    requested_at timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz,
    new_version  text,
    detail       jsonb NOT NULL DEFAULT '{}'::jsonb
);

-- Additive: when the worker took the request (lets a crashed run be recognised).
ALTER TABLE gold.retrain_requests ADD COLUMN IF NOT EXISTS started_at timestamptz;

CREATE TABLE IF NOT EXISTS gold.event_log (
    id         bigserial PRIMARY KEY,
    kind       text NOT NULL,
    entity     text,
    detail     text,
    created_at timestamptz NOT NULL DEFAULT now()
);
