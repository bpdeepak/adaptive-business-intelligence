-- Phase 4 demo scenario: simulate a critical drift finding to exercise the
-- governance loop end to end WITHOUT touching real production morphology.
--
--   list:                                                              \d
-- The injected row is a plain gold.model_drift row at a fresh computed_at,
-- marked `detail.injected = true` so the dashboard and docs stay honest.
-- The Go drift poller (ABI_DRIFT_POLL_EVERY, default 60s) picks it up as a new
-- (model, computed_at) run, emits drift_computed, and retrain-on-critical-drift
-- proposes `retrain_model` on the approval queue. A human then approves (worker
-- trains a candidate) or rejects (recorded reason, nothing executes).
--
-- Run:  Get-Content docs/phase4-demo.sql | docker exec -i abi-postgres psql -U abi -d abi
INSERT INTO gold.model_drift (model_name, model_version, computed_at, feature, psi, status, kind, detail)
SELECT 'churn_risk', model_version, now() - interval '20 seconds', 'days_between_orders', 0.413, 'critical', 'psi',
       '{"injected": true, "simulated_deterioration": "demo scenario to exercise the retrain governance loop"}'::jsonb
FROM gold.model_registry
WHERE model_name = 'churn_risk' AND status = 'active'
RETURNING id, model_name, status;