# ABI — Phase 1 dev targets (Linux/CI). On Windows prefer scripts/*.ps1.
.PHONY: infra-up infra-down data load dbt build-docs server producer app test integration-test smoke bootstrap smoke-check ml-sync ml-features train serve-models

infra-up:
	docker compose up -d --wait

infra-down:
	docker compose down

data:
	uv run python scripts/download_olist.py

load:
	uv run python loader/loader.py

dbt:
	uv run dbt build --project-dir dbt --profiles-dir dbt

build-docs:
	uv run dbt docs generate --project-dir dbt --profiles-dir dbt

# Primary Phase 1 binaries: the server hosts REST + SSE + gRPC + the realtime
# consumers; the producer replays history onto Redpanda.
server:
	cd api && go build -o bin/server ./cmd/server

producer:
	cd api && go build -o bin/producer ./cmd/producer

app: server producer

test:
	cd api && go test ./...

# Real-DB/real-broker integration tests (skip when the stack is down).
integration-test:
	cd api && go test -tags integration -v ./internal/store/ ./internal/realtime/

# Full Phase 1 pipeline in one shot (Linux/macOS/CI).
bootstrap:
	bash scripts/bootstrap.sh

smoke:
	powershell -File scripts/smoke_test.ps1

# Portable end-to-end smoke test (Linux/macOS/CI).
smoke-check:
	bash scripts/smoke_check.sh

# --- Phase 2: predictive layer (isolated `ml` uv dependency group) ---

# Install the ML stack (scikit-learn, lightgbm, xgboost, shap).
ml-sync:
	uv sync --group ml

# Build the two non-SQL feature stores (fraud orders, session corpus). The two
# SQL feature stores are materialized by `make dbt`.
ml-features:
	uv run --group ml python ml/build_fraud_features.py
	uv run --group ml python ml/replay_session_corpus.py

# Train + backtest all four model families, register versions, and write the
# serving manifest. Requires the gold feature tables (run `make dbt ml-features`).
train:
	uv run --group ml python ml/train_all.py

# Serve registered models over the stdlib scoring sidecar (POST /score).
serve-models:
	uv run --group ml python ml/serve.py

# --- Phase 3: agentic NL-BI (ml/agent) ---

# Serve the NL-BI agent sidecar (POST /query). mock = deterministic harness;
# live = Ollama/OpenAI-compatible LLM (ABI_LLM_BASE_URL / ABI_LLM_MODEL).
agent-mock:
	uv run --group ml python ml/agent/server.py --mode mock

agent-serve:
	uv run --group ml python ml/agent/server.py --mode live

# Regression gate: the 20-question eval against the deterministic fake DB.
agent-eval:
	uv run --group ml python -m ml.agent.eval_agent --db fake --llm mock

# Same questions against the dev Postgres (requires the stack + gold layer).
agent-eval-real:
	uv run --group ml python -m ml.agent.eval_agent --db real --llm mock

# Everything Python in this phase: ml unit tests + the deterministic eval gate.
agent-test:
	uv run --group ml python -m pytest ml/tests -q
	uv run --group ml python -m ml.agent.eval_agent --db fake --llm mock