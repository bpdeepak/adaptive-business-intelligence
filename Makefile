# ABI — Phase 1 dev targets (Linux/CI). On Windows prefer scripts/*.ps1.
.PHONY: infra-up infra-down data load dbt build-docs server producer app test integration-test smoke bootstrap smoke-check

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