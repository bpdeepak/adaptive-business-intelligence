# ABI — Phase 0 dev targets (Linux/CI). On Windows prefer scripts/*.ps1.
.PHONY: infra-up infra-down data load dbt build-docs api test smoke

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

api:
	cd api && go build -o bin/api ./cmd/api

test:
	cd api && go test ./...

smoke:
	powershell -File scripts/smoke_test.ps1