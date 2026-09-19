#!/usr/bin/env bash
#
# Phase 0 bootstrap (Linux/macOS/CI). Windows equivalent: scripts/bootstrap.ps1.
# Runs: infra up -> download data -> load bronze -> dbt build -> dbt docs -> build API.
#
# Usage:  bash scripts/bootstrap.sh  [-s infra|data|load]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SKIP_INFRA=0
SKIP_DATA=0
SKIP_LOAD=0
while getopts "s:" opt; do
  case "$opt" in
    s) case "$OPTARG" in
         infra) SKIP_INFRA=1 ;;
         data)  SKIP_DATA=1 ;;
         load)  SKIP_LOAD=1 ;;
         *) echo "unknown skip target: $OPTARG" >&2; exit 1 ;;
       esac ;;
    *) echo "usage: $0 [-s infra|data|load]" >&2; exit 1 ;;
  esac
done

if [ "$SKIP_INFRA" -ne 1 ]; then
  echo "==> Starting infrastructure (Postgres, MinIO, Redpanda)"
  docker compose up -d --wait
fi

if [ "$SKIP_DATA" -ne 1 ]; then
  echo "==> Downloading Olist dataset (public mirror)"
  uv run python scripts/download_olist.py
fi

if [ "$SKIP_LOAD" -ne 1 ]; then
  echo "==> Loading raw data into MinIO + Postgres bronze"
  uv run python loader/loader.py
fi

echo "==> Running dbt (silver + gold models + tests)"
uv run dbt build --project-dir dbt --profiles-dir dbt

echo "==> Generating dbt docs"
uv run dbt docs generate --project-dir dbt --profiles-dir dbt

echo "==> Building Go API"
(
  cd api
  go build -o bin/api ./cmd/api
)

echo ""
echo "Phase 0 pipeline ready."
echo "  Dashboard/API : (cd api && go run ./cmd/api) -> http://localhost:8080"
echo "  Smoke test    : bash scripts/smoke_check.sh"
echo "  MinIO console : http://localhost:9001  (minioadmin / minioadmin)"
echo "  dbt docs      : dbt/target/index.html"