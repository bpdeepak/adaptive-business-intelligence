#!/usr/bin/env bash
#
# Portable end-to-end smoke test (Linux/macOS/CI). Windows equivalent:
# scripts/smoke_test.ps1.
#
# Requires: Go, `uv run`d Python env, and the full pipeline already built
# (docker compose up, data downloaded, loader run, dbt build).
# Builds api/bin/api, starts it on a scratch port, asserts every endpoint
# returns the known-good Phase 0 numbers, then kills it.
#
# Usage:  bash scripts/smoke_check.sh   [BASE_URL] [HTTP_ADDR]
#         defaults: http://localhost:18085  :18085
set -euo pipefail

BASE_URL="${1:-http://localhost:18085}"
HTTP_ADDR="${2:-:18085}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export DATABASE_URL="${DATABASE_URL:-postgres://abi:abi@localhost:5432/abi}"
export ABI_HTTP_ADDR="${HTTP_ADDR}"

echo "==> Building API"
(
  cd "$ROOT/api"
  go build -o bin/api ./cmd/api
)

echo "==> Starting API at ${BASE_URL}"
"$ROOT/api/bin/api" &
API_PID=$!
cleanup() { kill "$API_PID" 2>/dev/null || true; wait "$API_PID" 2>/dev/null || true; }
trap cleanup EXIT

echo "==> Waiting for /healthz"
healthy=0
for _ in $(seq 1 30); do
  if curl -fsS --max-time 3 "$BASE_URL/healthz" >/dev/null 2>&1; then
    healthy=1
    break
  fi
  sleep 1
done
if [ "$healthy" -ne 1 ]; then
  echo "FAIL: API did not become healthy at $BASE_URL" >&2
  exit 1
fi
echo "  healthz ok"

# Assertions live in Python (stdlib urllib, no jq dependency).
python3 - "$BASE_URL" <<'PY'
import json
import urllib.request
import sys

base = sys.argv[1]

def get(path):
    with urllib.request.urlopen(f"{base}{path}", timeout=15) as resp:
        return json.load(resp)

def check(ok, msg):
    if not ok:
        print(f"FAIL: {msg}", file=sys.stderr)
        sys.exit(1)

health = get("/healthz")
check(health["data"]["status"] == "ok", "healthz not ok")

summary = get("/api/v1/summary")["data"]
check(summary["revenue"] > 0, "summary.revenue not positive")
check(summary["orders"] > 0, "summary.orders not positive")
check(abs(summary["revenue"] - 15739137.01) < 0.01, f"revenue {summary['revenue']} != 15739137.01")
check(summary["orders"] == 98206, f"orders {summary['orders']} != 98206")
check(abs(summary["aov"] - 160.27) < 0.01, f"aov {summary['aov']} != 160.27")
print(f"  summary ok  revenue={summary['revenue']:.2f} orders={summary['orders']} aov={summary['aov']:.2f} top={summary['top_category']}")

revenue = get("/api/v1/revenue/daily")["data"]
check(len(revenue) >= 2, "revenue/daily too few points")
check(len(revenue) == 613, f"revenue/daily points {len(revenue)} != 613")
print(f"  revenue/daily ok  {len(revenue)} points")

orders = get("/api/v1/orders/daily")["data"]
check(len(orders) >= 2, "orders/daily too few points")
check(len(orders) == 634, f"orders/daily points {len(orders)} != 634")
print(f"  orders/daily ok   {len(orders)} points")

cats = get("/api/v1/categories/top?metric=revenue&limit=5")["data"]
check(len(cats) > 0, "categories/top empty")
names = [c["category"] for c in cats]
check(names[0] == "bed_bath_table", f"top category {names[0]} != bed_bath_table")
print(f"  categories/top ok top5: {', '.join(names)}")

metrics = get("/api/v1/metrics")["data"]
check(len(metrics["metrics"]) >= 8, "metrics catalog too small")
check(any(m["name"] == "revenue" for m in metrics["metrics"]), "revenue metric missing")

print("\nSMOKE TEST PASSED")
PY