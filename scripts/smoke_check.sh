#!/usr/bin/env bash
#
# End-to-end smoke test for the Phase 1 realtime pipeline (Linux/macOS/CI).
# Windows equivalent: scripts/smoke_test.ps1.
#
# Requires: Go, the full pipeline already built and running
#   (docker compose up -d --wait, data downloaded, loader run, dbt build).
#
# Builds api/bin/server + api/bin/producer, starts them on scratch ports, then
# asserts:
#   - the Phase 0 batch endpoints return the known-good numbers
#   - the metrics catalog carries the Phase 1 realtime definitions (v1.1.0+)
#   - the live SSE stream emits metrics frames
#   - the realtime REST surface / the anomalies endpoint answer
#
# Usage:  bash scripts/smoke_check.sh   [BASE_URL] [HTTP_ADDR] [GRPC_ADDR]
#         defaults: http://localhost:18085  :18085  :18090
set -euo pipefail

BASE_URL="${1:-http://localhost:18085}"
HTTP_ADDR="${2:-:18085}"
GRPC_ADDR="${3:-:18090}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export DATABASE_URL="${DATABASE_URL:-postgres://abi:abi@localhost:5432/abi}"
export ABI_HTTP_ADDR="${HTTP_ADDR}"
export ABI_GRPC_ADDR="${GRPC_ADDR}"
export ABI_KAFKA_SEED_BROKERS="${ABI_KAFKA_SEED_BROKERS:-localhost:29092}"

echo "==> Building server + producer"
(
  cd "$ROOT/api"
  go build -o bin/server ./cmd/server
  go build -o bin/producer ./cmd/producer
)

echo "==> Starting server at ${BASE_URL}"
"$ROOT/api/bin/server" &
SERVER_PID=$!

echo "==> Starting producer (28800x -> 1 historical day ≈ 3s)"
ABI_SPEED_MULTIPLIER=28800 "$ROOT/api/bin/producer" &
PRODUCER_PID=$!

cleanup() {
  kill "$PRODUCER_PID" 2>/dev/null || true
  kill "$SERVER_PID" 2>/dev/null || true
  wait "$PRODUCER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "==> Waiting for /healthz"
healthy=0
for _ in $(seq 1 40); do
  if curl -fsS --max-time 3 "$BASE_URL/healthz" >/dev/null 2>&1; then
    healthy=1
    break
  fi
  sleep 1
done
if [ "$healthy" -ne 1 ]; then
  echo "FAIL: server did not become healthy at $BASE_URL" >&2
  exit 1
fi
echo "  healthz ok"

echo "==> Waiting for the first live 1-minute bucket"
buckets=0
for _ in $(seq 1 45); do
  if bucket_count="$(
    curl -fsS --max-time 3 "$BASE_URL/api/v1/realtime/metrics" 2>/dev/null \
      | python3 -c "import json,sys; d=json.load(sys.stdin)['data']; print(len(d.get('buckets', [])))" 2>/dev/null
  )" && [ "${bucket_count:-0}" -ge 1 ]; then
    buckets="${bucket_count}"
    break
  fi
  sleep 1
done
if [ "${buckets}" -lt 1 ]; then
  echo "FAIL: no realtime buckets appeared ($buckets)" >&2
  exit 1
fi
echo "  realtime buckets ok (${buckets} buckets)"

echo "==> Waiting for an SSE frame on /api/v1/stream/metrics"
# `head` closes the pipe after 400 bytes, which SIGPIPEs curl (exit 23); under
# `set -o pipefail` that aborted the script whenever frames arrived quickly. The
# assertion below is on the captured bytes, so the pipeline status is irrelevant.
SSE_OUT="$(curl -sN --max-time 12 "$BASE_URL/api/v1/stream/metrics" | head -c 400 || true)"
if ! printf '%s' "$SSE_OUT" | grep -q "event: metrics"; then
  echo "FAIL: no SSE metrics frame" >&2
  exit 1
fi
echo "  sse ok"

# Assertions live in Python (stdlib urllib, no jq dependency).
python3 - "$BASE_URL" <<'PY'
import json
import urllib.error
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
check(len(metrics["metrics"]) >= 11, "metrics catalog too small")
realnames = {m["name"] for m in metrics["metrics"]}
check({"revenue", "orders", "revenue_realtime", "orders_realtime", "active_sessions"} <= realnames,
      f"realtime metric definitions missing; have {sorted(realnames)}")
print(f"  metrics ok  {len(metrics['metrics'])} definitions (v{metrics.get('version')})")

rt = get("/api/v1/realtime/metrics")["data"]["buckets"]
check(len(rt) >= 1, "realtime metrics empty")
check("bucket_start" in rt[-1] and "revenue" in rt[-1] and "orders" in rt[-1], "bucket shape wrong")
print(f"  realtime metrics ok  latest bucket {rt[-1]['bucket_start']} revenue={rt[-1]['revenue']} orders={rt[-1]['orders']}")

anoms = get("/api/v1/anomalies")["data"]
assert isinstance(anoms, list), "anomalies must be a list"
print(f"  anomalies ok  {len(anoms)} open")

# --- Phase 2: predictive layer surface ---
# CI does not run `make train` and starts no sidecar, so the read endpoints
# must answer with well-formed (possibly empty) payloads and scoring must
# degrade to a clean 502/503 instead of hanging or 500ing.
reg = get("/api/v1/model-registry")["data"]
check(isinstance(reg, list), "model-registry data not a list")
print(f"  model-registry ok  {len(reg)} versions")

preds = get("/api/v1/predictions?limit=5")["data"]
check(isinstance(preds, list), "predictions data not a list")
latest = get("/api/v1/predictions/latest?limit=3")["data"]
check(isinstance(latest, list), "predictions/latest data not a list")
print(f"  predictions ok  {len(preds)} rows, latest {len(latest)}")

health = get("/api/v1/models/health")["data"]
check("active_models" in health and "sidecar" in health,
      f"models/health shape wrong: {health}")
print(f"  models/health ok  active={health['active_models']} sidecar={health['sidecar']}")

body = json.dumps({"model": "smoke-unknown", "features": {"x": 1.0}}).encode()
req = urllib.request.Request(
    f"{base}/api/v1/score", data=body,
    headers={"Content-Type": "application/json"})
try:
    with urllib.request.urlopen(req, timeout=10) as resp:
        code = resp.status
except urllib.error.HTTPError as e:  # graceful degrade is the expected path
    code = e.code
check(code in (502, 503), f"score without sidecar should return 502/503, got {code}")
print(f"  score degrades ok  {code} without sidecar")

# --- Phase 3/4 surfaces (the smoke test previously covered none of them) ---
# The governance endpoints are wired unconditionally in cmd/server, so they must
# answer with well-formed payloads even on an empty queue.
actions_list = get("/api/v1/actions?limit=5")["data"]
check(isinstance(actions_list, list), "actions data not a list")
print(f"  actions ok  {len(actions_list)} rows")

drift = get("/api/v1/model-drift?limit=5")["data"]
check(isinstance(drift, list), "model-drift data not a list")
print(f"  model-drift ok  {len(drift)} rows")

# Validation, not state: approving without a reason must be refused (400), and an
# unknown id must not 500.
def post(path, payload):
    req = urllib.request.Request(f"{base}{path}", data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code

code = post("/api/v1/actions/1/approve", {"actor": "smoke"})
check(code == 400, f"approve without a reason should be 400, got {code}")
code = post("/api/v1/actions/999999999/reject", {"reason": "smoke", "actor": "smoke"})
check(code == 409, f"deciding a missing action should be 409, got {code}")
print("  actions validation ok  reason required (400), missing action (409)")

# The agent sidecar is not started in CI: its health/query endpoints must degrade
# to a clean 503/502 rather than hang or 500.
try:
    with urllib.request.urlopen(f"{base}/api/v1/agent/health", timeout=10) as resp:
        agent_code = resp.status
except urllib.error.HTTPError as e:
    agent_code = e.code
check(agent_code in (200, 502, 503), f"agent/health should be 200/502/503, got {agent_code}")
print(f"  agent/health ok  {agent_code}")

print("\nSMOKE TEST PASSED")
PY

echo "==> Stopping server + producer"