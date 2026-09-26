#!/usr/bin/env sh
# One-shot data + model bootstrap for the deployed stack (runs in the Python image,
# `docker compose -f deploy/compose.prod.yml --profile bootstrap run --rm bootstrap`).
#   download Olist -> bronze (MinIO + Postgres) -> dbt build -> Python feature
#   stores -> train + register all models -> serving manifest.
# Idempotent by default: if active models are already registered it only refreshes
# dbt and the manifest (a fresh VM runs everything). FORCE_RETRAIN=1 retrains.
set -eu
cd /app

has_models() {
  python - <<'PY'
import sys, psycopg
from ml import common
try:
    with psycopg.connect(common.DATABASE_URL) as c:
        n = c.execute("select count(*) from gold.model_registry where status='active'").fetchone()[0]
except Exception:
    n = 0
sys.exit(0 if n >= 5 else 1)
PY
}

if [ "${FORCE_RETRAIN:-0}" != "1" ] && has_models; then
  echo "==> active models already registered: refreshing dbt + manifest only"
  dbt build --project-dir dbt --profiles-dir dbt
  (cd ml && python -c "import train_all; train_all.write_sidecar_manifest()")
  exit 0
fi

echo "==> downloading Olist (public mirror, size-verified)"
python scripts/download_olist.py
echo "==> loading bronze (MinIO + Postgres)"
python loader/loader.py
echo "==> dbt build (silver + gold + feature marts + tests)"
dbt build --project-dir dbt --profiles-dir dbt
echo "==> Python feature stores"
python ml/build_fraud_features.py
python ml/replay_session_corpus.py
echo "==> training + registering every model family, then the serving manifest"
python ml/train_all.py
echo "==> bootstrap complete"
