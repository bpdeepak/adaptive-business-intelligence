"""Phase 2 model factory: build feature tables, train + backtest all four
model families, register them in gold.model_registry, and write the serving
manifest consumed by ml/serve.py and Go.

Pipeline order: dbt materializes the two SQL feature stores
(gold.feature_forecast_weekly, gold.feature_customer_churn), then:

    uv run dbt build --project-dir dbt --profiles-dir dbt          # SQL features
    uv run python ml/build_fraud_features.py                       # fraud features
    uv run python ml/replay_session_corpus.py                      # session features
    uv run python ml/train_all.py                                  # this file

Use `--models forecast churn fraud bot` to pick families, `--skip-features`
if the feature tables already exist, and `--dry-run` to show what would run.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys

import psycopg.rows

import common


def run(cmd: list[str]) -> None:
    print(f"\n$ {' '.join(cmd)}")
    subprocess.run([sys.executable, *cmd], check=True)


def write_sidecar_manifest() -> None:
    """Write artifacts/sidecar_models.json for the serving sidecar + Go API."""
    with common.conn() as c:
        cur = c.cursor(row_factory=psycopg.rows.dict_row)
        cur.execute(
            """
            select model_name, model_version, status, task, grain,
                   artifact_path, features, metrics
            from gold.model_registry
            where status = 'active'
            order by model_name, model_version
            """
        )
        rows = cur.fetchall()
    models = []
    for r in rows:
        metrics = r["metrics"] or {}
        entry = {
            "name": r["model_name"],
            "version": r["model_version"],
            "task": r["task"],
            "grain": r["grain"],
            "artifact_path": r["artifact_path"],
            "features": r["features"] or [],
        }
        if r["task"] == "regression":
            entry["baseline_wmape"] = round(float(metrics.get("wmape", 0.10)), 4)
            by_code = metrics.get("wmape_by_category_code") or {}
            entry["baseline_wmape_by_code"] = {str(k): round(float(v), 4) for k, v in by_code.items()}
        elif metrics.get("recommended_threshold") is not None:
            entry["recommended_threshold"] = float(metrics["recommended_threshold"])
        models.append(entry)
    manifest = {"generated_at": common.now_tag(), "models": models}
    common.ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    path = common.ARTIFACTS_DIR / "sidecar_models.json"
    path.write_text(json.dumps(manifest, indent=2))
    print(f"serving manifest -> {path} ({len(models)} models)")


def main() -> int:
    ap = argparse.ArgumentParser(description="Train all Phase 2 model families")
    ap.add_argument("--models", nargs="*", default=["forecast", "churn", "fraud", "bot"],
                    help="families: forecast churn fraud bot")
    ap.add_argument("--skip-features", action="store_true",
                    help="assume feature tables already exist (skip builders)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    wanted = set(args.models)
    steps = []
    if not args.skip_features:
        steps.append(["ml/build_fraud_features.py"])
        steps.append(["ml/replay_session_corpus.py"])
    if "forecast" in wanted:
        steps.append(["ml/train_forecast.py"])
    if "churn" in wanted:
        steps.append(["ml/train_churn.py"])
    if "fraud" in wanted:
        steps.append(["ml/train_fraud.py"])
    if "bot" in wanted:
        steps.append(["ml/train_bot.py"])

    for step in steps:
        if args.dry_run:
            print("would run:", " ".join(step))
        else:
            run(step)

    if not args.dry_run and not args.skip_features:
        pass
    if not args.dry_run:
        write_sidecar_manifest()
    print("\nPhase 2 training complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())