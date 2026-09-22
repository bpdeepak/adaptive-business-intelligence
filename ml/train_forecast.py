"""Train + backtest the per-category weekly demand forecast (Phase 2).

Two LightGBM regressors — revenue and order count per (category, ISO week) —
trained on gold.feature_forecast_weekly with a per-category expanding-origin
backtest over the last quarter of weeks (horizon = 4 weeks, walk-forward).

Honesty notes:
- lag/rolling features are zero-filled on the dense spine, so they are true
  time-lags; backtest folds score only weeks strictly after their training cut.
- category enters as its revenue-rank code (numeric), so SHAP stays exact and
  the trees still get a meaningful category ordering.
"""
from __future__ import annotations

import argparse
import sys

import numpy as np
import pandas as pd
from lightgbm import LGBMRegressor

import backtest
import common
import explain

MODEL_REVENUE = "forecast_category_weekly_revenue"
MODEL_ORDERS = "forecast_category_weekly_orders"
HORIZON = 4
MIN_TRAIN_WEEKS = 16
TEST_FRAC = 0.25

FEATURES = [
    "category_code",
    "week_of_year",
    "year",
    "avg_order_value",
    "revenue_lag1",
    "revenue_lag2",
    "revenue_lag4",
    "revenue_lag8",
    "orders_lag1",
    "orders_lag2",
    "orders_lag4",
    "orders_lag8",
    "revenue_roll4_mean",
    "revenue_roll4_std",
    "orders_roll4_mean",
    "has_prior_week",
]


def load_data() -> tuple[pd.DataFrame, dict]:
    df = common.read_sql(
        """
        select category, week_start, revenue, orders, avg_order_value,
               year, week_of_year, revenue_lag1, revenue_lag2, revenue_lag4,
               revenue_lag8, orders_lag1, orders_lag2, orders_lag4, orders_lag8,
               revenue_roll4_mean, revenue_roll4_std, orders_roll4_mean,
               series_weeks, has_prior_week
        from gold.feature_forecast_weekly
        """
    )
    # keep series with enough history AND enough actual nonzero demand
    presence = df.groupby("category")["revenue"].apply(lambda s: (s > 0).sum())
    keep = presence[presence >= 16].index
    df = df[df["category"].isin(keep)].copy()

    revenue_by_cat = df.groupby("category")["revenue"].sum().sort_values(ascending=False)
    rank = {c: i for i, c in enumerate(revenue_by_cat.index)}
    df["category_code"] = df["category"].map(rank)

    numeric = FEATURES + ["revenue", "orders"]
    for col in numeric:
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["has_prior_week"] = df["has_prior_week"].astype(int)
    return df, rank


def rolling_origin_records(df: pd.DataFrame, target: str) -> pd.DataFrame:
    """Per-category expanding-origin backtest over the last TEST_FRAC of weeks."""
    records: list[dict] = []
    for cat, g in df.groupby("category", sort=False):
        g = g.sort_values("week_start").reset_index(drop=True)
        cutoff = int(len(g) * (1 - TEST_FRAC))
        n = len(g)
        start = cutoff
        fold = 0
        while start + HORIZON <= n:
            train = g.iloc[:start]
            test = g.iloc[start : start + HORIZON]
            m = LGBMRegressor(
                n_estimators=300,
                learning_rate=0.05,
                num_leaves=31,
                min_child_samples=30,
                subsample=0.8,
                colsample_bytree=0.9,
                random_state=42,
                verbose=-1,
            )
            m.fit(train[FEATURES], train[target])
            preds = m.predict(test[FEATURES])
            for i, row in test.iterrows():
                records.append(
                    {
                        "category": cat,
                        "week_start": row["week_start"],
                        "actual": float(row[target]),
                        "prediction": float(preds[test.index.get_loc(i)]),
                        "fold": fold,
                    }
                )
            start += HORIZON
            fold += 1
    return pd.DataFrame(records)


def train_final(df: pd.DataFrame, target: str) -> LGBMRegressor:
    m = LGBMRegressor(
        n_estimators=500,
        learning_rate=0.05,
        num_leaves=31,
        min_child_samples=30,
        subsample=0.8,
        colsample_bytree=0.9,
        random_state=42,
        verbose=-1,
    )
    m.fit(df[FEATURES], df[target])
    return m


def persist_backtest_predictions(records: pd.DataFrame, model_name: str, version: str) -> int:
    """Write the last fold's backtest predictions into gold.predictions so the
    API can show a forecast-vs-actual trail with explanations."""
    last_fold = records[records["fold"] == records["fold"].max()]
    if len(last_fold) > 2000:
        last_fold = last_fold.sample(n=2000, random_state=42)
    rows = [
        {
            "entity_id": f"{r['category']}@{r['week_start'].isoformat()}",
            "prediction": r["prediction"],
            "confidence": 0.0,
            "lower_bound": None,
            "upper_bound": None,
            "explanation": {},
            "metadata": {"horizon_weeks": HORIZON, "actual": r["actual"], "fold": int(r["fold"])},
        }
        for _, r in last_fold.iterrows()
    ]
    return common.write_predictions(model_name, version, "category_week", rows, batch_tag="backtest")


def main() -> int:
    ap = argparse.ArgumentParser(description="Train + backtest the category-week forecast")
    ap.add_argument("--skip-persist", action="store_true", help="backtest only, no registry/predictions writes")
    args = ap.parse_args()

    df, _rank = load_data()
    print(f"feature rows: {len(df):,} across {df['category'].nunique()} categories "
          f"[{df['week_start'].min()} .. {df['week_start'].max()}]")

    version = common.now_tag()
    for target, model_name in (( "revenue","forecast_category_weekly_revenue"), ("orders", "forecast_category_weekly_orders")):
        print(f"\n=== {model_name} (target={target}) ===")
        recs = rolling_origin_records(df, target)
        metrics = backtest.forecast_metrics(recs)
        print("  backtest:", {k: round(v, 4) for k, v in metrics.items()})
        print(f"  folds across all categories: {recs['fold'].nunique()}, test rows: {len(recs):,}")

        if args.skip_persist:
            continue

        model = train_final(df, target)
        artifact = common.save_artifact(
            model_name, version, model,
            {"target": target, "features": FEATURES,
             "backtest": {k: round(v, 4) for k, v in metrics.items()}},
        )
        # Global SHAP: which features drive this target most?
        sample = df.sample(n=min(3000, len(df)), random_state=42)
        importance = explain.global_importance(model, sample[FEATURES])
        top = {k: round(v, 6) for k, v in list(importance.items())[:10]}
        shap_png = explain.save_global_shap_plot(
            model, sample[FEATURES],
            f"{common.ARTIFACTS_DIR}/{model_name}-{version}-shap.png",
        )
        print("  top SHAP features:", top)

        trained_span = f"{df['week_start'].min()}..{df['week_start'].max()}"
        common.register_model(
            model_name, version,
            framework="lightgbm", task="regression", grain="category_week",
            artifact_path=str(artifact), params={"n_estimators": 500, "learning_rate": 0.05,
                                                  "horizon_weeks": HORIZON},
            metrics={**{k: round(v, 4) for k, v in metrics.items()},
                     "shap_top_features": top,
                     "shap_plot": (str(shap_png) if shap_png else None)},
            features=FEATURES,
            trained_on={"target": target, "n_rows": int(len(df)), "n_categories": int(df["category"].nunique())},
            trained_window={"start": str(df["week_start"].min()), "end": str(df["week_start"].max())},
        )
        written = persist_backtest_predictions(recs, model_name, version)
        print(f"  wrote {written:,} backtest predictions to gold.predictions")
    return 0


if __name__ == "__main__":
    sys.exit(main())