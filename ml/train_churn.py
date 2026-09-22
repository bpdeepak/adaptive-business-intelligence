"""Train + backtest the customer-churn classifier (Phase 2).

Model: XGBoost over gold.feature_customer_churn. Churn definition: a repeat
customer (>= 2 orders) churned when they made no purchase in the 90 days
following their penultimate order; label read from the observed last order, so
no right-censoring.

Honesty notes (findings documented in docs/phase2.md):
- evaluation is a strict time split by as_of_date (latest 20% held out);
- the feature store applies a label-availability rule (rows whose 90-day
  window is not yet in-data are excluded) and exposes NO as_of_year — the
  earlier build let the model lean on the year as a proxy for the
  marketplace's falling base churn rate (67% → 41% → 14%), which is exactly
  the non-stationarity a time split must not let the model route around;
  as_of_month is kept as genuine seasonality;
- base churn rate is ~31% (the Olist cohort of repeat customers is small and
  matures fast), so absolute AUC (~0.55-0.60) and top-5% lift over the regime
  base rate are the reported, honest figures.

Explainability: global SHAP + per-row SHAP persisted with every test prediction
in gold.predictions, and an actionable "top-100 at-risk customers" export.
"""
from __future__ import annotations

import argparse
import sys

import numpy as np
import pandas as pd
from xgboost import XGBClassifier

import backtest
import common
import evalwrite
import explain

MODEL_NAME = "churn_risk"
TEST_FRAC = 0.2

FEATURES = [
    "n_prior_orders", "total_spend_prior", "aov_prior", "max_order_value_prior",
    "avg_items_prior", "avg_review_prior", "review_count_prior",
    "avg_delivery_delay_days_prior", "lost_share_prior", "distinct_days_prior",
    "orders_last_90d_prior", "spend_last_90d_prior", "categories_prior",
    "days_since_first_order", "days_since_prior_order", "as_of_month",
]


def load_data() -> pd.DataFrame:
    df = common.read_sql(
        """
        select customer_unique_id, as_of_date, churned,
               n_prior_orders, total_spend_prior, aov_prior, max_order_value_prior,
               avg_items_prior, avg_review_prior, review_count_prior,
               avg_delivery_delay_days_prior, lost_share_prior, distinct_days_prior,
               orders_last_90d_prior, spend_last_90d_prior, categories_prior,
               days_since_first_order, days_since_prior_order, as_of_month
        from gold.feature_customer_churn
        """
    )
    for col in FEATURES + ["churned"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    df = df.fillna(0)
    return df


def make_model(train: pd.DataFrame) -> XGBClassifier:
    neg = int((train["churned"] == 0).sum())
    pos = int((train["churned"] == 1).sum())
    spw = max(1.0, neg / max(pos, 1))
    m = XGBClassifier(
        n_estimators=300,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=spw,
        random_state=42,
        eval_metric="auc",
        tree_method="hist",
    )
    m.fit(train[FEATURES], train["churned"])
    return m


def main() -> int:
    ap = argparse.ArgumentParser(description="Train the customer-churn classifier")
    ap.add_argument("--skip-persist", action="store_true")
    args = ap.parse_args()

    df = load_data()
    print(f"rows: {len(df):,} repeat customers, churn rate {df['churned'].mean():.3%}")

    train, test = backtest.time_split(df, time_col="as_of_date", test_frac=TEST_FRAC)
    print(f"time split by as_of_date: train {len(train):,} (until {train['as_of_date'].max().date()}), "
          f"test {len(test):,} (from {test['as_of_date'].min().date()})")

    model = make_model(train)
    metrics = evalwrite.evaluate_classifier(model, test[FEATURES], test["churned"],
                                            train_positive_rate=float(train["churned"].mean()))
    # Operating decision rule for Phase 4: churn outreach should act at the
    # recorded recommended_threshold (max recall at >= 85 % precision), never
    # at an undocumented 0.5.
    metrics["recommended_threshold"] = backtest.recommended_threshold(
        test["churned"].to_numpy(), model.predict_proba(test[FEATURES])[:, 1]
    )
    print("  metrics:", {k: round(v, 4) for k, v in metrics.items()})

    if args.skip_persist:
        return 0

    version = common.now_tag()
    artifact = common.save_artifact(
        MODEL_NAME, version, model,
        {"features": FEATURES, "metrics": {k: round(v, 4) for k, v in metrics.items()}},
    )
    train_positive = float(train["churned"].mean())
    importance = explain.global_importance(model, test[FEATURES])
    top = {k: round(v, 6) for k, v in list(importance.items())[:10]}
    shap_png = explain.save_global_shap_plot(model, test[FEATURES].sample(n=min(4000, len(test)), random_state=42),
                                             f"{common.ARTIFACTS_DIR}/{MODEL_NAME}-{version}-shap.png")

    common.register_model(
        MODEL_NAME, version,
        framework="xgboost", task="binary_classification", grain="customer",
        artifact_path=str(artifact),
        params={"n_estimators": 300, "max_depth": 5, "learning_rate": 0.05,
                "scale_pos_weight": "train-balanced", "churn_window_days": 90,
                "cohort": "repeat_customers", "as_of_year": False},
        metrics={**{k: round(v, 4) for k, v in metrics.items()},
                 "shap_top_features": top, "shap_plot": str(shap_png) if shap_png else None},
        features=FEATURES,
        trained_on={"n_train": int(len(train)), "n_test": int(len(test)),
                    "churn_rate_train": round(train_positive, 4),
                    "churn_rate_test": round(float(test["churned"].mean()), 4),
                    "label_availability": "as_of <= data_end - 90d"},
        trained_window={"start": str(train["as_of_date"].min().date()),
                        "end": str(test["as_of_date"].max().date())},
    )

    probs = model.predict_proba(test[FEATURES])[:, 1]
    written = evalwrite.persist_classifier_predictions(
        model, MODEL_NAME, version, "customer",
        test[FEATURES], test["churned"], test["customer_unique_id"].to_numpy(),
        max_rows=5000, extra_meta={"churn_window_days": 90, "cohort": "repeat_customers"},
    )
    print(f"  wrote {written:,} test predictions to gold.predictions")

    # actionable export: the 100 most at-risk customers from the test window
    out = test[["customer_unique_id", "as_of_date"]].copy()
    out["churn_risk"] = probs
    out = out.sort_values("churn_risk", ascending=False).head(100)
    risk_path = common.ARTIFACTS_DIR / f"{MODEL_NAME}-{version}-top100-risk.csv"
    out.to_csv(risk_path, index=False)
    print(f"  top-100 risk list -> {risk_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())