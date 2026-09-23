"""Train + backtest the transaction fraud-risk classifier (Phase 2).

Model: XGBoost over gold.feature_fraud_orders (synthetically-injected labels).
Time split by order timestamp (latest 20%). Feature encoding keeps every
column numeric (one-hot payment type, revenue-ranked category code) so SHAP
explanations are exact and serving is trivial.

Label patterns (see ml/build_fraud_features.py): velocity >=3 orders/24h,
price >= 8x trailing-90d category average, excessive installments on a
high-value order, plus a 0.4% noise floor. The model must rediscover these
from features, which the SHAP ranking will demonstrate.
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

MODEL_NAME = "fraud_risk"
TEST_FRAC = 0.2

BASE_FEATURES = [
    "order_value", "item_count", "freight_share", "payment_count",
    "installments_max", "categories_count", "price_vs_benchmark",
    "velocity_24h", "account_age_days", "order_hour", "is_weekend", "is_lost",
]


def load_data() -> tuple[pd.DataFrame, list[str]]:
    df = common.read_sql(
        """
        select order_id, customer_unique_id, category_primary, categories_count,
               order_purchase_timestamp, order_value, item_count, freight_share,
               payment_type_primary, payment_count, installments_max,
               price_vs_benchmark, velocity_24h, account_age_days, order_hour,
               is_weekend, is_lost, is_fraud
        from gold.feature_fraud_orders
        """
    )
    for col in BASE_FEATURES + ["is_fraud"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    pay = pd.get_dummies(df["payment_type_primary"], prefix="pay").astype(int)
    pay_cols = list(pay.columns)
    df = pd.concat([df.drop(columns=["payment_type_primary"]), pay], axis=1)

    # category as its revenue-rank code keeps the feature space small and
    # gives trees a meaningful ordering (mirrors the forecast model).
    rank = common.category_rank_from(df, "category_primary", "order_value")
    df["category_code"] = df["category_primary"].map(rank).fillna(len(rank)).astype(int)
    df = df.drop(columns=["category_primary"])

    features = BASE_FEATURES + pay_cols + ["category_code"]
    return df, features, rank


def make_model(train: pd.DataFrame, features: list[str]) -> XGBClassifier:
    neg = int((train["is_fraud"] == 0).sum())
    pos = int((train["is_fraud"] == 1).sum())
    spw = max(1.0, neg / max(pos, 1))
    m = XGBClassifier(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=spw,
        random_state=42,
        eval_metric="auc",
        tree_method="hist",
    )
    m.fit(train[features], train["is_fraud"])
    return m


def main() -> int:
    ap = argparse.ArgumentParser(description="Train the transaction fraud-risk classifier")
    ap.add_argument("--skip-persist", action="store_true")
    args = ap.parse_args()

    df, features, rank = load_data()
    print(f"rows: {len(df):,} orders, fraud rate {df['is_fraud'].mean():.3%}")

    train, test = backtest.time_split(df, time_col="order_purchase_timestamp", test_frac=TEST_FRAC)
    print(f"time split: train {len(train):,} (until {train['order_purchase_timestamp'].max().date()}), "
          f"test {len(test):,} (from {test['order_purchase_timestamp'].min().date()})")

    model = make_model(train, features)
    metrics = evalwrite.evaluate_classifier(model, test[features], test["is_fraud"],
                                            train_positive_rate=float(train["is_fraud"].mean()))
    test_probs = model.predict_proba(test[features])[:, 1]
    # Record the OPERATING THRESHOLD for Phase 4's hold-for-review playbook:
    # the most permissive cutoff whose precision clears 85 %. Never the naive
    # 0.5 — at fraud's ~1 % base rate, 0.5 flags more false positives than real.
    metrics["recommended_threshold"] = backtest.recommended_threshold(
        test["is_fraud"].to_numpy(), test_probs
    )
    print("  metrics:", {k: round(v, 4) for k, v in metrics.items()})

    if args.skip_persist:
        return 0

    version = common.now_tag()
    artifact = common.save_artifact(
        MODEL_NAME, version, model,
        {"features": features, "metrics": {k: round(v, 4) for k, v in metrics.items()}},
    )
    importance = explain.global_importance(model, test[features])
    top = {k: round(v, 6) for k, v in list(importance.items())[:10]}
    shap_png = explain.save_global_shap_plot(model, test[features].sample(n=min(4000, len(test)), random_state=42),
                                             f"{common.ARTIFACTS_DIR}/{MODEL_NAME}-{version}-shap.png")

    common.register_model(
        MODEL_NAME, version,
        framework="xgboost", task="binary_classification", grain="order",
        artifact_path=str(artifact),
        params={"n_estimators": 300, "max_depth": 6, "learning_rate": 0.05,
                "scale_pos_weight": "train-balanced", "synthetic_labels": True},
        metrics={**{k: round(v, 4) for k, v in metrics.items()},
                 "category_rank": rank,
                 "shap_top_features": top, "shap_plot": str(shap_png) if shap_png else None},
        features=features,
        trained_on={"n_train": int(len(train)), "n_test": int(len(test)),
                    "fraud_rate_train": round(float(train["is_fraud"].mean()), 4)},
        trained_window={"start": str(train["order_purchase_timestamp"].min().date()),
                        "end": str(test["order_purchase_timestamp"].max().date())},
    )

    written = evalwrite.persist_classifier_predictions(
        model, MODEL_NAME, version, "order",
        test[features], test["is_fraud"], test["order_id"].to_numpy(),
        max_rows=20000, extra_meta={"synthetic_labels": True},
    )
    print(f"  wrote {written:,} test predictions to gold.predictions")

    # actionable export: highest-risk orders in the test window
    probs = test_probs
    out = test[["order_id", "customer_unique_id", "order_purchase_timestamp", "order_value"]].copy()
    out["fraud_risk"] = probs
    out = out.sort_values("fraud_risk", ascending=False).head(100)
    risk_path = common.ARTIFACTS_DIR / f"{MODEL_NAME}-{version}-top100.csv"
    out.to_csv(risk_path, index=False)
    print(f"  top-100 risk list -> {risk_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())