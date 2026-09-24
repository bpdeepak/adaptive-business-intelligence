"""Train + backtest the bot-vs-human session classifier (Phase 2).

Model: LightGBM over gold.session_features (durable, deterministic corpus that
mirrors the replay's session synthesis — see ml/replay_session_corpus.py).
Time split by session_date (latest 20%).

The signal the model should rediscover: synthetic bot sessions fire 3..12 page
views in < 1 second (30-40 ms inter-click), browse only product/cart/checkout
pages, and show near-zero inter-click variance — while human sessions spread
45-344 s between clicks over 10-39 minutes and wander the full funnel.
"""
from __future__ import annotations

import argparse
import sys

import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier

import backtest
import common
import evalwrite
import explain

MODEL_NAME = "bot_score"
TEST_FRAC = 0.2

FEATURES = [
    "is_converting", "click_count", "duration_seconds", "click_interval_cv",
    "has_search", "has_product_page", "has_cart_page", "has_checkout_page",
    "page_types_distinct", "cart_added", "cart_value", "hour_of_day", "is_weekend",
]


def load_data() -> pd.DataFrame:
    df = common.read_sql(
        """
        select session_id, session_date, is_synthetic_bot, is_converting,
               click_count, duration_seconds, click_interval_cv, has_search,
               has_product_page, has_cart_page, has_checkout_page,
               page_types_distinct, cart_added, cart_value, hour_of_day, is_weekend
        from gold.session_features
        """
    )
    for col in FEATURES + ["is_synthetic_bot", "session_date"]:
        if col == "session_date":
            df[col] = pd.to_datetime(df[col])
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def make_model(train: pd.DataFrame) -> LGBMClassifier:
    neg = int((train["is_synthetic_bot"] == 0).sum())
    pos = int((train["is_synthetic_bot"] == 1).sum())
    spw = max(1.0, neg / max(pos, 1))
    m = LGBMClassifier(
        n_estimators=400,
        learning_rate=0.05,
        num_leaves=63,
        min_child_samples=100,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=spw,
        random_state=42,
        verbose=-1,
    )
    m.fit(train[FEATURES], train["is_synthetic_bot"])
    return m


def main() -> int:
    ap = argparse.ArgumentParser(description="Train the bot-vs-human session classifier")
    ap.add_argument("--skip-persist", action="store_true")
    ap.add_argument("--version", default=None, help="explicit model version tag (default: now_tag)")
    ap.add_argument("--candidate", action="store_true",
                    help="register as status='candidate' (never auto-promoted)")
    args = ap.parse_args()

    df = load_data()
    print(f"rows: {len(df):,} sessions, bot rate {df['is_synthetic_bot'].mean():.3%}")

    train, test = backtest.time_split(df, time_col="session_date", test_frac=TEST_FRAC)
    print(f"time split by session_date: train {len(train):,} (until {train['session_date'].max().date()}), "
          f"test {len(test):,} (from {test['session_date'].min().date()})")

    model = make_model(train)
    metrics = evalwrite.evaluate_classifier(model, test[FEATURES], test["is_synthetic_bot"],
                                            train_positive_rate=float(train["is_synthetic_bot"].mean()))
    # Operating decision rule for Phase 4 (flag for review/rate-limit): record
    # the most permissive threshold at >= 85 % precision; bot rate is only ~2 %,
    # so the naive 0.5 cutoff over-flags.
    metrics["recommended_threshold"] = backtest.recommended_threshold(
        test["is_synthetic_bot"].to_numpy(), model.predict_proba(test[FEATURES])[:, 1]
    )
    print("  metrics:", {k: round(v, 4) for k, v in metrics.items()})

    if args.skip_persist:
        return 0

    version = args.version or common.now_tag()

    # Phase 4 drift baseline: per-feature reference distributions of the
    # TRAINING matrix — same keys the Go score-writer persists into session
    # gold.predictions.features rows, so stream PSI measures against this.
    baseline = common.feature_distribution_baseline(train, FEATURES)
    artifact = common.save_artifact(
        MODEL_NAME, version, model,
        {"features": FEATURES, "metrics": {k: round(v, 4) for k, v in metrics.items()}},
    )
    importance = explain.global_importance(model, test[FEATURES].sample(n=min(20000, len(test)), random_state=42))
    top = {k: round(v, 6) for k, v in list(importance.items())[:10]}
    shap_png = explain.save_global_shap_plot(model, test[FEATURES].sample(n=min(4000, len(test)), random_state=42),
                                             f"{common.ARTIFACTS_DIR}/{MODEL_NAME}-{version}-shap.png")

    common.register_model(
        MODEL_NAME, version,
        framework="lightgbm", task="binary_classification", grain="session",
        artifact_path=str(artifact),
        params={"n_estimators": 400, "learning_rate": 0.05, "num_leaves": 63,
                "scale_pos_weight": "train-balanced", "corpus_source": "offline_replay"},
        metrics={**{k: round(v, 4) for k, v in metrics.items()},
                 "shap_top_features": top, "shap_plot": str(shap_png) if shap_png else None},
        features=FEATURES,
        trained_on={"n_train": int(len(train)), "n_test": int(len(test)),
                    "bot_rate_train": round(float(train["is_synthetic_bot"].mean()), 4)},
        trained_window={"start": str(train["session_date"].min().date()),
                        "end": str(test["session_date"].max().date())},
        status="candidate" if args.candidate else "active",
        drift_baseline=baseline,
    )

    written = evalwrite.persist_classifier_predictions(
        model, MODEL_NAME, version, "session",
        test[FEATURES], test["is_synthetic_bot"], test["session_id"].to_numpy(),
        max_rows=20000, extra_meta={"corpus_source": "offline_replay"},
    )
    print(f"  wrote {written:,} (sampled) test predictions to gold.predictions")

    # actionable export: highest-scoring bot-flagged sessions in the test window
    probs = model.predict_proba(test[FEATURES])[:, 1]
    out = test[["session_id", "session_date", "click_count", "duration_seconds"]].copy()
    out["bot_score"] = probs
    out = out.sort_values("bot_score", ascending=False).head(100)
    risk_path = common.ARTIFACTS_DIR / f"{MODEL_NAME}-{version}-top100.csv"
    out.to_csv(risk_path, index=False)
    print(f"  top-100 bot sessions -> {risk_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())