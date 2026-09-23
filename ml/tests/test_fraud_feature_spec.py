"""Contract tests for the shared fraud feature specification.

Phase 3 requires the Python trainer and the Go stream-score feature assembler to
agree on the fraud feature vector without duplicating it: the spec JSON
(api/internal/scorewriter/fraud_feature_spec.json) is the single source. The Go
side embeds it at build time and asserts its assembler output against it; this
file pins the Python side (train_fraud.py column construction + the sidecar
manifest when an artifacts/sidecar_models.json is present).
"""
from __future__ import annotations

import json
import pathlib

from ml import common, train_fraud

SPEC_PATH = (
    pathlib.Path(__file__).resolve().parents[2]
    / "api" / "internal" / "scorewriter" / "fraud_feature_spec.json"
)
MANIFEST_PATH = pathlib.Path(__file__).resolve().parents[2] / "artifacts" / "sidecar_models.json"
EXPECTED_PAY = [
    "pay_boleto",
    "pay_credit_card",
    "pay_debit_card",
    "pay_not_defined",
    "pay_unknown",
    "pay_voucher",
]


def _spec_names() -> list[str]:
    return [f["name"] for f in json.loads(SPEC_PATH.read_text(encoding="utf-8"))["features"]]


def test_spec_names_match_train_fraud_construction() -> None:
    names = _spec_names()
    pay = [n for n in names if n.startswith("pay_")]
    assert pay == sorted(pay), "one-hot columns must be the get_dummies sorted order"
    assert pay == EXPECTED_PAY
    assert names == train_fraud.BASE_FEATURES + pay + ["category_code"]
    assert len(set(names)) == len(names), "feature names must be unique"


def test_spec_names_match_sidecar_manifest_when_present() -> None:
    if not MANIFEST_PATH.exists():
        return  # manifest is generated; CI without artifacts skips this bound
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    fraud = next(m for m in manifest["models"] if m["name"] == "fraud_risk")
    assert _spec_names() == fraud["features"]


def test_category_rank_from_is_deterministic_and_descending() -> None:
    import pandas as pd

    df = pd.DataFrame({"cat": ["b", "a", "b", "c"], "v": [1, 10, 2, 5]})
    assert common.category_rank_from(df, "cat", "v") == {"a": 0, "c": 1, "b": 2}
    assert common.category_rank_from(df, "cat", "v") == common.category_rank_from(df, "cat", "v")