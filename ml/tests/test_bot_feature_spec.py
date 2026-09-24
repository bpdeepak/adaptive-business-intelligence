"""Contract test for the shared bot feature specification.

The Go score-writer maps a session.end event onto the bot model's features
(api/internal/scorewriter/features.go BotFeatures) and the Python trainer builds
the same vector (ml/train_bot.py FEATURES). The names are pinned to ONE file,
api/internal/scorewriter/bot_feature_spec.json; the Go test asserts BotFeatures
against it and this test asserts the trainer against it.
"""
from __future__ import annotations

import json
import pathlib

from ml import train_bot

SPEC_PATH = (
    pathlib.Path(__file__).resolve().parents[2]
    / "api" / "internal" / "scorewriter" / "bot_feature_spec.json"
)


def test_train_bot_features_match_the_shared_spec():
    spec = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
    assert [f["name"] for f in spec["features"]] == train_bot.FEATURES
