"""pytest bootstrap for the ml package.

The trainers and helpers (train_*.py, backtest.py, common.py, ...) are invoked
operationally as `python ml/train_*.py`, which puts the ml directory on
sys.path and lets them use flat sibling imports (`import backtest`). This
conftest reproduces that layout for `uv run python -m pytest ml/tests` so the
same modules resolve identically under test.
"""
from __future__ import annotations

import pathlib
import sys

ML_DIR = pathlib.Path(__file__).resolve().parent
if str(ML_DIR) not in sys.path:
    sys.path.insert(0, str(ML_DIR))