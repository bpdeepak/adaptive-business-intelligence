"""Grounding v2: every numeric claim in an agent answer must be traceable.

Rules (all enforced here, deliberately simple + testable):

* R1  Literal — the number appears (within tolerance) among the values the
     tools actually returned, OR in the prose-constants table.
* R2  Derived — the number is a documented construction over observed values:
     the exact sum, arithmetic mean or absolute difference of two observed
     values. (A proportion-of-anything rule is deliberately absent: it accepts
     *any* number, which would make grounding vacuous.)
* R3  Provenance — the answer must never claim a figure that sums batch and
     live_replay series. The pattern check rejects "combined/replay+batch"
     phrasings before any numeric check runs. Since every tool row is labeled,
     R1/R2 can only match values from one recorded observation at a time; a
     number made up by summing across labels would fail both.
* R4  Scores — when the answer references a model score for an entity, the
     (entity, value) pair must exist in model_scores observations; the model
     score claims are otherwise ungrounded.

Numbers are matched with a small tolerance (not string equality).
"""

from __future__ import annotations

import math
import re
from decimal import Decimal
from typing import Any, Iterable

from . import tools

# Fixed business constants the answer may repeat in prose without a tool call.
# Values mirrored from the trained registry rows (fraud/bot thresholds are read
# from metrics at deploy time; the agent tools surface them too).
PROSE_CONSTANTS: list[float] = [
    0.795,  # recommended fraud_risk threshold
    0.5,    # recommended bot_score threshold
    0.02,   # bot_score positive rate baseline
    0.01,   # fraud_risk positive rate baseline (approx, ≥1% floor)
    3.0,    # 3x-baseline rate-anomaly multiplier (Phase 3A)
    0.03,   # session conversion rate (simulator default)
    2880.0,  # replay speed multiplier
]

# A numeric *claim*: a decimal/integer run NOT flanked by hex letters (ids),
# date/time separators or a trailing ISO-`Z` (timestamps stay text). A trailing
# period is sentence punctuation and must NOT exclude the number it follows.
# Thousand-separator groups (98,207 / 1,711,258.08) belong to the number: a
# real LLM formats with commas even when the tools return plain numerals, and
# splitting on the comma would turn the claim into "98" + "207" (both
# unobserved) and wrongly refuse a correct answer.
_CLAIM_RX = re.compile(r"(?<![0-9a-fA-F.:/-])-?\d+(?:,\d{3})*(?:\.\d+)?(?![0-9a-fA-F:/z])")

# R3: never sums batch and live_replay. Triggered by a sentence that couples a
# batch term with a live term and then uses additive language. Bilaterally
# matched so either word order is caught.
_FORBIDDEN_MIX_RX = re.compile(
    r"\b(batch|whole[ -]?history|all[ -]?time)\b[^.!?\n]{0,140}"
    r"\b(live|replay|realtime|streaming)\b[^.!?\n]{0,70}"
    r"\b(?:total|combined|combining|sum|together|plus|added|both)\b"
    r"|\b(live|replay|realtime|streaming)\b[^.!?\n]{0,140}"
    r"\b(batch|whole[ -]?history|all[ -]?time)\b[^.!?\n]{0,70}"
    r"\b(?:total|combined|combining|sum|together|plus|added|both)\b",
    re.IGNORECASE,
)


class GroundingReport:
    def __init__(self, grounded: bool, reason: str = "", unmatched: list[float] | None = None):
        self.grounded = grounded
        self.reason = reason
        self.unmatched = unmatched or []

    def __bool__(self) -> bool:
        return self.grounded

    def summary(self) -> dict[str, Any]:
        return {"grounded": self.grounded, "reason": self.reason}


# Dates written in prose ("September 17, 2018") put the bare day-of-month into
# the text where the claim regex would read "17" as a numeric claim. Numbers
# 1..31 immediately after a month name are date artifacts, not claims (the day
# is dropped before extraction; a well-behaved answer writes real figures
# anywhere else).
_MONTH_DAY_RX = re.compile(
    r"\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|"
    r"aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\.?"
    r"\s+(\d{1,2})(?:st|nd|rd|th)?\b",
    re.IGNORECASE,
)


def _strip_month_day(text: str) -> str:
    def repl(m: re.Match[str]) -> str:
        day = int(m.group(1))
        if 1 <= day <= 31:
            return m.group(0).split()[0]  # keep "September", drop the day
        return m.group(0)

    return _MONTH_DAY_RX.sub(repl, text)


def extract_numbers(text: str) -> list[float]:
    """All numeric claims in the answer.

    Dates/times are filtered (ISO dates, times, prose month-day dates, and
    standalone years), as are digit runs inside hex identifiers (order/session
    ids like ``a1b2c3d4...`` — a digit flanked by hex letters is part of an id,
    not a claim).
    """
    out: list[float] = []
    for m in _CLAIM_RX.finditer(_strip_month_day(text)):
        tok = m.group(0)
        # ``98,207`` / ``1,711,258.08`` — drop the grouping commas, then parse.
        v = float(tok.replace(",", ""))
        # Standalone years (2016..2018 dataset, 2026 deploy) are text artifacts.
        if v == int(v) and 1900 <= v <= 2100:
            continue
        out.append(v)
    return out


def observed_values(observations: Iterable[tools.ToolObservation], constants: Iterable[float] | None = None) -> list[float]:
    vals: list[float] = list(constants or PROSE_CONSTANTS)
    for obs in observations:
        for row in obs.rows:
            for key, value in row.items():
                if isinstance(value, bool):
                    continue
                if isinstance(value, (int, float, Decimal)):
                    vals.append(float(value))
    return vals


def observed_by_label(
    observations: Iterable[tools.ToolObservation],
    constants: Iterable[float] | None = None,
) -> dict[str, list[float]]:
    """Observed values grouped by provenance label.

    Derivation (R2) is only ever computed WITHIN one label: a number made from
    combining batch and live_replay values is refused even when it is a clean
    sum of two observed values — provenance may not be mixed.
    """
    by_label: dict[str, list[float]] = {"constants": list(constants or PROSE_CONSTANTS)}
    for obs in observations:
        bucket = by_label.setdefault(obs.label, [])
        for row in obs.rows:
            for value in row.values():
                if isinstance(value, bool):
                    continue
                if isinstance(value, (int, float, Decimal)):
                    bucket.append(float(value))
    return by_label


def _close(a: float, b: float) -> bool:
    return abs(a - b) <= 1e-6 * max(1.0, abs(b)) + 1e-3


def _match_derived(candidate: float, by_label: dict[str, list[float]]) -> bool:
    """R2: a documented construction over observed values — WITHIN one label.

    Supported (deliberately narrow — falsifiability beats recall): the exact
    sum, arithmetic mean or absolute difference of two observed values (the
    difference branch answers comparison questions like "how much changed /
    how much more than": eval q21 is the regression pin). Values are grouped by
    provenance; batch and live_replay numbers may never be combined into a
    derivation. A proportion-of-anything rule is NOT supported: it accepts any
    number (every value is *some* percentage of some other value), which would
    make grounding vacuous.
    """
    for label, vals in by_label.items():
        n = len(vals)
        if n < 2:
            continue
        for i in range(n):
            for j in range(i + 1, n):
                if (
                    _close(candidate, vals[i] + vals[j])
                    or _close(candidate, (vals[i] + vals[j]) / 2.0)
                    or _close(candidate, abs(vals[i] - vals[j]))
                ):
                    return True
    return False


def ground_answer(answer: str, observations: list[tools.ToolObservation]) -> GroundingReport:
    if not answer or not answer.strip():
        return GroundingReport(False, "empty answer")

    if _FORBIDDEN_MIX_RX.search(answer):
        return GroundingReport(False, "answer combines batch and live_replay figures")

    numbers = extract_numbers(answer)
    if not numbers:
        # No numeric claims — nothing to verify, but also nothing much said.
        return GroundingReport(True, "no numeric claims")

    obs_vals = observed_values(observations)
    by_label = observed_by_label(observations)
    unmatched: list[float] = []
    for n in numbers:
        if any(_close(n, v) for v in obs_vals):
            continue
        if _match_derived(n, by_label):
            continue
        unmatched.append(n)

    if unmatched:
        pretty = ", ".join(f"{u:g}" for u in unmatched[:8])
        return GroundingReport(False, f"ungrounded numbers: {pretty}", unmatched=unmatched)

    return GroundingReport(True, "all claims traced")


def score_claims_grounded(answer: str, observations: list[tools.ToolObservation]) -> GroundingReport:
    """R4: entity-specific model-score claims must match gold.predictions rows.

    Two checks:
    * Any entity-style token (order/session/customer hex id) named in the
      answer must exist among the OBSERVED predictions rows — a score quoted
      for an entity the tools never returned is fabrication.
    * When such an entity is observed and the answer places a decimal next to
      it, that decimal must equal the recorded prediction (contradictions are
      refused).
    """
    known_entities: set[str] = set()
    for obs in observations:
        if obs.label != tools.PREDICTIONS:
            continue
        for row in obs.rows:
            entity = str(row.get("entity_id", ""))
            pred = row.get("prediction")
            if not entity:
                continue
            known_entities.add(entity)
            if not isinstance(pred, (int, float)):
                continue
            pos = answer.lower().find(entity.lower())
            if pos < 0:
                continue  # entity not claimed in the answer
            # Look for the score *after* the entity mention. Decimals only:
            # entity ids are hex (their digits must not be picked up), and the
            # claim "entity 0.9..." is the only number adjacent in a grounded
            # answer.
            after = answer[pos + len(entity) : pos + len(entity) + 120]
            nearby = re.search(r"-?\d+\.\d+(?:[eE][-+]?\d+)?", after)
            if nearby:
                claimed = float(nearby.group(0))
                if not _close(claimed, float(pred)):
                    return GroundingReport(
                        False,
                        f"score claim for {entity} ({claimed:g}) contradicts gold.predictions ({float(pred):g})",
                    )

    # Fabricated-entity guard: any hex entity token quoted without ever having
    # been returned by a predictions observation is refused.
    for token in re.findall(r"\b[0-9a-f]{24,40}\b", answer.lower()):
        if token not in known_entities:
            return GroundingReport(
                False, f"score referenced for entity {token} not present in model_scores"
            )
    return GroundingReport(True, "entity score claims traced")