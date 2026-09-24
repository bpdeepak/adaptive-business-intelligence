"""Grounding v2: every numeric claim in an agent answer must be traceable.

Rules (all enforced here, deliberately simple + testable):

* R1  Literal — the number appears (within tolerance) among the values the
     tools actually returned, a numeric parameter the agent passed to a tool
     ("last 4 weeks" -> weeks=4), or the small prose-constants table (simulation
     config only; never model thresholds, which come from the registry).
* R2  Derived — the number is a documented construction over observed values:
     the exact sum, arithmetic mean or absolute difference of two observed
     values OF THE SAME COLUMN (same tool, same provenance label, same field —
     e.g. two rows of `revenue`), never values from different columns: pairing
     any two numbers in a result set "derives" a surprising share of arbitrary
     integers (measured ~7% on the small fixtures) and makes grounding close to
     vacuous. A difference that the sentence characterises with a direction word
     (rose / fell / up / down …) must also cite both operands and the direction
     must agree with them ("fell by 30" about a rising series is refused).
     (A proportion-of-anything rule is deliberately absent: it accepts *any*
     number.)
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

# Fixed constants the answer may repeat in prose without a tool call: only
# simulation/config values stated in the system prompt. Model thresholds and
# baseline rates are deliberately NOT here — they live in the model registry and
# change on every retrain (the fraud threshold moved 0.795 -> 0.81), so a number
# quoted from memory must come from a registry tool observation, not from a
# stale constant that "grounds" itself.
PROSE_CONSTANTS: list[float] = [
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


# A 1900..2100 integer is a year (a text artifact, not a claim) ONLY in a year-like
# context: "in 2017", "since 2016", "from 2016 to 2018", "September 17, 2018". A bare
# "2000" ("a change of 2000", "2,000 orders") is an ordinary number and must be
# verified — exempting the whole range silently left ~200 integers unchecked.
_MONTHS = (r"(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|"
           r"aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)")
_YEAR_CONTEXT_RX = re.compile(
    r"(?:\b(?:in|since|during|year|years|fy|until|through|between|from|by|circa)\s+"
    r"|\b(?:19|20)\d\d\s+(?:to|and|-|–)\s+"
    r"|\b" + _MONTHS + r"\.?,?\s+(?:\d{1,2}(?:st|nd|rd|th)?,?\s+)?)$",
    re.IGNORECASE)


def _is_year_artifact(text: str, start: int, tok: str, value: float) -> bool:
    if "," in tok or "." in tok or value != int(value) or not (1900 <= value <= 2100):
        return False
    return bool(_YEAR_CONTEXT_RX.search(text[max(0, start - 40):start]))


def extract_number_spans(text: str) -> list[tuple[float, int]]:
    """All numeric claims with the character offset they start at (offsets are
    into the month-day-stripped text, which is what sentence splitting also uses).

    Dates/times are filtered (ISO dates, times, prose month-day dates, and years
    in a year-like context), as are digit runs inside hex identifiers
    (order/session ids like ``a1b2c3d4...`` — a digit flanked by hex letters is
    part of an id, not a claim).
    """
    out: list[tuple[float, int]] = []
    stripped = _strip_month_day(text)
    for m in _CLAIM_RX.finditer(stripped):
        tok = m.group(0)
        # ``98,207`` / ``1,711,258.08`` — drop the grouping commas, then parse.
        v = float(tok.replace(",", ""))
        if _is_year_artifact(stripped, m.start(), tok, v):
            continue
        out.append((v, m.start()))
    return out


def extract_numbers(text: str) -> list[float]:
    """All numeric claims in the answer (see extract_number_spans)."""
    return [v for v, _ in extract_number_spans(text)]


def observed_values(observations: Iterable[tools.ToolObservation], constants: Iterable[float] | None = None) -> list[float]:
    vals: list[float] = list(constants or PROSE_CONSTANTS)
    for obs in observations:
        # Numbers the agent itself passed to a tool ("the last 4 weeks" -> weeks=4)
        # are echoes of the request, not claims about the data. They count for R1
        # only (never for derivation).
        for value in (obs.params or {}).values():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                vals.append(float(value))
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
    """Observed values grouped by provenance label (kept for callers/tests that
    want the per-label view; derivation now uses observed_by_column)."""
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


def observed_by_column(
    observations: Iterable[tools.ToolObservation],
) -> dict[tuple[str, str, str], list[float]]:
    """Observed numeric values grouped by (provenance label, tool, column).

    R2 derives only WITHIN one group: two rows of the same field from the same
    tool. That keeps a derivation semantically meaningful (a gap between two
    revenues, a change between two weekly orders counts) and can never mix
    batch with live_replay, nor an orders count with a revenue.
    """
    groups: dict[tuple[str, str, str], list[float]] = {}
    for obs in observations:
        for row in obs.rows:
            for key, value in row.items():
                if isinstance(value, bool):
                    continue
                if isinstance(value, (int, float, Decimal)):
                    groups.setdefault((obs.label, obs.tool, key), []).append(float(value))
    return groups


def _close(a: float, b: float) -> bool:
    return abs(a - b) <= 1e-6 * max(1.0, abs(b)) + 1e-3


_UP_RX = re.compile(
    r"\b(rose|rise|rises|risen|rising|increase[ds]?|increasing|grew|grow|grows|grown|growth|"
    r"(?<![-\w])up|higher|gain(?:ed|s)?|climbed|jumped|surged?|improved?)\b", re.IGNORECASE)
# (the lookbehind keeps hyphenated words such as "runner-up" from reading as a direction)
_DOWN_RX = re.compile(
    r"\b(fell|fall|falls|fallen|falling|decrease[ds]?|decreasing|dropped|drop|drops|(?<![-\w])down|lower|"
    r"declin\w+|shrank|shrink\w*|dipped|reduc\w+|worsen\w*)\b", re.IGNORECASE)

_SENTENCE_RX = re.compile(r"[^.!?\n]+(?:[.!?](?!\s|$)[^.!?\n]*)*")


def _derived_pairs(candidate: float, groups: dict[tuple[str, str, str], list[float]]):
    """Yield (op, a, b) for every same-column pair whose sum, mean or absolute
    difference equals `candidate` (R2)."""
    for vals in groups.values():
        n = len(vals)
        for i in range(n):
            for j in range(i + 1, n):
                x, y = vals[i], vals[j]
                if _close(candidate, x + y):
                    yield "sum", x, y
                if _close(candidate, (x + y) / 2.0):
                    yield "mean", x, y
                if _close(candidate, abs(x - y)):
                    yield "diff", x, y


def _match_derived(candidate: float, groups: dict[tuple[str, str, str], list[float]]) -> bool:
    """R2 without the direction check (kept for callers/tests): is `candidate`
    a sum, mean or absolute difference of two same-column observed values?"""
    return next(_derived_pairs(candidate, groups), None) is not None


def _sentence_around(text: str, pos: int) -> tuple[str, int]:
    """The sentence containing character `pos` and the offset it starts at."""
    for m in _SENTENCE_RX.finditer(text):
        if m.start() <= pos < m.end():
            return m.group(0), m.start()
    return text, 0


def _direction_verdict(text: str, pos: int, x: float, y: float) -> str | None:
    """Check a directional claim about a derived difference.

    Returns None when the sentence makes no directional claim (a neutral "gap of
    30" is fine) or the direction agrees with the operands; otherwise a refusal
    reason. The operands are read in the order the sentence mentions them
    ("from 90 to 120": before = 90, after = 120).
    """
    sentence, _ = _sentence_around(_strip_month_day(text), pos)
    up, down = bool(_UP_RX.search(sentence)), bool(_DOWN_RX.search(sentence))
    if not (up or down):
        return None
    if up and down:
        return "ambiguous direction in a derived difference"
    mentioned = extract_number_spans(sentence)  # (value, offset within the sentence)
    xs = [p for v, p in mentioned if _close(v, x)]
    ys = [p for v, p in mentioned if _close(v, y)]
    if not xs or not ys:
        return "a directional claim about a derived difference must cite both values it compares"
    # Operand order of appearance: the earlier-mentioned one is "before".
    if min(xs) <= min(ys):
        before, after = x, y
    else:
        before, after = y, x
    if up and after > before:
        return None
    if down and after < before:
        return None
    return f"direction contradicts the observed values ({before:g} -> {after:g})"


def ground_answer(answer: str, observations: list[tools.ToolObservation]) -> GroundingReport:
    if not answer or not answer.strip():
        return GroundingReport(False, "empty answer")

    if _FORBIDDEN_MIX_RX.search(answer):
        return GroundingReport(False, "answer combines batch and live_replay figures")

    spans = extract_number_spans(answer)
    if not spans:
        # No numeric claims — nothing to verify, but also nothing much said.
        return GroundingReport(True, "no numeric claims")

    obs_vals = observed_values(observations)
    groups = observed_by_column(observations)
    unmatched: list[float] = []
    reasons: list[str] = []
    for n, pos in spans:
        if any(_close(n, v) for v in obs_vals):
            continue
        matched = False
        direction_reason = ""
        for op, x, y in _derived_pairs(n, groups):
            if op != "diff":
                matched = True
                break
            verdict = _direction_verdict(answer, pos, x, y)
            if verdict is None:
                matched = True
                break
            direction_reason = verdict
        if matched:
            continue
        unmatched.append(n)
        if direction_reason:
            reasons.append(f"{n:g}: {direction_reason}")

    if unmatched:
        pretty = ", ".join(f"{u:g}" for u in unmatched[:8])
        detail = f"ungrounded numbers: {pretty}"
        if reasons:
            detail += " (" + "; ".join(reasons[:3]) + ")"
        return GroundingReport(False, detail, unmatched=unmatched)

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