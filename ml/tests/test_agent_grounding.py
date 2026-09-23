"""Unit tests for grounding v2 (3C guardrails: R1 literal, R2 derived,
R3 batch/live provenance, R4 score-entity, honest refusal path)."""
from __future__ import annotations

from ml.agent import grounding, tools


def obs(label: str = tools.BATCH, rows: list[dict] | None = None) -> tools.ToolObservation:
    o = tools.ToolObservation(
        tool="batch_overview", params={}, label=label, rows=rows or []
    )
    o.render = tools.render_observation(o)
    return o


def test_literal_number_grounded() -> None:
    o = obs(rows=[{"total_orders": 100, "valid_revenue": 1234.56}])
    r = grounding.ground_answer("The dataset has 100 orders.", [o])
    assert r.grounded, r.reason


def test_sum_of_two_observed_grounded() -> None:
    o = obs(rows=[{"a": 10, "b": 20, "c": 30}])
    r = grounding.ground_answer("Combined they total 30.", [o])
    assert r.grounded, r.reason


def test_mean_of_two_observed_grounded() -> None:
    o = obs(rows=[{"a": 10, "b": 30}])
    r = grounding.ground_answer("The midpoint is 20.", [o])
    assert r.grounded, r.reason


def test_difference_of_two_observed_grounded() -> None:
    # R2 difference branch: the gap is NOT an observed value — it must be
    # derived (q21 is the eval-level pin for this).
    o = obs(rows=[{"top_revenue": 1711258.08, "second_revenue": 1653730.45}])
    r = grounding.ground_answer("The gap between the two is 57527.63.", [o])
    assert r.grounded, r.reason


def test_difference_signed_prose_grounded() -> None:
    o = obs(rows=[{"this_week": 120.0, "last_week": 90.0}])
    r = grounding.ground_answer("Revenue rose from 90 to 120, an increase of 30.", [o])
    assert r.grounded, r.reason


def test_difference_across_labels_not_grounded() -> None:
    # Two single-valued observations, one batch one live_replay: no label holds
    # the pair needed for a within-label derivation, and R3's additive phrasing
    # is absent here (the review's cross-label trap), so 60 must stay unmatched.
    b = obs(label=tools.BATCH, rows=[{"revenue": 100.0}])
    lv = obs(label=tools.LIVE_REPLAY, rows=[{"revenue": 40.0}])
    r = grounding.ground_answer("The gap between batch and live revenue was 60.", [b, lv])
    assert not r.grounded
    assert 60.0 in r.unmatched


def test_invented_number_not_grounded() -> None:
    o = obs(rows=[{"total_orders": 99441, "avg_valid_order_value": 160.26}])
    r = grounding.ground_answer("I believe the share above 20% is 37.2%.", [o])
    assert not r.grounded
    assert 37.2 in r.unmatched


def test_sentence_final_decimal_extracted() -> None:
    nums = grounding.extract_numbers("The threshold is 0.795.")
    assert nums == [0.795]


def test_hex_id_digits_are_not_claims() -> None:
    ans = "Order a1b2c3d4e5f60718293a4b5c6d7e8f90 scored 0.93."
    assert grounding.extract_numbers(ans) == [0.93]


def test_year_is_not_a_claim() -> None:
    assert grounding.extract_numbers("Since 2016 the dataset grew.") == []


def test_thousands_separators_are_one_claim() -> None:
    # A real LLM formats with commas even when tools return plain numerals;
    # the grounder must read "98,207" as one claim and not "98"+"207".
    assert grounding.extract_numbers("The dataset has 98,207 valid orders.") == [98207.0]
    assert grounding.extract_numbers("Revenue was $1,711,258.08.") == [1711258.08]


def test_thousands_separated_claim_grounded() -> None:
    o = obs(rows=[{"valid_orders": 98207.0}])
    r = grounding.ground_answer("The dataset has 98,207 valid orders.", [o])
    assert r.grounded, r.reason


def test_true_number_with_comma_not_wrongly_refused() -> None:
    # Regression for the live-LLM observation: q01/q04 were refused even
    # though the numbers (98,207 valid orders / 1,711,258.08 revenue) were
    # exactly the observed values, because the comma split the claim.
    o = obs(rows=[{"valid_orders": 98207.0, "valid_revenue": 15739137.01}])
    r = grounding.ground_answer(
        "The dataset contains a total of 98,207 valid orders and 15,739,137.01 in revenue.",
        [o],
    )
    assert r.grounded, r.reason


def test_comma_sentence_separator_not_grouped() -> None:
    # "orders 98, the top category" — comma + non-digit must NOT extend "98".
    assert grounding.extract_numbers("We saw 98 orders, the top category led.") == [98.0]


def test_prose_month_day_is_not_a_claim() -> None:
    # Live-LLM wart: "September 17, 2018" leaked the day "17" as a claim and
    # wrongly refused a correct answer. A 1..31 after a month name is a date.
    assert grounding.extract_numbers("Starting from September 17, 2018.") == []
    assert grounding.extract_numbers("As of December 31st revenue was 5,000.") == [5000.0]


def test_prose_date_answer_grounded() -> None:
    # Regression for the live q06 weekly question: the model's substance was
    # right (tail weeks ~empty), but the prose date tripped the grounder.
    o = obs(rows=[{"orders": 3, "revenue": 0.0}, {"orders": 2, "revenue": 0.0}])
    r = grounding.ground_answer(
        "Revenue has been zero for the last four weeks starting September 17, 2018.",
        [o],
    )
    assert r.grounded, r.reason


def test_empty_answer_ungrounded() -> None:
    r = grounding.ground_answer("", [obs()])
    assert not r.grounded


def test_no_numeric_claims_vacuous_grounded() -> None:
    r = grounding.ground_answer("Revenue clearly grew over time.", [obs()])
    assert r.grounded


def test_forbidden_mix_batch_then_live() -> None:
    o1 = obs(label=tools.BATCH, rows=[{"revenue": 87126.0}])
    o2 = obs(label=tools.LIVE_REPLAY, rows=[{"revenue": 4.0}])
    txt = "The batch weekly revenue combined with live replay plus together is the total."
    r = grounding.ground_answer(txt, [o1, o2])
    assert not r.grounded
    assert "combines batch and live_replay" in r.reason


def test_forbidden_mix_live_then_batch() -> None:
    o1 = obs(label=tools.LIVE_REPLAY, rows=[{"revenue": 4.0}])
    o2 = obs(label=tools.BATCH, rows=[{"revenue": 87126.0}])
    txt = "Live replay and batch figures added together sum the true total."
    r = grounding.ground_answer(txt, [o1, o2])
    assert not r.grounded


def test_merge_live_and_batch_numbers_ungrounded() -> None:
    # A number that only exists as batch+live (R1/R2 fail: values from
    # different labels are never mixed by the grounder because labels are
    # distinct rows and the candidate is not observed nor a 2-combination).
    b = obs(label=tools.BATCH, rows=[{"revenue": 100.0}])
    lv = obs(label=tools.LIVE_REPLAY, rows=[{"revenue": 50.0}])
    r = grounding.ground_answer("The true total was 150.0.", [b, lv])
    assert not r.grounded


def test_score_claim_matching_grounded() -> None:
    o = obs(
        label=tools.PREDICTIONS,
        rows=[{"entity_id": "a1b2c3d4e5f60718293a4b5c6d7e8f90", "prediction": 0.93}],
    )
    r = grounding.score_claims_grounded(
        "Order a1b2c3d4e5f60718293a4b5c6d7e8f90 has fraud risk 0.93.", [o]
    )
    assert r.grounded, r.reason


def test_score_claim_contradiction_ungrounded() -> None:
    o = obs(
        label=tools.PREDICTIONS,
        rows=[{"entity_id": "a1b2c3d4e5f60718293a4b5c6d7e8f90", "prediction": 0.93}],
    )
    r = grounding.score_claims_grounded(
        "Order a1b2c3d4e5f60718293a4b5c6d7e8f90 has fraud risk 0.42.", [o]
    )
    assert not r.grounded


def test_invented_score_for_unknown_entity_ungrounded() -> None:
    o = obs(label=tools.PREDICTIONS, rows=[{"entity_id": "a1b2c3d4e5f60718293a4b5c6d7e8f90", "prediction": 0.93}])
    # The entity is not in gold.predictions observations at all.
    r = grounding.score_claims_grounded(
        "Order deadbeefdeadbeefdeadbeefdeadbeef has fraud risk 0.93.", [o]
    )
    assert not r.grounded