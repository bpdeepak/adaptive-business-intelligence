"""End-to-end regression for the Phase 3 agent loop (deterministic: mock LLM +
fake tool back-end). Runs the full eval_questions.json set through the same
evaluator as the CLI, and asserts the guardrail contract directly:
unanswerable -> honest refusal, batch+live sums rejected, revisions <= 1."""
from __future__ import annotations

import json
import pathlib
import sys

import pytest

from ml.agent import agent as agentmod
from ml.agent import fakes
from ml.agent import eval_agent
from ml.agent.llm import MockLLM

QUESTIONS_PATH = pathlib.Path(__file__).resolve().parents[1] / "agent" / "eval_questions.json"


def _load() -> list[dict]:
    return json.loads(QUESTIONS_PATH.read_text(encoding="utf-8"))["questions"]


def test_full_fake_eval_suite_green(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["eval_agent", "--db", "fake", "--llm", "mock"])
    assert eval_agent.main() == 0


def test_contract_fields_present() -> None:
    questions = _load()
    q = next(x for x in questions if x["id"] == "q01")
    agent = agentmod.Agent(
        llm=MockLLM({q["question"][:120]: q["script"]}), dsn="fake://unused",
        ctx_factory=fakes.fake_context,
    )
    resp = agent.answer(q["question"])
    for key in ("question", "answer", "grounded", "refused", "revisions",
                "turns", "tool_uses", "sources"):
        assert key in resp, key


def test_unanswerable_questions_are_refused() -> None:
    questions = _load()
    agent = agentmod.Agent(
        llm=MockLLM({q["question"][:120]: q["script"] for q in questions},
                    fallback_answer="I don't have data for that."),
        dsn="fake://unused", ctx_factory=fakes.fake_context,
    )
    for entry in questions:
        if entry["answerable"]:
            continue
        resp = agent.answer(entry["question"])
        assert resp["refused"], f"{entry['id']} should refuse: {resp['answer']!r}"
        assert not resp["grounded"]
        assert resp["revisions"] <= 1


def test_batch_live_sum_is_rejected() -> None:
    questions = _load()
    q = next(x for x in questions if x["id"] == "q17")
    agent = agentmod.Agent(
        llm=MockLLM({q["question"][:120]: q["script"]}), dsn="fake://unused",
        ctx_factory=fakes.fake_context,
    )
    resp = agent.answer(q["question"])
    assert resp["refused"]
    assert "live" in resp["answer"].lower() or "replay" in resp["answer"].lower()


def test_revision_bounded_to_one() -> None:
    # A stubborn answer repeats the same invented number after the revision
    # prompt: the agent must refuse after exactly ONE revision, never two.
    questions = _load()
    q = next(x for x in questions if x["id"] == "q14")
    agent = agentmod.Agent(
        llm=MockLLM({q["question"][:120]: q["script"]}), dsn="fake://unused",
        ctx_factory=fakes.fake_context,
    )
    resp = agent.answer(q["question"])
    assert resp["revisions"] == 1
    assert resp["refused"]


def test_answerable_questions_ground() -> None:
    questions = _load()
    agent = agentmod.Agent(
        llm=MockLLM({q["question"][:120]: q["script"] for q in questions},
                    fallback_answer="I don't have data for that."),
        dsn="fake://unused", ctx_factory=fakes.fake_context,
    )
    for entry in questions:
        if not entry["answerable"]:
            continue
        resp = agent.answer(entry["question"])
        assert resp["grounded"], f"{entry['id']} should ground: {resp['answer']!r}"
        assert not resp["refused"]