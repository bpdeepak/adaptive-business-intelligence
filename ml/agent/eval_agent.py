"""Evaluate the Phase 3 agent against eval_questions.json.

Usage:
    uv run --group ml python -m ml.agent.eval_agent --db fake --llm mock   # deterministic regression
    uv run --group ml python -m ml.agent.eval_agent --db real --llm mock   # SQL correctness against the DB
    uv run --group ml python -m ml.agent.eval_agent --db real --llm live   # full live eval (Ollama)
    uv run --group ml python -m ml.agent.eval_agent --db real --llm live --one "question"

Checks mirror the eval JSON `check` specs and re-run the SAME tool the agent
used, so expectations can never drift from the SQL. Exits 1 when any question
fails its check.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from .agent import Agent, mock_from_env
from . import fakes, tools
from .llm import LLMClient, MockLLM

ROOT = Path(__file__).parent
QUESTIONS = ROOT / "eval_questions.json"


def load_questions() -> list[dict[str, Any]]:
    data = json.loads(QUESTIONS.read_text(encoding="utf-8"))
    return data["questions"]


def tool_rows(ctx: tools.ToolContext, tool: str, params: dict[str, Any] | None) -> list[dict[str, Any]]:
    spec = tools.TOOL_BY_NAME[tool]
    return spec.run(ctx, params or {})


def _numbers_in(answer: str) -> list[float]:
    from .grounding import extract_numbers  # reuse the same extraction

    return extract_numbers(answer)


def _close(a: float, b: float) -> bool:
    return abs(a - b) <= 1e-6 * max(1.0, abs(b)) + 1e-2


def check_question(entry: dict[str, Any], answer: str, ctx: tools.ToolContext) -> tuple[bool, str]:
    q = entry["check"]
    kind = q["type"]
    if kind == "number":
        rows = tool_rows(ctx, q["tool"], q.get("params"))
        if not rows:
            return False, f"tool {q['tool']} returned no rows"
        expected = rows[0][q["key"]]
        hits = _numbers_in(answer)
        if not any(_close(h, float(expected)) for h in hits):
            return False, f"expected {float(expected)} ({q['key']}) in answer; numbers found: {hits}"
        return True, f"{q['key']}={expected}"
    if kind == "numbers":
        rows = tool_rows(ctx, q["tool"], q.get("params"))
        if not rows:
            return False, f"tool {q['tool']} returned no rows"
        expected = [float(r[k]) for r in rows for k in q["keys"] if k in r]
        if not expected:
            return False, f"no values collected for keys {q['keys']} from {q['tool']} (rows={rows!r:.200})"
        hits = _numbers_in(answer)
        missing = [e for e in expected if not any(_close(h, e) for h in hits)]
        if missing:
            return False, f"missing expected values {missing[:4]} in answer; found {hits}"
        return True, f"{len(expected)} values matched"
    if kind == "numbers_head":
        # First-row values for the keys (the answer template renders one row).
        rows = tool_rows(ctx, q["tool"], q.get("params"))
        if not rows:
            return False, f"tool {q['tool']} returned no rows"
        expected = [float(rows[0][k]) for k in q["keys"] if k in rows[0]]
        if not expected:
            return False, f"no head values for keys {q['keys']} from {q['tool']} (rows={rows!r:.200})"
        hits = _numbers_in(answer)
        missing = [e for e in expected if not any(_close(h, e) for h in hits)]
        if missing:
            return False, f"missing head values {missing[:4]} in answer; found {hits}"
        return True, f"{len(expected)} head values matched"
    if kind == "label":
        rows = tool_rows(ctx, q["tool"], q.get("params"))
        if not rows:
            return False, f"tool {q['tool']} returned no rows"
        expected = str(rows[0][q["key"]])
        if expected not in answer:
            return False, f"expected label {expected!r} in answer"
        return True, f"label {expected!r} present"
    if kind == "score_pair":
        rows = tool_rows(ctx, q["tool"], q.get("params"))
        if not rows:
            return False, f"tool {q['tool']} returned no rows"
        entity = str(rows[0]["entity_id"])
        pred = float(rows[0]["prediction"])
        hits = _numbers_in(answer)
        if entity not in answer:
            return False, "missing entity id in answer"
        if not any(_close(h, pred) for h in hits):
            return False, f"missing prediction {pred} in answer"
        return True, "score pair present"
    if kind == "anomaly":
        # Deterministic-suite guard: anomaly rows must be cited verbatim.
        rows = tool_rows(ctx, q["tool"], q.get("params"))
        anom = [r for r in rows if "severity" in r and "observed" in r and "expected" in r]
        if not anom:
            return False, "no anomaly rows returned by live_realtime"
        hits = _numbers_in(answer)
        for r in anom:
            if str(r["severity"]) not in answer:
                return False, f"missing severity {r['severity']!r} in answer"
            for k in ("observed", "expected"):
                if not any(_close(h, float(r[k])) for h in hits):
                    return False, f"missing {k}={float(r[k])} in answer"
        return True, f"{len(anom)} anomaly rows cited"
    if kind == "derived":
        # A number the tools never return directly (e.g. the revenue gap
        # between the top two categories): re-run the same tool, take the
        # absolute difference of the first two rows' key, and require it in the
        # answer. Mirrors the grounder's R2 difference branch (q21).
        rows = tool_rows(ctx, q["tool"], q.get("params"))
        if len(rows) < 2:
            return False, f"tool {q['tool']} returned fewer than 2 rows (derived check)"
        op = q.get("op", "diff")
        if op != "diff":
            return False, f"unsupported derived op {op!r}"
        a, b = float(rows[0][q["key"]]), float(rows[1][q["key"]])
        expected = abs(a - b)
        hits = _numbers_in(answer)
        if not any(_close(h, expected) for h in hits):
            return False, f"expected derived diff {expected} in answer; numbers found: {hits}"
        return True, f"derived diff {expected} present"
    if kind == "regex":
        return bool(re.search(q["pattern"], answer)), f"regex {q['pattern']!r}"
    if kind == "unanswerable":
        lower = answer.lower()
        refused_like = (
            "can't" in lower or "cannot" in lower or "unable" in lower
            or "no data" in lower or "could not" in lower or "won't guess" in lower
        )
        if refused_like:
            return True, "refused honestly"
        return False, f"expected a refusal, got: {answer[:160]!r}"
    return False, f"unknown check type {kind!r}"


def build_agent(db: str, llm: str) -> Agent:
    questions = load_questions()
    script: dict[str, list[dict[str, Any]]] = {}
    if llm == "mock":
        for entry in questions:
            script[entry["question"][:120]] = entry["script"]
        fallback = "I don't have data for that."
        agent = Agent(
            llm=MockLLM(script, fallback_answer=fallback),
            dsn="fake://unused",
            ctx_factory=None,
        )
        if db == "real":
            agent.dsn = os.getenv("DATABASE_URL", "postgresql://abi:abi@localhost:5432/abi")
        else:
            agent.ctx_factory = fakes.fake_context
        return agent
    # live LLM
    base = os.getenv("ABI_LLM_BASE_URL", "http://127.0.0.1:11434")
    model = os.getenv("ABI_LLM_MODEL", "qwen2.5:7b-instruct")
    agent = Agent(
        llm=LLMClient(base, model),
        dsn=os.getenv("DATABASE_URL", "postgresql://abi:abi@localhost:5432/abi"),
    )
    if db == "fake":
        agent.ctx_factory = fakes.fake_context
    return agent


def make_ctx(db: str) -> tools.ToolContext:
    if db == "fake":
        return fakes.fake_context()
    return tools.ToolContext(dsn=os.getenv("DATABASE_URL", "postgresql://abi:abi@localhost:5432/abi"))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", choices=["fake", "real"], default="fake")
    ap.add_argument("--llm", choices=["mock", "live"], default="mock")
    ap.add_argument("--one", default=None)
    ap.add_argument("--questions", default=str(QUESTIONS))
    args = ap.parse_args()

    questions = json.loads(Path(args.questions).read_text(encoding="utf-8"))["questions"]
    if args.one:
        questions = [q for q in questions if q["question"] == args.one or q["id"] == args.one]
        if not questions:
            print(f"no question matching {args.one!r}")
            return 2

    agent = build_agent(args.db, args.llm)
    ctx = make_ctx(args.db)

    ok_total = 0
    fail_total = 0
    skip_total = 0
    for entry in questions:
        if entry.get("skip_when") == args.db:
            print(f"[SKIP] {entry['id']} (intended for db={entry.get('skip_when')})")
            skip_total += 1
            continue
        resp = agent.answer(entry["question"])
        ok, detail = check_question(entry, resp["answer"], ctx)
        status = "PASS" if ok else "FAIL"
        if ok:
            ok_total += 1
        else:
            fail_total += 1
        print(
            f"[{status}] {entry['id']} answerable={entry['answerable']} "
            f"grounded={resp['grounded']} refused={resp['refused']} revisions={resp['revisions']} "
            f"turns={resp['turns']} :: {detail}"
        )
        if not ok:
            print(f"        answer: {resp['answer'][:200]!r}")

    print(f"\n{ok_total} passed, {fail_total} failed, {skip_total} skipped")
    return 1 if fail_total else 0


if __name__ == "__main__":
    sys.exit(main())