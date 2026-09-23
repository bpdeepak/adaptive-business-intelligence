"""The 3C agent loop: think -> tool-call -> observe -> (ground | revise | refuse).

`Agent.answer(question)` returns a response contract dict:

    {
      "question", "answer", "grounded": bool, "refused": bool,
      "revisions": int (0..MAX_REVISIONS), "turns": int,
      "tool_uses": [ {tool, params, label, note, error} ... ],
      "sources": [provenance labels used]
    }

Flow:
1. Run the chat loop up to MAX_TURNS; each assistant turn may emit tool calls
   (executed against Postgres) or a final answer.
2. On a final answer, run grounding v2 (numeric trace + provenance conflict +
   entity score check). If it fails and revisions < MAX_REVISIONS, feed the
   grounder's report back and ask the model to fix it — bounded to one pass.
3. Still failing -> refuse: answer states plainly that the claim can't be
   confirmed, grounded=false, refused=true. Tool failures also produce a
   refused response (guardrail: no hallucinated numbers on tool errors).
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any

from . import grounding, tools
from .llm import LLMClient, LLMError, MockLLM

logger = logging.getLogger("abi.agent")

MAX_TURNS = 6
MAX_REVISIONS = 1
REVISION_PROMPT = """
Your previous answer could not be grounded in the observations you made with
tools. Rephrase it so that EVERY number you state appears verbatim in the tool
results (or is a sum/mean/percentage of numbers that do). If you cannot support
the claim, say so plainly — do not guess.
The grounding report was: {report}
"""

SYSTEM_PROMPT = """You are the analytics assistant for ABI, an e-commerce business-intelligence platform.
Answer the user's question ONLY from the tool results you gather. Rules:

1. Use tools for every number you state. Never invent figures.
2. Tool results carry a provenance label: batch (whole historical dataset),
   live_replay (the accelerated replay — never additive with batch),
   registry (model metadata), predictions (persisted model scores — the only
   source of scores).
3. Never combine batch and live_replay figures in one total.
4. When a tool errors, answer honestly that the data is unavailable - do not
   guess.
5. Keep answers concise (2-6 sentences). State numbers as given by tools."""


@dataclass
class Agent:
    llm: Any
    dsn: str
    log: Any = field(default=logger)
    max_turns: int = MAX_TURNS
    max_revisions: int = MAX_REVISIONS
    ctx_factory: Any = field(default=None)  # Callable[[], ToolContext]; overrides dsn

    def _new_ctx(self) -> tools.ToolContext:
        if self.ctx_factory is not None:
            return self.ctx_factory()
        return tools.ToolContext(dsn=self.dsn, log=self.log)

    def answer(self, question: str) -> dict[str, Any]:
        ctx = self._new_ctx()
        observations: list[tools.ToolObservation] = []
        tool_uses: list[dict[str, Any]] = []
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": question},
        ]
        spec = tools.spec_dicts()
        refused = False
        final_txt: str | None = None
        turns = 0
        revisions = 0

        for turn in range(self.max_turns):
            turns = turn + 1
            try:
                result = self.llm.chat(messages, tools=spec)
            except LLMError as e:
                return self._refuse(question, f"the model backend is unavailable: {e}", observations, tool_uses, turns)

            if result.tool_calls:
                for idx, call in enumerate(result.tool_calls):
                    cid = f"call_{turn}_{idx}"
                    obs, use = self._execute(ctx, call.name, call.arguments)
                    observations.append(obs)
                    tool_uses.append(use)
                    messages.append(
                        {
                            "role": "assistant",
                            "content": None,
                            "tool_calls": [
                                {
                                    "id": cid,
                                    "type": "function",
                                    "function": {"name": call.name, "arguments": json.dumps(call.arguments)},
                                }
                            ],
                        }
                    )
                    messages.append({"role": "tool", "tool_call_id": cid, "content": obs.render})
                continue

            text = (result.content or "").strip()
            if not text:
                messages.append({"role": "assistant", "content": "I need more information; let me query the data."})
                continue

            report = grounding.ground_answer(text, observations)
            score_report = grounding.score_claims_grounded(text, observations)
            if report.grounded and score_report.grounded:
                final_txt = text
                break
            reasons = " | ".join(x.reason for x in (report, score_report) if not x.grounded)
            if revisions < self.max_revisions:
                revisions += 1
                messages.append({"role": "assistant", "content": text})
                messages.append({"role": "user", "content": REVISION_PROMPT.format(report=reasons)})
                continue
            final_txt = text
            refused = True
            break

        answer_txt = final_txt or ""
        if refused:
            if not answer_txt:
                answer_txt = "I could not ground that answer; please try a more specific question."
            answer_txt = (
                "I'm unable to confirm that answer from the data — some claims could not be grounded "
                "in the tool results, so I won't guess. " + answer_txt
            )

        return {
            "question": question,
            "answer": answer_txt,
            "grounded": not refused,
            "refused": refused,
            "revisions": revisions,
            "turns": turns,
            "tool_uses": tool_uses,
            "sources": sorted({u["label"] for u in tool_uses}),
        }

    def _execute(
        self, ctx: tools.ToolContext, name: str, params: dict[str, Any]
    ) -> tuple[tools.ToolObservation, dict[str, Any]]:
        spec = tools.TOOL_BY_NAME.get(name)
        if spec is None:
            obs = tools.ToolObservation(tool=name, params=params, label="?", error=f"unknown tool {name}")
            obs.render = tools.render_observation(obs)
            use = {"tool": name, "params": params, "label": "?", "note": obs.error}
            return obs, use
        try:
            rows = spec.run(ctx, params)
            obs = tools.ToolObservation(tool=name, params=params, label=spec.label, rows=rows, note="")
            obs.render = tools.render_observation(obs)
            use = {"tool": name, "params": params, "label": spec.label, "note": ""}
        except Exception as e:  # noqa: BLE001 - surfaced honestly to the model
            note = f"tool failed: {e}"
            obs = tools.ToolObservation(tool=name, params=params, label=spec.label, rows=[], error=note)
            obs.render = tools.render_observation(obs)
            use = {"tool": name, "params": params, "label": spec.label, "note": note}
            logger.warning("agent tool %s failed: %s", name, e)
        return obs, use

    @staticmethod
    def _refuse(
        question: str,
        reason: str,
        observations: list[tools.ToolObservation],
        tool_uses: list[dict[str, Any]],
        turns: int,
    ) -> dict[str, Any]:
        return {
            "question": question,
            "answer": f"I can't answer that right now: {reason}",
            "grounded": False,
            "refused": True,
            "revisions": 0,
            "turns": turns,
            "tool_uses": tool_uses,
            "sources": sorted({u["label"] for u in tool_uses}),
        }


def from_env() -> Agent:
    """Build the live agent from ABI_LLM_BASE_URL / ABI_LLM_MODEL."""
    base = os.getenv("ABI_LLM_BASE_URL", "http://127.0.0.1:11434")
    model = os.getenv("ABI_LLM_MODEL", "qwen2.5:7b-instruct")
    dsn = os.getenv("DATABASE_URL", "postgresql://abi:abi@localhost:5432/abi")
    return Agent(llm=LLMClient(base, model), dsn=dsn)


def mock_from_env() -> Agent:
    """Build an agent whose LLM is the deterministic eval harness.

    The script map comes from ml/agent/eval_questions.json (the same file the
    eval regression gates on), so `python ml/agent/server.py --mode mock`
    answers the eval questions verbatim with grounded, deterministic answers
    and refuses everything else with a stable fallback — no Ollama required.
    """
    dsn = os.getenv("DATABASE_URL", "postgresql://abi:abi@localhost:5432/abi")
    script: dict[str, list[dict[str, Any]]] = {}
    questions_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "eval_questions.json")
    try:
        with open(questions_path, encoding="utf-8") as f:
            data = json.load(f)
        for entry in data["questions"]:
            script[entry["question"][:120]] = entry.get("script", [])
    except (OSError, KeyError, TypeError, json.JSONDecodeError) as e:
        logger.warning("mock mode: could not load eval scripts (%s); using empty script map", e)
    return Agent(
        llm=MockLLM(script, fallback_answer="I don't have data for that."),
        dsn=dsn,
    )