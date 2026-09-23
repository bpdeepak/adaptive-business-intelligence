"""Phase 3 agentic NL-BI (3C): a grounded, tool-using conversational agent over
the ABI semantic layer, backed by a local free LLM (Ollama / Qwen2.5) or the
deterministic mock harness used in tests.

Guardrails (see module docstrings):
* fixed curated tools, provenance-labeled (batch / live_replay / registry /
  predictions) — no free-form SQL, batch and replay never addable;
* model scores only ever come from `model_scores` (gold.predictions);
* tool failures surface as honest refusals;
* every numeric claim is checked by grounding v2 (literal / derived /
  prose-constants, never string equality);
* exactly one bounded revision pass before a grounded=false refusal.
"""

from . import tools, grounding, llm

__all__ = ["tools", "grounding", "llm"]