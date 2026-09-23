"""OpenAI-compatible chat client for the Phase 3 agent, plus the deterministic
mock harness used by the eval regression tests (no Ollama required in CI).

The default target is Ollama's OpenAI endpoint (/v1/chat/completions) serving a
local free model such as qwen2.5:7b-instruct. The mock implements the same
``chat()`` contract so the agent loop, tools and grounder are exercised
identically in CI and live.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ChatToolCall:
    name: str
    arguments: dict[str, Any]


@dataclass
class ChatResult:
    content: str = ""
    tool_calls: list[ChatToolCall] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)


class LLMError(RuntimeError):
    """Raised when the LLM backend is unreachable or misbehaves."""


class LLMClient:
    """Thin urllib client for POST {base}/v1/chat/completions (OpenAI shape)."""

    def __init__(self, base_url: str, model: str, timeout: float = 180.0):
        base_url = (base_url or "").rstrip("/")
        if not base_url:
            raise ValueError("ABI_LLM_BASE_URL is required")
        self.base_url = base_url
        self.model = model
        self.timeout = timeout

    def chat(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        max_tokens: int = 900,
        temperature: float = 0.1,
    ) -> ChatResult:
        body: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": False,
        }
        if tools:
            body["tools"] = tools
            body["tool_choice"] = "auto"

        payload = json.dumps(body).encode("utf-8")
        url = f"{self.base_url}/v1/chat/completions"
        req = urllib.request.Request(
            url, data=payload, headers={"Content-Type": "application/json"}
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            raise LLMError(f"LLM HTTP {e.code}: {e.read().decode('utf-8', 'replace')[:300]}") from e
        except (urllib.error.URLError, TimeoutError) as e:
            raise LLMError(f"LLM unreachable at {url}: {e}") from e

        return parse_chat_result(data)


def parse_chat_result(data: dict[str, Any]) -> ChatResult:
    """Extract text + tool calls from a chat completion payload."""
    choice = (data.get("choices") or [{}])[0]
    message = choice.get("message") or {}
    content = message.get("content") or ""
    calls: list[ChatToolCall] = []
    for tc in message.get("tool_calls") or []:
        fn = tc.get("function") or {}
        name = fn.get("name", "")
        args: dict[str, Any] = {}
        raw_args = fn.get("arguments")
        if isinstance(raw_args, str) and raw_args.strip():
            try:
                args = json.loads(raw_args)
            except json.JSONDecodeError:
                args = {"_raw": raw_args}
        elif isinstance(raw_args, dict):
            args = raw_args
        calls.append(ChatToolCall(name=name, arguments=args))
    return ChatResult(content=content, tool_calls=calls, raw=data)


class MockLLM:
    """Deterministic chat backend driven by a script.

    ``script`` maps a normalized question key to a list of steps. Each step is a
    ChatResult-equivalent:
      * {"tool": name, "params": {...}}   -> a tool call to emit
      * {"answer": "text"}                -> a final answer
      * {"answer_template": "total={total_orders}"}
          -> an answer whose {placeholders} are filled from the values the
             tools returned (parsed from the tool messages in the conversation),
             so the final answer is *actually grounded* in observed rows.
    Once the script is exhausted it repeats its last answer step — that is what
    makes the one-revision-pass refusal path deterministic (the model persists
    in its claim after the revision prompt, so grounding refuses it).

    Used by the eval harness so the regression test exercises the full agent
    loop (tool execution + grounding + revision) without Ollama.
    """

    def __init__(self, script: dict[str, list[dict[str, Any]]], fallback_answer: str = ""):
        self.script = script
        self.fallback_answer = fallback_answer
        self.calls: list[dict[str, Any]] = []

    def chat(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        max_tokens: int = 900,
        temperature: float = 0.1,
    ) -> ChatResult:
        self.calls.append({"messages": messages, "tools": tools})
        key = self._key(messages)
        steps = self.script.get(key, [])
        pos = self._pos_for(messages)
        if pos < len(steps):
            step = steps[pos]
            if "tool" in step:
                return ChatResult(tool_calls=[ChatToolCall(step["tool"], step.get("params", {}))])
            return ChatResult(content=self._resolve_answer(step, messages))
        # Script exhausted: repeat the last answer (or refuse) — deterministic
        # persistence used to exercise the bounded-revision refusal path.
        for step in reversed(steps):
            if "answer" in step or "answer_template" in step:
                return ChatResult(content=self._resolve_answer(step, messages))
        return ChatResult(content=self.fallback_answer)

    @staticmethod
    def _resolve_answer(step: dict[str, Any], messages: list[dict[str, Any]]) -> str:
        template = step.get("answer_template")
        if not template:
            return step.get("answer", "")
        values: dict[str, Any] = {}
        for m in messages:
            if m.get("role") != "tool":
                continue
            for line in (m.get("content") or "").splitlines():
                if "=" not in line or line.lstrip().startswith("[tool"):
                    continue
                for token in line.split(","):
                    if "=" not in token:
                        continue
                    k, _, v = token.partition("=")
                    k = k.strip()
                    v = v.strip()
                    # Skip empty / NULL column pads from union-row renders so a
                    # real value from a later row wins (setdefault order).
                    if k and v and v not in ("None", "null", "NULL"):
                        values.setdefault(k, v)
        try:
            return template.format(**values)
        except (KeyError, ValueError):
            return template

    @staticmethod
    def _key(messages: list[dict[str, Any]]) -> str:
        for m in messages:
            if m.get("role") == "user":
                return m.get("content", "")[:120]
        return ""

    def _pos_for(self, messages: list[dict[str, Any]]) -> int:
        # Every assistant message contributes one consumed step (a tool call or
        # an answer); count them to derive the current position.
        return sum(1 for m in messages if m.get("role") == "assistant")