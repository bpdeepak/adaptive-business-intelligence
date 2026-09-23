"""Regression for the agent HTTP sidecar (ml/agent/server.py).

The sidecar is the serving contract the Go API proxies: /health must answer
without crashing the request thread, and /query must return the full response
contract with grounded/refused/revisions/turns/tool_uses/sources. These tests
spin the stdlib server on an ephemeral port in a thread and hit it over real
HTTP. The three bugs they pin down were all found at live run:
  * log_message crashing every request (ThreadingHTTPServer has no log_message),
  * mock mode answering with empty text (empty script map + no fallback),
  * /health reporting the env mode instead of the CLI-selected mode.
"""
from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

from ml.agent import server as server_mod
from ml.agent.llm import MockLLM


class _Fixture:
    def __init__(self) -> None:
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), server_mod._Handler)
        agent = server_mod._agent_mod.mock_from_env()  # use the real mock loader
        server_mod._Handler.agent = agent
        server_mod._Handler.agent_mode = "mock"
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.httpd.server_address[1]}"

    def get(self, path: str) -> dict:
        with urllib.request.urlopen(self.base + path, timeout=10) as resp:
            return json.loads(resp.read().decode())

    def post_json(self, path: str, payload: dict) -> dict:
        req = urllib.request.Request(
            self.base + path,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode())

    def close(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


def _question_by_id(qid: str) -> str:
    import json as _json
    import pathlib

    path = pathlib.Path(__file__).resolve().parents[1] / "agent" / "eval_questions.json"
    entries = _json.loads(path.read_text(encoding="utf-8"))["questions"]
    return next(e["question"] for e in entries if e["id"] == qid)


def test_health_reports_mode_without_crashing() -> None:
    fix = _Fixture()
    try:
        body = fix.get("/health")
        assert body == {"status": "ok", "mode": "mock"}
    finally:
        fix.close()


def test_query_grounded_answer_contract() -> None:
    fix = _Fixture()
    try:
        body = fix.post_json("/query", {"question": _question_by_id("q01")})
        assert body["grounded"] is True
        assert body["refused"] is False
        assert "99441" in body["answer"]
        assert body["turns"] == 2
        assert body["sources"] == ["batch"]
        assert body["tool_uses"][0]["label"] == "batch"
    finally:
        fix.close()


def test_query_batch_live_mix_refused() -> None:
    fix = _Fixture()
    try:
        body = fix.post_json("/query", {"question": _question_by_id("q17")})
        assert body["grounded"] is False
        assert body["refused"] is True
        assert body["revisions"] <= 1
        assert {"batch", "live_replay"} <= set(body["sources"])
    finally:
        fix.close()


def test_query_empty_question_400() -> None:
    fix = _Fixture()
    try:
        req = urllib.request.Request(
            fix.base + "/query",
            data=b'{"question":""}',
            headers={"Content-Type": "application/json"},
        )
        try:
            urllib.request.urlopen(req, timeout=10)
            raise AssertionError("expected HTTP 400")
        except urllib.error.HTTPError as e:
            assert e.code == 400
    finally:
        fix.close()


def test_query_unknown_question_does_not_crash() -> None:
    # Any question without a mock script must yield a stable answer (the
    # fallback), never a hang or an empty-text loop.
    fix = _Fixture()
    try:
        body = fix.post_json("/query", {"question": "What is the meaning of life, the universe, and everything?"})
        assert body["answer"]  # non-empty
        assert isinstance(body["grounded"], bool)
        assert isinstance(body["refused"], bool)
    finally:
        fix.close()