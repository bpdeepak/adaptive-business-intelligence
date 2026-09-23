"""Phase 3 agent service (stdlib HTTP, mirrors the model sidecar pattern).

    GET  /health   -> {"status":"ok","mode":"live"|"mock"}
    POST /query    -> {"question": ...} => the full response contract:
                      {"question","answer","grounded","refused","revisions",
                       "turns","tool_uses","sources"}

The LLM backend defaults to a local Ollama (ABI_LLM_BASE_URL / ABI_LLM_MODEL).
Run with `--mode mock` (or ABI_AGENT_MODE=mock) to use the deterministic
harness instead — it serves the eval_questions.json scripts, so smoke tests
need no installed model.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import pathlib
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

# The sidecar is documented to run as a plain script (`python ml/agent/server.py
# --mode mock`) AND as a module (`python -m ml.agent.server`). When invoked as
# a script, the package-relative imports below have no parent package, so put
# the repo root on sys.path and reference the package absolutely.
if __package__ in (None, ""):
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))
    import ml.agent.agent as _agent_mod
    from ml.agent.agent import Agent
else:
    from . import agent as _agent_mod
    from .agent import Agent

ADDR = os.getenv("ABI_AGENT_ADDR", "127.0.0.1:8094")
MODE = os.getenv("ABI_AGENT_MODE", "live")

_LOG = logging.getLogger("abi.agent.server")


class _Handler(BaseHTTPRequestHandler):
    agent: Agent
    agent_mode: str = MODE  # overridden at bind time from the CLI/env setting

    def log_message(self, fmt: str, *args: Any) -> None:  # quieter logs
        # The stdlib BaseHTTPRequestHandler writes request lines to stderr;
        # route them through a logger instead of the (missing) server method.
        _LOG.debug("request: " + (fmt % args if args else fmt))

    def do_GET(self) -> None:  # noqa: N802
        if self.path.startswith("/health"):
            self._json(200, {"status": "ok", "mode": self.agent_mode})
            return
        self._json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path.startswith("/query"):
            try:
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length) or b"{}")
                question = str(body.get("question", "")).strip()
            except (ValueError, json.JSONDecodeError) as e:
                self._json(400, {"error": f"bad body: {e}"})
                return
            if not question:
                self._json(400, {"error": "missing question"})
                return
            try:
                resp = self.agent.answer(question)
            except Exception as e:  # noqa: BLE001 - honest tool failure
                self._json(200, {
                    "question": question,
                    "answer": f"I can't answer that right now: the agent pipeline failed ({e}).",
                    "grounded": False,
                    "refused": True,
                    "revisions": 0,
                    "turns": 0,
                    "tool_uses": [],
                    "sources": [],
                })
                return
            self._json(200, resp)
            return
        self._json(404, {"error": "not found"})

    def _json(self, code: int, payload: dict[str, Any]) -> None:
        data = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def main() -> None:
    ap = argparse.ArgumentParser(description="Phase 3 agent HTTP service")
    ap.add_argument("--addr", default=ADDR)
    ap.add_argument("--mode", choices=["live", "mock"], default=MODE)
    args = ap.parse_args()

    if args.mode == "live":
        agent = _agent_mod.from_env()
    else:
        agent = _agent_mod.mock_from_env()

    host, _, port = args.addr.rpartition(":")
    server = ThreadingHTTPServer((host or "127.0.0.1", int(port)), _Handler)
    _Handler.agent = agent
    _Handler.agent_mode = args.mode
    print(f"agent service on {args.addr} (mode={args.mode})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()