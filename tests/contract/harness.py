from __future__ import annotations

import json
import os
import shlex
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import requests


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_HOST = "127.0.0.1"
DEFAULT_STARTUP_TIMEOUT_SECONDS = 15.0


def split_command(raw: str) -> list[str]:
    return shlex.split(raw, posix=os.name != "nt")


def resolve_contract_target() -> str:
    target = (os.getenv("CHATMOCK_CONTRACT_TARGET") or "python").strip().lower()
    return target or "python"


def resolve_cli_command() -> list[str] | None:
    raw = (os.getenv("CHATMOCK_CLI_CMD") or "").strip()
    if not raw:
        return None
    return split_command(raw)


def resolve_server_command() -> list[str] | None:
    raw = (os.getenv("CHATMOCK_SERVER_CMD") or "").strip()
    if not raw:
        return None
    return split_command(raw)


def resolve_base_url() -> str | None:
    value = (os.getenv("CHATMOCK_BASE_URL") or "").strip().rstrip("/")
    return value or None


def missing_server_reason(target: str | None = None) -> str:
    selected = (target or resolve_contract_target()).strip().lower() or "python"
    if selected == "rust":
        return "Rust target requested but CHATMOCK_BASE_URL or CHATMOCK_SERVER_CMD is not configured."
    return "Contract tests require CHATMOCK_BASE_URL or CHATMOCK_SERVER_CMD."


def require_contract_configuration() -> None:
    if resolve_base_url() or resolve_server_command():
        return
    raise unittest.SkipTest(missing_server_reason())


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((DEFAULT_HOST, 0))
        return int(sock.getsockname()[1])


def _write_fake_auth(home_dir: Path) -> None:
    home_dir.mkdir(parents=True, exist_ok=True)
    auth_path = home_dir / "auth.json"
    auth_path.write_text(
        json.dumps(
            {
                "OPENAI_API_KEY": "contract-api-key",
                "tokens": {
                    "access_token": "contract-access-token",
                    "account_id": "contract-account-id",
                    "id_token": "contract-id-token",
                },
            }
        ),
        encoding="utf-8",
    )


def _extract_text_fragments(payload: Any) -> list[str]:
    fragments: list[str] = []
    if isinstance(payload, str):
        fragments.append(payload)
        return fragments
    if isinstance(payload, list):
        for item in payload:
            fragments.extend(_extract_text_fragments(item))
        return fragments
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key == "text" and isinstance(value, str):
                fragments.append(value)
            else:
                fragments.extend(_extract_text_fragments(value))
    return fragments


class _ThreadedHTTPServer(HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class _FakeUpstreamState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._requests: list[dict[str, Any]] = []

    def record(self, payload: dict[str, Any]) -> None:
        with self._lock:
            self._requests.append(payload)

    def last_request(self) -> dict[str, Any] | None:
        with self._lock:
            if not self._requests:
                return None
            return dict(self._requests[-1])


class _FakeUpstreamHandler(BaseHTTPRequestHandler):
    server_version = "ChatMockContractFakeUpstream/1.0"

    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_POST(self) -> None:  # noqa: N802
        content_length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(content_length)
        payload = json.loads(raw_body.decode("utf-8"))
        self.server.state.record(  # type: ignore[attr-defined]
            {
                "path": self.path,
                "headers": dict(self.headers.items()),
                "json": payload,
            }
        )

        input_text = " ".join(_extract_text_fragments(payload.get("input")))
        if "contract-upstream-error" in input_text:
            body = (
                "gateway meltdown before JSON. "
                "Authorization: Bearer auth-secret-123. "
                "Bearer bearer-secret-456. "
                "sk-live-super-secret-789. "
                "session_id=session-secret-abc. "
                "access_token=access-secret-def. "
                "token=token-secret-ghi."
            )
            encoded = body.encode("utf-8")
            self.send_response(502)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)
            self.wfile.flush()
            return

        if "contract-responses-backfill" in input_text:
            events = [
                {
                    "type": "response.created",
                    "response": {"id": "resp_contract_backfill", "object": "response", "status": "in_progress"},
                },
                {
                    "type": "response.output_item.done",
                    "item": {
                        "type": "message",
                        "role": "assistant",
                        "id": "msg_contract_backfill",
                        "content": [{"type": "output_text", "text": "assistant output"}],
                    },
                },
                {
                    "type": "response.completed",
                    "response": {
                        "id": "resp_contract_backfill",
                        "object": "response",
                        "status": "completed",
                        "output": [],
                    },
                },
            ]
        else:
            events = [
                {"type": "response.output_text.delta", "delta": "hello from contract upstream"},
                {"type": "response.completed", "response": {"id": "resp_contract_chat"}},
            ]

        body = b"".join(
            [f"data: {json.dumps(event)}\n\n".encode("utf-8") for event in events] + [b"data: [DONE]\n\n"]
        )
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()


@dataclass
class FakeUpstreamServer:
    server: _ThreadedHTTPServer
    thread: threading.Thread
    state: _FakeUpstreamState
    url: str

    @classmethod
    def start(cls) -> "FakeUpstreamServer":
        state = _FakeUpstreamState()
        port = _pick_free_port()
        server = _ThreadedHTTPServer((DEFAULT_HOST, port), _FakeUpstreamHandler)
        server.state = state  # type: ignore[attr-defined]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        return cls(server=server, thread=thread, state=state, url=f"http://{DEFAULT_HOST}:{port}")

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def last_request(self) -> dict[str, Any] | None:
        return self.state.last_request()


@dataclass
class ContractRuntime:
    base_url: str
    target: str
    fake_upstream: FakeUpstreamServer | None = None
    process: subprocess.Popen[str] | None = None
    temp_dir: tempfile.TemporaryDirectory[str] | None = None
    log_path: Path | None = None
    _log_handle: Any | None = None

    @classmethod
    def start_from_env(cls) -> "ContractRuntime":
        require_contract_configuration()

        base_url = resolve_base_url()
        server_cmd = resolve_server_command()
        if base_url and not server_cmd:
            return cls(base_url=base_url, target=resolve_contract_target())

        fake_upstream = FakeUpstreamServer.start()
        temp_dir = tempfile.TemporaryDirectory(prefix="chatmock-contract-")
        temp_path = Path(temp_dir.name)
        log_path = temp_path / "server.log"
        log_handle = log_path.open("w", encoding="utf-8")
        auth_home = temp_path / "auth-home"
        _write_fake_auth(auth_home)

        port = _pick_free_port()
        runtime = cls(
            base_url=f"http://{DEFAULT_HOST}:{port}",
            target=resolve_contract_target(),
            fake_upstream=fake_upstream,
            temp_dir=temp_dir,
            log_path=log_path,
            _log_handle=log_handle,
        )

        if server_cmd is None:
            raise unittest.SkipTest(missing_server_reason(runtime.target))

        environment = os.environ.copy()
        environment.update(
            {
                "CHATGPT_LOCAL_HOME": str(auth_home),
                "CHATGPT_RESPONSES_URL": f"{fake_upstream.url}/backend-api/codex/responses",
                "PYTHONUNBUFFERED": "1",
            }
        )
        command = list(server_cmd) + ["--host", DEFAULT_HOST, "--port", str(port)]
        runtime.process = subprocess.Popen(
            command,
            cwd=str(REPO_ROOT),
            env=environment,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        runtime._wait_until_ready()
        return runtime

    def close(self) -> None:
        if self.process is not None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        if self._log_handle is not None:
            self._log_handle.close()
        if self.fake_upstream is not None:
            self.fake_upstream.close()
        if self.temp_dir is not None:
            self.temp_dir.cleanup()

    def get(self, path: str) -> requests.Response:
        return requests.get(f"{self.base_url}{path}", timeout=10)

    def post(self, path: str, payload: dict[str, Any]) -> requests.Response:
        return requests.post(f"{self.base_url}{path}", json=payload, timeout=10)

    def _wait_until_ready(self) -> None:
        deadline = time.time() + DEFAULT_STARTUP_TIMEOUT_SECONDS
        last_error: Exception | None = None
        while time.time() < deadline:
            if self.process is not None and self.process.poll() is not None:
                raise AssertionError(self._startup_failure_message())
            try:
                response = self.get("/v1/models")
                if response.status_code == 200:
                    return
            except Exception as exc:  # pragma: no cover - startup polling only
                last_error = exc
            time.sleep(0.25)
        raise AssertionError(self._startup_failure_message(last_error))

    def _startup_failure_message(self, last_error: Exception | None = None) -> str:
        details = ""
        if self.log_path is not None and self.log_path.exists():
            details = self.log_path.read_text(encoding="utf-8")
        message = "Contract server failed to start."
        if last_error is not None:
            message += f" Last error: {last_error!r}."
        if details:
            message += f" Logs:\n{details}"
        return message