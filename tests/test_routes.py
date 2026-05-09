from __future__ import annotations

import json
import socket
import sys
import threading
import time
import unittest
from unittest.mock import patch

import chatmock.cli as cli
import chatmock.responses_api as responses_api
from chatmock.app import create_app
from chatmock.session import reset_session_state
from websockets.sync.client import connect as ws_connect


class FakeUpstream:
    def __init__(
        self,
        events: list[dict[str, object]] | None = None,
        *,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        content: bytes | None = None,
        text: str = "",
    ) -> None:
        self._events = events
        self.status_code = status_code
        self.headers = headers or {}
        self.content = content or b""
        self.text = text

    def iter_lines(self, decode_unicode: bool = False):
        for event in self._events or []:
            payload = f"data: {json.dumps(event)}"
            yield payload if decode_unicode else payload.encode("utf-8")

    def iter_content(self, chunk_size=None):
        if self.content:
            yield self.content
            return
        for event in self._events or []:
            payload = f"data: {json.dumps(event)}\n\n".encode("utf-8")
            yield payload

    def json(self):
        return json.loads(self.content.decode("utf-8"))

    def close(self) -> None:
        return None


class StartupModeTests(unittest.TestCase):
    def test_create_app_defaults_base_instructions_mode(self) -> None:
        app = create_app()
        self.assertEqual(app.config["BASE_INSTRUCTIONS_MODE"], "fallback")

    def test_create_app_accepts_explicit_base_instructions_modes(self) -> None:
        for mode in ("always", "fallback", "off"):
            with self.subTest(mode=mode):
                app = create_app(base_instructions_mode=mode)
                self.assertEqual(app.config["BASE_INSTRUCTIONS_MODE"], mode)

    @patch("chatmock.cli.cmd_serve", return_value=0)
    def test_cli_serve_defaults_base_instructions_mode(self, mock_cmd_serve) -> None:
        with patch.object(sys, "argv", ["chatmock", "serve"]):
            with self.assertRaises(SystemExit) as raised:
                cli.main()

        self.assertEqual(raised.exception.code, 0)
        self.assertEqual(mock_cmd_serve.call_args.kwargs["base_instructions_mode"], "fallback")

    @patch("chatmock.cli.cmd_serve", return_value=0)
    def test_cli_serve_accepts_explicit_base_instructions_modes(self, mock_cmd_serve) -> None:
        for mode in ("always", "fallback", "off"):
            with self.subTest(mode=mode):
                with patch.object(sys, "argv", ["chatmock", "serve", "--base-instructions-mode", mode]):
                    with self.assertRaises(SystemExit) as raised:
                        cli.main()

                self.assertEqual(raised.exception.code, 0)
                self.assertEqual(mock_cmd_serve.call_args.kwargs["base_instructions_mode"], mode)

    @patch("chatmock.cli.cmd_serve", return_value=0)
    def test_cli_serve_rejects_invalid_base_instructions_mode(self, mock_cmd_serve) -> None:
        with patch.object(sys, "argv", ["chatmock", "serve", "--base-instructions-mode", "invalid"]):
            with self.assertRaises(SystemExit) as raised:
                cli.main()

        self.assertEqual(raised.exception.code, 2)
        mock_cmd_serve.assert_not_called()
class RouteTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_session_state()
        self.app = create_app()
        self.client = self.app.test_client()

    def test_openai_models_list(self) -> None:
        response = self.client.get("/v1/models")
        body = response.get_json()
        self.assertEqual(response.status_code, 200)
        model_ids = [item["id"] for item in body["data"]]
        self.assertIn("gpt-5.4", model_ids)
        self.assertIn("gpt-5.4-mini", model_ids)
        self.assertIn("gpt-5.3-codex-spark", model_ids)

    def test_ollama_tags_list(self) -> None:
        response = self.client.get("/api/tags")
        body = response.get_json()
        self.assertEqual(response.status_code, 200)
        model_names = [item["name"] for item in body["models"]]
        self.assertIn("gpt-5.4", model_names)
        self.assertIn("gpt-5.4-mini", model_names)

    @patch("chatmock.routes_openai.start_upstream_request")
    def test_chat_completions(self, mock_start) -> None:
        mock_start.return_value = (
            FakeUpstream(
                [
                    {"type": "response.output_text.delta", "delta": "hello"},
                    {"type": "response.completed", "response": {"id": "resp-openai"}},
                ]
            ),
            None,
        )
        response = self.client.post(
            "/v1/chat/completions",
            json={"model": "gpt5.4-mini", "messages": [{"role": "user", "content": "hi"}]},
        )
        body = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(body["choices"][0]["message"]["content"], "hello")
        self.assertEqual(body["model"], "gpt5.4-mini")

    @patch("chatmock.routes_openai.start_upstream_request")
    def test_completions_always_mode_injects_builtin_instructions(self, mock_start) -> None:
        app = create_app(base_instructions_mode="always")
        app.config["BASE_INSTRUCTIONS"] = "built-in completions instructions"
        client = app.test_client()
        mock_start.return_value = (
            FakeUpstream(
                [
                    {"type": "response.output_text.delta", "delta": "hello"},
                    {"type": "response.completed", "response": {"id": "resp-openai"}},
                ]
            ),
            None,
        )

        response = client.post(
            "/v1/completions",
            json={"model": "gpt-5.4", "prompt": "hi"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            mock_start.call_args.kwargs["instructions"],
            "built-in completions instructions",
        )

    @patch("chatmock.routes_openai.start_upstream_request")
    def test_completions_fallback_mode_injects_builtin_instructions(self, mock_start) -> None:
        app = create_app(base_instructions_mode="fallback")
        app.config["BASE_INSTRUCTIONS"] = "built-in completions instructions"
        client = app.test_client()
        mock_start.return_value = (
            FakeUpstream(
                [
                    {"type": "response.output_text.delta", "delta": "hello"},
                    {"type": "response.completed", "response": {"id": "resp-openai"}},
                ]
            ),
            None,
        )

        response = client.post(
            "/v1/completions",
            json={"model": "gpt-5.4", "prompt": "hi"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            mock_start.call_args.kwargs["instructions"],
            "built-in completions instructions",
        )

    @patch("chatmock.routes_openai.start_upstream_request")
    def test_completions_off_mode_sends_no_builtin_instructions(self, mock_start) -> None:
        app = create_app(base_instructions_mode="off")
        app.config["BASE_INSTRUCTIONS"] = "built-in completions instructions"
        client = app.test_client()
        mock_start.return_value = (
            FakeUpstream(
                [
                    {"type": "response.output_text.delta", "delta": "hello"},
                    {"type": "response.completed", "response": {"id": "resp-openai"}},
                ]
            ),
            None,
        )

        response = client.post(
            "/v1/completions",
            json={"model": "gpt-5.4", "prompt": "hi"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(mock_start.call_args.kwargs["instructions"])

    def test_instructions_for_model_returns_none_when_builtin_instructions_unset(self) -> None:
        instructions = responses_api.instructions_for_model(
            {
                "BASE_INSTRUCTIONS": None,
                "GPT5_CODEX_INSTRUCTIONS": None,
            },
            "gpt-5.4",
        )

        self.assertIsNone(instructions)

    def test_instructions_for_model_codex_returns_none_when_builtin_instructions_unset(self) -> None:
        instructions = responses_api.instructions_for_model(
            {
                "BASE_INSTRUCTIONS": None,
                "GPT5_CODEX_INSTRUCTIONS": None,
            },
            "gpt-5.3-codex",
        )

        self.assertIsNone(instructions)

    @patch("chatmock.routes_openai.start_upstream_request")
    def test_chat_completions_always_mode_injects_builtin_instructions(self, mock_start) -> None:
        app = create_app(base_instructions_mode="always")
        app.config["BASE_INSTRUCTIONS"] = "built-in openai instructions"
        client = app.test_client()
        mock_start.return_value = (
            FakeUpstream(
                [
                    {"type": "response.output_text.delta", "delta": "hello"},
                    {"type": "response.completed", "response": {"id": "resp-openai"}},
                ]
            ),
            None,
        )

        response = client.post(
            "/v1/chat/completions",
            json={"model": "gpt-5.4", "messages": [{"role": "user", "content": "hi"}]},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            mock_start.call_args.kwargs["instructions"],
            "built-in openai instructions",
        )

    @patch("chatmock.routes_openai.start_upstream_request")
    def test_chat_completions_always_mode_injects_builtin_instructions_even_with_system_message(
        self, mock_start
    ) -> None:
        app = create_app(base_instructions_mode="always")
        app.config["BASE_INSTRUCTIONS"] = "built-in openai instructions"
        client = app.test_client()
        mock_start.return_value = (
            FakeUpstream(
                [
                    {"type": "response.output_text.delta", "delta": "hello"},
                    {"type": "response.completed", "response": {"id": "resp-openai"}},
                ]
            ),
            None,
        )

        response = client.post(
            "/v1/chat/completions",
            json={
                "model": "gpt-5.4",
                "messages": [
                    {"role": "system", "content": "client system prompt"},
                    {"role": "user", "content": "hi"},
                ],
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            mock_start.call_args.kwargs["instructions"],
            "built-in openai instructions",
        )
        self.assertEqual(
            mock_start.call_args.args[1],
            [
                {
                    "type": "message",
                    "role": "system",
                    "content": [{"type": "input_text", "text": "client system prompt"}],
                },
                {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "hi"}],
                },
            ],
        )

    @patch("chatmock.routes_openai.start_upstream_request")
    def test_chat_completions_fallback_mode_injects_builtin_instructions_without_system_message(
        self, mock_start
    ) -> None:
        app = create_app(base_instructions_mode="fallback")
        app.config["BASE_INSTRUCTIONS"] = "built-in openai instructions"
        client = app.test_client()
        mock_start.return_value = (
            FakeUpstream(
                [
                    {"type": "response.output_text.delta", "delta": "hello"},
                    {"type": "response.completed", "response": {"id": "resp-openai"}},
                ]
            ),
            None,
        )

        response = client.post(
            "/v1/chat/completions",
            json={"model": "gpt-5.4", "messages": [{"role": "user", "content": "hi"}]},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            mock_start.call_args.kwargs["instructions"],
            "built-in openai instructions",
        )

    @patch("chatmock.routes_openai.start_upstream_request")
    def test_chat_completions_fallback_mode_skips_builtin_instructions_for_system_message(self, mock_start) -> None:
        app = create_app(base_instructions_mode="fallback")
        app.config["BASE_INSTRUCTIONS"] = "built-in openai instructions"
        client = app.test_client()
        mock_start.return_value = (
            FakeUpstream(
                [
                    {"type": "response.output_text.delta", "delta": "hello"},
                    {"type": "response.completed", "response": {"id": "resp-openai"}},
                ]
            ),
            None,
        )

        response = client.post(
            "/v1/chat/completions",
            json={
                "model": "gpt-5.4",
                "messages": [
                    {"role": "system", "content": "client system prompt"},
                    {"role": "user", "content": "hi"},
                ],
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(mock_start.call_args.kwargs["instructions"])

    @patch("chatmock.routes_openai.start_upstream_request")
    def test_chat_completions_off_mode_sends_no_builtin_instructions(self, mock_start) -> None:
        app = create_app(base_instructions_mode="off")
        app.config["BASE_INSTRUCTIONS"] = "built-in openai instructions"
        client = app.test_client()
        mock_start.return_value = (
            FakeUpstream(
                [
                    {"type": "response.output_text.delta", "delta": "hello"},
                    {"type": "response.completed", "response": {"id": "resp-openai"}},
                ]
            ),
            None,
        )

        response = client.post(
            "/v1/chat/completions",
            json={"model": "gpt-5.4", "messages": [{"role": "user", "content": "hi"}]},
        )

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(mock_start.call_args.kwargs["instructions"])

    @patch("chatmock.routes_openai.start_upstream_request")
    def test_chat_completions_preserves_upstream_json_error_object(self, mock_start) -> None:
        upstream_error = {
            "error": {
                "message": "Unknown tool: tool_search",
                "type": "invalid_request_error",
                "param": "tools[0].name",
                "code": "unknown_tool",
            }
        }
        mock_start.return_value = (
            FakeUpstream(
                status_code=400,
                content=json.dumps(upstream_error).encode("utf-8"),
                text=json.dumps(upstream_error),
            ),
            None,
        )

        response = self.client.post(
            "/v1/chat/completions",
            json={"model": "gpt-5.4", "messages": [{"role": "user", "content": "hi"}]},
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json(), upstream_error)

    @patch("chatmock.routes_openai.start_upstream_request")
    def test_chat_completions_preserve_unknown_model_id(self, mock_start) -> None:
        mock_start.return_value = (
            FakeUpstream(
                [
                    {"type": "response.output_text.delta", "delta": "hello"},
                    {"type": "response.completed", "response": {"id": "resp-openai"}},
                ]
            ),
            None,
        )
        requested_model = "unknown-model-xyz"

        response = self.client.post(
            "/v1/chat/completions",
            json={"model": requested_model, "messages": [{"role": "user", "content": "hi"}]},
        )

        self.assertEqual(response.status_code, 200)
        normalized_model = mock_start.call_args.args[0]
        self.assertEqual(normalized_model, requested_model)

    @patch("chatmock.routes_openai.start_upstream_request")
    def test_chat_completions_honors_debug_model_override(self, mock_start) -> None:
        app = create_app(debug_model="gpt-5.4")
        client = app.test_client()
        mock_start.return_value = (
            FakeUpstream(
                [
                    {"type": "response.output_text.delta", "delta": "hello"},
                    {"type": "response.completed", "response": {"id": "resp-openai"}},
                ]
            ),
            None,
        )
        response = client.post(
            "/v1/chat/completions",
            json={"model": "gpt-5.3-codex", "messages": [{"role": "user", "content": "hi"}]},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(mock_start.call_args.args[0], "gpt-5.4")

    @patch("chatmock.routes_ollama.start_upstream_request")
    def test_ollama_chat(self, mock_start) -> None:
        mock_start.return_value = (
            FakeUpstream(
                [
                    {"type": "response.output_text.delta", "delta": "hello"},
                    {"type": "response.completed"},
                ]
            ),
            None,
        )
        response = self.client.post(
            "/api/chat",
            json={"model": "gpt-5.4", "messages": [{"role": "user", "content": "hi"}], "stream": False},
        )
        body = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(body["message"]["content"], "hello")
        self.assertEqual(body["model"], "gpt-5.4")

    @patch("chatmock.routes_ollama.start_upstream_request")
    def test_ollama_chat_always_mode_injects_builtin_instructions(self, mock_start) -> None:
        app = create_app(base_instructions_mode="always")
        app.config["BASE_INSTRUCTIONS"] = "built-in ollama instructions"
        client = app.test_client()
        mock_start.return_value = (
            FakeUpstream(
                [
                    {"type": "response.output_text.delta", "delta": "hello"},
                    {"type": "response.completed"},
                ]
            ),
            None,
        )

        response = client.post(
            "/api/chat",
            json={"model": "gpt-5.4", "messages": [{"role": "user", "content": "hi"}], "stream": False},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            mock_start.call_args.kwargs["instructions"],
            "built-in ollama instructions",
        )

    @patch("chatmock.routes_ollama.start_upstream_request")
    def test_ollama_chat_always_mode_injects_builtin_instructions_even_with_system_message(
        self, mock_start
    ) -> None:
        app = create_app(base_instructions_mode="always")
        app.config["BASE_INSTRUCTIONS"] = "built-in ollama instructions"
        client = app.test_client()
        mock_start.return_value = (
            FakeUpstream(
                [
                    {"type": "response.output_text.delta", "delta": "hello"},
                    {"type": "response.completed"},
                ]
            ),
            None,
        )

        response = client.post(
            "/api/chat",
            json={
                "model": "gpt-5.4",
                "messages": [
                    {"role": "system", "content": "client system prompt"},
                    {"role": "user", "content": "hi"},
                ],
                "stream": False,
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            mock_start.call_args.kwargs["instructions"],
            "built-in ollama instructions",
        )
        self.assertEqual(
            mock_start.call_args.args[1],
            [
                {
                    "type": "message",
                    "role": "system",
                    "content": [{"type": "input_text", "text": "client system prompt"}],
                },
                {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "hi"}],
                },
            ],
        )

    @patch("chatmock.routes_ollama.start_upstream_request")
    def test_ollama_chat_fallback_mode_injects_builtin_instructions_without_system_message(
        self, mock_start
    ) -> None:
        app = create_app(base_instructions_mode="fallback")
        app.config["BASE_INSTRUCTIONS"] = "built-in ollama instructions"
        client = app.test_client()
        mock_start.return_value = (
            FakeUpstream(
                [
                    {"type": "response.output_text.delta", "delta": "hello"},
                    {"type": "response.completed"},
                ]
            ),
            None,
        )

        response = client.post(
            "/api/chat",
            json={"model": "gpt-5.4", "messages": [{"role": "user", "content": "hi"}], "stream": False},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            mock_start.call_args.kwargs["instructions"],
            "built-in ollama instructions",
        )

    @patch("chatmock.routes_ollama.start_upstream_request")
    def test_ollama_chat_fallback_mode_skips_builtin_instructions_for_system_message(self, mock_start) -> None:
        app = create_app(base_instructions_mode="fallback")
        app.config["BASE_INSTRUCTIONS"] = "built-in ollama instructions"
        client = app.test_client()
        mock_start.return_value = (
            FakeUpstream(
                [
                    {"type": "response.output_text.delta", "delta": "hello"},
                    {"type": "response.completed"},
                ]
            ),
            None,
        )

        response = client.post(
            "/api/chat",
            json={
                "model": "gpt-5.4",
                "messages": [
                    {"role": "system", "content": "client system prompt"},
                    {"role": "user", "content": "hi"},
                ],
                "stream": False,
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(mock_start.call_args.kwargs["instructions"])

    @patch("chatmock.routes_ollama.start_upstream_request")
    def test_ollama_chat_off_mode_sends_no_builtin_instructions(self, mock_start) -> None:
        app = create_app(base_instructions_mode="off")
        app.config["BASE_INSTRUCTIONS"] = "built-in ollama instructions"
        client = app.test_client()
        mock_start.return_value = (
            FakeUpstream(
                [
                    {"type": "response.output_text.delta", "delta": "hello"},
                    {"type": "response.completed"},
                ]
            ),
            None,
        )

        response = client.post(
            "/api/chat",
            json={"model": "gpt-5.4", "messages": [{"role": "user", "content": "hi"}], "stream": False},
        )

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(mock_start.call_args.kwargs["instructions"])

    @patch("chatmock.routes_ollama.start_upstream_request")
    def test_ollama_chat_honors_debug_model_override(self, mock_start) -> None:
        app = create_app(debug_model="gpt-5.4")
        client = app.test_client()
        mock_start.return_value = (
            FakeUpstream(
                [
                    {"type": "response.output_text.delta", "delta": "hello"},
                    {"type": "response.completed"},
                ]
            ),
            None,
        )
        response = client.post(
            "/api/chat",
            json={"model": "gpt-5.3-codex", "messages": [{"role": "user", "content": "hi"}], "stream": False},
        )
        body = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(mock_start.call_args.args[0], "gpt-5.4")
        self.assertEqual(body["model"], "gpt-5.4")

    @patch("chatmock.routes_openai.start_upstream_request")
    def test_chat_completions_fast_mode_sets_priority_service_tier(self, mock_start) -> None:
        mock_start.return_value = (
            FakeUpstream(
                [
                    {"type": "response.output_text.delta", "delta": "hello"},
                    {"type": "response.completed", "response": {"id": "resp-openai"}},
                ]
            ),
            None,
        )
        response = self.client.post(
            "/v1/chat/completions",
            json={
                "model": "gpt-5.4",
                "fast_mode": True,
                "messages": [{"role": "user", "content": "hi"}],
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(mock_start.call_args.kwargs["service_tier"], "priority")

    @patch("chatmock.routes_openai.start_upstream_request")
    def test_chat_completions_fast_mode_false_overrides_server_default(self, mock_start) -> None:
        app = create_app(fast_mode=True)
        client = app.test_client()
        mock_start.return_value = (
            FakeUpstream(
                [
                    {"type": "response.output_text.delta", "delta": "hello"},
                    {"type": "response.completed", "response": {"id": "resp-openai"}},
                ]
            ),
            None,
        )
        response = client.post(
            "/v1/chat/completions",
            json={
                "model": "gpt-5.4",
                "fast_mode": False,
                "messages": [{"role": "user", "content": "hi"}],
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertIsNone(mock_start.call_args.kwargs["service_tier"])

    @patch("chatmock.routes_openai.start_upstream_request")
    def test_chat_completions_rejects_unsupported_explicit_fast_mode(self, mock_start) -> None:
        response = self.client.post(
            "/v1/chat/completions",
            json={
                "model": "gpt-5.3-codex",
                "fast_mode": True,
                "messages": [{"role": "user", "content": "hi"}],
            },
        )
        body = response.get_json()
        self.assertEqual(response.status_code, 400)
        self.assertIn("Fast mode is not supported", body["error"]["message"])
        mock_start.assert_not_called()

    @patch("chatmock.routes_openai.start_upstream_raw_request")
    def test_responses_route_returns_completed_response_object(self, mock_start) -> None:
        mock_start.return_value = (
            FakeUpstream(
                [
                    {
                        "type": "response.created",
                        "response": {"id": "resp_123", "object": "response", "status": "in_progress"},
                    },
                    {
                        "type": "response.completed",
                        "response": {
                            "id": "resp_123",
                            "object": "response",
                            "status": "completed",
                            "output": [],
                        },
                    },
                ],
                headers={"Content-Type": "text/event-stream"},
            ),
            None,
        )
        response = self.client.post(
            "/v1/responses",
            json={"model": "gpt5.4-mini", "input": "hello"},
        )
        body = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(body["id"], "resp_123")
        outbound_payload = mock_start.call_args.args[0]
        self.assertEqual(outbound_payload["model"], "gpt-5.4-mini")
        self.assertEqual(outbound_payload["store"], False)
        self.assertEqual(
            outbound_payload["input"],
            [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hello"}]}],
        )
        self.assertEqual(outbound_payload["reasoning"]["effort"], "medium")
        self.assertIsInstance(outbound_payload["prompt_cache_key"], str)

    @patch("chatmock.routes_openai.start_upstream_raw_request")
    def test_responses_route_fallback_injects_base_instructions_when_omitted(self, mock_start) -> None:
        app = create_app(base_instructions_mode="fallback")
        app.config["BASE_INSTRUCTIONS"] = "server base instructions"
        app.config["GPT5_CODEX_INSTRUCTIONS"] = "server codex instructions"
        client = app.test_client()
        mock_start.return_value = (
            FakeUpstream(
                [
                    {
                        "type": "response.created",
                        "response": {"id": "resp_fallback", "object": "response", "status": "in_progress"},
                    },
                    {
                        "type": "response.completed",
                        "response": {
                            "id": "resp_fallback",
                            "object": "response",
                            "status": "completed",
                            "output": [],
                        },
                    },
                ],
                headers={"Content-Type": "text/event-stream"},
            ),
            None,
        )

        response = client.post(
            "/v1/responses",
            json={"model": "gpt-5.4", "input": "hello"},
        )

        self.assertEqual(response.status_code, 200)
        outbound_payload = mock_start.call_args.args[0]
        self.assertEqual(outbound_payload["instructions"], "server base instructions")

    @patch("chatmock.routes_openai.start_upstream_raw_request")
    def test_responses_route_off_mode_sends_empty_instructions_placeholder(self, mock_start) -> None:
        app = create_app(base_instructions_mode="off")
        app.config["BASE_INSTRUCTIONS"] = "server base instructions"
        app.config["GPT5_CODEX_INSTRUCTIONS"] = "server codex instructions"
        client = app.test_client()
        mock_start.return_value = (
            FakeUpstream(
                [
                    {
                        "type": "response.created",
                        "response": {"id": "resp_off", "object": "response", "status": "in_progress"},
                    },
                    {
                        "type": "response.completed",
                        "response": {
                            "id": "resp_off",
                            "object": "response",
                            "status": "completed",
                            "output": [],
                        },
                    },
                ],
                headers={"Content-Type": "text/event-stream"},
            ),
            None,
        )

        response = client.post(
            "/v1/responses",
            json={"model": "gpt-5.4", "input": "hello"},
        )

        self.assertEqual(response.status_code, 200)
        outbound_payload = mock_start.call_args.args[0]
        self.assertEqual(outbound_payload["instructions"], "")

    @patch("chatmock.routes_openai.start_upstream_raw_request")
    def test_responses_route_preserves_explicit_empty_instructions(self, mock_start) -> None:
        app = create_app(base_instructions_mode="fallback")
        app.config["BASE_INSTRUCTIONS"] = "server base instructions"
        app.config["GPT5_CODEX_INSTRUCTIONS"] = "server codex instructions"
        client = app.test_client()
        mock_start.return_value = (
            FakeUpstream(
                [
                    {
                        "type": "response.created",
                        "response": {"id": "resp_client", "object": "response", "status": "in_progress"},
                    },
                    {
                        "type": "response.completed",
                        "response": {
                            "id": "resp_client",
                            "object": "response",
                            "status": "completed",
                            "output": [],
                        },
                    },
                ],
                headers={"Content-Type": "text/event-stream"},
            ),
            None,
        )

        response = client.post(
            "/v1/responses",
            json={"model": "gpt-5.4", "input": "hello", "instructions": ""},
        )

        self.assertEqual(response.status_code, 200)
        outbound_payload = mock_start.call_args.args[0]
        self.assertEqual(outbound_payload["instructions"], "")

    @patch("chatmock.routes_openai.start_upstream_raw_request")
    def test_responses_route_always_mode_preserves_client_instructions(self, mock_start) -> None:
        app = create_app(base_instructions_mode="always")
        app.config["BASE_INSTRUCTIONS"] = "server base instructions"
        app.config["GPT5_CODEX_INSTRUCTIONS"] = "server codex instructions"
        client = app.test_client()
        mock_start.return_value = (
            FakeUpstream(
                [
                    {
                        "type": "response.created",
                        "response": {"id": "resp_client_always", "object": "response", "status": "in_progress"},
                    },
                    {
                        "type": "response.completed",
                        "response": {
                            "id": "resp_client_always",
                            "object": "response",
                            "status": "completed",
                            "output": [],
                        },
                    },
                ],
                headers={"Content-Type": "text/event-stream"},
            ),
            None,
        )

        response = client.post(
            "/v1/responses",
            json={"model": "gpt-5.4", "input": "hello", "instructions": "client instructions"},
        )

        self.assertEqual(response.status_code, 200)
        outbound_payload = mock_start.call_args.args[0]
        self.assertEqual(outbound_payload["instructions"], "client instructions")

    @patch("chatmock.routes_openai.start_upstream_raw_request")
    def test_responses_route_sends_empty_instructions_when_builtin_instruction_config_is_missing_or_empty(
        self, mock_start
    ) -> None:
        for base_instructions, codex_instructions in ((None, None), ("", "")):
            with self.subTest(base_instructions=base_instructions, codex_instructions=codex_instructions):
                app = create_app(base_instructions_mode="fallback")
                app.config["BASE_INSTRUCTIONS"] = base_instructions
                app.config["GPT5_CODEX_INSTRUCTIONS"] = codex_instructions
                client = app.test_client()
                mock_start.reset_mock()
                mock_start.return_value = (
                    FakeUpstream(
                        [
                            {
                                "type": "response.created",
                                "response": {
                                    "id": "resp_missing_builtin",
                                    "object": "response",
                                    "status": "in_progress",
                                },
                            },
                            {
                                "type": "response.completed",
                                "response": {
                                    "id": "resp_missing_builtin",
                                    "object": "response",
                                    "status": "completed",
                                    "output": [],
                                },
                            },
                        ],
                        headers={"Content-Type": "text/event-stream"},
                    ),
                    None,
                )

                response = client.post(
                    "/v1/responses",
                    json={"model": "gpt-5.4", "input": "hello"},
                )

                self.assertEqual(response.status_code, 200)
                outbound_payload = mock_start.call_args.args[0]
                self.assertEqual(outbound_payload["instructions"], "")

    @patch("chatmock.routes_openai.start_upstream_raw_request")
    def test_responses_route_honors_debug_model_override(self, mock_start) -> None:
        app = create_app(debug_model="gpt-5.4")
        client = app.test_client()
        mock_start.return_value = (
            FakeUpstream(
                [
                    {
                        "type": "response.created",
                        "response": {"id": "resp_debug", "object": "response", "status": "in_progress"},
                    },
                    {
                        "type": "response.completed",
                        "response": {
                            "id": "resp_debug",
                            "object": "response",
                            "status": "completed",
                            "output": [],
                        },
                    },
                ],
                headers={"Content-Type": "text/event-stream"},
            ),
            None,
        )
        response = client.post(
            "/v1/responses",
            json={"model": "gpt-5.3-codex", "input": "hello"},
        )
        self.assertEqual(response.status_code, 200)
        outbound_payload = mock_start.call_args.args[0]
        self.assertEqual(outbound_payload["model"], "gpt-5.4")

    @patch("chatmock.routes_openai.start_upstream_raw_request")
    def test_responses_route_strips_unsupported_max_output_tokens(self, mock_start) -> None:
        mock_start.return_value = (
            FakeUpstream(
                [
                    {
                        "type": "response.created",
                        "response": {"id": "resp_limit", "object": "response", "status": "in_progress"},
                    },
                    {
                        "type": "response.completed",
                        "response": {
                            "id": "resp_limit",
                            "object": "response",
                            "status": "completed",
                            "output": [],
                        },
                    },
                ],
                headers={"Content-Type": "text/event-stream"},
            ),
            None,
        )
        response = self.client.post(
            "/v1/responses",
            json={"model": "gpt-5.4", "input": "hello", "max_output_tokens": 20},
        )
        self.assertEqual(response.status_code, 200)
        outbound_payload = mock_start.call_args.args[0]
        self.assertNotIn("max_output_tokens", outbound_payload)

    @patch("chatmock.routes_openai.start_upstream_raw_request")
    def test_responses_route_does_not_use_previous_response_id_for_http_follow_up(self, mock_start) -> None:
        mock_start.side_effect = [
            (
                FakeUpstream(
                    [
                        {
                            "type": "response.created",
                            "response": {"id": "resp_1", "object": "response", "status": "in_progress"},
                        },
                        {
                            "type": "response.output_item.done",
                            "item": {
                                "type": "message",
                                "role": "assistant",
                                "id": "msg_1",
                                "content": [{"type": "output_text", "text": "assistant output"}],
                            },
                        },
                        {
                            "type": "response.completed",
                            "response": {"id": "resp_1", "object": "response", "status": "completed", "output": []},
                        },
                    ],
                    headers={"Content-Type": "text/event-stream"},
                ),
                None,
            ),
            (
                FakeUpstream(
                    [
                        {
                            "type": "response.created",
                            "response": {"id": "resp_2", "object": "response", "status": "in_progress"},
                        },
                        {
                            "type": "response.completed",
                            "response": {"id": "resp_2", "object": "response", "status": "completed", "output": []},
                        },
                    ],
                    headers={"Content-Type": "text/event-stream"},
                ),
                None,
            ),
        ]

        first = self.client.post("/v1/responses", json={"model": "gpt-5.4", "input": "hello"})
        second = self.client.post(
            "/v1/responses",
            json={
                "model": "gpt-5.4",
                "input": [
                    {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hello"}]},
                    {"type": "message", "role": "assistant", "id": "msg_1", "content": [{"type": "output_text", "text": "assistant output"}]},
                    {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "second"}]},
                ],
            },
        )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        outbound_payload = mock_start.call_args_list[1].args[0]
        self.assertNotIn("previous_response_id", outbound_payload)
        self.assertEqual(
            outbound_payload["input"],
            [
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hello"}]},
                {"type": "message", "role": "assistant", "id": "msg_1", "content": [{"type": "output_text", "text": "assistant output"}]},
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "second"}]},
            ],
        )

    @patch("chatmock.routes_openai.start_upstream_raw_request")
    def test_responses_route_falls_back_to_full_create_when_non_input_fields_change(self, mock_start) -> None:
        mock_start.side_effect = [
            (
                FakeUpstream(
                    [
                        {
                            "type": "response.created",
                            "response": {"id": "resp_1", "object": "response", "status": "in_progress"},
                        },
                        {
                            "type": "response.completed",
                            "response": {"id": "resp_1", "object": "response", "status": "completed", "output": []},
                        },
                    ],
                    headers={"Content-Type": "text/event-stream"},
                ),
                None,
            ),
            (
                FakeUpstream(
                    [
                        {
                            "type": "response.created",
                            "response": {"id": "resp_2", "object": "response", "status": "in_progress"},
                        },
                        {
                            "type": "response.completed",
                            "response": {"id": "resp_2", "object": "response", "status": "completed", "output": []},
                        },
                    ],
                    headers={"Content-Type": "text/event-stream"},
                ),
                None,
            ),
        ]

        headers = {"X-Session-Id": "session-fixed"}
        first = self.client.post("/v1/responses", json={"model": "gpt-5.4", "input": "hello"}, headers=headers)
        second = self.client.post(
            "/v1/responses",
            json={
                "model": "gpt-5.4",
                "instructions": "changed",
                "input": [
                    {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hello"}]},
                    {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "second"}]},
                ],
            },
            headers=headers,
        )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        outbound_payload = mock_start.call_args_list[1].args[0]
        self.assertNotIn("previous_response_id", outbound_payload)
        self.assertEqual(
            outbound_payload["input"],
            [
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hello"}]},
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "second"}]},
            ],
        )

    @patch("chatmock.routes_openai.start_upstream_raw_request")
    def test_responses_route_clears_reuse_state_after_error(self, mock_start) -> None:
        mock_start.side_effect = [
            (
                FakeUpstream(
                    [
                        {"type": "response.created", "response": {"id": "resp_1"}},
                        {"type": "response.completed", "response": {"id": "resp_1", "output": []}},
                    ],
                    headers={"Content-Type": "text/event-stream"},
                ),
                None,
            ),
            (
                FakeUpstream(
                    [
                        {"type": "response.failed", "response": {"error": {"message": "boom"}}},
                    ],
                    headers={"Content-Type": "text/event-stream"},
                ),
                None,
            ),
            (
                FakeUpstream(
                    [
                        {"type": "response.created", "response": {"id": "resp_3"}},
                        {"type": "response.completed", "response": {"id": "resp_3", "output": []}},
                    ],
                    headers={"Content-Type": "text/event-stream"},
                ),
                None,
            ),
        ]

        headers = {"X-Session-Id": "session-fixed"}
        first = self.client.post("/v1/responses", json={"model": "gpt-5.4", "input": "hello"}, headers=headers)
        second = self.client.post(
            "/v1/responses",
            json={
                "model": "gpt-5.4",
                "input": [
                    {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hello"}]},
                    {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "second"}]},
                ],
            },
            headers=headers,
        )
        third = self.client.post(
            "/v1/responses",
            json={
                "model": "gpt-5.4",
                "input": [
                    {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hello"}]},
                    {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "second"}]},
                    {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "third"}]},
                ],
            },
            headers=headers,
        )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 502)
        self.assertEqual(third.status_code, 200)
        outbound_payload = mock_start.call_args_list[2].args[0]
        self.assertNotIn("previous_response_id", outbound_payload)
        self.assertEqual(
            outbound_payload["input"],
            [
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hello"}]},
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "second"}]},
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "third"}]},
            ],
        )

    @patch("chatmock.routes_openai.start_upstream_raw_request")
    def test_responses_route_stream_passthrough(self, mock_start) -> None:
        chunk = b'data: {"type":"response.output_text.delta","delta":"hello"}\n\n'
        mock_start.return_value = (
            FakeUpstream(
                headers={"Content-Type": "text/event-stream"},
                content=chunk,
            ),
            None,
        )
        response = self.client.post(
            "/v1/responses",
            json={"model": "gpt-5.4", "input": "hello", "stream": True},
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn("response.output_text.delta", response.get_data(as_text=True))

    @patch("chatmock.routes_openai.start_upstream_raw_request")
    def test_responses_route_rejects_unsupported_explicit_priority(self, mock_start) -> None:
        response = self.client.post(
            "/v1/responses",
            json={"model": "gpt-5.3-codex", "input": "hello", "service_tier": "priority"},
        )
        body = response.get_json()
        self.assertEqual(response.status_code, 400)
        self.assertIn("Fast mode is not supported", body["error"]["message"])
        mock_start.assert_not_called()

    @patch("chatmock.websocket_routes.get_effective_chatgpt_auth", return_value=("token", "acct"))
    @patch("chatmock.websocket_routes.connect_upstream_websocket")
    def test_responses_websocket_rewrites_response_create(self, mock_connect, _mock_auth) -> None:
        class FakeUpstreamWebsocket:
            def __init__(self) -> None:
                self.sent: list[str] = []
                self._messages = [
                    json.dumps({"type": "response.created", "response": {"id": "resp_ws_1"}}),
                    json.dumps({
                        "type": "response.output_item.done",
                        "item": {
                            "type": "message",
                            "role": "assistant",
                            "id": "msg_1",
                            "content": [{"type": "output_text", "text": "assistant output"}],
                        },
                    }),
                    json.dumps({"type": "response.completed", "response": {"id": "resp_ws_1"}}),
                    json.dumps({"type": "response.created", "response": {"id": "resp_ws_2"}}),
                    json.dumps({"type": "response.completed", "response": {"id": "resp_ws_2"}}),
                ]

            def send(self, message: str) -> None:
                self.sent.append(message)

            def recv(self) -> str:
                return self._messages.pop(0)

            def close(self) -> None:
                return None

        fake_upstream = FakeUpstreamWebsocket()
        mock_connect.return_value = fake_upstream

        app = create_app()

        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        host, port = sock.getsockname()
        sock.close()

        server_thread = threading.Thread(
            target=app.run,
            kwargs={
                "host": host,
                "port": port,
                "use_reloader": False,
                "threaded": True,
            },
            daemon=True,
        )
        server_thread.start()
        time.sleep(0.5)

        with ws_connect(f"ws://{host}:{port}/v1/responses") as client:
            client.send(json.dumps({"type": "response.create", "model": "gpt-5.4", "input": "hello", "fast_mode": True}))
            first = json.loads(client.recv())
            assistant = json.loads(client.recv())
            second = json.loads(client.recv())
            client.send(
                json.dumps(
                    {
                        "type": "response.create",
                        "model": "gpt-5.4",
                        "fast_mode": True,
                        "input": [
                            {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hello"}]},
                            {"type": "message", "role": "assistant", "id": "msg_1", "content": [{"type": "output_text", "text": "assistant output"}]},
                            {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "second"}]},
                        ],
                    }
                )
            )
            third = json.loads(client.recv())
            fourth = json.loads(client.recv())

        self.assertEqual(first["type"], "response.created")
        self.assertEqual(assistant["type"], "response.output_item.done")
        self.assertEqual(second["type"], "response.completed")
        self.assertEqual(third["type"], "response.created")
        self.assertEqual(fourth["type"], "response.completed")
        outbound = json.loads(fake_upstream.sent[0])
        self.assertEqual(outbound["model"], "gpt-5.4")
        self.assertEqual(outbound["service_tier"], "priority")
        self.assertEqual(outbound["type"], "response.create")
        self.assertEqual(
            outbound["input"],
            [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hello"}]}],
        )
        self.assertIn("prompt_cache_key", outbound)
        follow_up = json.loads(fake_upstream.sent[1])
        self.assertEqual(follow_up["previous_response_id"], "resp_ws_1")
        self.assertEqual(
            follow_up["input"],
            [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "second"}]}],
        )


if __name__ == "__main__":
    unittest.main()
