from __future__ import annotations

import unittest

from tests.contract.harness import ContractRuntime


class OpenAIHTTPContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.runtime = ContractRuntime.start_from_env()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.runtime.close()

    def test_models_list_exposes_known_public_models(self) -> None:
        response = self.runtime.get("/v1/models")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        model_ids = [item["id"] for item in body["data"]]
        self.assertIn("gpt-5.4", model_ids)
        self.assertIn("gpt-5.4-mini", model_ids)
        self.assertIn("gpt-5.3-codex-spark", model_ids)

    def test_chat_completions_non_stream_contract(self) -> None:
        response = self.runtime.post(
            "/v1/chat/completions",
            {
                "model": "gpt5.4-mini",
                "messages": [{"role": "user", "content": "contract-chat-completions"}],
            },
        )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["choices"][0]["message"]["content"], "hello from contract upstream")
        self.assertEqual(body["model"], "gpt5.4-mini")

    def test_chat_completions_upstream_error_contract(self) -> None:
        response = self.runtime.post(
            "/v1/chat/completions",
            {
                "model": "gpt-5.4",
                "messages": [{"role": "user", "content": "contract-upstream-error"}],
            },
        )

        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.headers["Access-Control-Allow-Origin"], "*")
        message = response.json()["error"]["message"]
        self.assertIn("502", message)
        self.assertIn("text/plain", message)
        self.assertIn("gateway meltdown before JSON", message)
        for secret in (
            "auth-secret-123",
            "bearer-secret-456",
            "sk-live-super-secret-789",
            "session-secret-abc",
            "access-secret-def",
            "token-secret-ghi",
        ):
            self.assertNotIn(secret, message)

    def test_responses_normalization_and_aggregation_contract(self) -> None:
        response = self.runtime.post(
            "/v1/responses",
            {"model": "gpt5.4-mini", "input": "contract-responses-backfill"},
        )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["id"], "resp_contract_backfill")
        self.assertEqual(body["output"][0]["id"], "msg_contract_backfill")
        self.assertEqual(body["output"][0]["content"][0]["text"], "assistant output")

    def test_responses_outbound_normalization_contract(self) -> None:
        if self.runtime.fake_upstream is None:
            self.skipTest(
                "Outbound /v1/responses normalization assertions require CHATMOCK_SERVER_CMD so the harness can observe the managed fake upstream."
            )

        response = self.runtime.post(
            "/v1/responses",
            {"model": "gpt5.4-mini", "input": "contract-responses-backfill"},
        )

        self.assertEqual(response.status_code, 200)
        outbound = self.runtime.fake_upstream.last_request()
        self.assertIsNotNone(outbound)
        assert outbound is not None
        self.assertEqual(outbound["path"], "/backend-api/codex/responses")
        self.assertEqual(outbound["json"]["model"], "gpt-5.4-mini")
        self.assertFalse(outbound["json"]["store"])
        self.assertEqual(
            outbound["json"]["input"],
            [
                {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "contract-responses-backfill"}],
                }
            ],
        )