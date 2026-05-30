from __future__ import annotations

import unittest

from chatmock.upstream_errors import build_upstream_error


class BrokenString:
    def __str__(self) -> str:
        raise RuntimeError("broken string conversion")


class UpstreamErrorFormatterTests(unittest.TestCase):
    def test_build_upstream_error_returns_message_only_with_available_context(self) -> None:
        payload = build_upstream_error(
            "Upstream failure",
            phase="sse_parse",
            status_code=503,
            content_type="text/plain; charset=utf-8",
            exception=ValueError("bad upstream chunk"),
            body="gateway timed out while parsing event stream",
        )

        self.assertEqual(set(payload.keys()), {"error"})
        self.assertEqual(set(payload["error"].keys()), {"message"})

        message = payload["error"]["message"]
        self.assertIn("Upstream failure", message)
        self.assertIn("phase=sse_parse", message)
        self.assertIn("upstream_status=503", message)
        self.assertIn("content_type=text/plain; charset=utf-8", message)
        self.assertIn("exception=ValueError: bad upstream chunk", message)
        self.assertIn("body=gateway timed out while parsing event stream", message)

    def test_build_upstream_error_redacts_sensitive_values_and_truncates_long_details(self) -> None:
        body = (
            "Authorization: Bearer secret-auth "+
            "Bearer session-secret "+
            "sk-1234567890abcdef "+
            "session_id=session-secret "+
            "access_token=access-secret "+
            "token=token-secret " +
            ("x" * 400)
        )

        payload = build_upstream_error(
            "Upstream failure",
            body=body,
            exception="Bearer exception-secret " + ("y" * 300),
        )

        message = payload["error"]["message"]

        self.assertNotIn("secret-auth", message)
        self.assertNotIn("session-secret", message)
        self.assertNotIn("access-secret", message)
        self.assertNotIn("token-secret", message)
        self.assertNotIn("sk-1234567890abcdef", message)
        self.assertIn("Authorization: Bearer [redacted]", message)
        self.assertIn("Bearer [redacted]", message)
        self.assertIn("session_id=[redacted]", message)
        self.assertIn("access_token=[redacted]", message)
        self.assertIn("token=[redacted]", message)
        self.assertIn("...(truncated)", message)
        self.assertLessEqual(len(message), 500)

    def test_build_upstream_error_keeps_already_redacted_bearer_tokens_stable(self) -> None:
        payload = build_upstream_error(
            "Upstream failure",
            body="Authorization: Bearer [redacted] Bearer [redacted]",
            exception="Bearer [redacted]",
        )

        message = payload["error"]["message"]

        self.assertIn("Authorization: Bearer [redacted]", message)
        self.assertEqual(message.count("Authorization: Bearer [redacted]"), 1)
        self.assertEqual(message.count("Bearer [redacted]"), 3)

    def test_build_upstream_error_handles_unexpected_input_types_without_raising(self) -> None:
        payload = build_upstream_error(
            None,
            phase={"phase": "receive"},
            status_code="502",
            content_type=["text/plain"],
            exception=BrokenString(),
            body=b"binary body\xff",
        )

        self.assertEqual(set(payload.keys()), {"error"})
        self.assertEqual(set(payload["error"].keys()), {"message"})
        self.assertIsInstance(payload["error"]["message"], str)
        self.assertIn("Upstream error", payload["error"]["message"])
        self.assertIn("body=binary body", payload["error"]["message"])
        self.assertIn("exception=<unprintable BrokenString>", payload["error"]["message"])


if __name__ == "__main__":
    unittest.main()