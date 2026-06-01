from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from tests.contract import harness


class ContractCommandResolutionTests(unittest.TestCase):
    def test_resolve_cli_command_from_env(self) -> None:
        with patch.dict(os.environ, {"CHATMOCK_CLI_CMD": 'python -m chatmock.cli'}, clear=True):
            self.assertEqual(harness.resolve_cli_command(), ["python", "-m", "chatmock.cli"])

    def test_resolve_server_command_from_env(self) -> None:
        with patch.dict(os.environ, {"CHATMOCK_SERVER_CMD": 'python -m chatmock.cli serve'}, clear=True):
            self.assertEqual(
                harness.resolve_server_command(),
                ["python", "-m", "chatmock.cli", "serve"],
            )

    def test_rust_target_without_server_is_explicitly_skipped(self) -> None:
        with patch.dict(os.environ, {"CHATMOCK_CONTRACT_TARGET": "rust"}, clear=True):
            with self.assertRaises(unittest.SkipTest) as error:
                harness.require_contract_configuration()
        self.assertIn("Rust target requested", str(error.exception))