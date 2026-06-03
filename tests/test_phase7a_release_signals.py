from __future__ import annotations

from pathlib import Path
import tomllib
import unittest


ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "pyproject.toml"
BUILD_SCRIPT = ROOT / "build.py"


class Phase7AReleaseSignalsTests(unittest.TestCase):
    def test_pyproject_declares_transition_status(self) -> None:
        data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))

        self.assertEqual(data["project"]["scripts"]["chatmock"], "chatmock.cli:main")

        release_transition = data["tool"]["chatmock"]["release_transition"]
        self.assertEqual(release_transition["primary_implementation"], "rust")
        self.assertEqual(release_transition["rust_workspace_path"], "chatmock-rs")
        self.assertEqual(release_transition["python_fallback_status"], "explicit-legacy-python-cli")
        self.assertEqual(release_transition["python_fallback_removal"], "deferred")
        self.assertEqual(release_transition["gui_status"], "deferred-python-pyinstaller")
        self.assertEqual(release_transition["release_workflow_status"], "python-release-path-unchanged")

    def test_build_script_marks_gui_as_deferred_legacy_path(self) -> None:
        build_script = BUILD_SCRIPT.read_text(encoding="utf-8")

        self.assertIn('LEGACY_GUI_TRANSITION_STATUS = "deferred-python-pyinstaller"', build_script)
        self.assertIn("Python/PyInstaller GUI packaging remains the deferred legacy fallback", build_script)
        self.assertIn("Rust server remains the primary implementation target", build_script)