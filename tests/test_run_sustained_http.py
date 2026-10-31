"""Safety and diagnostic contracts for the owned sustained HTTP runner."""

import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/run-sustained-http.py"
REVISION = "a" * 40


def load_runner():
    spec = importlib.util.spec_from_file_location("sustained_http_runner", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SustainedHttpRunnerTests(unittest.TestCase):
    def test_cli_accepts_explicit_binary_state_and_revision(self):
        run = subprocess.run([sys.executable, str(SCRIPT), "--help"],
                             capture_output=True, text=True, timeout=5)
        self.assertEqual(run.returncode, 0)
        for flag in ("--binary", "--state", "--revision"):
            self.assertIn(flag, run.stdout)
        self.assertNotIn("--remove-binary-on-exit", run.stdout)

    def test_invalid_revision_never_creates_state(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "new-state"
            run = subprocess.run([
                sys.executable, str(SCRIPT), "--binary", "/bin/true",
                "--state", str(state), "--revision", "short",
                "--duration", "20", "--sample-seconds", "5",
            ], capture_output=True, text=True, timeout=5)
            self.assertNotEqual(run.returncode, 0)
            self.assertFalse(state.exists())

    def test_existing_state_is_never_reused_or_deleted(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state"
            state.mkdir()
            marker = state / "keep"
            marker.write_text("owned", encoding="utf-8")
            run = subprocess.run([
                sys.executable, str(SCRIPT), "--binary", "/bin/true",
                "--state", str(state), "--revision", REVISION,
                "--duration", "20", "--sample-seconds", "5",
            ], capture_output=True, text=True, timeout=5)
            self.assertNotEqual(run.returncode, 0)
            self.assertEqual(marker.read_text(encoding="utf-8"), "owned")

    def test_gap_events_keep_first_and_last_with_bounded_history(self):
        runner = load_runner()
        record = runner.new_viewer_record()
        for index in range(100):
            runner.record_gap(record, "media", 3100 + index, index + 0.25)
        self.assertEqual(record["media_gap_count_over_3000_ms"], 100)
        self.assertEqual(record["first_media_gap_over_3000_ms"]["elapsed_seconds"], 0.25)
        self.assertEqual(record["last_media_gap_over_3000_ms"]["elapsed_seconds"], 99.25)
        self.assertLessEqual(len(record["media_gap_events"]), 32)

    def test_checkpoint_write_is_private_and_atomic(self):
        runner = load_runner()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "checkpoint.json"
            runner.save(path, {"state": "running"})
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            runner.save(path, {"state": "completed"})
            self.assertEqual(path.read_text(encoding="utf-8"),
                             '{"state":"completed"}')
            self.assertEqual(sorted(p.name for p in Path(directory).iterdir()),
                             ["checkpoint.json"])


if __name__ == "__main__":
    unittest.main()
