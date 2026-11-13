"""Sustained HTTP release-gate behavior with owned synthetic reports."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/assess-sustained-http.py"
REVISION = "a" * 40
MIB = 1024 * 1024
GIB = 1024 * MIB


def complete_report():
    samples = [
        {"elapsed": i * 60, "memory_bytes": (64 if i == 0 else 80) * MIB,
         "memory_peak_bytes": 80 * MIB, "media_dir_bytes": 8 * MIB,
         "oom_kills": 0}
        for i in range(1440)
    ]
    viewers = {
        f"{stream}:{index}": {
            "bytes": 8 * GIB,
            "connections": 2000 if index == 0 else 1,
            "statuses": {"200": 2000 if index == 0 else 1},
            "errors": {}, "sync_errors": 0,
            "media_gap_max_ms": 1000,
            "reconnect_gap_max_ms": 2000 if index == 0 else 0,
            "first_byte_max_ms": 500,
            "last_media_elapsed": 86399,
        }
        for stream in ("soak_a", "soak_b") for index in range(4)
    }
    return {
        "revision": REVISION[:7], "state": "completed",
        "duration_seconds": 86400, "elapsed_seconds": 86400.2,
        "sample_seconds": 60, "streams": 2, "viewers_per_stream": 4,
        "samples": samples, "viewers": viewers,
        "metadata_checks": {"success": 1440, "errors": {}},
        "daemon_exited_early": False,
    }


def assess(report):
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "report.json"
        path.write_text(json.dumps(report), encoding="utf-8")
        path.chmod(0o600)
        run = subprocess.run(
            [sys.executable, str(SCRIPT), "--report", str(path),
             "--revision", REVISION],
            capture_output=True, text=True, timeout=5,
        )
    return run, json.loads(run.stdout) if run.stdout else {}


class SustainedHttpGateTests(unittest.TestCase):
    def test_wrong_revision_cannot_qualify_a_completed_report(self):
        report = complete_report()
        report["revision"] = "b" * 7
        run, result = assess(report)
        self.assertEqual(run.returncode, 1)
        self.assertIn("revision_mismatch", result["issues"])

    def test_world_readable_or_linked_report_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            path.write_text(json.dumps(complete_report()), encoding="utf-8")
            path.chmod(0o644)
            linked = Path(directory) / "linked.json"
            linked.symlink_to(path)
            for candidate in (path, linked):
                with self.subTest(candidate=candidate.name):
                    run = subprocess.run(
                        [sys.executable, str(SCRIPT), "--report", str(candidate),
                         "--revision", REVISION],
                        capture_output=True, text=True, timeout=5,
                    )
                    self.assertEqual(run.returncode, 2)
                    self.assertEqual(json.loads(run.stdout)["issues"], ["invalid_report"])

    def test_unbounded_numeric_field_fails_closed(self):
        report = complete_report()
        report["samples"][1]["memory_bytes"] = 10 ** 400
        run, result = assess(report)
        self.assertEqual(run.returncode, 2)
        self.assertEqual(result["issues"], ["invalid_report"])

    def test_cgroup_high_water_exceeds_limit_even_when_samples_are_low(self):
        report = complete_report()
        report["samples"][1]["memory_peak_bytes"] = 700 * MIB
        run, result = assess(report)
        self.assertEqual(run.returncode, 1)
        self.assertIn("memory_limit_exceeded", result["issues"])

    def test_sampled_memory_exceeds_limit_even_when_high_water_is_wrong(self):
        report = complete_report()
        report["samples"][-1]["memory_bytes"] = 700 * MIB
        run, result = assess(report)
        self.assertEqual(run.returncode, 1)
        self.assertIn("memory_limit_exceeded", result["issues"])

    def test_startup_growth_is_reported_but_gate_uses_one_hour_baseline(self):
        report = complete_report()
        for sample in report["samples"]:
            sample["memory_bytes"] = 96 * MIB
            sample["memory_peak_bytes"] = 160 * MIB
        report["samples"][0]["memory_bytes"] = 32 * MIB
        report["samples"][-1]["memory_bytes"] = 160 * MIB

        run, result = assess(report)

        self.assertEqual(run.returncode, 0)
        self.assertEqual(result["issues"], [])
        self.assertEqual(result["sampled_memory_growth_mib"], 128.0)
        self.assertEqual(result["post_warmup_memory_growth_mib"], 64.0)
        self.assertEqual(result["memory_growth_warmup_seconds"], 3600)

    def test_post_warmup_growth_above_limit_fails(self):
        report = complete_report()
        for sample in report["samples"]:
            sample["memory_bytes"] = 96 * MIB
            sample["memory_peak_bytes"] = 161 * MIB
        report["samples"][-1]["memory_bytes"] = 160 * MIB + 1

        run, result = assess(report)

        self.assertEqual(run.returncode, 1)
        self.assertIn("memory_growth_exceeded", result["issues"])

    def test_first_sample_after_warmup_is_the_baseline(self):
        report = complete_report()
        for sample in report["samples"]:
            sample["memory_bytes"] = 100 * MIB
            sample["memory_peak_bytes"] = 164 * MIB
        report["samples"][59]["memory_bytes"] = 32 * MIB
        report["samples"][60]["elapsed"] = 3601
        report["samples"][-1]["memory_bytes"] = 164 * MIB

        run, result = assess(report)

        self.assertEqual(run.returncode, 0)
        self.assertEqual(result["post_warmup_memory_growth_mib"], 64.0)

    def test_missing_warmup_baseline_fails_closed(self):
        report = complete_report()
        for index, sample in enumerate(report["samples"]):
            sample["elapsed"] = index * 2

        run, result = assess(report)

        self.assertEqual(run.returncode, 1)
        self.assertIn("samples_incomplete", result["issues"])
        self.assertIsNone(result["post_warmup_memory_growth_mib"])
        self.assertEqual(result["memory_growth_warmup_seconds"], 3600)

    def test_finite_viewer_counters_cannot_overflow_the_summary(self):
        report = complete_report()
        for viewer in report["viewers"].values():
            viewer["bytes"] = 1e308
        run, result = assess(report)
        self.assertEqual(run.returncode, 2)
        self.assertIn("invalid_report", result["issues"])
        self.assertNotIn("Infinity", run.stdout)

    def test_deeply_nested_report_returns_fixed_error_code(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            path.write_text('{"x":' * 10000 + '0' + '}' * 10000,
                            encoding="utf-8")
            path.chmod(0o600)
            run = subprocess.run(
                [sys.executable, str(SCRIPT), "--report", str(path),
                 "--revision", REVISION],
                capture_output=True, text=True, timeout=5,
            )
        self.assertEqual(run.returncode, 2)
        self.assertEqual(json.loads(run.stdout)["issues"], ["invalid_report"])
        self.assertEqual(run.stderr, "")

    @unittest.skipUnless(os.geteuid() == 0, "administrative report read requires root")
    def test_root_can_assess_owner_only_service_report(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            path.write_text(json.dumps(complete_report()), encoding="utf-8")
            path.chmod(0o600)
            os.chown(path, 65534, -1)
            run = subprocess.run(
                [sys.executable, str(SCRIPT), "--report", str(path),
                 "--revision", REVISION],
                capture_output=True, text=True, timeout=5,
            )
        self.assertEqual(run.returncode, 0)
        self.assertEqual(json.loads(run.stdout)["result"], "pass")

    def test_completed_clean_run_passes(self):
        run, result = assess(complete_report())
        self.assertEqual(run.returncode, 0)
        self.assertEqual(result["result"], "pass")
        self.assertEqual(result["issues"], [])

    def test_completed_run_with_shared_media_pause_fails(self):
        report = complete_report()
        for name in ("soak_a:1", "soak_a:2"):
            report["viewers"][name]["media_gap_max_ms"] = 3500
        run, result = assess(report)
        self.assertEqual(run.returncode, 1)
        self.assertIn("media_gap_exceeded", result["issues"])

    def test_initial_media_delay_fails_even_with_no_interpacket_gap(self):
        report = complete_report()
        report["viewers"]["soak_a:1"]["first_byte_max_ms"] = 3500
        run, result = assess(report)
        self.assertEqual(run.returncode, 1)
        self.assertIn("initial_media_gap_exceeded", result["issues"])

    def test_media_stopping_before_deadline_fails_even_with_no_interpacket_gap(self):
        report = complete_report()
        report["viewers"]["soak_b:2"]["last_media_elapsed"] = 86396
        run, result = assess(report)
        self.assertEqual(run.returncode, 1)
        self.assertIn("final_media_gap_exceeded", result["issues"])

    def test_media_at_exact_final_gap_limit_passes(self):
        report = complete_report()
        report["viewers"]["soak_b:2"]["last_media_elapsed"] = 86397
        report["viewers"]["soak_a:1"]["first_byte_max_ms"] = 3000
        run, result = assess(report)
        self.assertEqual(run.returncode, 0)
        self.assertEqual(result["issues"], [])

    def test_missing_boundary_measurement_fails_closed(self):
        report = complete_report()
        del report["viewers"]["soak_b:2"]["last_media_elapsed"]
        run, result = assess(report)
        self.assertEqual(run.returncode, 2)
        self.assertIn("invalid_report", result["issues"])

    def test_last_media_after_report_end_is_invalid(self):
        report = complete_report()
        report["viewers"]["soak_b:2"]["last_media_elapsed"] = 86401
        run, result = assess(report)
        self.assertEqual(run.returncode, 2)
        self.assertIn("invalid_report", result["issues"])

    def test_running_or_sparse_report_cannot_pass(self):
        report = complete_report()
        report["state"] = "running"
        report["samples"] = report["samples"][:100]
        run, result = assess(report)
        self.assertEqual(run.returncode, 1)
        self.assertIn("run_incomplete", result["issues"])
        self.assertIn("samples_incomplete", result["issues"])

    def test_oom_growth_and_viewer_error_fail_without_echoing_error_text(self):
        report = complete_report()
        report["samples"][-1]["memory_bytes"] = 200 * MIB
        report["samples"][-1]["oom_kills"] = 1
        report["viewers"]["soak_b:3"]["errors"] = {"private-token-sentinel": 1}
        run, result = assess(report)
        self.assertEqual(run.returncode, 1)
        self.assertIn("memory_growth_exceeded", result["issues"])
        self.assertIn("oom_kill", result["issues"])
        self.assertIn("viewer_error", result["issues"])
        self.assertNotIn("private-token-sentinel", run.stdout)

    def test_oom_before_first_sample_fails(self):
        report = complete_report()
        for sample in report["samples"]:
            sample["oom_kills"] = 1
        run, result = assess(report)
        self.assertEqual(run.returncode, 1)
        self.assertIn("oom_kill", result["issues"])


if __name__ == "__main__":
    unittest.main()
