"""Native live-probe regressions that need no vendor service."""

import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/qualify-live-vendor.py"
SPEC = importlib.util.spec_from_file_location("qualify_live_vendor", SCRIPT)
PROBE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROBE)


class SourceArgumentsTests(unittest.TestCase):
    def test_daemon_accepted_numeric_server_roots_remain_usable(self):
        with tempfile.TemporaryDirectory() as directory:
            key = Path(directory) / "cluster.key"
            key.write_text("test-key", encoding="utf-8")
            key.chmod(0o600)
            for source in ("m4s://127.1", "m4s://127.0.0.1."):
                with self.subTest(source=source):
                    argv = ["probe", "--binary", "/bin/true", "--source", source,
                            "--stream", "live", "--cluster-key-file", str(key),
                            "--topology"]
                    with patch.object(sys, "argv", argv):
                        try:
                            parsed = PROBE.arguments()
                        except SystemExit:
                            self.fail(f"valid server root rejected: {source}")
                        self.assertEqual(parsed.source, source)

    def test_non_root_sources_are_rejected_before_starting_local_daemon(self):
        with tempfile.TemporaryDirectory() as directory:
            key = Path(directory) / "cluster.key"
            key.write_text("test-key", encoding="utf-8")
            key.chmod(0o600)
            for source in (
                "m4s://origin.example/live",
                "m4s://origin.example/\n",
                "m4s://origin.example\\live",
                "m4s://origin.example%2Flive",
                "m4s://origin.example:0",
                "m4s://origin^.example",
                "m4s://origin|.example",
                "m4s://origin<.example",
                "m4s://origin>.example",
            ):
                with self.subTest(source=source):
                    result = subprocess.run(
                        ["python3", str(SCRIPT), "--binary", "/bin/true",
                         "--source", source, "--stream", "live",
                         "--cluster-key-file", str(key), "--topology"],
                        capture_output=True, text=True, timeout=5,
                    )
                    self.assertEqual(result.returncode, 2)
                    self.assertIn("server-root", result.stderr)


class DaemonCredentialsTests(unittest.TestCase):
    def test_probe_daemons_get_fresh_admin_passwords_and_no_inherited_credentials(self):
        with patch.dict("os.environ", {
            "FLUSSONIX_ADMIN_PASSWORD": "operator-admin",
            "FLUSSONIX_IPTV_DATA": "/private/catalog",
            "PATH": "/usr/bin",
        }, clear=True):
            first = PROBE.private_daemon_env("probe-peer")
            second = PROBE.private_daemon_env("probe-peer")
        self.assertEqual(first["PATH"], "/usr/bin")
        self.assertEqual(first["FLUSSONIX_PEER_KEY"], "probe-peer")
        self.assertEqual(second["FLUSSONIX_PEER_KEY"], "probe-peer")
        self.assertNotIn("FLUSSONIX_IPTV_DATA", first)
        self.assertNotIn("FLUSSONIX_IPTV_DATA", second)
        for environment in (first, second):
            password = environment["FLUSSONIX_ADMIN_PASSWORD"]
            self.assertGreaterEqual(len(password), 32)
            self.assertNotEqual(password, "operator-admin")
            self.assertNotEqual(password, "owned-vendor-probe-admin")
        self.assertNotEqual(first["FLUSSONIX_ADMIN_PASSWORD"],
                            second["FLUSSONIX_ADMIN_PASSWORD"])


class M4fAnnouncementTests(unittest.TestCase):
    def test_relay_accepts_origin_sequence_instead_of_requiring_first_segment(self):
        timestamp = "2026/10/26/20/07/30"
        self.assertEqual(PROBE.parse_m4f_announcement(f"1 {timestamp}-0"), timestamp)
        self.assertEqual(PROBE.parse_m4f_announcement(f"238941 {timestamp}-54"), timestamp)

    def test_invalid_announcement_cannot_be_used_as_segment_path(self):
        for line in (
            "x 2026/10/26/20/07/30-0",
            "2 ../../etc/passwd-0",
            "2 2026/10/26/20/07/30-0 more",
        ):
            with self.subTest(line=line):
                self.assertIsNone(PROBE.parse_m4f_announcement(line))


class WorkerSnapshotTests(unittest.TestCase):
    def test_snapshot_exposes_only_requested_worker_and_bounded_status_fields(self):
        node = {
            "secret": "cluster-secret",
            "streams": [
                {"name": "other", "stats": {"bytes_in": 999}},
                {"name": "requested", "ready": True, "secret": "viewer-token",
                 "stats": {"status": "running", "bytes_in": 1880,
                           "restart_count": 2, "retry_in_ms": None,
                           "media_age_ms": 450, "last_error": "input_closed",
                           "secret": "private-url"}},
            ],
        }
        snapshot = PROBE.cdn_worker_snapshot(node, "requested")
        self.assertEqual(snapshot, {
            "ready": True,
            "status": "running",
            "bytes_in": 1880,
            "restart_count": 2,
            "retry_in_ms": None,
            "media_age_ms": 450,
            "last_error": "input_closed",
        })
        self.assertNotIn("secret", repr(snapshot))
        self.assertNotIn("999", repr(snapshot))

    def test_missing_worker_returns_bounded_absence(self):
        self.assertEqual(PROBE.cdn_worker_snapshot({"streams": []}, "requested"),
                         {"present": False})


class NoMediaDiagnosticsTests(unittest.TestCase):
    def test_failed_m4f_probe_retains_bounded_worker_history_and_safe_stop_codes(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "daemon.log"
            log.write_text(
                'WARN flussonix::private_input: private media input failed '
                'protocol="m4f" phase="control_read" failure=Transient '
                'last_http_status=200 last_private_ack=true metadata_sent=false '
                'cluster_key=secret\n'
                'DEBUG flussonix::worker_lifecycle: worker failure recorded '
                'site=1193 reason=input_closed token=secret\n', encoding="utf-8")
            samples = [{"status": "starting", "bytes_in": n} for n in range(10)]
            result = PROBE.m4f_no_media_diagnostics(log, samples)
        self.assertEqual([row["bytes_in"] for row in result["cdn_worker_samples"]],
                         list(range(2, 10)))
        self.assertEqual(result["source_failure"]["phase"], "control_read")
        self.assertEqual(result["lifecycle"], [
            {"event": "worker_failure", "site": 1193, "reason": "input_closed"}
        ])
        self.assertNotIn("secret", repr(result))


class PrivateInputSnapshotTests(unittest.TestCase):
    def test_only_allowlisted_m4f_failure_fields_leave_the_private_log(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "daemon.log"
            log.write_text(
                'WARN flussonix::private_input: private media input failed '
                'protocol="m4f" phase="segment_response" failure=Transient '
                'last_http_status=404 last_private_ack=true metadata_sent=true '
                'cluster_key=secret token=secret\n', encoding="utf-8")
            snapshot = PROBE.private_input_failure(log)
        self.assertEqual(snapshot, {
            "phase": "segment_response", "failure": "Transient",
            "last_http_status": 404, "last_private_ack": True,
            "metadata_sent": True,
        })
        self.assertNotIn("secret", repr(snapshot))

    def test_unrelated_or_malformed_log_lines_are_not_reported(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "daemon.log"
            log.write_text(
                'WARN other: private media input failed protocol="m4f" '
                'phase="control_read" failure=Transient last_http_status=200 '
                'last_private_ack=true metadata_sent=false\n'
                'WARN flussonix::private_input: private media input failed '
                'protocol="m4s" phase="control_read" failure=Transient '
                'last_http_status=200 last_private_ack=true metadata_sent=false\n'
                'WARN flussonix::private_input: private media input failed '
                'protocol="m4f" phase="unknown" failure=Transient '
                'last_http_status=200 last_private_ack=true metadata_sent=false\n',
                encoding="utf-8")
            self.assertIsNone(PROBE.private_input_failure(log))


class WorkerLifecycleSnapshotTests(unittest.TestCase):
    def test_only_fixed_lifecycle_codes_and_classes_leave_the_private_log(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "daemon.log"
            log.write_text(
                'DEBUG flussonix::worker_lifecycle: shared input stopped before worker cancellation '
                'reason=Authority(Coordination(Lease(Expired))) cluster_key=secret\n'
                'DEBUG flussonix::worker_lifecycle: packager stdout closed before worker cancellation '
                'bytes_in=0 token=secret\n'
                'DEBUG flussonix::worker_lifecycle: worker failure recorded '
                'site=1479 reason=input_closed token=secret\n', encoding="utf-8")
            snapshot = PROBE.worker_lifecycle_events(log)
        self.assertEqual(snapshot, [
            {"event": "shared_input_stopped", "reason": "Authority"},
            {"event": "packager_stdout_closed", "bytes_in": 0},
            {"event": "worker_failure", "site": 1479, "reason": "input_closed"},
        ])
        self.assertNotIn("secret", repr(snapshot))

    def test_unrelated_lifecycle_lines_are_ignored(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "daemon.log"
            log.write_text(
                'DEBUG other: shared input stopped before worker cancellation reason=Input\n'
                'DEBUG flussonix::worker_lifecycle: shared input stopped before worker cancellation '
                'reason=CredentialLeak\n', encoding="utf-8")
            self.assertEqual(PROBE.worker_lifecycle_events(log), [])


if __name__ == "__main__":
    unittest.main()
