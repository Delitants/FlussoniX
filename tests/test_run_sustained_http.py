"""Safety and diagnostic contracts for the owned sustained HTTP runner."""

import importlib.util
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/run-sustained-http.py"
REVISION = "a" * 40


def load_runner():
    spec = importlib.util.spec_from_file_location("sustained_http_runner", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def serve_source_responses(responses):
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            received.append((self.path, self.headers.get("Authorization")))
            body = responses.pop(0) if len(responses) > 1 else responses[0]
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, _format, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, received


class SustainedHttpRunnerTests(unittest.TestCase):
    def test_source_observer_samples_both_streams_and_stops_without_raw_config(self):
        runner = load_runner()
        secret = "tshttp://private.example/secret?token=do-not-record"
        response = json.dumps({
            "inputs": [{"url": secret}],
            "stats": {"status": "running", "media_age_ms": 1500,
                      "bytes_in": 100},
        }).encode()
        server, thread, _ = serve_source_responses([response])
        state = {name: runner.new_source_record() for name in runner.STREAMS}
        stop = threading.Event()
        started = time.monotonic()
        observer = threading.Thread(
            target=runner.observe_source,
            args=(server.server_port, "secret", started, started + 5,
                  stop, state, threading.Lock()),
        )
        try:
            observer.start()
            time.sleep(1.2)
            stop.set()
            observer.join(timeout=1.5)
            self.assertFalse(observer.is_alive())
            for record in state.values():
                self.assertGreaterEqual(record["samples"], 2)
                self.assertEqual(record["errors"], 0)
            self.assertNotIn(secret, json.dumps(state))
            self.assertNotIn("secret", json.dumps(state))
        finally:
            stop.set()
            observer.join(timeout=2)
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_source_observer_stops_after_blocked_api_request(self):
        runner = load_runner()
        requested = threading.Event()

        class SlowHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                requested.set()
                time.sleep(2)
                try:
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b"{}")
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def log_message(self, _format, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), SlowHandler)
        server.daemon_threads = True
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        state = {name: runner.new_source_record() for name in runner.STREAMS}
        stop = threading.Event()
        started = time.monotonic()
        observer = threading.Thread(
            target=runner.observe_source,
            args=(server.server_port, "secret", started, started + 5,
                  stop, state, threading.Lock()),
        )
        try:
            observer.start()
            self.assertTrue(requested.wait(timeout=1))
            stop.set()
            observer.join(timeout=1.5)
            self.assertFalse(observer.is_alive())
            self.assertGreaterEqual(sum(record["errors"] for record in state.values()), 1)
        finally:
            stop.set()
            observer.join(timeout=2)
            server.shutdown()
            server.server_close()
            server_thread.join(timeout=2)

    def test_source_stall_episode_matches_viewer_time_base(self):
        runner = load_runner()
        record = runner.new_source_record()
        runner.record_source_sample(record, 0.0, 40, 100)
        runner.record_source_sample(record, 1.5, 1500, 100)
        runner.record_source_sample(record, 4.5, 4500, 100)
        runner.record_source_sample(record, 5.0, 20, 600)
        self.assertEqual(record["samples"], 4)
        self.assertEqual(record["max_age_ms"], 4500)
        self.assertEqual(record["max_poll_interval_ms"], 3000)
        self.assertEqual(record["stalls_over_1000_ms_count"], 1)
        self.assertEqual(record["stalls"], [{
            "start_elapsed_seconds": 1.5, "end_elapsed_seconds": 5.0,
            "max_age_ms": 4500, "bytes_in_at_start": 100,
            "bytes_in_at_end": 600, "observation_errors": 0,
        }])

    def test_source_stall_history_is_bounded_but_first_and_last_survive(self):
        runner = load_runner()
        record = runner.new_source_record()
        for index in range(100):
            runner.record_source_sample(record, index * 2.0, 1500, index * 10)
            runner.record_source_sample(record, index * 2.0 + 1.0, 10,
                                        index * 10 + 1)
        self.assertEqual(record["stalls_over_1000_ms_count"], 100)
        self.assertEqual(record["first_stall"]["start_elapsed_seconds"], 0.0)
        self.assertEqual(record["last_stall"]["start_elapsed_seconds"], 198.0)
        self.assertEqual(len(record["stalls"]), 32)
        self.assertEqual(record["stalls"][0]["start_elapsed_seconds"], 136.0)

    def test_source_poll_error_marks_episode_incomplete(self):
        runner = load_runner()
        record = runner.new_source_record()
        runner.record_source_sample(record, 1.5, 1500, 100)
        runner.record_source_error(record, 2.0)
        self.assertEqual(record["first_stall"]["observation_errors"], 1)
        self.assertEqual(record["open_stall"]["observation_errors"], 1)
        runner.record_source_sample(record, 4.0, 20, 600)
        self.assertEqual(record["errors"], 1)
        self.assertEqual(record["max_poll_interval_ms"], 2000)
        self.assertEqual(record["stalls"][0]["observation_errors"], 1)

    def test_source_errors_locate_missing_evidence_between_successes(self):
        runner = load_runner()
        record = runner.new_source_record()
        runner.record_source_sample(record, 1.0, 20, 100)
        runner.record_source_error(record, 1.5)
        runner.record_source_error(record, 2.0)
        self.assertEqual(record["open_error_gap"]["start_elapsed_seconds"], 1.0)
        self.assertEqual(record["open_error_gap"]["last_error_elapsed_seconds"], 2.0)
        runner.record_source_sample(record, 2.5, 20, 200)
        self.assertEqual(record["error_gaps_count"], 1)
        self.assertEqual(record["error_gaps"], [{
            "start_elapsed_seconds": 1.0, "first_error_elapsed_seconds": 1.5,
            "last_error_elapsed_seconds": 2.0, "end_elapsed_seconds": 2.5,
            "errors": 2, "had_prior_sample": True,
        }])
        self.assertIsNone(record["open_error_gap"])

    def test_source_error_and_poll_gap_histories_are_bounded(self):
        runner = load_runner()
        record = runner.new_source_record()
        for index in range(100):
            runner.record_source_error(record, index * 2.0 + 0.5)
            runner.record_source_sample(record, index * 2.0 + 1.0, 20, index)
        self.assertEqual(record["error_gaps_count"], 100)
        self.assertEqual(record["first_error_gap"]["first_error_elapsed_seconds"], 0.5)
        self.assertEqual(record["last_error_gap"]["first_error_elapsed_seconds"], 198.5)
        self.assertEqual(len(record["error_gaps"]), 32)
        self.assertEqual(record["first_error_gap"]["had_prior_sample"], False)
        self.assertEqual(record["poll_gaps_over_750_ms_count"], 99)
        self.assertEqual(len(record["poll_gaps"]), 32)
        self.assertEqual(record["first_poll_gap"]["start_elapsed_seconds"], 1.0)
        self.assertEqual(record["last_poll_gap"]["end_elapsed_seconds"], 198.5)

    def test_source_stats_trickle_has_absolute_deadline(self):
        runner = load_runner()
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.settimeout(2)
        server_stop = threading.Event()

        def serve():
            try:
                connection, _ = listener.accept()
                with connection:
                    connection.recv(4096)
                    connection.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\n")
                    while not server_stop.wait(0.05):
                        connection.sendall(b"x")
            except OSError:
                pass

        server = threading.Thread(target=serve, daemon=True)
        server.start()
        start = time.monotonic()
        try:
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with self.assertRaises((OSError, RuntimeError)):
                runner.source_stats(opener, listener.getsockname()[1], "secret", "soak_a")
            self.assertLess(time.monotonic() - start, 1.5)
        finally:
            server_stop.set()
            listener.close()
            server.join(timeout=2)

    def test_source_stats_projects_only_counters_from_authenticated_response(self):
        runner = load_runner()
        secret = "tshttp://private.example/input?token=do-not-record"
        response = json.dumps({
            "name": "soak_a", "inputs": [{"url": secret}],
            "stats": {"status": "running", "media_age_ms": 1234,
                      "bytes_in": 5678, "private_debug": secret},
        }).encode()
        server, thread, received = serve_source_responses([response])
        try:
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            result = runner.source_stats(opener, server.server_port, "secret", "soak_a")
            self.assertEqual(result, (1234, 5678))
            self.assertEqual(received[0][0], "/streamer/api/v3/streams/soak_a")
            self.assertEqual(received[0][1], "Basic YWRtaW46c2VjcmV0")
            self.assertNotIn(secret, json.dumps(result))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_source_stats_rejects_invalid_counters_and_oversized_body(self):
        runner = load_runner()
        responses = [
            json.dumps({"stats": {"media_age_ms": True, "bytes_in": 10}}).encode(),
            json.dumps({"stats": {"media_age_ms": -1, "bytes_in": 10}}).encode(),
            json.dumps({"stats": {"media_age_ms": 1, "bytes_in": "10"}}).encode(),
            b"x" * 1_000_001,
        ]
        server, thread, _ = serve_source_responses(responses)
        try:
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            for _ in range(4):
                with self.assertRaises((RuntimeError, ValueError)):
                    runner.source_stats(opener, server.server_port, "secret", "soak_a")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_cli_accepts_explicit_binary_state_and_revision(self):
        run = subprocess.run([sys.executable, str(SCRIPT), "--help"],
                             capture_output=True, text=True, timeout=5)
        self.assertEqual(run.returncode, 0)
        for flag in ("--binary", "--state", "--revision", "--observe-source"):
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

    def test_slow_trickle_cannot_hold_viewer_past_deadline(self):
        runner = load_runner()
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.settimeout(2)
        server_stop = threading.Event()

        def serve():
            try:
                connection, _ = listener.accept()
                with connection:
                    connection.recv(4096)
                    connection.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\n")
                    while not server_stop.wait(0.05):
                        connection.sendall(b"G")
            except (OSError, TimeoutError):
                pass

        server = threading.Thread(target=serve, daemon=True)
        server.start()
        stop = threading.Event()
        started = time.monotonic()
        viewers = {"soak_a:1": runner.new_viewer_record()}
        reader = threading.Thread(
            target=runner.viewer,
            args=("soak_a", 1, listener.getsockname()[1], started,
                  started + 0.35, stop, viewers, threading.Lock()),
            daemon=True,
        )
        reader.start()
        reader.join(timeout=1.5)
        held_past_deadline = reader.is_alive()
        stop.set()
        server_stop.set()
        listener.close()
        reader.join(timeout=2)
        server.join(timeout=2)
        self.assertFalse(held_past_deadline)

    @unittest.skipUnless(sys.platform.startswith("linux"), "requires Linux process groups")
    def test_cleanup_reaps_descendant_after_daemon_leader_exits(self):
        runner = load_runner()
        with tempfile.TemporaryDirectory() as directory:
            child_path = Path(directory) / "child.pid"
            code = (
                "import os,sys,time; p=os.fork(); "
                "open(sys.argv[1],'w').write(str(p)) if p else time.sleep(30)"
            )
            leader = subprocess.Popen([sys.executable, "-c", code, str(child_path)],
                                      start_new_session=True)
            try:
                leader.wait(timeout=5)
                child_pid = int(child_path.read_text())
                (Path(directory) / "cgroup.procs").write_text(
                    f"{os.getpid()}\n{child_pid}\n", encoding="utf-8")
                runner.terminate_owned_scope(Path(directory), grace_seconds=0.2)
                for _ in range(30):
                    stat_path = Path(f"/proc/{child_pid}/stat")
                    if not stat_path.exists() or stat_path.read_text().split(") ", 1)[1][0] == "Z":
                        break
                    time.sleep(0.05)
                else:
                    self.fail("orphaned child remained running")
            finally:
                if child_path.exists():
                    try:
                        os.kill(int(child_path.read_text()), signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    def test_log_drain_discards_bytes_after_limit(self):
        runner = load_runner()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "daemon.log"
            read_fd, write_fd = os.pipe()
            os.write(write_fd, b"x" * 4096)
            os.close(write_fd)
            status = {"truncated": False}
            with os.fdopen(read_fd, "rb") as source, path.open("wb") as output:
                runner.drain_log(source, output, 1024, status)
            self.assertEqual(path.stat().st_size, 1024)
            self.assertTrue(status["truncated"])


if __name__ == "__main__":
    unittest.main()
