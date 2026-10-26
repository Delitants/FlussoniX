"""Credential-transport guard for the optional reference inventory tool."""

import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "tools" / "inspect_demo.py"


class InspectDemoTests(unittest.TestCase):
    def server(self, proxy=False):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.server.seen.append(self.headers.get("Authorization"))
                if proxy:
                    self.send_error(502)
                    return
                body = b'{"stats":{}}' if self.path.startswith("/streamer/api/v3/config") else b'{"streams":[]}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.seen = []
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 2)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server

    def test_remote_http_is_rejected_before_password_input(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "inventory.json"
            environment = os.environ.copy()
            environment.pop("FLUSSONIX_REFERENCE_PASSWORD", None)
            result = subprocess.run(
                [sys.executable, str(SCRIPT), "http://192.0.2.10:8080", "--output", str(output)],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                env=environment,
                timeout=5,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("--allow-insecure-http", result.stderr)
            self.assertNotIn("Reference server password", result.stderr)
            self.assertFalse(output.exists())

    def test_loopback_http_never_forwards_basic_credentials_to_environment_proxy(self):
        origin = self.server()
        proxy = self.server(proxy=True)
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "inventory.json"
            environment = os.environ.copy()
            environment.update({
                "FLUSSONIX_REFERENCE_PASSWORD": "synthetic-secret",
                "HTTP_PROXY": f"http://127.0.0.1:{proxy.server_port}",
                "http_proxy": f"http://127.0.0.1:{proxy.server_port}",
                "NO_PROXY": "",
                "no_proxy": "",
            })
            result = subprocess.run(
                [sys.executable, str(SCRIPT), f"http://127.0.0.1:{origin.server_port}",
                 "--output", str(output)],
                capture_output=True,
                text=True,
                env=environment,
                timeout=5,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(len(origin.seen), 2)
            self.assertEqual(proxy.seen, [])
            self.assertTrue(output.exists())


if __name__ == "__main__":
    unittest.main()
