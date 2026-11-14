"""Boundary timing in the sustained HTTP runner, using an owned fake response."""

from pathlib import Path
import runpy
import threading
import unittest
from unittest import mock


RUNNER = runpy.run_path(str(Path(__file__).resolve().parents[1] / "scripts/run-sustained-http.py"))


class SustainedHttpRunnerBoundaryTests(unittest.TestCase):
    def test_first_media_includes_viewer_thread_start_delay(self):
        stop = threading.Event()
        response = mock.Mock(status=200)

        def first_packet(_size):
            stop.set()
            return b"\x47" * 188

        response.read1.side_effect = first_packet
        connection = mock.Mock()
        connection.getresponse.return_value = response
        record = RUNNER["new_viewer_record"]()

        with mock.patch.object(RUNNER["http"].client, "HTTPConnection", return_value=connection), \
             mock.patch.object(RUNNER["time"], "monotonic", return_value=5.0):
            RUNNER["viewer"]("soak_a", 1, 1, 0.0, 100.0, stop,
                             {"soak_a:1": record}, threading.Lock())

        self.assertEqual(record["bytes"], 188)
        self.assertEqual(record["first_byte_max_ms"], 5000)


if __name__ == "__main__":
    unittest.main()
