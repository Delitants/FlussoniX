"""Native live-probe regressions that need no vendor service."""

import importlib.util
from pathlib import Path
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/qualify-live-vendor.py"
SPEC = importlib.util.spec_from_file_location("qualify_live_vendor", SCRIPT)
PROBE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROBE)


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


if __name__ == "__main__":
    unittest.main()
