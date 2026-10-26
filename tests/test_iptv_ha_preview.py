"""Release-gate checks that do not start a database or media daemon."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class IptvHaGatePreviewTests(unittest.TestCase):
    def test_missing_explicit_postgres_fails_before_running_cargo(self):
        with tempfile.TemporaryDirectory() as directory:
            cargo = Path(directory) / 'cargo'
            cargo.write_text("#!/bin/sh\nprintf 'cargo unexpectedly started\\n' >&2\nexit 75\n")
            cargo.chmod(0o755)
            env = os.environ.copy()
            env.update({
                'PATH': f'{directory}:/usr/bin:/bin',
                'FLUSSONIX_TEST_PG_BINDIR': '/absent/flussonix-test-pg/bin',
                'FLUSSONIX_TEST_PG_SHAREDIR': '/absent/flussonix-test-pg/share',
            })
            result = subprocess.run(
                ['bash', str(ROOT / 'scripts/qualify-iptv-ha.sh')],
                cwd=ROOT, env=env, capture_output=True, text=True, timeout=5,
                check=False,
            )
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn('native PostgreSQL', result.stderr)
        self.assertIn('FLUSSONIX_TEST_PG_BINDIR', result.stderr)
        self.assertNotIn('cargo unexpectedly started', result.stderr)


if __name__ == '__main__':
    unittest.main()
