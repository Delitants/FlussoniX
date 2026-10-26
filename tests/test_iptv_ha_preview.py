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

    def test_pg_config_selection_is_forwarded_to_rust_fixture(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tools = root / 'tools'
            bindir = root / 'pg-bin'
            sharedir = root / 'pg-share'
            for path in (tools, bindir, sharedir):
                path.mkdir()
            (sharedir / 'postgres.bki').write_text('fixture\n')
            for name in ('initdb', 'postgres', 'pg_basebackup', 'pg_dump', 'pg_restore'):
                binary = bindir / name
                binary.write_text('#!/bin/sh\nexit 0\n')
                binary.chmod(0o755)
            pg_config = tools / 'pg_config'
            pg_config.write_text(
                '#!/bin/sh\ncase "$1" in\n'
                '  --bindir) printf "%s\\n" "$TEST_PG_BIN";;\n'
                '  --sharedir) printf "%s\\n" "$TEST_PG_SHARE";;\n'
                'esac\n'
            )
            pg_config.chmod(0o755)
            cargo = tools / 'cargo'
            cargo.write_text(
                '#!/bin/sh\nprintf "%s|%s\\n" "$FLUSSONIX_TEST_PG_BINDIR" '
                '"$FLUSSONIX_TEST_PG_SHAREDIR" >> "$CARGO_CAPTURE"\n'
            )
            cargo.chmod(0o755)
            capture = root / 'cargo-env'
            env = os.environ.copy()
            env.pop('FLUSSONIX_TEST_PG_BINDIR', None)
            env.pop('FLUSSONIX_TEST_PG_SHAREDIR', None)
            env.update({
                'PATH': f'{tools}:/usr/bin:/bin',
                'TEST_PG_BIN': str(bindir),
                'TEST_PG_SHARE': str(sharedir),
                'CARGO_CAPTURE': str(capture),
            })
            result = subprocess.run(
                ['bash', str(ROOT / 'scripts/qualify-iptv-ha.sh')],
                cwd=ROOT, env=env, capture_output=True, text=True, timeout=5,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                capture.read_text().splitlines(),
                [f'{bindir}|{sharedir}'] * 4,
            )


if __name__ == '__main__':
    unittest.main()
