"""A packaged release must be checked before its daemon is executed."""
import hashlib
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'scripts/smoke-release-archive.py'
REVISION = '1234567890abcdef1234567890abcdef12345678'


class ReleaseArchiveSmokeTests(unittest.TestCase):
    def test_rejects_checksum_mismatch_before_opening_archive(self):
        with tempfile.TemporaryDirectory(prefix='flussonix-smoke-test-') as temp:
            archive = Path(temp) / 'flussonix-test.tar.gz'
            archive.write_bytes(b'not a real archive')
            Path(str(archive) + '.sha256').write_text('0' * 64 + '  ' + archive.name + '\n')
            result = subprocess.run([sys.executable, str(SCRIPT), '--archive', str(archive),
                                     '--revision', REVISION], text=True,
                                    capture_output=True, timeout=5)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('checksum mismatch', result.stderr)

    def test_rejects_wrong_revision_before_launching_daemon(self):
        with tempfile.TemporaryDirectory(prefix='flussonix-smoke-test-') as temp:
            archive = Path(temp) / 'flussonix-test.tar.gz'
            archive.write_bytes(b'not a real archive')
            digest = hashlib.sha256(archive.read_bytes()).hexdigest()
            Path(str(archive) + '.sha256').write_text(digest + '  ' + archive.name + '\n')
            result = subprocess.run([sys.executable, str(SCRIPT), '--archive', str(archive),
                                     '--revision', 'wrong'], text=True,
                                    capture_output=True, timeout=5)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('revision must', result.stderr)

    def test_rejects_linked_checksum_before_opening_archive(self):
        with tempfile.TemporaryDirectory(prefix='flussonix-smoke-test-') as temp:
            archive = Path(temp) / 'flussonix-test.tar.gz'
            archive.write_bytes(b'not a real archive')
            checksum = Path(temp) / 'elsewhere.sha256'
            checksum.write_text(hashlib.sha256(archive.read_bytes()).hexdigest()
                                + '  ' + archive.name + '\n')
            Path(str(archive) + '.sha256').symlink_to(checksum)
            result = subprocess.run([sys.executable, str(SCRIPT), '--archive', str(archive),
                                     '--revision', REVISION], text=True,
                                    capture_output=True, timeout=5)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('checksum sidecar must be a bounded regular file', result.stderr)
