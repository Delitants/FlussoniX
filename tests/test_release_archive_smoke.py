"""A packaged release must be checked before its daemon is executed."""
import hashlib
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'scripts/smoke-release-archive.py'
REVISION = '1234567890abcdef1234567890abcdef12345678'
SPEC = importlib.util.spec_from_file_location('release_archive_smoke', SCRIPT)
smoke_script = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(smoke_script)


class ReleaseArchiveSmokeTests(unittest.TestCase):
    def test_smoke_daemon_does_not_inherit_host_configuration(self):
        with tempfile.TemporaryDirectory(prefix='flussonix-smoke-test-') as temp:
            destination = Path(temp) / 'package'
            destination.mkdir()
            captured = {}

            def capture_launch(_command, **kwargs):
                captured.update(kwargs['env'])
                raise RuntimeError('launch intercepted')

            parent_env = {
                'FLUSSONIX_IPTV_HA_DB_CONFIG': '/etc/live-iptv-db.json',
                'FLUSSONIX_SRTP_RECEIVER_LEDGER': '/etc/live-srtp-ledger',
                'FLUSSONIX_ALLOW_INSECURE_REMOTE_MANAGEMENT': 'true',
                'HTTP_PROXY': 'http://example.invalid',
            }
            with mock.patch.dict(os.environ, parent_env, clear=True), \
                 mock.patch.object(smoke_script.subprocess, 'Popen',
                                   side_effect=capture_launch):
                with self.assertRaisesRegex(RuntimeError, 'launch intercepted'):
                    smoke_script.smoke(destination)

            self.assertEqual(captured['FLUSSONIX_ADMIN_USER'], 'admin')
            self.assertTrue(captured['FLUSSONIX_ADMIN_PASSWORD'])
            self.assertTrue(captured['FLUSSONIX_PEER_KEY'])
            self.assertTrue(captured['FLUSSONIX_SRT_PLAY_PASSPHRASE'])
            self.assertNotIn('FLUSSONIX_IPTV_HA_DB_CONFIG', captured)
            self.assertNotIn('FLUSSONIX_SRTP_RECEIVER_LEDGER', captured)
            self.assertNotIn('FLUSSONIX_ALLOW_INSECURE_REMOTE_MANAGEMENT', captured)
            self.assertNotIn('HTTP_PROXY', captured)

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
