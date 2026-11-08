"""Exercise generated service artifacts through systemd's real unit verifier."""
import hashlib
import json
import os
from pathlib import Path
import pwd
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'scripts/service-preview.py'


class ServicePreviewTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='flussonix-service-test-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.root.chmod(0o755)
        self.account = pwd.getpwnam('nobody') if os.geteuid() == 0 else pwd.getpwuid(os.geteuid())
        self.release = self.root / 'release'
        (self.release / 'bin').mkdir(parents=True)
        (self.release / 'web').mkdir()
        shutil.copy2('/usr/bin/true', self.release / 'bin/flussonix')
        (self.release / 'web/index.html').write_text('<html>Owned service fixture</html>')
        # Model the packaged public release independently of the caller's umask.
        for directory in (self.release, self.release / 'bin', self.release / 'web'):
            directory.chmod(0o755)
        (self.release / 'web/index.html').chmod(0o644)
        files = {str(p.relative_to(self.release)): {'sha256': hashlib.sha256(p.read_bytes()).hexdigest(),
                 'bytes': p.stat().st_size, 'mode': oct(p.stat().st_mode & 0o777)}
                 for p in self.release.rglob('*') if p.is_file()}
        (self.release / 'manifest.json').write_text(json.dumps({'format': 1, 'name': 'FlussoniX', 'files': files}))
        self.state = self.root / 'state'
        self.state.mkdir(mode=0o700)
        if os.geteuid() == 0:
            os.chown(self.state, self.account.pw_uid, self.account.pw_gid)
        (self.state / 'config.json').write_text('{"streams":[]}')
        (self.state / 'config.json').chmod(0o600)
        if os.geteuid() == 0:
            os.chown(self.state / 'config.json', self.account.pw_uid, self.account.pw_gid)
        self.credentials = self.root / 'credentials.env'
        self.unit = self.root / 'flussonix-owned.service'
        self.env = {k: v for k, v in os.environ.items() if not k.startswith('FLUSSONIX_')}
        self.password = 'owned "quote" \\ $ # % ` secret'
        self.peer = 'owned-peer-key-123456'
        self.env.update(FLUSSONIX_ADMIN_USER='owned-admin', FLUSSONIX_ADMIN_PASSWORD=self.password,
                        FLUSSONIX_PEER_KEY=self.peer)

    def command(self, *args):
        return subprocess.run([sys.executable, str(SCRIPT), *map(str, args)], env=self.env,
                              capture_output=True, text=True, timeout=10)

    def create_credentials(self):
        result = self.command('credentials', '--from-env', '--output', self.credentials)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result

    def render(self, *extra):
        return self.command('render', '--instance', 'owned', '--release-dir', self.release,
                            '--state-dir', self.state, '--credentials', self.credentials,
                            '--user', self.account.pw_name, '--listen', '127.0.0.1:18210',
                            '--ffmpeg', Path('/usr/bin/true').resolve(), '--output', self.unit, *extra)

    def tls_pair(self, name):
        cert = self.root / f'{name}.crt'
        key = self.root / f'{name}.key'
        cert.write_text('owned certificate fixture')
        key.write_text('owned private key fixture')
        cert.chmod(0o644)
        key.chmod(0o600)
        if os.geteuid() == 0:
            os.chown(key, self.account.pw_uid, self.account.pw_gid)
        return cert, key

    def test_render_adds_stable_srtp_history_without_initializing_it(self):
        self.create_credentials()
        ledger = self.state / 'srtp-receiver-ledger'
        lock = self.state / 'srtp-receiver-ledger.lock'
        result = self.render()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f'"--srtp-receiver-ledger" "{ledger}"', self.unit.read_text())
        self.assertFalse(ledger.exists())
        self.assertFalse(lock.exists())
        self.unit.unlink()

        ledger.write_bytes(b'FXSRTP1\n')
        lock.write_bytes(b'')
        for path in (ledger, lock):
            path.chmod(0o600)
            if os.geteuid() == 0:
                os.chown(path, self.account.pw_uid, self.account.pw_gid)
        result = self.render()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f'"--srtp-receiver-ledger" "{ledger}"', self.unit.read_text())
        self.assertEqual(ledger.read_bytes(), b'FXSRTP1\n')

    def test_unsafe_existing_srtp_history_or_lock_refuses_unit(self):
        self.create_credentials()
        ledger = self.state / 'srtp-receiver-ledger'
        lock = self.state / 'srtp-receiver-ledger.lock'
        for target in (ledger, lock):
            with self.subTest(target=target.name, defect='symlink'):
                self.unit.unlink(missing_ok=True)
                target.symlink_to(self.state / 'config.json')
                result = self.render()
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.unit.exists())
                target.unlink()
        ledger.write_bytes(b'FXSRTP1\n')
        lock.write_bytes(b'')
        for path in (ledger, lock):
            path.chmod(0o600)
            if os.geteuid() == 0:
                os.chown(path, self.account.pw_uid, self.account.pw_gid)
        for target in (ledger, lock):
            with self.subTest(target=target.name, defect='public-mode'):
                self.unit.unlink(missing_ok=True)
                target.chmod(0o644)
                result = self.render()
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.unit.exists())
                target.chmod(0o600)
        ledger.write_bytes(b'wrong\n')
        self.unit.unlink(missing_ok=True)
        result = self.render()
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.unit.exists())

    def test_combined_secure_listeners_render_a_valid_unit(self):
        if not shutil.which('systemd-analyze'):
            self.skipTest('requires systemd-analyze')
        self.create_credentials()
        https_cert, https_key = self.tls_pair('https')
        rtsps_cert, rtsps_key = self.tls_pair('rtsps')
        fx_cert, fx_key = self.tls_pair('fxlink')
        result = self.render('--https-listen', '127.0.0.1:18443', '--https-cert', https_cert,
                             '--https-key', https_key, '--https-only',
                             '--rtsp-listen', '127.0.0.1:18554', '--rtsps-listen', '127.0.0.1:18555',
                             '--rtsps-cert', rtsps_cert, '--rtsps-key', rtsps_key,
                             '--rtsp-udp-ports', '22000-22003', '--rtsp-udp-mbps', '42',
                             '--fxlink-listen', '127.0.0.1:18447', '--fxlink-cert', fx_cert,
                             '--fxlink-key', fx_key)
        self.assertEqual(result.returncode, 0, result.stderr)
        unit = self.unit.read_text()
        for expected in ('"--https-listen" "127.0.0.1:18443"', '"--https-only"',
                         '"--rtsps-listen" "127.0.0.1:18555"',
                         '"--rtsp-udp-ports" "22000-22003"',
                         '"--rtsp-udp-mbps" "42"',
                         '"--fxlink-listen" "127.0.0.1:18447"'):
            self.assertIn(expected, unit)
        verify = subprocess.run(['systemd-analyze', 'verify', '--man=no', str(self.unit)],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(verify.returncode, 0, verify.stderr)

    def test_incomplete_tls_and_invalid_udp_controls_create_no_unit(self):
        self.create_credentials()
        cert, key = self.tls_pair('https')
        for extra in [('--https-only',), ('--https-listen', '127.0.0.1:18443'),
                      ('--https-listen', '127.0.0.1:18443', '--https-cert', cert),
                      ('--https-cert', cert, '--https-key', key),
                      ('--rtsps-listen', '127.0.0.1:18555'),
                      ('--fxlink-listen', '127.0.0.1:18447'),
                      ('--rtsp-udp-ports', '22000-22001'),
                      ('--rtsp-listen', '127.0.0.1:18554', '--rtsp-udp-ports', '22001-22002'),
                      ('--rtsp-listen', '127.0.0.1:18554', '--rtsp-udp-ports', '22000-22257'),
                      ('--rtsp-listen', '127.0.0.1:18554', '--rtsp-udp-mbps', '42'),
                      ('--rtsp-listen', '127.0.0.1:18554', '--rtsp-udp-ports', '22000-22001',
                       '--rtsp-udp-mbps', '10001')]:
            with self.subTest(extra=extra):
                self.unit.unlink(missing_ok=True)
                result = self.render(*extra)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.unit.exists())

    def test_overlapping_tcp_bind_and_public_private_key_are_rejected(self):
        self.create_credentials()
        cert, key = self.tls_pair('https')
        for address in ('127.0.0.1:18210', '0.0.0.0:18210'):
            with self.subTest(address=address):
                self.unit.unlink(missing_ok=True)
                result = self.render('--https-listen', address, '--https-cert', cert, '--https-key', key)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.unit.exists())
        key.chmod(0o644)
        self.unit.unlink(missing_ok=True)
        result = self.render('--https-listen', '127.0.0.1:18443', '--https-cert', cert, '--https-key', key)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.unit.exists())
        if os.geteuid() == 0:
            key.chmod(0o600)
            os.chown(key, 0, 0)
            result = self.render('--https-listen', '127.0.0.1:18443', '--https-cert', cert, '--https-key', key)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(self.unit.exists())

    def test_srt_options_render_without_exposing_passphrase(self):
        self.env['FLUSSONIX_SRT_PLAY_PASSPHRASE'] = 'owned-srt-passphrase-123!'
        saved = self.command('credentials', '--from-env', '--with-srt-passphrase', '--output', self.credentials)
        self.assertEqual(saved.returncode, 0, saved.stderr)
        result = self.render('--srt-play-listen', '127.0.0.1:18448',
                             '--srt-play-public-url', 'srt://cdn.example.test:18448',
                             '--srt-play-latency', '750', '--srt-play-client-limit', '128',
                             '--srt-play-allow-direct-token')
        self.assertEqual(result.returncode, 0, result.stderr)
        unit = self.unit.read_text()
        for expected in ('"--srt-play-listen" "127.0.0.1:18448"',
                         '"--srt-play-public-url" "srt://cdn.example.test:18448"',
                         '"--srt-play-latency" "750"',
                         '"--srt-play-client-limit" "128"',
                         '"--srt-play-allow-direct-token"'):
            self.assertIn(expected, unit)
        self.assertIn('FLUSSONIX_SRT_PLAY_PASSPHRASE=', self.credentials.read_text())
        self.assertNotIn(self.env['FLUSSONIX_SRT_PLAY_PASSPHRASE'], unit + saved.stdout + saved.stderr
                         + result.stdout + result.stderr)

    def test_srt_dependent_options_and_udp_collision_create_no_unit(self):
        self.create_credentials()
        cert, key = self.tls_pair('fxlink')
        invalid = [('--srt-play-public-url', 'srt://cdn.example.test:18448'),
                   ('--srt-play-latency', '750'), ('--srt-play-client-limit', '128'),
                   ('--srt-play-allow-direct-token',),
                   ('--srt-play-listen', '127.0.0.1:18448', '--srt-play-latency', '0'),
                   ('--srt-play-listen', '127.0.0.1:18448', '--srt-play-latency', '10001'),
                   ('--srt-play-listen', '127.0.0.1:18448', '--srt-play-client-limit', '0'),
                   ('--srt-play-listen', '127.0.0.1:18448', '--srt-play-client-limit', '4097'),
                   ('--srt-play-listen', '127.0.0.1:18448', '--srt-play-public-url',
                    'srt://cdn.example.test:18448?token=private'),
                   ('--srt-play-listen', '127.0.0.1:18448', '--fxlink-listen', '0.0.0.0:18448',
                    '--fxlink-cert', cert, '--fxlink-key', key)]
        for extra in invalid:
            with self.subTest(extra=extra):
                self.unit.unlink(missing_ok=True)
                result = self.render(*extra)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.unit.exists())

    def test_srt_passphrase_opt_in_rejects_missing_and_invalid_values(self):
        for value in (None, 'too-short', 'x' * 80, 'bad\nline', 'nonascii-\u00e9-key'):
            with self.subTest(value_kind='missing' if value is None else len(value)):
                self.credentials.unlink(missing_ok=True)
                if value is None:
                    self.env.pop('FLUSSONIX_SRT_PLAY_PASSPHRASE', None)
                else:
                    self.env['FLUSSONIX_SRT_PLAY_PASSPHRASE'] = value
                result = self.command('credentials', '--from-env', '--with-srt-passphrase',
                                      '--output', self.credentials)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.credentials.exists())
                if value is not None:
                    self.assertNotIn(value, result.stdout + result.stderr)

    def test_cluster_cdn_options_render_in_stable_order(self):
        self.create_credentials()
        ca = self.root / 'iptv-ca.crt'
        ca.write_text('owned CA fixture')
        ca.chmod(0o644)
        result = self.render('--role', 'cdn', '--node-name', 'cdn-east', '--uplink-mbps', '750.5',
                             '--client-limit', '5000', '--uplink-interface', 'eth0', '--drain',
                             '--iptv-data', self.state / 'iptv.json',
                             '--iptv-public-origin', 'https://cdn.example.test',
                             '--iptv-authority', 'https://authority-a.example.test',
                             '--iptv-authority-secondary', 'https://authority-b.example.test',
                             '--iptv-authority-cluster-id', 'a3192376-6df5-4b76-a15c-43942fd3d466',
                             '--iptv-authority-ca', ca)
        self.assertEqual(result.returncode, 0, result.stderr)
        unit = self.unit.read_text()
        for expected in ('"--role" "cdn"', '"--node-name" "cdn-east"',
                         '"--uplink-mbps" "750.5"', '"--client-limit" "5000"',
                         '"--uplink-interface" "eth0"', '"--drain"',
                         '"--iptv-authority-secondary" "https://authority-b.example.test"'):
            self.assertIn(expected, unit)
        self.assertLess(unit.index('"--role"'), unit.index('"--iptv-authority"'))

    def test_ha_authority_unit_has_https_and_private_db_path(self):
        if not shutil.which('systemd-analyze'):
            self.skipTest('requires systemd-analyze')
        self.create_credentials()
        cert, key = self.tls_pair('https')
        database = self.root / 'iptv-ha-db.json'
        database.write_text('{"owned":"private database fixture"}')
        database.chmod(0o600)
        if os.geteuid() == 0:
            os.chown(database, self.account.pw_uid, self.account.pw_gid)
        result = self.render('--https-listen', '127.0.0.1:18443', '--https-cert', cert,
                             '--https-key', key, '--https-only', '--iptv-lease-authority',
                             '--iptv-ha-db-config', database,
                             '--iptv-authority', 'https://authority-a.example.test',
                             '--role', 'source', '--node-name', 'authority-a')
        self.assertEqual(result.returncode, 0, result.stderr)
        unit = self.unit.read_text()
        self.assertIn('"--iptv-lease-authority"', unit)
        self.assertIn(f'"--iptv-ha-db-config" "{database}"', unit)
        self.assertNotIn('private database fixture', unit + result.stdout + result.stderr)
        verify = subprocess.run(['systemd-analyze', 'verify', '--man=no', str(self.unit)],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(verify.returncode, 0, verify.stderr)

    def test_invalid_iptv_and_cluster_options_create_no_unit(self):
        self.create_credentials()
        cert, key = self.tls_pair('https')
        database = self.root / 'iptv-ha-db.json'
        database.write_text('{"owned":"private database fixture"}')
        database.chmod(0o600)
        if os.geteuid() == 0:
            os.chown(database, self.account.pw_uid, self.account.pw_gid)
        uuid = 'a3192376-6df5-4b76-a15c-43942fd3d466'
        invalid = [('--iptv-lease-authority',),
                   ('--iptv-lease-authority', '--iptv-authority', 'https://authority-a.example.test'),
                   ('--iptv-ha-db-config', database),
                   ('--iptv-data', self.root / 'outside-state.json'),
                   ('--iptv-authority', 'https://authority-a.example.test',
                    '--iptv-authority-secondary', 'https://authority-a.example.test',
                    '--iptv-authority-cluster-id', uuid),
                   ('--iptv-authority', 'https://[2001:db8::1]',
                    '--iptv-authority-secondary', 'https://[2001:db8:0:0:0:0:0:1]',
                    '--iptv-authority-cluster-id', uuid),
                   ('--iptv-authority', 'https://authority-a.example.test',
                    '--iptv-authority-secondary', 'https://authority-b.example.test',
                    '--iptv-authority-cluster-id', '00000000-0000-0000-0000-000000000000'),
                   ('--iptv-authority', 'https://authority-a.example.test',
                    '--iptv-authority-secondary', 'http://authority-b.example.test',
                    '--iptv-authority-cluster-id', uuid),
                   ('--iptv-authority', 'https://authority-a.example.test',
                    '--iptv-authority-secondary', 'https://authority-b.example.test'),
                   ('--iptv-authority', 'https://user:pass@authority-a.example.test'),
                   ('--iptv-authority', 'https://authority-a.example.test/%n'),
                   ('--role', 'load-balancer'), ('--node-name', 'bad%name'),
                   ('--uplink-interface', 'bad interface'), ('--uplink-mbps', '0'),
                   ('--uplink-mbps', 'nan'), ('--uplink-mbps', '1e3'),
                   ('--client-limit', '0'), ('--client-limit', '18446744073709551616')]
        for extra in invalid:
            with self.subTest(extra=extra):
                self.unit.unlink(missing_ok=True)
                result = self.render(*extra)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.unit.exists())
        database.chmod(0o644)
        self.unit.unlink(missing_ok=True)
        result = self.render('--https-listen', '127.0.0.1:18443', '--https-cert', cert,
                             '--https-key', key, '--iptv-lease-authority',
                             '--iptv-ha-db-config', database,
                             '--iptv-authority', 'https://authority-a.example.test')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.unit.exists())

    def test_generated_unit_passes_systemd_and_keeps_secrets_out(self):
        if not shutil.which('systemd-analyze'):
            self.skipTest('requires systemd-analyze')
        self.create_credentials()
        result = self.render()
        self.assertEqual(result.returncode, 0, result.stderr)
        verify = subprocess.run(['systemd-analyze', 'verify', '--man=no', str(self.unit)],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(verify.returncode, 0, verify.stderr)
        # The unit is an executable artifact; only paths to protected secrets may appear.
        self.assertNotIn(self.password, self.unit.read_text() + result.stdout + result.stderr)
        self.assertNotIn(self.peer, self.unit.read_text() + result.stdout + result.stderr)
        self.assertEqual((self.state / 'config.json').read_text(), '{"streams":[]}')

    def test_credentials_are_private_and_existing_files_are_preserved(self):
        result = self.create_credentials()
        self.assertEqual(self.credentials.stat().st_mode & 0o777, 0o600)
        self.assertNotIn(self.password, result.stdout + result.stderr)
        before = self.credentials.read_bytes()
        again = self.command('credentials', '--from-env', '--output', self.credentials)
        self.assertNotEqual(again.returncode, 0)
        self.assertEqual(self.credentials.read_bytes(), before)
        self.assertEqual(self.render().returncode, 0)
        before = self.unit.read_bytes()
        self.assertNotEqual(self.render().returncode, 0)
        self.assertEqual(self.unit.read_bytes(), before)

    def test_bad_credentials_do_not_create_output(self):
        for password, peer in [('', self.peer), ('line\nbreak', self.peer), ('valid', 'short'),
                               ('nonascii-\u00e9', self.peer), ('valid', 'nonascii-key-\u2603')]:
            with self.subTest(password_kind=len(password), key_kind=len(peer)):
                self.env.update(FLUSSONIX_ADMIN_PASSWORD=password, FLUSSONIX_PEER_KEY=peer)
                result = self.command('credentials', '--from-env', '--output', self.credentials)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.credentials.exists())

    def test_basic_username_cannot_contain_colon(self):
        self.env['FLUSSONIX_ADMIN_USER'] = 'owned:admin'
        result = self.command('credentials', '--from-env', '--output', self.credentials)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.credentials.exists())

    def test_invalid_unit_identity_user_listener_or_path_does_not_write(self):
        self.create_credentials()
        for args in [('--instance', '../flussonic'), ('--user', 'root'),
                     ('--listen', '127.0.0.1:80'), ('--listen', 'localhost:18210'),
                     ('--listen', '127.0.0.1:18210\nExecStart=/bin/false'),
                     ('--release-dir', str(self.release) + '%i')]:
            with self.subTest(args=args):
                result = self.render(*args)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.unit.exists())

    def test_state_and_credentials_cannot_be_inside_release(self):
        self.create_credentials()
        for extra in [('--state-dir', self.release / 'state'), ('--credentials', self.release / 'secret.env')]:
            with self.subTest(option=extra[0]):
                result = self.render(*extra)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.unit.exists())

    def test_unsafe_credentials_and_changed_release_are_rejected(self):
        self.create_credentials()
        self.credentials.chmod(0o644)
        self.assertNotEqual(self.render().returncode, 0)
        self.credentials.chmod(0o600)
        (self.release / 'web/index.html').write_text('changed after packaging')
        self.assertNotEqual(self.render().returncode, 0)
        self.assertFalse(self.unit.exists())

    def test_writable_credential_ancestor_cannot_supply_service_environment(self):
        self.create_credentials()
        writable = self.root / 'writable-credentials'
        writable.mkdir()
        writable.chmod(0o777)
        self.credentials = self.credentials.rename(writable / 'credentials.env')
        result = self.render()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('ancestor', result.stderr)
        self.assertFalse(self.unit.exists())

    def test_writable_state_ancestor_cannot_replace_service_config(self):
        self.create_credentials()
        writable = self.root / 'writable-state'
        writable.mkdir()
        writable.chmod(0o777)
        self.state = self.state.rename(writable / 'state')
        result = self.render()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('ancestor', result.stderr)
        self.assertFalse(self.unit.exists())

    def test_writable_release_paths_cannot_become_service_executables(self):
        self.create_credentials()
        for path in (self.root, self.release, self.release / 'bin',
                     self.release / 'bin/flussonix', self.release / 'manifest.json'):
            with self.subTest(path=path):
                original = path.stat().st_mode & 0o7777
                path.chmod(original | 0o022)
                try:
                    result = self.render()
                    self.assertNotEqual(result.returncode, 0, result.stdout)
                    self.assertFalse(self.unit.exists())
                finally:
                    path.chmod(original)
                    if self.unit.exists():
                        self.unit.unlink()

    def test_root_render_rejects_release_owned_by_service_account(self):
        if os.geteuid() != 0:
            self.skipTest('root ownership applies to root-rendered service units')
        self.create_credentials()
        os.chown(self.release, self.account.pw_uid, self.account.pw_gid)
        result = self.render()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertFalse(self.unit.exists())

    def test_writable_ffmpeg_executable_cannot_be_used_by_service(self):
        self.create_credentials()
        ffmpeg = self.root / 'ffmpeg'
        shutil.copy2('/usr/bin/true', ffmpeg)
        ffmpeg.chmod(0o777)
        result = self.render('--ffmpeg', ffmpeg)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertFalse(self.unit.exists())

    def test_credential_symlinks_cannot_be_followed_or_overwritten(self):
        target = self.root / 'untouched'
        target.write_text('untouched')
        self.credentials.symlink_to(target)
        result = self.command('credentials', '--from-env', '--output', self.credentials)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(target.read_text(), 'untouched')
        self.assertNotEqual(self.render().returncode, 0)
        self.assertFalse(self.unit.exists())

    def test_resolved_parent_cannot_introduce_unit_substitutions(self):
        unsafe = self.root / 'unsafe%H'
        unsafe.mkdir()
        alias = self.root / 'safe-alias'
        alias.symlink_to(unsafe, target_is_directory=True)
        result = self.command('credentials', '--from-env', '--output', alias / 'credentials.env')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((unsafe / 'credentials.env').exists())

    def test_malformed_manifest_reports_error_without_traceback(self):
        self.create_credentials()
        for document in [[], {'format': 1, 'name': 'FlussoniX', 'files': {
                'bin/flussonix': None, 'web/index.html': {}}}]:
            with self.subTest(document=document):
                (self.release / 'manifest.json').write_text(json.dumps(document))
                result = self.render()
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn('Traceback', result.stderr)
                self.assertFalse(self.unit.exists())

    def test_invalid_ipv6_brackets_are_rejected(self):
        self.create_credentials()
        for listen in ['[[::1]]:18210', '[127.0.0.1]:18210', '::1:18210']:
            with self.subTest(listen=listen):
                self.assertNotEqual(self.render('--listen', listen).returncode, 0)
                self.assertFalse(self.unit.exists())

    def test_maximum_escaped_credentials_can_be_consumed_by_renderer(self):
        self.env.update(FLUSSONIX_ADMIN_USER='$' * 4096, FLUSSONIX_ADMIN_PASSWORD='"' * 4096,
                        FLUSSONIX_PEER_KEY='\\' * 4096)
        self.create_credentials()
        result = self.render()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_unlisted_public_web_content_rejects_service(self):
        self.create_credentials()
        for variant in ['.env', 'config.json', 'unlisted-link']:
            path = self.release / 'web' / variant
            if variant == 'unlisted-link':
                path.symlink_to(self.credentials)
            else:
                path.write_text('owned-private-sentinel')
            try:
                result = self.render()
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.unit.exists())
            finally:
                path.unlink()

    def test_unit_cannot_be_written_to_public_web_tree(self):
        self.create_credentials()
        output = self.release / 'web/flussonix-owned.service'
        self.assertNotEqual(self.render('--output', output).returncode, 0)
        self.assertFalse(output.exists())


if __name__ == '__main__':
    unittest.main()
