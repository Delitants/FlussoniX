"""Debian package construction, inspection, and lifecycle boundary tests."""
import gzip
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'scripts/package-deb.py'
REVISION = 'abcdef0123456789abcdef0123456789abcdef01'
EPOCH = 1_700_000_000


class PackageDebTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='flussonix-deb-test-')
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)

    def fixture_archive(self, name='input', *, revision=REVISION, architecture='x86_64',
                        version='0.11.0', extra_files=None, unlisted_files=None,
                        genuine=True):
        binary = Path('/bin/true').read_bytes()
        payload = {
            'bin/flussonix': (binary, 0o755),
            'web/index.html': (b'<html><title>FlussoniX</title></html>\n', 0o644),
            'installation.md': (b'# Install FlussoniX\n', 0o644),
            'LICENSE': (b'Apache License fixture\n', 0o644),
            'NOTICE': (b'FlussoniX fixture notice\n', 0o644),
        }
        payload.update(extra_files or {})
        manifest = {
            'format': 1,
            'name': 'FlussoniX',
            'version': version,
            'source_revision': revision,
            'source_date_epoch': EPOCH,
            'architecture': architecture,
            'files': {
                path: {
                    'sha256': hashlib.sha256(data).hexdigest(),
                    'bytes': len(data),
                    'mode': oct(mode),
                }
                for path, (data, mode) in sorted(payload.items())
            },
        }
        archive = self.directory / f'{name}.tar.gz'
        if genuine:
            prefix = f'flussonix-{version}-linux-{architecture}-{revision[:12]}'
            all_payload = {
                **payload,
                'manifest.json': (
                    (json.dumps(manifest, indent=2, sort_keys=True) + '\n').encode(),
                    0o644,
                ),
                **(unlisted_files or {}),
            }
            with archive.open('wb') as raw:
                with gzip.GzipFile(filename='', fileobj=raw, mode='wb', mtime=EPOCH) as compressed:
                    with tarfile.open(fileobj=compressed, mode='w', format=tarfile.PAX_FORMAT) as tar:
                        directories = {prefix}
                        for relative in all_payload:
                            path = Path(prefix) / relative
                            directories.update(parent.as_posix() for parent in path.parents
                                               if parent != Path('.'))
                        for directory in sorted(directories):
                            entry = tarfile.TarInfo(directory)
                            entry.type = tarfile.DIRTYPE
                            entry.mode = 0o755
                            entry.uid = entry.gid = 0
                            entry.mtime = EPOCH
                            tar.addfile(entry)
                        for relative, (data, mode) in sorted(all_payload.items()):
                            entry = tarfile.TarInfo(f'{prefix}/{relative}')
                            entry.size = len(data)
                            entry.mode = mode
                            entry.uid = entry.gid = 0
                            entry.mtime = EPOCH
                            tar.addfile(entry, io.BytesIO(data))
        else:
            archive.write_bytes(b'PK\x03\x04not-a-gzip-tar-release')
        checksum = archive.with_suffix(archive.suffix + '.sha256')
        checksum.write_text(
            f'{hashlib.sha256(archive.read_bytes()).hexdigest()}  {archive.name}\n'
        )
        return archive, checksum

    def build(self, archive, checksum, output='deb-output', *, revision=REVISION, extra=()):
        destination = self.directory / output
        result = subprocess.run(
            [sys.executable, str(SCRIPT), '--archive', str(archive),
             '--checksum', str(checksum), '--output-dir', str(destination),
             '--revision', revision, '--iteration', '1', *extra],
            capture_output=True, text=True, timeout=30,
        )
        return result, destination

    def successful(self, output='deb-output'):
        archive, checksum = self.fixture_archive(output + '-archive')
        result, destination = self.build(archive, checksum, output)
        self.assertEqual(result.returncode, 0, result.stderr)
        packages = list(destination.glob('flussonix_0.11.0-1_amd64.deb'))
        self.assertEqual(len(packages), 1)
        return packages[0], archive

    def test_rejects_checksum_and_source_revision_mismatch(self):
        archive, checksum = self.fixture_archive('checksum')
        checksum.write_text('0' * 64 + f'  {archive.name}\n')
        result, destination = self.build(archive, checksum, 'bad-checksum')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(list(destination.glob('*')) if destination.exists() else False)

        archive, checksum = self.fixture_archive('revision')
        result, destination = self.build(
            archive, checksum, 'bad-revision', revision='1' * 40
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(list(destination.glob('*')) if destination.exists() else False)

    def test_rejects_manifest_unlisted_member_and_wrong_architecture(self):
        archive, checksum = self.fixture_archive(
            'unlisted', unlisted_files={'private.env': (b'SECRET=bad\n', 0o600)}
        )
        result, destination = self.build(archive, checksum, 'bad-unlisted')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(list(destination.glob('*')) if destination.exists() else False)

        archive, checksum = self.fixture_archive('wrong-arch', architecture='aarch64')
        result, destination = self.build(archive, checksum, 'bad-arch')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(list(destination.glob('*')) if destination.exists() else False)

    def test_rejects_non_genuine_release_archive(self):
        archive, checksum = self.fixture_archive('zip-content', genuine=False)
        result, destination = self.build(archive, checksum, 'bad-format')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(list(destination.glob('*')) if destination.exists() else False)

    def test_accepts_exact_manifest_qualified_legacy_baseline(self):
        module = self.load_builder()
        legacy_revision = '1' * 40
        archive, _checksum = self.fixture_archive(
            'legacy-manifest', revision=legacy_revision, version='0.10.0',
            extra_files={'README.md': (b'# Legacy FlussoniX\n', 0o644)},
        )
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        baseline = {
            'version': '0.10.0',
            'iteration': 1,
            'architecture': 'x86_64',
            'source_revision': legacy_revision,
            'sha256': digest,
            'supports_init': False,
        }
        with mock.patch.object(module, 'baselines', return_value=[baseline]):
            release = module.load_archive(
                archive, digest, legacy_revision, current_version='0.11.0'
            )
        self.assertEqual(release['version'], '0.10.0')
        self.assertTrue(release['legacy'])

    def load_builder(self):
        self.assertTrue(SCRIPT.is_file(), 'Debian package builder is missing')
        spec = importlib.util.spec_from_file_location('flussonix_package_deb', SCRIPT)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_ffmpeg_contract_requires_ffmpeg_and_ffprobe(self):
        module = self.load_builder()
        with self.assertRaisesRegex(ValueError, 'ffprobe'):
            module.verify_ffmpeg_contract(
                lambda _package: {'/usr/bin/ffmpeg', '/usr/share/doc/ffmpeg/copyright'}
            )

    def test_undeclared_elf_dependency_is_rejected(self):
        module = self.load_builder()
        with self.assertRaisesRegex(ValueError, 'undeclared'):
            module.ensure_declared_dependencies(
                {'libc6', 'libssl3'}, {'libc6', 'ffmpeg'}
            )

    def test_built_package_has_fixed_payload_dependencies_and_inspection(self):
        if not shutil.which('dpkg-deb'):
            self.skipTest('requires dpkg-deb')
        package, archive = self.successful()
        info = subprocess.run(
            ['dpkg-deb', '--info', str(package)], capture_output=True,
            text=True, timeout=10,
        )
        self.assertEqual(info.returncode, 0, info.stderr)
        contents = subprocess.run(
            ['dpkg-deb', '--contents', str(package)], capture_output=True,
            text=True, timeout=10,
        )
        self.assertEqual(contents.returncode, 0, contents.stderr)
        self.assertNotIn('/opt/flussonic', contents.stdout)
        control = subprocess.run(
            ['dpkg-deb', '--field', str(package)], capture_output=True,
            text=True, check=True, timeout=10,
        ).stdout
        fields = {}
        current = None
        for line in control.splitlines():
            if line.startswith(' ') and current:
                fields[current] += ' ' + line.strip()
            elif ': ' in line:
                current, value = line.split(': ', 1)
                fields[current] = value
        self.assertEqual(fields['Package'], 'flussonix')
        self.assertEqual(fields['Version'], '0.11.0-1')
        self.assertEqual(fields['Architecture'], 'amd64')
        depends = {item.strip() for item in fields['Depends'].split(',')}
        for dependency in [
            'ffmpeg', 'libsrt1.5-gnutls', 'libsrtp2-1', 'ca-certificates',
            'adduser', 'init-system-helpers', 'libc6 (>= 2.39)',
        ]:
            self.assertIn(dependency, depends)
        self.assertEqual(
            {item.strip() for item in fields['Recommends'].split(',')},
            {'tesseract-ocr', 'tesseract-ocr-eng', 'vainfo',
             'intel-gpu-tools', 'pciutils'},
        )

        extracted = self.directory / 'extracted'
        subprocess.run(['dpkg-deb', '--extract', str(package), str(extracted)],
                       check=True, timeout=10)
        files = {
            path.relative_to(extracted).as_posix()
            for path in extracted.rglob('*') if path.is_file()
        }
        self.assertEqual(files, {
            'usr/bin/flussonix',
            'usr/lib/systemd/system/flussonix.service',
            'usr/lib/tmpfiles.d/flussonix.conf',
            'usr/share/flussonix/web/index.html',
            'usr/share/doc/flussonix/installation.md',
            'usr/share/doc/flussonix/LICENSE',
            'usr/share/doc/flussonix/NOTICE',
        })
        all_paths = {path.relative_to(extracted).as_posix() for path in extracted.rglob('*')}
        self.assertIn('etc/flussonix', all_paths)
        self.assertIn('etc/systemd/system/flussonix.service.d', all_paths)
        self.assertIn('var/lib/flussonix', all_paths)
        self.assertFalse(any('credentials.env' in path or '10-setup.conf' in path
                             for path in all_paths))
        self.assertNotIn(b'/opt/flussonic', package.read_bytes())
        self.assertEqual((extracted / 'etc/flussonix').stat().st_mode & 0o777, 0o700)
        self.assertEqual((extracted / 'var/lib/flussonix').stat().st_mode & 0o777, 0o700)
        self.assertEqual((extracted / 'usr/share/doc/flussonix').stat().st_mode & 0o777, 0o755)
        unit = extracted / 'usr/lib/systemd/system/flussonix.service'
        unit_text = unit.read_text()
        for contract in [
            'ConditionPathExists=/etc/flussonix/credentials.env',
            'User=flussonix', 'Group=flussonix', 'UMask=0077',
            'ExecStart=/usr/bin/flussonix', 'ReadWritePaths=/var/lib/flussonix',
        ]:
            self.assertIn(contract, unit_text)
        self.assertNotIn('FLUSSONIX_ADMIN_PASSWORD=', unit_text)
        self.assertNotIn('FLUSSONIX_PEER_KEY=', unit_text)
        verify_root = self.directory / 'systemd-verify-root'
        shutil.copytree(extracted, verify_root)
        system_units = verify_root / 'usr/lib/systemd/system'
        for target in ('sysinit.target', 'basic.target', 'network-online.target',
                       'multi-user.target'):
            (system_units / target).write_text(f'[Unit]\nDescription={target}\n')
        verify = subprocess.run(
            ['systemd-analyze', 'verify', '--man=no', f'--root={verify_root}',
             '/usr/lib/systemd/system/flussonix.service'],
            capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(verify.returncode, 0, verify.stderr)

        control_root = self.directory / 'control'
        subprocess.run(
            ['dpkg-deb', '--control', str(package), str(control_root)],
            check=True, timeout=10,
        )
        for script in ('preinst', 'postinst', 'prerm', 'postrm'):
            path = control_root / script
            self.assertEqual(path.stat().st_mode & 0o777, 0o755)
            self.assertNotIn('flussonix --init', path.read_text())

        inspection = package.with_suffix(package.suffix + '.inspection.json')
        checksum = package.with_suffix(package.suffix + '.sha256')
        self.assertTrue(inspection.is_file())
        self.assertEqual(
            checksum.read_text(),
            f'{hashlib.sha256(package.read_bytes()).hexdigest()}  {package.name}\n',
        )
        report = json.loads(inspection.read_text())
        self.assertEqual(report['format'], 1)
        self.assertEqual(report['archive']['sha256'], hashlib.sha256(archive.read_bytes()).hexdigest())
        self.assertEqual(report['archive']['source_revision'], REVISION)
        self.assertEqual(report['package']['version'], '0.11.0-1')
        self.assertEqual(set(report['files']), {'/' + path for path in files})
        for path, row in report['files'].items():
            data = (extracted / path.removeprefix('/')).read_bytes()
            self.assertEqual(row['sha256'], hashlib.sha256(data).hexdigest())

    def test_same_archive_builds_identical_deb(self):
        archive, checksum = self.fixture_archive('deterministic')
        first, first_dir = self.build(archive, checksum, 'first')
        second, second_dir = self.build(archive, checksum, 'second')
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(
            next(first_dir.glob('*.deb')).read_bytes(),
            next(second_dir.glob('*.deb')).read_bytes(),
        )

    def maintainer_environment(self, name, *, active=False, enabled=False):
        root = self.directory / name
        root.mkdir()
        commands = self.directory / f'{name}-commands'
        commands.mkdir()
        log = self.directory / f'{name}.log'
        systemctl = commands / 'systemctl'
        systemctl.write_text(
            '#!/bin/sh\n'
            'printf "systemctl %s\\n" "$*" >> "$FLUSSONIX_TEST_LOG"\n'
            'case "$1" in\n'
            '  is-active) [ "$FLUSSONIX_TEST_ACTIVE" = 1 ];;\n'
            '  is-enabled) [ "$FLUSSONIX_TEST_ENABLED" = 1 ];;\n'
            '  *) exit 0;;\n'
            'esac\n'
        )
        getent = commands / 'getent'
        getent.write_text(
            '#!/bin/sh\n[ -f "$FLUSSONIX_TEST_ACCOUNT" ]\n'
        )
        adduser = commands / 'adduser'
        adduser.write_text(
            '#!/bin/sh\nprintf "adduser %s\\n" "$*" >> "$FLUSSONIX_TEST_LOG"\n'
            'touch "$FLUSSONIX_TEST_ACCOUNT"\n'
        )
        chown = commands / 'chown'
        chown.write_text(
            '#!/bin/sh\nprintf "chown %s\\n" "$*" >> "$FLUSSONIX_TEST_LOG"\n'
        )
        for command in (systemctl, getent, adduser, chown):
            command.chmod(0o755)
        environment = {
            **os.environ,
            'PATH': str(commands) + ':/usr/bin:/bin',
            'DPKG_ROOT': str(root),
            'FLUSSONIX_TEST_LOG': str(log),
            'FLUSSONIX_TEST_ACTIVE': '1' if active else '0',
            'FLUSSONIX_TEST_ENABLED': '1' if enabled else '0',
            'FLUSSONIX_TEST_ACCOUNT': str(self.directory / f'{name}.account'),
        }
        return root, log, environment

    def run_maintainer(self, script, action, environment, *arguments):
        return subprocess.run(
            [str(ROOT / 'packaging/debian' / script), action, *arguments],
            env=environment, capture_output=True, text=True, timeout=10,
        )

    def test_fresh_postinst_creates_private_layout_but_stays_disabled_and_stopped(self):
        root, log, environment = self.maintainer_environment('fresh')
        result = self.run_maintainer('postinst', 'configure', environment)
        self.assertEqual(result.returncode, 0, result.stderr)
        repeated = self.run_maintainer('postinst', 'configure', environment)
        self.assertEqual(repeated.returncode, 0, repeated.stderr)
        self.assertEqual((root / 'etc/flussonix').stat().st_mode & 0o777, 0o700)
        self.assertEqual((root / 'etc/systemd/system/flussonix.service.d').stat().st_mode & 0o777,
                         0o755)
        self.assertEqual((root / 'var/lib/flussonix').stat().st_mode & 0o777, 0o700)
        commands = log.read_text()
        self.assertIn('adduser --system', commands)
        self.assertEqual(commands.count('adduser --system'), 1)
        self.assertIn('systemctl daemon-reload', commands)
        self.assertNotRegex(commands, r'systemctl (enable|start|restart|try-restart)')

    def test_upgrade_preserves_active_and_enabled_axes_independently(self):
        for active in (False, True):
            for enabled in (False, True):
                with self.subTest(active=active, enabled=enabled):
                    name = f'upgrade-{int(active)}-{int(enabled)}'
                    root, log, environment = self.maintainer_environment(
                        name, active=active, enabled=enabled
                    )
                    before = self.run_maintainer(
                        'preinst', 'upgrade', environment, '0.10.0-1'
                    )
                    self.assertEqual(before.returncode, 0, before.stderr)
                    after = self.run_maintainer(
                        'postinst', 'configure', environment, '0.10.0-1'
                    )
                    self.assertEqual(after.returncode, 0, after.stderr)
                    commands = log.read_text()
                    self.assertNotRegex(commands, r'systemctl (enable|disable)')
                    if active:
                        self.assertIn('systemctl try-restart flussonix.service', commands)
                    else:
                        self.assertNotIn('try-restart', commands)
                    self.assertFalse((root / 'run/flussonix/package-was-active').exists())

    def test_remove_retains_configuration_and_mutable_state(self):
        root, log, environment = self.maintainer_environment('remove')
        configuration = root / 'etc/flussonix/credentials.env'
        state = root / 'var/lib/flussonix/config.json'
        configuration.parent.mkdir(parents=True)
        state.parent.mkdir(parents=True)
        configuration.write_text('FLUSSONIX_ADMIN_PASSWORD=private\n')
        state.write_text('{}\n')

        before = self.run_maintainer('prerm', 'remove', environment)
        after = self.run_maintainer('postrm', 'remove', environment)
        self.assertEqual(before.returncode, 0, before.stderr)
        self.assertEqual(after.returncode, 0, after.stderr)
        self.assertTrue(configuration.exists())
        self.assertTrue(state.exists())
        self.assertIn('systemctl stop flussonix.service', log.read_text())

    def test_purge_removes_only_safe_generated_files_and_is_idempotent(self):
        root, _log, environment = self.maintainer_environment('purge')
        config = root / 'etc/flussonix'
        dropins = root / 'etc/systemd/system/flussonix.service.d'
        state = root / 'var/lib/flussonix'
        config.mkdir(parents=True, mode=0o700)
        dropins.mkdir(parents=True)
        state.mkdir(parents=True)
        state_file = state / 'config.json'
        state_file.write_text('{}\n')
        generated = {
            config / 'credentials.env': 0o600,
            config / '.credentials.env.staged': 0o600,
            config / '.credentials.env.backup': 0o600,
            config / '.setup-transaction.json': 0o600,
            config / '.setup-transaction.json.tmp': 0o600,
            dropins / '10-setup.conf': 0o644,
            dropins / '.10-setup.conf.staged': 0o644,
            dropins / '.10-setup.conf.backup': 0o644,
        }
        for path, mode in generated.items():
            path.write_text('owned generated file\n')
            path.chmod(mode)

        for _ in range(2):
            result = self.run_maintainer('postrm', 'purge', environment)
            self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(state_file.exists())
        self.assertFalse(any(path.exists() for path in generated))
        self.assertFalse(config.exists())
        self.assertFalse(dropins.exists())

    def test_purge_leaves_symlinks_wrong_modes_and_operator_files_untouched(self):
        root, _log, environment = self.maintainer_environment('unsafe-purge')
        config = root / 'etc/flussonix'
        dropins = root / 'etc/systemd/system/flussonix.service.d'
        config.mkdir(parents=True, mode=0o700)
        dropins.mkdir(parents=True)
        outside = root / 'outside-secret'
        outside.write_text('must survive\n')
        credentials = config / 'credentials.env'
        credentials.symlink_to(outside)
        wrong_mode = dropins / '10-setup.conf'
        wrong_mode.write_text('operator-owned override\n')
        wrong_mode.chmod(0o600)
        operator = config / 'server-chain.pem'
        operator.write_text('operator certificate\n')

        result = self.run_maintainer('postrm', 'purge', environment)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(credentials.is_symlink())
        self.assertEqual(outside.read_text(), 'must survive\n')
        self.assertTrue(wrong_mode.exists())
        self.assertTrue(operator.exists())

    def test_postinst_refuses_linked_package_directories_without_touching_targets(self):
        root, _log, environment = self.maintainer_environment('linked-layout')
        outside = root / 'operator-directory'
        outside.mkdir(mode=0o755)
        (root / 'etc').mkdir()
        (root / 'etc/flussonix').symlink_to(outside)

        result = self.run_maintainer('postinst', 'configure', environment)
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue((root / 'etc/flussonix').is_symlink())
        self.assertEqual(outside.stat().st_mode & 0o777, 0o755)


if __name__ == '__main__':
    unittest.main()
