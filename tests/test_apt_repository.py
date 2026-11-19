"""Signed APT repository construction and tamper-boundary tests."""
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'scripts/build-apt-repository.py'


class AptRepositoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        for executable in ('apt-ftparchive', 'dpkg-deb', 'gpg'):
            if not shutil.which(executable):
                raise unittest.SkipTest(f'requires {executable}')
        cls.shared = tempfile.TemporaryDirectory(prefix='flussonix-apt-key-')
        cls.gnupg = Path(cls.shared.name) / 'gnupg'
        cls.gnupg.mkdir(mode=0o700)
        cls.gpg_env = {**os.environ, 'GNUPGHOME': str(cls.gnupg), 'LC_ALL': 'C'}
        subprocess.run(
            ['gpg', '--batch', '--passphrase', '', '--quick-gen-key',
             'FlussoniX Repository Test <repository-test@flussonix.invalid>',
             'ed25519', 'cert', '1d'], env=cls.gpg_env, check=True,
            capture_output=True, text=True, timeout=30,
        )
        listing = subprocess.check_output(
            ['gpg', '--batch', '--with-colons', '--fingerprint', '--list-secret-keys'],
            env=cls.gpg_env, text=True, timeout=10,
        )
        cls.primary_fingerprint = cls._fingerprints(listing)[0]
        subprocess.run(
            ['gpg', '--batch', '--passphrase', '', '--quick-add-key',
             cls.primary_fingerprint, 'ed25519', 'sign', '1d'],
            env=cls.gpg_env, check=True, capture_output=True, text=True, timeout=30,
        )
        listing = subprocess.check_output(
            ['gpg', '--batch', '--with-colons', '--fingerprint', '--list-secret-keys'],
            env=cls.gpg_env, text=True, timeout=10,
        )
        cls.signing_fingerprint = cls._fingerprints(listing)[-1]
        cls.public_key = Path(cls.shared.name) / 'test-archive-key.gpg'
        with cls.public_key.open('wb') as output:
            subprocess.run(
                ['gpg', '--batch', '--export', cls.primary_fingerprint],
                env=cls.gpg_env, stdout=output, check=True, timeout=10,
            )

    @classmethod
    def tearDownClass(cls):
        cls.shared.cleanup()

    @staticmethod
    def _fingerprints(listing):
        return [line.split(':')[9] for line in listing.splitlines()
                if line.startswith('fpr:')]

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='flussonix-apt-test-')
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.current = self.package('0.11.0-1')
        self.previous = self.package('0.10.0-1')

    def package(self, version):
        build = self.directory / ('build-' + version)
        control = build / 'DEBIAN'
        executable = build / 'usr/bin/flussonix'
        control.mkdir(parents=True)
        executable.parent.mkdir(parents=True)
        control.joinpath('control').write_text(
            'Package: flussonix\n'
            f'Version: {version}\n'
            'Section: video\nPriority: optional\nArchitecture: amd64\n'
            'Maintainer: FlussoniX Test <test@flussonix.invalid>\n'
            'Description: repository test fixture\n'
        )
        executable.write_bytes(b'fixture-' + version.encode() + b'\n')
        executable.chmod(0o755)
        package = self.directory / f'flussonix_{version}_amd64.deb'
        subprocess.run(
            ['dpkg-deb', '--root-owner-group', '--build', str(build), str(package)],
            check=True, capture_output=True, text=True, timeout=20,
        )
        digest = hashlib.sha256(package.read_bytes()).hexdigest()
        package.with_suffix(package.suffix + '.sha256').write_text(
            f'{digest}  {package.name}\n'
        )
        report = {
            'format': 1,
            'archive': {
                'sha256': 'a' * 64,
                'source_revision': 'b' * 40,
                'version': version.rsplit('-', 1)[0],
                'architecture': 'x86_64',
                'legacy': version.startswith('0.10.'),
            },
            'package': {
                'name': 'flussonix', 'version': version, 'architecture': 'amd64',
                'depends': [], 'recommends': [],
            },
            'files': {
                '/usr/bin/flussonix': {
                    'sha256': hashlib.sha256(executable.read_bytes()).hexdigest(),
                    'bytes': executable.stat().st_size,
                    'mode': '0o755',
                },
            },
        }
        package.with_suffix(package.suffix + '.inspection.json').write_text(
            json.dumps(report, indent=2, sort_keys=True) + '\n'
        )
        return package

    def build(self, packages=None, output='repository', public_key=None, fingerprint=None):
        destination = self.directory / output
        command = [sys.executable, str(SCRIPT)]
        for package in packages or [self.current, self.previous]:
            command.extend(['--package', str(package)])
        command.extend([
            '--public-key', str(public_key or self.public_key),
            '--signing-fingerprint', fingerprint or self.signing_fingerprint,
            '--output-dir', str(destination),
        ])
        result = subprocess.run(
            command, env=self.gpg_env, capture_output=True, text=True, timeout=60,
        )
        return result, destination

    def test_builds_deterministic_signed_repository_with_current_and_prior(self):
        first, repository = self.build(output='first')
        self.assertEqual(first.returncode, 0, first.stderr)
        second, other = self.build(output='second')
        self.assertEqual(second.returncode, 0, second.stderr)

        packages_path = repository / 'dists/noble/main/binary-amd64/Packages'
        packages = packages_path.read_bytes()
        self.assertEqual(
            packages,
            (other / 'dists/noble/main/binary-amd64/Packages').read_bytes(),
        )
        self.assertEqual(
            gzip.decompress(
                (repository / 'dists/noble/main/binary-amd64/Packages.gz').read_bytes()
            ),
            packages,
        )
        text = packages.decode()
        self.assertEqual(text.count('Package: flussonix\n'), 2)
        for package in (self.current, self.previous):
            self.assertIn(f'Version: {package.name.split("_")[1]}\n', text)
            self.assertIn(
                f'Filename: pool/main/f/flussonix/{package.name}\n', text
            )
            self.assertIn(
                f'SHA256: {hashlib.sha256(package.read_bytes()).hexdigest()}\n', text
            )

        expected = {
            'dists/noble/InRelease', 'dists/noble/Release',
            'dists/noble/Release.gpg',
            'dists/noble/main/binary-amd64/Packages',
            'dists/noble/main/binary-amd64/Packages.gz',
            f'pool/main/f/flussonix/{self.current.name}',
            f'pool/main/f/flussonix/{self.previous.name}',
            'flussonix-archive-keyring.gpg', 'install.sh', 'SHA256SUMS',
        }
        actual = {path.relative_to(repository).as_posix()
                  for path in repository.rglob('*') if path.is_file()}
        self.assertEqual(actual, expected)
        installer = (repository / 'install.sh').read_text()
        self.assertIn(f'EXPECTED_FINGERPRINT={self.primary_fingerprint}', installer)
        self.assertIn("ROOT_PREFIX=''", installer)
        self.assertIn("PATH=/usr/sbin:/usr/bin:/sbin:/bin", installer)
        self.assertNotRegex(installer, r'@[A-Z_]+@')
        release = (repository / 'dists/noble/Release').read_text()
        for line in (
            'Origin: FlussoniX', 'Label: FlussoniX', 'Suite: stable',
            'Codename: noble', 'Architectures: amd64', 'Components: main',
        ):
            self.assertIn(line + '\n', release)
        for relative in (
            'main/binary-amd64/Packages', 'main/binary-amd64/Packages.gz'
        ):
            payload = repository / 'dists/noble' / relative
            self.assertRegex(
                release,
                rf'{hashlib.sha256(payload.read_bytes()).hexdigest()}\s+'
                rf'{payload.stat().st_size}\s+{relative}',
            )
        self.verify_signatures(repository)

        sums = (repository / 'SHA256SUMS').read_text().splitlines()
        summed = {line.split('  ', 1)[1] for line in sums}
        self.assertEqual(summed, actual - {'SHA256SUMS'})
        for line in sums:
            digest, relative = line.split('  ', 1)
            self.assertEqual(
                hashlib.sha256((repository / relative).read_bytes()).hexdigest(), digest
            )
        self.assertFalse(any('private' in path.lower() or 'secret' in path.lower()
                             for path in actual))

    def test_checked_in_production_key_is_public_and_names_current_signing_subkey(self):
        key = ROOT / 'packaging/apt/flussonix-archive-keyring.gpg'
        expected_signing = (ROOT / 'packaging/apt/signing-key-fingerprint.txt').read_text().strip()
        listing = subprocess.run(
            ['gpg', '--batch', '--no-options', '--show-keys', '--with-colons',
             '--fingerprint', str(key)],
            check=True, capture_output=True, text=True, timeout=10,
        ).stdout
        records = [line.split(':') for line in listing.splitlines()]
        self.assertEqual([row[0] for row in records if row[0] in {'pub', 'sub'}],
                         ['pub', 'sub'])
        fingerprints = [row[9] for row in records if row[0] == 'fpr']
        self.assertEqual(len(fingerprints), 2)
        self.assertEqual(fingerprints[-1], expected_signing)
        self.assertRegex(expected_signing, r'^[0-9A-F]{40}$')
        packets = subprocess.run(
            ['gpg', '--batch', '--no-options', '--list-packets', str(key)],
            check=True, capture_output=True, text=True, timeout=10,
        ).stdout
        self.assertNotIn(':secret key packet:', packets)
        self.assertNotIn(':secret sub key packet:', packets)

    def verify_signatures(self, repository):
        with tempfile.TemporaryDirectory(prefix='flussonix-apt-verify-') as name:
            home = Path(name)
            home.chmod(0o700)
            environment = {**os.environ, 'GNUPGHOME': str(home), 'LC_ALL': 'C'}
            subprocess.run(
                ['gpg', '--batch', '--import',
                 str(repository / 'flussonix-archive-keyring.gpg')],
                env=environment, check=True, capture_output=True, text=True, timeout=10,
            )
            for command in (
                ['gpg', '--batch', '--verify',
                 str(repository / 'dists/noble/InRelease')],
                ['gpg', '--batch', '--verify',
                 str(repository / 'dists/noble/Release.gpg'),
                 str(repository / 'dists/noble/Release')],
            ):
                subprocess.run(
                    command, env=environment, check=True,
                    capture_output=True, text=True, timeout=10,
                )

    def test_rejects_substituted_key_and_missing_or_changed_sidecars(self):
        substituted_home = self.directory / 'substituted-home'
        substituted_home.mkdir(mode=0o700)
        environment = {**os.environ, 'GNUPGHOME': str(substituted_home), 'LC_ALL': 'C'}
        subprocess.run(
            ['gpg', '--batch', '--passphrase', '', '--quick-gen-key',
             'Substituted Key <substituted@invalid>', 'ed25519', 'sign', '1d'],
            env=environment, check=True, capture_output=True, text=True, timeout=30,
        )
        wrong_key = self.directory / 'wrong.gpg'
        with wrong_key.open('wb') as output:
            subprocess.run(['gpg', '--batch', '--export'], env=environment,
                           stdout=output, check=True, timeout=10)
        result, _ = self.build(public_key=wrong_key, output='wrong-key')
        self.assertNotEqual(result.returncode, 0)

        secret_key = self.directory / 'secret-key.gpg'
        with secret_key.open('wb') as output:
            subprocess.run(
                ['gpg', '--batch', '--export-secret-keys', self.primary_fingerprint],
                env=self.gpg_env, stdout=output, check=True, timeout=10,
            )
        result, _ = self.build(public_key=secret_key, output='secret-key')
        self.assertNotEqual(result.returncode, 0)

        inspection = self.current.with_suffix(self.current.suffix + '.inspection.json')
        saved = inspection.read_bytes()
        inspection.unlink()
        result, _ = self.build(output='missing-inspection')
        self.assertNotEqual(result.returncode, 0)
        inspection.write_bytes(saved)

        self.current.write_bytes(self.current.read_bytes() + b'repacked')
        result, _ = self.build(output='repacked')
        self.assertNotEqual(result.returncode, 0)

    def test_rejects_wrong_inspection_version_and_architecture(self):
        inspection = self.current.with_suffix(self.current.suffix + '.inspection.json')
        report = json.loads(inspection.read_text())
        for field, value in (('version', '9.9.9-1'), ('architecture', 'arm64')):
            changed = json.loads(json.dumps(report))
            changed['package'][field] = value
            inspection.write_text(json.dumps(changed) + '\n')
            result, _ = self.build(output='wrong-' + field)
            self.assertNotEqual(result.returncode, 0)
        inspection.write_text(json.dumps(report) + '\n')

    def test_changed_release_metadata_and_package_fail_signature_or_checksum(self):
        result, repository = self.build(output='tamper')
        self.assertEqual(result.returncode, 0, result.stderr)
        release = repository / 'dists/noble/Release'
        release.write_text(release.read_text().replace('Suite: stable', 'Suite: unstable'))
        with self.assertRaises(subprocess.CalledProcessError):
            self.verify_signatures(repository)


if __name__ == '__main__':
    unittest.main()
