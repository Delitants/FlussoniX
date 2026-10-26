"""Archive-boundary tests; the tiny ELF fixture is not a runnable daemon."""
import hashlib
import inspect
import json
import os
from pathlib import Path
import re
import runpy
import struct
import subprocess
import sys
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'scripts/package-preview.py'
REVISION = '1234567890abcdef1234567890abcdef12345678'
EPOCH = 1_600_000_000


class PackagePreviewTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='flussonix-package-test-')
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.binary = self.directory / 'flussonix'
        ident = b'\x7fELF' + bytes([2, 1, 1]) + bytes(9)
        self.binary.write_bytes(struct.pack('<16sHHIQQQIHHHHHH', ident, 3, 62, 1,
                                            0, 0, 0, 0, 64, 56, 0, 64, 0, 0))
        self.web = self.directory / 'web'
        (self.web / 'assets').mkdir(parents=True)
        (self.web / 'index.html').write_text(
            '<html><script type="module" src="/admin/assets/app.js"></script>'
            '<link rel="stylesheet" href="/admin/assets/app.css"></html>')
        (self.web / 'assets/app.js').write_text('document.title = "FlussoniX";\n')
        (self.web / 'assets/app.css').write_text('body { color: white; }\n')
        # Sibling runtime state is deliberately excluded from an archive.
        (self.directory / 'config.json').write_text('{"password":"not-public"}')
        (self.directory / '.env').write_text('SECRET=not-public')

    def package(self, output='output', extra=()):
        destination = self.directory / output
        result = subprocess.run([sys.executable, str(SCRIPT), '--binary', str(self.binary),
                                 '--web-dir', str(self.web), '--output-dir', str(destination),
                                 '--revision', REVISION, '--epoch', str(EPOCH), *extra],
                                capture_output=True, text=True, timeout=10)
        return result, destination

    def successful(self, output='output'):
        result, destination = self.package(output)
        self.assertEqual(result.returncode, 0, result.stderr)
        archives = list(destination.glob('*.tar.gz'))
        self.assertEqual(len(archives), 1)
        return archives[0]

    def test_archive_contains_only_payload_and_verifiable_manifest(self):
        archive = self.successful()
        checksum = Path(str(archive) + '.sha256').read_text()
        self.assertEqual(checksum, hashlib.sha256(archive.read_bytes()).hexdigest() + '  ' + archive.name + '\n')
        self.assertEqual(int.from_bytes(archive.read_bytes()[4:8], 'little'), EPOCH)
        with tarfile.open(archive) as tar:
            files = {Path(m.name).relative_to(archive.name[:-7]).as_posix(): m
                     for m in tar.getmembers() if m.isfile()}
            expected = {'bin/flussonix', 'web/index.html', 'web/assets/app.js',
                        'web/assets/app.css', 'README.md', 'installation.md',
                        'iptv-transport.md', 'iptv-ha.md', 'manifest.json', 'LICENSE', 'NOTICE',
                        'vendor/quinn-proto/LICENSE-APACHE',
                        'vendor/quinn-proto/LICENSE-MIT',
                        'bin/flussonix-service', 'service-installation.md',
                        'THIRD_PARTY_NOTICES.txt', 'third_party/manifest.json'}
            bundle = json.loads((ROOT / 'third_party/manifest.json').read_text())
            expected.update('third_party/' + entry['path'] for package in bundle['packages']
                            for entry in package['files'])
            self.assertEqual(set(files), expected)
            self.assertEqual(files['bin/flussonix'].mode, 0o755)
            self.assertEqual(files['bin/flussonix-service'].mode, 0o755)
            self.assertEqual(files['web/assets/app.js'].mode, 0o644)
            manifest = json.loads(tar.extractfile(files['manifest.json']).read())
            self.assertEqual(manifest['source_revision'], REVISION)
            self.assertEqual(manifest['source_date_epoch'], EPOCH)
            self.assertEqual(manifest['architecture'], 'x86_64')
            self.assertEqual(set(manifest['files']), set(files) - {'manifest.json'})
            for name, row in manifest['files'].items():
                data = tar.extractfile(files[name]).read()
                self.assertEqual(row['sha256'], hashlib.sha256(data).hexdigest())
                self.assertEqual(row['bytes'], len(data))
            for member in tar.getmembers():
                self.assertEqual((member.uid, member.gid, member.mtime), (0, 0, EPOCH))
                self.assertFalse(member.issym() or member.islnk())

    def test_same_inputs_ignore_source_mtime_and_permissions(self):
        first = self.successful('first').read_bytes()
        for path in [self.binary, *self.web.rglob('*')]:
            os.utime(path, (EPOCH + 999, EPOCH + 999))
            if path.is_file():
                path.chmod(0o600)
        second = self.successful('second').read_bytes()
        self.assertEqual(first, second)

    def test_packaged_guides_have_no_missing_relative_markdown_links(self):
        archive = self.successful()
        with tarfile.open(archive) as tar:
            files = {Path(member.name).relative_to(archive.name[:-7]).as_posix(): member
                     for member in tar.getmembers() if member.isfile()}
            for name, member in files.items():
                if not name.endswith('.md'):
                    continue
                guide = tar.extractfile(member).read().decode('utf-8')
                for target in re.findall(r'\]\(([^)#]+\.md)(?:#[^)]*)?\)', guide):
                    self.assertIn((Path(name).parent / target).as_posix(), files,
                                  f'{name} links to missing {target}')

    def test_archive_includes_current_ha_guide_and_offline_install_link(self):
        archive = self.successful()
        with tarfile.open(archive) as tar:
            prefix = archive.name[:-7] + '/'
            self.assertTrue(prefix + 'iptv-ha.md' in tar.getnames(),
                            'HA guide missing from archive')
            guide = tar.extractfile(prefix + 'iptv-ha.md').read()
            self.assertEqual(guide, (ROOT / 'docs/iptv-ha.md').read_bytes())
            self.assertIn(b'./bin/flussonix --iptv-lease-authority', guide)
            self.assertIn(b'source checkout', guide)
            for name in ('README.md', 'installation.md'):
                installation = tar.extractfile(prefix + name).read().decode('utf-8')
                self.assertIn('](iptv-ha.md)', installation)

    def test_archive_carries_own_and_vendored_license_texts(self):
        archive = self.successful()
        required = ['LICENSE', 'NOTICE', 'vendor/quinn-proto/LICENSE-APACHE',
                    'vendor/quinn-proto/LICENSE-MIT']
        with tarfile.open(archive) as tar:
            for name in required:
                member = archive.name[:-7] + '/' + name
                self.assertIn(member, tar.getnames())
                self.assertEqual(tar.extractfile(member).read(), (ROOT / name).read_bytes())

    def test_archive_carries_locked_third_party_texts(self):
        archive = self.successful()
        bundle = json.loads((ROOT / 'third_party/manifest.json').read_text())
        required = {'THIRD_PARTY_NOTICES.txt', 'third_party/manifest.json'}
        required.update('third_party/' + entry['path']
                        for package in bundle['packages'] for entry in package['files'])
        with tarfile.open(archive) as tar:
            members = {Path(m.name).relative_to(archive.name[:-7]).as_posix(): m
                       for m in tar.getmembers() if m.isfile()}
            self.assertTrue(required <= members.keys())
            for package in bundle['packages']:
                for entry in package['files']:
                    data = tar.extractfile(members['third_party/' + entry['path']]).read()
                    self.assertEqual(hashlib.sha256(data).hexdigest(), entry['sha256'])

    def test_existing_output_is_never_overwritten(self):
        archive = self.successful()
        before = {p.name: p.read_bytes() for p in archive.parent.iterdir()}
        result, destination = self.package()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual({p.name: p.read_bytes() for p in destination.iterdir()}, before)

    def test_private_or_linked_web_files_reject_export(self):
        for variant in ['private-file', 'linked-file', 'linked-directory']:
            with self.subTest(variant=variant):
                path = self.web / ('assets/linked' if variant == 'linked-directory' else 'assets/extra.js')
                if variant == 'private-file':
                    path = self.web / '.env'
                    path.write_text('SECRET=not-public')
                else:
                    path.symlink_to(self.directory if variant == 'linked-directory' else self.directory / '.env')
                try:
                    result, destination = self.package(variant)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertFalse(list(destination.glob('*.tar.gz*')))
                finally:
                    path.unlink()

    def test_missing_or_wrong_base_ui_entrypoint_rejects_export(self):
        (self.web / 'assets/app.js').unlink()
        result, destination = self.package('missing')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(list(destination.glob('*.tar.gz*')))
        (self.web / 'assets/app.js').write_text('ready')
        (self.web / 'index.html').write_text('<script src="/assets/app.js"></script>')
        result, destination = self.package('wrong-base')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(list(destination.glob('*.tar.gz*')))

    def test_invalid_binary_or_revision_rejects_export(self):
        result, destination = self.package('revision', ['--revision', '../not-a-revision'])
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(list(destination.glob('*.tar.gz*')))
        self.binary.write_bytes(b'not a Linux executable')
        result, destination = self.package('binary')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(list(destination.glob('*.tar.gz*')))

    def test_other_os_abi_and_empty_ui_reference_reject_export(self):
        original = self.binary.read_bytes()
        with self.subTest(boundary='other-os'):
            data = bytearray(original)
            data[7] = 9  # ELFOSABI_FREEBSD is not a Linux archive.
            self.binary.write_bytes(data)
            result, destination = self.package('other-os')
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(list(destination.glob('*.tar.gz*')))
        self.binary.write_bytes(original)
        with self.subTest(boundary='empty-ui-reference'):
            (self.web / 'index.html').write_text('<script src></script>')
            result, destination = self.package('empty-reference')
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(list(destination.glob('*.tar.gz*')))

    def test_aarch64_header_is_labeled_without_claiming_runtime_compatibility(self):
        data = bytearray(self.binary.read_bytes())
        data[18:20] = struct.pack('<H', 183)
        self.binary.write_bytes(data)
        archive = self.successful()
        self.assertIn('-linux-aarch64-', archive.name)
        with tarfile.open(archive) as tar:
            member = archive.name[:-7] + '/manifest.json'
            self.assertEqual(json.loads(tar.extractfile(member).read())['architecture'], 'aarch64')

    def test_html_base_cannot_redirect_local_entrypoints(self):
        for name, base in [('external-base', 'https://outside.example/'), ('wrong-base', '/other/')]:
            with self.subTest(base=base):
                (self.web / 'index.html').write_text(
                    f'<base href="{base}"><script src="assets/app.js"></script>')
                result, destination = self.package(name)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(list(destination.glob('*.tar.gz*')))

    def test_browser_attribute_rules_cannot_hide_external_entrypoints(self):
        cases = [
            '<script src="https://outside.example/a.js" src="/admin/assets/app.js"></script>',
            '<link rel="stylesheet" href="https://outside.example/a.css" href="/admin/assets/app.css">',
            '<link rel="STYLESHEET" href="https://outside.example/a.css">',
            '<link rel="MODULEPRELOAD" href="https://outside.example/a.js">',
            '<link rel="ICON" href="https://outside.example/icon.svg">',
        ]
        for i, html in enumerate(cases):
            with self.subTest(html=html):
                (self.web / 'index.html').write_text(html + '<script src="/admin/assets/app.js"></script>')
                result, destination = self.package(f'attributes-{i}')
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(list(destination.glob('*.tar.gz*')))


class NoticeBundleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='flussonix-notices-test-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'third_party/licenses/rust/example-1.0.0').mkdir(parents=True)
        (self.root / 'Cargo.lock').write_text('locked rust')
        (self.root / 'Cargo.toml').write_text('manifest rust')
        (self.root / 'web').mkdir()
        (self.root / 'web/package-lock.json').write_text('locked web')
        (self.root / 'web/package.json').write_text('manifest web')
        (self.root / 'vendor/quinn-proto').mkdir(parents=True)
        (self.root / 'vendor/quinn-proto/Cargo.toml').write_text('patched transport')
        notice = b'Example license text\n'
        (self.root / 'third_party/licenses/rust/example-1.0.0/LICENSE').write_bytes(notice)
        index = b'Example third-party notices\n'
        (self.root / 'THIRD_PARTY_NOTICES.txt').write_bytes(index)
        self.manifest = {
            'format': 1,
            'cargo_lock_sha256': hashlib.sha256(b'locked rust').hexdigest(),
            'web_lock_sha256': hashlib.sha256(b'locked web').hexdigest(),
            'cargo_manifest_sha256': hashlib.sha256(b'manifest rust').hexdigest(),
            'web_manifest_sha256': hashlib.sha256(b'manifest web').hexdigest(),
            'vendor_quinn_manifest_sha256': hashlib.sha256(b'patched transport').hexdigest(),
            'notices_sha256': hashlib.sha256(index).hexdigest(),
            'targets': ['x86_64-unknown-linux-gnu'],
            'packages': [{'ecosystem': 'rust', 'name': 'example', 'version': '1.0.0',
                          'declared_license': 'MIT', 'files': [{
                              'path': 'licenses/rust/example-1.0.0/LICENSE',
                              'sha256': hashlib.sha256(notice).hexdigest(),
                              'bytes': len(notice)}]}],
        }
        self.save_manifest()

    def save_manifest(self):
        (self.root / 'third_party/manifest.json').write_text(json.dumps(self.manifest))

    def bundle(self, arch=None):
        functions = runpy.run_path(str(SCRIPT))
        self.assertIn('read_notice_bundle', functions)
        if arch is not None:
            self.assertIn('arch', inspect.signature(functions['read_notice_bundle']).parameters)
        return (functions['read_notice_bundle'](self.root) if arch is None else
                functions['read_notice_bundle'](self.root, arch))

    def test_locked_notice_bundle_loads_exact_texts(self):
        payload = self.bundle()
        self.assertEqual(payload['third_party/licenses/rust/example-1.0.0/LICENSE'][0],
                         b'Example license text\n')
        self.assertEqual(payload['THIRD_PARTY_NOTICES.txt'][0],
                         b'Example third-party notices\n')

    def test_stale_lock_and_changed_notice_are_rejected(self):
        (self.root / 'Cargo.lock').write_text('new dependency')
        with self.assertRaisesRegex(ValueError, 'Cargo.lock'):
            self.bundle()
        (self.root / 'Cargo.lock').write_text('locked rust')
        (self.root / 'third_party/licenses/rust/example-1.0.0/LICENSE').write_text('changed')
        with self.assertRaisesRegex(ValueError, 'license'):
            self.bundle()

    def test_changed_dependency_manifests_are_rejected(self):
        for name in ('Cargo.toml', 'web/package.json', 'vendor/quinn-proto/Cargo.toml'):
            with self.subTest(name=name):
                path = self.root / name
                original = path.read_bytes()
                path.write_bytes(original + b' changed')
                with self.assertRaisesRegex(ValueError, name.replace('.', r'\.')):
                    self.bundle()
                path.write_bytes(original)

    def test_unlisted_or_linked_notice_is_rejected(self):
        extra = self.root / 'third_party/licenses/rust/example-1.0.0/extra.txt'
        extra.write_text('unlisted')
        with self.assertRaisesRegex(ValueError, 'unlisted'):
            self.bundle()
        extra.unlink()
        license_file = self.root / 'third_party/licenses/rust/example-1.0.0/LICENSE'
        license_file.unlink()
        license_file.symlink_to(self.root / 'Cargo.lock')
        with self.assertRaises(ValueError):
            self.bundle()

    def test_architecture_must_be_in_notice_scope(self):
        with self.assertRaisesRegex(ValueError, 'architecture'):
            self.bundle('aarch64')


if __name__ == '__main__':
    unittest.main()
