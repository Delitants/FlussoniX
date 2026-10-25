#!/usr/bin/env python3
"""Package already-built Linux artifacts. Requires Python 3.11+, no third-party modules."""
import argparse
import gzip
import hashlib
from html.parser import HTMLParser
import io
import json
import os
from pathlib import Path
import re
import stat
import struct
import subprocess
import tarfile
import tempfile
import tomllib
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
PUBLIC_SUFFIXES = {'.js', '.css', '.woff', '.woff2', '.ttf', '.svg', '.png', '.jpg',
                   '.jpeg', '.webp', '.avif', '.ico', '.gif'}


def read_regular(path, limit):
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC), 'rb') as f:
        metadata = os.fstat(f.fileno())
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > limit:
            raise ValueError('package inputs must be bounded regular files')
        data = f.read(limit + 1)
        if len(data) > limit:
            raise ValueError('package input exceeds size limit')
        return data


def architecture(binary):
    if len(binary) < 64 or binary[:7] != b'\x7fELF\x02\x01\x01' or binary[7] not in [0, 3]:
        raise ValueError('binary must be a little-endian 64-bit Linux ELF executable')
    kind, machine = struct.unpack_from('<HH', binary, 16)
    if kind not in [2, 3] or machine not in [62, 183]:
        raise ValueError('supported Linux binary architectures are x86_64 and aarch64')
    return {62: 'x86_64', 183: 'aarch64'}[machine]


class Entrypoints(HTMLParser):
    def __init__(self):
        super().__init__()
        self.paths = []
        self.scripts = 0

    def handle_starttag(self, tag, attrs):
        if tag in ['script', 'link', 'base'] and len({name for name, _ in attrs}) != len(attrs):
            raise ValueError('built UI entrypoints cannot contain duplicate attributes')
        attrs = dict(attrs)
        if tag == 'base' and 'href' in attrs:
            raise ValueError('built UI cannot override the /admin/ entrypoint base')
        raw = None
        if tag == 'script' and 'src' in attrs:
            raw = attrs['src']
            if not raw:
                raise ValueError('UI script entrypoint must have a local asset path')
            self.scripts += 1
        if tag == 'link' and set((attrs.get('rel') or '').lower().split()) & {
                'stylesheet', 'modulepreload', 'icon', 'preload', 'prefetch'}:
            raw = attrs.get('href')
        if raw is None:
            return
        value = urlsplit(raw)
        if value.scheme or value.netloc:
            raise ValueError('UI entrypoints must be included local assets')
        path = unquote(value.path)
        if path.startswith('/'):
            if not path.startswith('/admin/'):
                raise ValueError('absolute UI entrypoints must use the /admin/ base')
            path = path.removeprefix('/admin/')
        if not path or '..' in Path(path).parts or Path(path).is_absolute():
            raise ValueError('invalid UI entrypoint path')
        self.paths.append(path.removeprefix('./'))


def web_payload(directory):
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError('web directory must be a real built UI directory')
    paths = []
    for path in directory.rglob('*'):
        if len(paths) >= 2048:
            raise ValueError('built UI exceeds 2048 entries')
        paths.append(path)
    payload = {}
    total = 0
    for path in sorted(paths):
        relative = path.relative_to(directory)
        if path.is_symlink() or any(not re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.-]*', p) for p in relative.parts):
            raise ValueError('built UI cannot contain links or hidden/private entries')
        if path.is_dir():
            if relative.parts[0] != 'assets':
                raise ValueError('unexpected built UI directory')
            continue
        if relative.as_posix() != 'index.html' and not (
                relative.parts[0] == 'assets' and path.suffix.lower() in PUBLIC_SUFFIXES):
            raise ValueError('unexpected built UI file; supply the public build output only')
        data = read_regular(path, 64 * 1024 * 1024)
        total += len(data)
        if total > 128 * 1024 * 1024:
            raise ValueError('built UI exceeds 128 MiB')
        payload['web/' + relative.as_posix()] = (data, 0o644)
    if 'web/index.html' not in payload:
        raise ValueError('built UI index.html is missing')
    entrypoints = Entrypoints()
    entrypoints.feed(payload['web/index.html'][0].decode('utf-8'))
    if not entrypoints.scripts or any('web/' + p not in payload for p in entrypoints.paths):
        raise ValueError('built UI references missing entrypoint assets')
    return payload


def read_notice_bundle(root, arch=None):
    """Load a checked-in third-party bundle bound to both dependency locks."""
    directory = root / 'third_party'
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError('third-party notice bundle is missing or linked')
    manifest_bytes = read_regular(directory / 'manifest.json', 1024 * 1024)
    manifest = json.loads(manifest_bytes)
    if manifest.get('format') != 1 or not isinstance(manifest.get('packages'), list):
        raise ValueError('invalid third-party notice manifest')
    if not 1 <= len(manifest['packages']) <= 512:
        raise ValueError('third-party package inventory is empty or too large')
    targets = manifest.get('targets')
    if (not isinstance(targets, list) or not targets or len(targets) > 8 or
            any(not isinstance(target, str) for target in targets)):
        raise ValueError('invalid third-party notice architecture scope')
    if arch is not None and f'{arch}-unknown-linux-gnu' not in targets:
        raise ValueError('third-party notice bundle does not cover binary architecture')
    for label, path in [('Cargo.lock', root / 'Cargo.lock'),
                        ('web/package-lock.json', root / 'web/package-lock.json'),
                        ('Cargo.toml', root / 'Cargo.toml'),
                        ('web/package.json', root / 'web/package.json'),
                        ('vendor/quinn-proto/Cargo.toml', root / 'vendor/quinn-proto/Cargo.toml')]:
        field = {'Cargo.lock': 'cargo_lock_sha256',
                 'web/package-lock.json': 'web_lock_sha256',
                 'Cargo.toml': 'cargo_manifest_sha256',
                 'web/package.json': 'web_manifest_sha256',
                 'vendor/quinn-proto/Cargo.toml': 'vendor_quinn_manifest_sha256'}[label]
        if manifest.get(field) != hashlib.sha256(read_regular(path, 4 * 1024 * 1024)).hexdigest():
            raise ValueError(f'third-party notice bundle is stale for {label}')
    notices = read_regular(root / 'THIRD_PARTY_NOTICES.txt', 1024 * 1024)
    if manifest.get('notices_sha256') != hashlib.sha256(notices).hexdigest():
        raise ValueError('third-party notice index hash differs from manifest')
    payload = {'THIRD_PARTY_NOTICES.txt': (notices, 0o644),
               'third_party/manifest.json': (manifest_bytes, 0o644)}
    names = set()
    total = 0
    for package in manifest['packages']:
        if not isinstance(package, dict) or package.get('ecosystem') not in ('rust', 'web'):
            raise ValueError('invalid third-party package entry')
        for field in ('name', 'version', 'declared_license'):
            value = package.get(field)
            if not isinstance(value, str) or not 1 <= len(value) <= 256:
                raise ValueError('invalid third-party package metadata')
        files = package.get('files')
        if not isinstance(files, list) or not 1 <= len(files) <= 32:
            raise ValueError('third-party package is missing license/notice files')
        for entry in files:
            if not isinstance(entry, dict) or not isinstance(entry.get('path'), str):
                raise ValueError('invalid third-party license path')
            relative = Path(entry['path'])
            if (relative.is_absolute() or relative.as_posix() != entry['path'] or
                    len(relative.parts) < 4 or
                    relative.parts[0] != 'licenses' or
                    relative.parts[1] != package['ecosystem'] or
                    any(part in ('', '.', '..') or not re.fullmatch(r'[A-Za-z0-9_. -]+', part)
                        for part in relative.parts) or
                    entry['path'] in names):
                raise ValueError('unsafe or duplicate third-party license path')
            names.add(entry['path'])
            source = directory / relative
            if source.is_symlink() or any(parent.is_symlink() for parent in source.parents if parent != directory):
                raise ValueError('linked third-party license file')
            data = read_regular(source, 1024 * 1024)
            if entry.get('bytes') != len(data) or entry.get('sha256') != hashlib.sha256(data).hexdigest():
                raise ValueError('third-party license file differs from manifest')
            total += len(data)
            if total > 16 * 1024 * 1024 or len(names) > 1024:
                raise ValueError('third-party notice bundle exceeds size limit')
            payload['third_party/' + entry['path']] = (data, 0o644)
    for path in directory.rglob('*'):
        if path.is_symlink():
            raise ValueError('linked third-party notice path')
        if path.is_file() and path.relative_to(directory).as_posix() not in names | {'manifest.json'}:
            raise ValueError('unlisted third-party notice file')
    return payload


def package(args):
    if not re.fullmatch(r'[0-9a-f]{40}', args.revision):
        raise ValueError('revision must be a full lowercase 40-character source commit')
    epoch = args.epoch
    if epoch is None:
        supplied = os.environ.get('SOURCE_DATE_EPOCH')
        if supplied is None:
            supplied = subprocess.check_output(
                ['git', '-C', str(ROOT), 'show', '-s', '--format=%ct', args.revision],
                text=True, timeout=5).strip()
        epoch = int(supplied)
    if not 0 <= epoch <= 0xffffffff:
        raise ValueError('source epoch must be between 0 and 4294967295')
    version = tomllib.loads((ROOT / 'Cargo.toml').read_text())['package']['version']
    if not re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.-]+)?', version):
        raise ValueError('unsupported package version label')
    binary = read_regular(args.binary, 128 * 1024 * 1024)
    arch = architecture(binary)
    if args.output_dir.resolve().is_relative_to(args.web_dir.resolve()):
        raise ValueError('output directory must be outside the built UI')
    installation = read_regular(ROOT / 'docs/installation.md', 1024 * 1024)
    payload = {'bin/flussonix': (binary, 0o755),
               'README.md': (installation, 0o644),
               'installation.md': (installation, 0o644),
               'iptv-transport.md': (read_regular(ROOT / 'docs/iptv-transport.md', 1024 * 1024), 0o644),
               'bin/flussonix-service': (read_regular(ROOT / 'scripts/service-preview.py', 1024 * 1024), 0o755),
               'service-installation.md': (read_regular(ROOT / 'docs/service-installation.md', 1024 * 1024), 0o644)}
    for name in ('LICENSE', 'NOTICE', 'vendor/quinn-proto/LICENSE-APACHE',
                 'vendor/quinn-proto/LICENSE-MIT'):
        payload[name] = (read_regular(ROOT / name, 1024 * 1024), 0o644)
    payload.update(read_notice_bundle(ROOT, arch))
    payload.update(web_payload(args.web_dir))
    manifest = {'format': 1, 'name': 'FlussoniX', 'version': version,
                'source_revision': args.revision, 'source_date_epoch': epoch,
                'architecture': arch, 'files': {name: {'sha256': hashlib.sha256(data).hexdigest(),
                 'bytes': len(data), 'mode': oct(mode)} for name, (data, mode) in sorted(payload.items())}}
    payload['manifest.json'] = ((json.dumps(manifest, indent=2, sort_keys=True) + '\n').encode(), 0o644)
    stem = f'flussonix-{version}-linux-{arch}-{args.revision[:12]}'
    args.output_dir.mkdir(parents=True, exist_ok=True)
    archive = args.output_dir / (stem + '.tar.gz')
    checksum = args.output_dir / (stem + '.tar.gz.sha256')
    if archive.exists() or archive.is_symlink() or checksum.exists() or checksum.is_symlink():
        raise ValueError('package output already exists; use a new output directory')
    with tempfile.TemporaryDirectory(prefix='.flussonix-package-', dir=args.output_dir) as temp:
        temp = Path(temp)
        built = temp / 'archive'
        with built.open('wb') as raw:
            with gzip.GzipFile(filename='', fileobj=raw, mode='wb', mtime=epoch, compresslevel=9) as compressed:
                with tarfile.open(fileobj=compressed, mode='w', format=tarfile.PAX_FORMAT) as tar:
                    names = {stem: None}
                    for name, content in payload.items():
                        path = Path(stem) / name
                        for parent in path.parents:
                            if parent != Path('.'):
                                names[parent.as_posix()] = None
                        names[path.as_posix()] = content
                    for name, content in sorted(names.items()):
                        entry = tarfile.TarInfo(name)
                        entry.uid = entry.gid = 0
                        entry.uname = entry.gname = ''
                        entry.mtime = epoch
                        if content is None:
                            entry.type = tarfile.DIRTYPE
                            entry.mode = 0o755
                            tar.addfile(entry)
                        else:
                            data, entry.mode = content
                            entry.size = len(data)
                            tar.addfile(entry, io.BytesIO(data))
            raw.flush()
            os.fsync(raw.fileno())
        digest = hashlib.sha256(built.read_bytes()).hexdigest()
        sidecar = temp / 'checksum'
        sidecar.write_text(digest + '  ' + archive.name + '\n')
        # Publish complete files without ever replacing an existing artifact.
        linked = False
        try:
            os.link(built, archive)
            linked = True
            os.link(sidecar, checksum)
        except BaseException:
            if linked and archive.stat().st_ino == built.stat().st_ino:
                archive.unlink()
            raise
    return {'archive': str(archive), 'checksum': str(checksum), 'sha256': digest,
            'source_revision': args.revision, 'architecture': arch, 'files': len(payload)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True, help='already-built Linux release executable')
    parser.add_argument('--web-dir', type=Path, required=True, help='built Vite output (web/dist)')
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--revision', required=True, help='source commit used for the supplied artifacts')
    parser.add_argument('--epoch', type=int, help='otherwise SOURCE_DATE_EPOCH or the source commit timestamp')
    args = parser.parse_args()
    try:
        print(json.dumps(package(args), sort_keys=True))
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        parser.exit(1, f'Cannot package preview: {error}\n')


if __name__ == '__main__':
    main()
