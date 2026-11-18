#!/usr/bin/env python3
"""Build a deterministic Debian package from one manifest-qualified release archive."""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import struct
import subprocess
import sys
import tarfile
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parents[1]
DEBIAN = ROOT / 'packaging/debian'
BASE_DEPENDENCIES = [
    'ffmpeg',
    'libsrt1.5-gnutls',
    'libsrtp2-1',
    'ca-certificates',
    'adduser',
    'init-system-helpers',
    'libc6 (>= 2.39)',
]
RECOMMENDS = ['tesseract-ocr', 'tesseract-ocr-eng', 'vainfo', 'intel-gpu-tools', 'pciutils']
MAX_ARCHIVE = 1024 * 1024 * 1024
MAX_FILE = 256 * 1024 * 1024
MAX_TOTAL = 1024 * 1024 * 1024
MAX_MEMBERS = 8192


def read_regular(path, limit):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > limit:
            raise ValueError(f'{path} must be a bounded regular file')
        with os.fdopen(descriptor, 'rb', closefd=False) as source:
            data = source.read(limit + 1)
        if len(data) > limit:
            raise ValueError(f'{path} exceeds its size limit')
        return data
    finally:
        os.close(descriptor)


def sha256_file(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > MAX_ARCHIVE:
            raise ValueError('release archive must be a bounded regular file')
        digest = hashlib.sha256()
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
        return digest.hexdigest()
    finally:
        os.close(descriptor)


def read_archive_magic(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > MAX_ARCHIVE:
            raise ValueError('release archive must be a bounded regular file')
        return os.read(descriptor, 4)
    finally:
        os.close(descriptor)


def verify_checksum(archive, checksum):
    text = read_regular(checksum, 4096).decode('ascii')
    match = re.fullmatch(r'([0-9a-f]{64})  ([^\n/]+)\n', text)
    if not match or match.group(2) != archive.name:
        raise ValueError('checksum file must name the adjacent release archive exactly')
    actual = sha256_file(archive)
    if match.group(1) != actual:
        raise ValueError('release archive checksum mismatch')
    return actual


def binary_architecture(binary):
    if len(binary) < 64 or binary[:7] != b'\x7fELF\x02\x01\x01' or binary[7] not in (0, 3):
        raise ValueError('release binary must be a little-endian 64-bit Linux ELF')
    kind, machine = struct.unpack_from('<HH', binary, 16)
    if kind not in (2, 3) or machine not in (62, 183):
        raise ValueError('release binary has an unsupported architecture')
    return {62: 'x86_64', 183: 'aarch64'}[machine]


def safe_relative(name):
    path = PurePosixPath(name)
    if (path.is_absolute() or not path.parts or any(part in ('', '.', '..') for part in path.parts)
            or len(name) > 1024):
        raise ValueError('release archive contains an unsafe path')
    return path


def baselines():
    value = json.loads(read_regular(DEBIAN / 'baselines.json', 128 * 1024))
    if value.get('format') != 1 or not isinstance(value.get('legacy_archives'), list):
        raise ValueError('invalid Debian package baseline registry')
    return value['legacy_archives']


def load_archive(archive, digest, revision, current_version):
    if read_archive_magic(archive)[:3] != b'\x1f\x8b\x08':
        raise ValueError('release archive must contain genuine gzip-compressed tar data')
    payload = {}
    directories = set()
    total = 0
    prefix = None
    manifest_bytes = None
    legacy_baseline = next((row for row in baselines() if row.get('sha256') == digest), None)
    with tarfile.open(archive, mode='r:gz') as bundle:
        members = bundle.getmembers()
        if not 1 <= len(members) <= MAX_MEMBERS:
            raise ValueError('release archive member count is invalid')
        seen = set()
        for member in members:
            path = safe_relative(member.name)
            if member.name in seen:
                raise ValueError('release archive contains duplicate members')
            seen.add(member.name)
            if prefix is None:
                prefix = path.parts[0]
            if path.parts[0] != prefix:
                raise ValueError('release archive must have one top-level directory')
            relative = PurePosixPath(*path.parts[1:])
            if member.issym() or member.islnk() or member.isdev() or member.isfifo():
                raise ValueError('release archive contains a linked or special member')
            if member.isdir():
                directories.add(relative.as_posix())
                continue
            if not member.isfile() or not relative.parts or member.size > MAX_FILE:
                raise ValueError('release archive contains an invalid file')
            total += member.size
            if total > MAX_TOTAL:
                raise ValueError('release archive payload exceeds its size limit')
            extracted = bundle.extractfile(member)
            if extracted is None:
                raise ValueError('release archive file cannot be read')
            data = extracted.read(MAX_FILE + 1)
            if len(data) != member.size:
                raise ValueError('release archive file size changed while reading')
            if relative.as_posix() == 'manifest.json':
                manifest_bytes = data
            else:
                payload[relative.as_posix()] = (data, member.mode & 0o777)
    legacy = legacy_baseline
    if manifest_bytes is None:
        if legacy is None:
            raise ValueError('release archive manifest is missing')
        if legacy.get('source_revision') != revision:
            raise ValueError('legacy release revision does not match its pinned baseline')
        version = legacy.get('version')
        architecture = legacy.get('architecture')
        epoch = min((member.mtime for member in members), default=0)
        manifest_files = set(payload)
    else:
        try:
            manifest = json.loads(manifest_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError('release archive manifest is invalid') from error
        expected_version = legacy.get('version') if legacy is not None else current_version
        expected_architecture = (legacy.get('architecture') if legacy is not None
                                 else manifest.get('architecture'))
        expected_revision = (legacy.get('source_revision') if legacy is not None
                             else revision)
        if (manifest.get('format') != 1 or manifest.get('name') != 'FlussoniX'
                or manifest.get('version') != expected_version
                or manifest.get('source_revision') != expected_revision
                or manifest.get('source_revision') != revision
                or manifest.get('architecture') != expected_architecture
                or manifest.get('architecture') not in ('x86_64', 'aarch64')
                or not isinstance(manifest.get('source_date_epoch'), int)
                or not isinstance(manifest.get('files'), dict)):
            raise ValueError('release archive manifest identity does not match this build')
        version = manifest['version']
        architecture = manifest['architecture']
        epoch = manifest['source_date_epoch']
        manifest_files = set(manifest['files'])
        if manifest_files != set(payload):
            raise ValueError('release archive has manifest-unlisted or missing files')
        for name, (data, mode) in payload.items():
            row = manifest['files'].get(name)
            if (not isinstance(row, dict) or row.get('sha256') != hashlib.sha256(data).hexdigest()
                    or row.get('bytes') != len(data) or row.get('mode') != oct(mode)):
                raise ValueError(f'release archive member differs from manifest: {name}')
    required = ({'bin/flussonix', 'web/index.html', 'README.md'} if legacy is not None else
                {'bin/flussonix', 'web/index.html', 'installation.md', 'LICENSE', 'NOTICE'})
    if not required <= manifest_files:
        raise ValueError('release archive lacks required package payload')
    actual_architecture = binary_architecture(payload['bin/flussonix'][0])
    if actual_architecture != architecture or architecture != 'x86_64':
        raise ValueError('first Debian repository accepts x86_64 release archives only')
    if legacy is not None and legacy.get('iteration') != 1:
        raise ValueError('unsupported legacy package iteration')
    return {
        'payload': payload,
        'version': version,
        'architecture': architecture,
        'source_revision': revision,
        'source_date_epoch': epoch,
        'legacy': legacy is not None,
    }


def verify_ffmpeg_contract(file_lister=None):
    if file_lister is None:
        def file_lister(package):
            result = subprocess.run(
                ['/usr/bin/dpkg-query', '--listfiles', package],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode != 0:
                raise ValueError('Ubuntu ffmpeg package metadata is unavailable')
            return set(result.stdout.splitlines())
    files = set(file_lister('ffmpeg'))
    for executable in ('/usr/bin/ffmpeg', '/usr/bin/ffprobe'):
        if executable not in files:
            raise ValueError(f'Ubuntu ffmpeg package does not provide {executable}')
    return True


def dependency_name(value):
    return value.split(' ', 1)[0].split(':', 1)[0]


def ensure_declared_dependencies(derived, declared):
    derived_names = {dependency_name(value) for value in derived}
    declared_names = {dependency_name(value) for value in declared}
    missing = sorted(derived_names - declared_names)
    if missing:
        raise ValueError('undeclared ELF dependencies: ' + ', '.join(missing))


def elf_dependencies(binary, temporary, allow_empty=False):
    debian = temporary / 'debian'
    debian.mkdir()
    (debian / 'control').write_text(
        'Source: flussonix\nSection: video\nPriority: optional\n'
        'Maintainer: FlussoniX <noreply@flussonix.invalid>\nStandards-Version: 4.6.2\n\n'
        'Package: flussonix\nArchitecture: amd64\nDescription: dependency probe\n'
    )
    probe = temporary / 'flussonix'
    probe.write_bytes(binary)
    probe.chmod(0o755)
    result = subprocess.run(
        ['/usr/bin/dpkg-shlibdeps', '-O', '-e./flussonix'], cwd=temporary,
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        raise ValueError('dpkg-shlibdeps failed: ' + result.stderr.strip()[:1024])
    line = next((line for line in result.stdout.splitlines()
                 if line.startswith('shlibs:Depends=')), None)
    if line is None:
        if allow_empty:
            return []
        raise ValueError('dpkg-shlibdeps did not report dependencies')
    return [item.strip() for item in line.removeprefix('shlibs:Depends=').split(',') if item.strip()]


def install_file(path, data, mode, epoch):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    path.chmod(mode)
    os.utime(path, (epoch, epoch), follow_symlinks=False)


def normalize_tree(root, epoch):
    private_directories = {
        root / 'etc/flussonix',
        root / 'var/lib/flussonix',
    }
    for path in sorted(root.rglob('*')):
        if path.is_dir():
            path.chmod(0o700 if path in private_directories else 0o755)
        os.utime(path, (epoch, epoch), follow_symlinks=False)
    os.utime(root, (epoch, epoch), follow_symlinks=False)


def render_control(version, dependencies):
    template = read_regular(DEBIAN / 'control.in', 128 * 1024).decode('utf-8')
    rendered = template.replace('@VERSION@', version).replace(
        '@DEPENDS@', ', '.join(dependencies)
    )
    if re.search(r'@[A-Z_]+@', rendered):
        raise ValueError('unresolved Debian control template field')
    return rendered.encode()


def package(args):
    if not re.fullmatch(r'[0-9a-f]{40}', args.revision):
        raise ValueError('revision must be a full lowercase 40-character commit')
    if not 1 <= args.iteration <= 999:
        raise ValueError('package iteration must be between 1 and 999')
    if args.output_dir.exists() or args.output_dir.is_symlink():
        raise ValueError('package output directory already exists')
    digest = verify_checksum(args.archive, args.checksum)
    current_version = tomllib.loads(
        read_regular(ROOT / 'Cargo.toml', 1024 * 1024).decode('utf-8')
    )['package']['version']
    release = load_archive(args.archive, digest, args.revision, current_version)
    verify_ffmpeg_contract()
    parent = args.output_dir.parent.resolve()
    if not parent.is_dir():
        raise ValueError('package output parent does not exist')
    with tempfile.TemporaryDirectory(prefix='.flussonix-deb-', dir=parent) as temporary_name:
        temporary = Path(temporary_name)
        dependencies = elf_dependencies(
            release['payload']['bin/flussonix'][0], temporary,
            allow_empty=release['legacy'],
        )
        final_dependencies = list(BASE_DEPENDENCIES)
        known = {dependency_name(value) for value in final_dependencies}
        for dependency in dependencies:
            if dependency_name(dependency) not in known:
                final_dependencies.append(dependency)
                known.add(dependency_name(dependency))
        ensure_declared_dependencies(dependencies, final_dependencies)

        tree = temporary / 'tree'
        control_dir = tree / 'DEBIAN'
        control_dir.mkdir(parents=True)
        epoch = release['source_date_epoch']
        install_file(control_dir / 'control', render_control(
            f"{release['version']}-{args.iteration}", final_dependencies
        ), 0o644, epoch)
        for name in ('preinst', 'postinst', 'prerm', 'postrm'):
            install_file(control_dir / name, read_regular(DEBIAN / name, 128 * 1024), 0o755, epoch)

        payload = release['payload']
        install_file(tree / 'usr/bin/flussonix', payload['bin/flussonix'][0], 0o755, epoch)
        for name, (data, mode) in sorted(payload.items()):
            if name.startswith('web/'):
                install_file(tree / 'usr/share/flussonix' / name, data, mode, epoch)
            elif name not in ('bin/flussonix',) and not name.startswith('bin/'):
                install_file(tree / 'usr/share/doc/flussonix' / name, data, mode, epoch)
        install_file(tree / 'usr/lib/systemd/system/flussonix.service',
                     read_regular(DEBIAN / 'flussonix.service', 128 * 1024), 0o644, epoch)
        install_file(tree / 'usr/lib/tmpfiles.d/flussonix.conf',
                     read_regular(DEBIAN / 'flussonix.tmpfiles', 128 * 1024), 0o644, epoch)
        for directory, mode in [
            (tree / 'etc/flussonix', 0o700),
            (tree / 'etc/systemd/system/flussonix.service.d', 0o755),
            (tree / 'var/lib/flussonix', 0o700),
        ]:
            directory.mkdir(parents=True, exist_ok=True)
            directory.chmod(mode)
        normalize_tree(tree, epoch)

        stem = f"flussonix_{release['version']}-{args.iteration}_amd64.deb"
        built = temporary / stem
        environment = {**os.environ, 'SOURCE_DATE_EPOCH': str(epoch), 'TZ': 'UTC', 'LC_ALL': 'C'}
        result = subprocess.run(
            ['/usr/bin/dpkg-deb', '--root-owner-group', '--uniform-compression',
             '-Zxz', '-z9', '--build', str(tree), str(built)],
            env=environment, capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0:
            raise ValueError('dpkg-deb failed: ' + result.stderr.strip()[:1024])

        installed = {}
        for path in sorted(tree.rglob('*')):
            if path.is_file() and 'DEBIAN' not in path.relative_to(tree).parts:
                relative = '/' + path.relative_to(tree).as_posix()
                data = read_regular(path, MAX_FILE)
                installed[relative] = {
                    'sha256': hashlib.sha256(data).hexdigest(),
                    'bytes': len(data),
                    'mode': oct(path.stat().st_mode & 0o777),
                }
        report = {
            'format': 1,
            'archive': {
                'sha256': digest,
                'source_revision': release['source_revision'],
                'version': release['version'],
                'architecture': release['architecture'],
                'legacy': release['legacy'],
            },
            'package': {
                'name': 'flussonix',
                'version': f"{release['version']}-{args.iteration}",
                'architecture': 'amd64',
                'depends': final_dependencies,
                'recommends': RECOMMENDS,
            },
            'files': installed,
        }
        package_digest = sha256_file(built)
        inspection = temporary / (stem + '.inspection.json')
        inspection.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
        checksum = temporary / (stem + '.sha256')
        checksum.write_text(f'{package_digest}  {stem}\n')
        os.utime(inspection, (epoch, epoch))
        os.utime(checksum, (epoch, epoch))

        published = temporary / 'published'
        published.mkdir(mode=0o755)
        for source in (built, inspection, checksum):
            os.replace(source, published / source.name)
        os.replace(published, args.output_dir)
    print(json.dumps({
        'package': str(args.output_dir / stem),
        'sha256': package_digest,
        'archive_sha256': digest,
        'source_revision': release['source_revision'],
    }, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive', type=Path, required=True)
    parser.add_argument('--checksum', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--revision', required=True)
    parser.add_argument('--iteration', type=int, default=1)
    arguments = parser.parse_args()
    package(arguments)


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, subprocess.SubprocessError, tarfile.TarError) as error:
        print(f'Debian package build failed: {error}', file=sys.stderr)
        raise SystemExit(1)
