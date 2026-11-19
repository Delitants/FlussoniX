#!/usr/bin/env python3
"""Build and verify the signed FlussoniX Ubuntu 24.04 APT repository."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import shlex
import stat
import subprocess
import sys
import tarfile
import tempfile


ROOT = Path(__file__).resolve().parents[1]
APT_FILES = ROOT / 'packaging/apt'
MAX_PACKAGE = 1024 * 1024 * 1024
MAX_SIDECAR = 16 * 1024 * 1024
MAX_FILES = 16384
REPOSITORY_URL = 'https://delitants.github.io/FlussoniX/apt'


def read_regular(path, limit):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > limit:
            raise ValueError(f'{path} must be a bounded regular file')
        data = b''
        while len(data) <= limit:
            chunk = os.read(descriptor, min(1024 * 1024, limit + 1 - len(data)))
            if not chunk:
                break
            data += chunk
        if len(data) > limit:
            raise ValueError(f'{path} exceeds its size limit')
        return data
    finally:
        os.close(descriptor)


def sha256_file(path, limit=MAX_PACKAGE):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > limit:
            raise ValueError(f'{path} must be a bounded regular file')
        digest = hashlib.sha256()
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
        return digest.hexdigest()
    finally:
        os.close(descriptor)


def command(arguments, *, environment=None, cwd=None, input_bytes=None, timeout=60):
    result = subprocess.run(
        arguments, cwd=cwd, env=environment, input=input_bytes,
        capture_output=True, timeout=timeout,
    )
    if result.returncode != 0:
        detail = result.stderr.decode('utf-8', 'replace').strip()[:2048]
        raise ValueError(f'{Path(arguments[0]).name} failed: {detail}')
    return result.stdout


def package_field(package, field):
    value = command(['/usr/bin/dpkg-deb', '--field', str(package), field], timeout=20)
    return value.decode('utf-8', 'strict').strip()


def safe_package_path(name):
    path = PurePosixPath(name.removeprefix('./'))
    if path.is_absolute() or not path.parts or any(part in ('', '.', '..') for part in path.parts):
        raise ValueError('Debian package contains an unsafe path')
    return '/' + path.as_posix()


def inspect_package_files(package):
    process = subprocess.Popen(
        ['/usr/bin/dpkg-deb', '--fsys-tarfile', str(package)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    files = {}
    try:
        if process.stdout is None:
            raise ValueError('dpkg-deb did not provide a payload stream')
        with tarfile.open(fileobj=process.stdout, mode='r|*') as archive:
            count = 0
            for member in archive:
                count += 1
                if count > MAX_FILES:
                    raise ValueError('Debian package contains too many members')
                if member.isdir():
                    continue
                if not member.isfile() or member.issym() or member.islnk():
                    raise ValueError('Debian package contains a linked or special member')
                if member.size > MAX_PACKAGE:
                    raise ValueError('Debian package member exceeds its size limit')
                source = archive.extractfile(member)
                if source is None:
                    raise ValueError('Debian package member cannot be read')
                digest = hashlib.sha256()
                size = 0
                while True:
                    chunk = source.read(1024 * 1024)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > MAX_PACKAGE:
                        raise ValueError('Debian package member exceeds its size limit')
                    digest.update(chunk)
                if size != member.size:
                    raise ValueError('Debian package member changed while reading')
                path = safe_package_path(member.name)
                if path in files:
                    raise ValueError('Debian package contains duplicate members')
                files[path] = {
                    'sha256': digest.hexdigest(),
                    'bytes': size,
                    'mode': oct(member.mode & 0o777),
                }
        process.stdout.close()
        stderr = process.stderr.read() if process.stderr is not None else b''
        if process.wait(timeout=20) != 0:
            raise ValueError('dpkg-deb failed: ' + stderr.decode('utf-8', 'replace')[:2048])
    except Exception:
        process.kill()
        process.wait()
        raise
    return files


def verify_package(package):
    if package.name != os.path.basename(package) or package.suffix != '.deb':
        raise ValueError('package must be a directly named .deb file')
    checksum_path = package.with_suffix(package.suffix + '.sha256')
    inspection_path = package.with_suffix(package.suffix + '.inspection.json')
    checksum = read_regular(checksum_path, 4096).decode('ascii')
    match = re.fullmatch(r'([0-9a-f]{64})  ([^/\n]+)\n', checksum)
    if not match or match.group(2) != package.name:
        raise ValueError('package checksum must name the adjacent package exactly')
    digest = sha256_file(package)
    if digest != match.group(1):
        raise ValueError('package checksum mismatch')
    try:
        inspection = json.loads(read_regular(inspection_path, MAX_SIDECAR))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError('package inspection record is invalid') from error
    if inspection.get('format') != 1 or not isinstance(inspection.get('package'), dict):
        raise ValueError('package inspection record has an unsupported format')
    metadata = inspection['package']
    actual = {
        'name': package_field(package, 'Package'),
        'version': package_field(package, 'Version'),
        'architecture': package_field(package, 'Architecture'),
    }
    if actual['name'] != 'flussonix' or actual['architecture'] != 'amd64':
        raise ValueError('repository accepts only the flussonix amd64 package')
    if any(metadata.get(field) != value for field, value in actual.items()):
        raise ValueError('package metadata differs from its inspection record')
    if not re.fullmatch(r'[0-9][0-9A-Za-z.+:~\-]*', actual['version']):
        raise ValueError('package version is invalid')
    archive = inspection.get('archive')
    if (not isinstance(archive, dict)
            or not actual['version'].startswith(str(archive.get('version', '')) + '-')):
        raise ValueError('package version differs from its release archive')
    expected_files = inspection.get('files')
    if not isinstance(expected_files, dict) or inspect_package_files(package) != expected_files:
        raise ValueError('package payload differs from its inspection record')
    if sha256_file(package) != digest:
        raise ValueError('package changed while it was being verified')
    return {'path': package, 'digest': digest, **actual}


def gpg_environment(home):
    environment = {**os.environ, 'GNUPGHOME': str(home), 'LC_ALL': 'C', 'LANG': 'C'}
    environment.pop('GPG_AGENT_INFO', None)
    return environment


def key_records(listing):
    records = []
    pending = None
    for line in listing.decode('utf-8', 'strict').splitlines():
        fields = line.split(':')
        if fields[0] in ('pub', 'sub', 'sec', 'ssb'):
            pending = {'kind': fields[0], 'capabilities': fields[11] if len(fields) > 11 else ''}
        elif fields[0] == 'fpr' and pending is not None:
            records.append({**pending, 'fingerprint': fields[9]})
            pending = None
    return records


def validate_keys(public_key, signing_fingerprint, signing_home, temporary):
    if not re.fullmatch(r'[0-9A-F]{40}', signing_fingerprint):
        raise ValueError('signing fingerprint must be an exact uppercase OpenPGP fingerprint')
    public_home = temporary / 'public-key-home'
    public_home.mkdir(mode=0o700)
    public_env = gpg_environment(public_home)
    supplied_key = temporary / 'supplied-public-key.gpg'
    supplied_key.write_bytes(read_regular(public_key, MAX_SIDECAR))
    supplied_key.chmod(0o600)
    command(['/usr/bin/gpg', '--batch', '--no-options', '--import', str(supplied_key)],
            environment=public_env, timeout=20)
    public_listing = command(
        ['/usr/bin/gpg', '--batch', '--no-options', '--with-colons', '--fingerprint',
         '--list-keys'], environment=public_env, timeout=20,
    )
    public_records = key_records(public_listing)
    primary = [row for row in public_records if row['kind'] == 'pub']
    if len(primary) != 1:
        raise ValueError('public key file must contain exactly one primary key')
    secret_listing = command(
        ['/usr/bin/gpg', '--batch', '--no-options', '--with-colons', '--fingerprint',
         '--list-secret-keys'], environment=public_env, timeout=20,
    )
    if any(row['kind'] in ('sec', 'ssb') for row in key_records(secret_listing)):
        raise ValueError('public key file contains private key material')
    if signing_fingerprint not in {row['fingerprint'] for row in public_records}:
        raise ValueError('signing fingerprint is absent from the supplied public key')
    secret_listing = command(
        ['/usr/bin/gpg', '--batch', '--no-options', '--with-colons', '--fingerprint',
         '--list-secret-keys', signing_fingerprint],
        environment=gpg_environment(signing_home), timeout=20,
    )
    signing = [row for row in key_records(secret_listing)
               if row['fingerprint'] == signing_fingerprint
               and row['kind'] in ('sec', 'ssb')
               and 's' in row['capabilities'].lower()]
    if len(signing) != 1:
        raise ValueError('exact signing key is unavailable or lacks signing capability')
    exported = command(
        ['/usr/bin/gpg', '--batch', '--no-options', '--export', primary[0]['fingerprint']],
        environment=public_env, timeout=20,
    )
    if not exported:
        raise ValueError('public archive key export is empty')
    return primary[0]['fingerprint'], exported


def render_install_script(primary_fingerprint, *, repository_url=REPOSITORY_URL,
                          root_prefix='',
                          command_path='/usr/sbin:/usr/bin:/sbin:/bin'):
    if (not re.fullmatch(r'[0-9A-F]{40}', primary_fingerprint)
            or not repository_url.startswith('https://')
            or (root_prefix and not Path(root_prefix).is_absolute())
            or any(not value or not Path(value).is_absolute()
                   for value in command_path.split(':'))):
        raise ValueError('bootstrap renderer received an invalid identity or path')
    template = read_regular(APT_FILES / 'install.sh.in', 256 * 1024).decode('utf-8')
    rendered = template.replace(
        '@PUBLIC_KEY_FINGERPRINT@', shlex.quote(primary_fingerprint)
    ).replace('@REPOSITORY_URL@', shlex.quote(repository_url)).replace(
        '@ROOT_PREFIX@', shlex.quote(root_prefix)
    ).replace(
        '@COMMAND_PATH@', shlex.quote(command_path)
    )
    if re.search(r'@[A-Z_]+@', rendered):
        raise ValueError('bootstrap template contains unresolved fields')
    return rendered.encode()


def write_file(path, data, mode=0o644):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    path.chmod(mode)


def release_metadata(repository):
    options = {
        'Origin': 'FlussoniX', 'Label': 'FlussoniX', 'Suite': 'stable',
        'Codename': 'noble', 'Architectures': 'amd64', 'Components': 'main',
        'Description': 'FlussoniX signed package repository',
    }
    arguments = ['/usr/bin/apt-ftparchive']
    for field, value in options.items():
        arguments.extend(['-o', f'APT::FTPArchive::Release::{field}={value}'])
    arguments.extend(['release', 'dists/noble'])
    return command(arguments, cwd=repository, timeout=30)


def sign_release(repository, signing_home, signing_fingerprint):
    environment = gpg_environment(signing_home)
    release = repository / 'dists/noble/Release'
    common = ['/usr/bin/gpg', '--batch', '--yes', '--no-options',
              '--local-user', signing_fingerprint + '!', '--digest-algo', 'SHA256']
    command(common + ['--clearsign', '--output',
                      str(repository / 'dists/noble/InRelease'), str(release)],
            environment=environment, timeout=30)
    command(common + ['--detach-sign', '--output',
                      str(repository / 'dists/noble/Release.gpg'), str(release)],
            environment=environment, timeout=30)


def write_checksums(repository):
    rows = []
    for path in sorted(repository.rglob('*')):
        if path.is_file() and path.name != 'SHA256SUMS':
            relative = path.relative_to(repository).as_posix()
            rows.append(f'{sha256_file(path)}  {relative}\n')
    write_file(repository / 'SHA256SUMS', ''.join(rows).encode())


def verify_repository(repository, primary_fingerprint, temporary):
    sums = read_regular(repository / 'SHA256SUMS', MAX_SIDECAR).decode('ascii')
    expected = {}
    for line in sums.splitlines():
        match = re.fullmatch(r'([0-9a-f]{64})  ([^\n]+)', line)
        if not match or match.group(2) in expected:
            raise ValueError('repository checksum manifest is invalid')
        expected[match.group(2)] = match.group(1)
    actual = {path.relative_to(repository).as_posix(): sha256_file(path)
              for path in repository.rglob('*')
              if path.is_file() and path.name != 'SHA256SUMS'}
    if actual != expected:
        raise ValueError('repository checksum manifest does not cover the completed tree')
    verify_home = temporary / 'verify-home'
    verify_home.mkdir(mode=0o700)
    environment = gpg_environment(verify_home)
    key = repository / 'flussonix-archive-keyring.gpg'
    command(['/usr/bin/gpg', '--batch', '--no-options', '--import', str(key)],
            environment=environment, timeout=20)
    listing = command(
        ['/usr/bin/gpg', '--batch', '--no-options', '--with-colons', '--fingerprint',
         '--list-keys'], environment=environment, timeout=20,
    )
    primaries = [row['fingerprint'] for row in key_records(listing) if row['kind'] == 'pub']
    if primaries != [primary_fingerprint]:
        raise ValueError('published archive key fingerprint changed')
    command(['/usr/bin/gpg', '--batch', '--no-options', '--verify',
             str(repository / 'dists/noble/InRelease')], environment=environment, timeout=20)
    command(['/usr/bin/gpg', '--batch', '--no-options', '--verify',
             str(repository / 'dists/noble/Release.gpg'),
             str(repository / 'dists/noble/Release')], environment=environment, timeout=20)


def build(args):
    if args.output_dir.exists() or args.output_dir.is_symlink():
        raise ValueError('repository output directory already exists')
    if not args.output_dir.parent.resolve().is_dir():
        raise ValueError('repository output parent does not exist')
    packages = [verify_package(path) for path in args.package]
    versions = [package['version'] for package in packages]
    if len(packages) not in (1, 2) or len(set(versions)) != len(versions):
        raise ValueError('repository requires one or two distinct package versions')
    signing_home_value = os.environ.get('GNUPGHOME')
    if not signing_home_value:
        raise ValueError('GNUPGHOME must name the isolated signing keyring')
    signing_home_input = Path(signing_home_value)
    if (not signing_home_input.is_absolute() or signing_home_input.is_symlink()
            or not signing_home_input.is_dir()
            or signing_home_input.stat().st_mode & 0o077
            or signing_home_input.stat().st_uid != os.geteuid()):
        raise ValueError('GNUPGHOME must be a private directory')
    signing_home = signing_home_input.resolve()
    parent = args.output_dir.parent.resolve()
    with tempfile.TemporaryDirectory(prefix='.flussonix-apt-', dir=parent) as name:
        temporary = Path(name)
        primary_fingerprint, exported_key = validate_keys(
            args.public_key, args.signing_fingerprint, signing_home, temporary
        )
        repository = temporary / 'repository'
        pool = repository / 'pool/main/f/flussonix'
        pool.mkdir(parents=True)
        for package in sorted(packages, key=lambda item: item['path'].name):
            target = pool / package['path'].name
            shutil.copyfile(package['path'], target)
            target.chmod(0o644)
            if sha256_file(target) != package['digest']:
                raise ValueError('package changed while it was copied into the repository')
        index_directory = repository / 'dists/noble/main/binary-amd64'
        index_directory.mkdir(parents=True)
        packages_index = command(
            ['/usr/bin/apt-ftparchive', 'packages', 'pool/main/f/flussonix'],
            cwd=repository, timeout=30,
        )
        write_file(index_directory / 'Packages', packages_index)
        write_file(index_directory / 'Packages.gz', gzip.compress(packages_index, 9, mtime=0))
        write_file(repository / 'dists/noble/Release', release_metadata(repository))
        write_file(repository / 'flussonix-archive-keyring.gpg', exported_key)
        write_file(repository / 'install.sh', render_install_script(primary_fingerprint), 0o755)
        sign_release(repository, signing_home, args.signing_fingerprint)
        write_checksums(repository)
        verify_repository(repository, primary_fingerprint, temporary)
        os.replace(repository, args.output_dir)
    print(json.dumps({
        'output': str(args.output_dir),
        'primary_fingerprint': primary_fingerprint,
        'signing_fingerprint': args.signing_fingerprint,
        'packages': sorted(versions),
    }, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--package', action='append', type=Path, required=True)
    parser.add_argument('--public-key', type=Path, required=True)
    parser.add_argument('--signing-fingerprint', required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    arguments = parser.parse_args()
    build(arguments)


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, subprocess.SubprocessError, tarfile.TarError) as error:
        print(f'APT repository build failed: {error}', file=sys.stderr)
        raise SystemExit(1)
