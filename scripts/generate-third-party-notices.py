#!/usr/bin/env python3
"""Generate a locked Linux release notice bundle from local Cargo/npm sources."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parents[1]
TARGETS = ('x86_64-unknown-linux-gnu', 'aarch64-unknown-linux-gnu')
NOTICE_NAME = re.compile(r'^(LICENSE|LICENCE|COPYING|COPYRIGHT|NOTICE|PATENTS|UNLICENSE)(?:$|[._ -])', re.I)
SAFE_PART = re.compile(r'[A-Za-z0-9_. -]+')
CRATES_IO_SOURCE = 'registry+https://github.com/rust-lang/crates.io-index'


def digest(data):
    return hashlib.sha256(data).hexdigest()


def parse_tree(*texts):
    """Return the union of exact name/version pairs, excluding this project."""
    packages = set()
    for text in texts:
        for line in text.splitlines():
            match = re.match(r'^([^ ]+) v([^ ]+)', line)
            if match and match.group(1) != 'flussonix':
                packages.add(match.groups())
    return packages


def license_paths(source):
    """Include nested upstream license and notice texts without source files."""
    paths = []
    for path in source.rglob('*'):
        if NOTICE_NAME.match(path.name):
            if path.is_symlink():
                raise ValueError(f'linked license/notice source: {path}')
            if path.is_file():
                relative = path.relative_to(source)
                if any(not SAFE_PART.fullmatch(part) or part in ('.', '..') for part in relative.parts):
                    raise ValueError(f'unsafe license/notice source: {path}')
                paths.append(relative)
    if not paths:
        raise ValueError(f'no license/notice texts in {source}')
    return sorted(paths, key=lambda path: path.as_posix())


def source_text(path):
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024 * 1024:
        raise ValueError(f'missing, linked or oversized license/notice text: {path}')
    data = path.read_bytes()
    if not data:
        raise ValueError(f'empty license/notice text: {path}')
    return data


def copy_notices(source, destination, prefix):
    files = []
    for relative in license_paths(source):
        data = source_text(source / relative)
        payload = (Path('licenses') / prefix / relative).as_posix()
        target = destination / payload
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        files.append({'path': payload, 'upstream_path': relative.as_posix(),
                      'sha256': digest(data), 'bytes': len(data)})
    return files


def registry_source(cargo_home, name, version):
    sources = list((cargo_home / 'registry/src').glob(f'*/{name}-{version}'))
    if len(sources) != 1 or sources[0].is_symlink():
        raise ValueError(f'exact Cargo source missing or ambiguous: {name} {version}')
    return sources[0]


def rust_source(root, cargo_home, name, version, source_id):
    if source_id is None and name == 'quinn-proto':
        return root / 'vendor/quinn-proto'
    if source_id != CRATES_IO_SOURCE:
        raise ValueError(f'unsupported Cargo registry/source for {name} {version}')
    return registry_source(cargo_home, name, version)


def generate(root, output, cargo_home):
    if output.exists() or output.is_symlink():
        raise ValueError('notice output already exists')
    lock_data = (root / 'Cargo.lock').read_bytes()
    web_data = (root / 'web/package-lock.json').read_bytes()
    locked = tomllib.loads(lock_data.decode())
    lock_entries = {(item['name'], item['version']): item for item in locked['package']}
    trees = [subprocess.check_output(
        ['cargo', 'tree', '--locked', '--offline', '--target', target,
         '-e', 'normal,build', '--prefix', 'none', '--format', '{p}'],
        cwd=root, text=True, timeout=30) for target in TARGETS]
    rust_packages = parse_tree(*trees)
    web_packages = json.loads(web_data)['packages']
    stage = Path(tempfile.mkdtemp(prefix='.notices-', dir=output.parent))
    try:
        bundle = stage / 'third_party'
        bundle.mkdir()
        packages = []
        for name, version in sorted(rust_packages):
            if not SAFE_PART.fullmatch(name) or not SAFE_PART.fullmatch(version):
                raise ValueError('unsafe Cargo package name/version')
            locked_package = lock_entries.get((name, version))
            if locked_package is None:
                raise ValueError(f'Cargo tree package absent from lock: {name} {version}')
            source = rust_source(root, cargo_home, name, version,
                                 locked_package.get('source'))
            metadata = tomllib.loads((source / 'Cargo.toml').read_text())['package']
            if metadata['name'] != name or metadata['version'] != version:
                raise ValueError(f'Cargo source metadata differs: {name} {version}')
            declared = metadata.get('license') or ('file: ' + metadata['license-file']
                                                    if metadata.get('license-file') else None)
            if not declared:
                raise ValueError(f'Cargo source has no declared license: {name} {version}')
            files = copy_notices(source, bundle, Path('rust') / f'{name}-{version}')
            packages.append({'ecosystem': 'rust', 'name': name, 'version': version,
                             'declared_license': declared, 'files': files})
        for key, record in sorted(web_packages.items()):
            if not key or record.get('dev'):
                continue
            if not key.startswith('node_modules/'):
                raise ValueError(f'unexpected web package key: {key}')
            name = key.removeprefix('node_modules/')
            version = record['version']
            if not SAFE_PART.fullmatch(name) or not SAFE_PART.fullmatch(version):
                raise ValueError(f'unsafe web package name/version: {name} {version}')
            source = root / 'web/node_modules' / name
            metadata = json.loads((source / 'package.json').read_text())
            if metadata['name'] != name or metadata['version'] != version:
                raise ValueError(f'installed web package differs from lock: {name} {version}')
            declared = metadata.get('license') or record.get('license')
            if not isinstance(declared, str) or not declared:
                raise ValueError(f'web package has no declared license: {name} {version}')
            files = copy_notices(source, bundle, Path('web') / f'{name}-{version}')
            packages.append({'ecosystem': 'web', 'name': name, 'version': version,
                             'declared_license': declared, 'files': files})
        packages.sort(key=lambda item: (item['ecosystem'], item['name'], item['version']))
        index = ['FlussoniX third-party license and notice inventory', '',
                 'These dependencies retain their upstream terms. Separately installed media',
                 'libraries, GPU drivers and system libraries are not bundled by this inventory.', '']
        for package in packages:
            index.append(f"{package['ecosystem']}: {package['name']} {package['version']} "
                         f"({package['declared_license']})")
            index.extend('  third_party/' + item['path'] for item in package['files'])
            index.append('')
        notices = '\n'.join(index).encode()
        (stage / 'THIRD_PARTY_NOTICES.txt').write_bytes(notices)
        manifest = {'format': 1, 'targets': list(TARGETS),
                    'cargo_lock_sha256': digest(lock_data), 'web_lock_sha256': digest(web_data),
                    'cargo_manifest_sha256': digest((root / 'Cargo.toml').read_bytes()),
                    'web_manifest_sha256': digest((root / 'web/package.json').read_bytes()),
                    'vendor_quinn_manifest_sha256': digest(
                        (root / 'vendor/quinn-proto/Cargo.toml').read_bytes()),
                    'notices_sha256': digest(notices), 'packages': packages}
        (bundle / 'manifest.json').write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
        os.rename(stage, output)
    except BaseException:
        shutil.rmtree(stage)
        raise
    return {'packages': len(packages), 'rust': len(rust_packages),
            'web': len(packages) - len(rust_packages),
            'files': sum(len(item['files']) for item in packages)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--cargo-home', type=Path,
                        default=Path(os.environ.get('CARGO_HOME', Path.home() / '.cargo')))
    args = parser.parse_args()
    try:
        print(json.dumps(generate(args.root, args.output_dir, args.cargo_home), sort_keys=True))
    except (ValueError, OSError, subprocess.SubprocessError, KeyError) as error:
        parser.exit(1, f'Cannot generate notices: {error}\n')


if __name__ == '__main__':
    main()
