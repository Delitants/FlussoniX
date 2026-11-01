#!/usr/bin/env python3
"""Verify and start an actual packaged release on private loopback ports."""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import secrets
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request


MAX_ARCHIVE = 128 * 1024 * 1024
MAX_PAYLOAD = 256 * 1024 * 1024


def digest(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def verify_and_extract(archive, revision, destination):
    if not re.fullmatch(r'[0-9a-f]{40}', revision):
        raise ValueError('revision must be a full lowercase 40-character commit')
    info = archive.stat(follow_symlinks=False)
    if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_ARCHIVE:
        raise ValueError('archive must be a bounded regular file')
    expected = digest(archive) + '  ' + archive.name + '\n'
    sidecar = Path(str(archive) + '.sha256')
    sidecar_info = sidecar.stat(follow_symlinks=False)
    if not stat.S_ISREG(sidecar_info.st_mode) or sidecar_info.st_size > 256:
        raise ValueError('checksum sidecar must be a bounded regular file')
    if sidecar.read_text() != expected:
        raise ValueError('checksum mismatch')
    prefix = archive.name.removesuffix('.tar.gz')
    if not archive.name.endswith('.tar.gz') or not re.fullmatch(r'flussonix-[A-Za-z0-9_.-]+', prefix):
        raise ValueError('unexpected archive name')
    with tarfile.open(archive, 'r:gz') as tar:
        members = []
        payload_bytes = 0
        for member in tar:
            members.append(member)
            payload_bytes += member.size
            if len(members) > 1024 or payload_bytes > MAX_PAYLOAD:
                raise ValueError('archive exceeds extraction limit')
        by_name = {}
        for member in members:
            parts = member.name.split('/')
            if (not parts or parts[0] != prefix or any(p in ('', '.', '..') for p in parts)
                    or not (member.isfile() or member.isdir())
                    or member.mode & ~0o777 or member.name in by_name):
                raise ValueError('unsafe archive member')
            by_name[member.name] = member
        manifest_name = prefix + '/manifest.json'
        manifest_member = by_name.get(manifest_name)
        if manifest_member is None or not manifest_member.isfile() or manifest_member.size > 1024 * 1024:
            raise ValueError('missing bounded manifest')
        manifest = json.load(tar.extractfile(manifest_member))
        if (not isinstance(manifest, dict) or manifest.get('source_revision') != revision
                or manifest.get('format') != 1):
            raise ValueError('archive revision or manifest format mismatch')
        files = manifest.get('files')
        if not isinstance(files, dict) or set(files) != {
                name.removeprefix(prefix + '/') for name, member in by_name.items()
                if member.isfile() and name != manifest_name}:
            raise ValueError('archive files differ from manifest')
        for name, member in by_name.items():
            if not member.isfile():
                continue
            relative = PurePosixPath(name).relative_to(prefix)
            target = destination.joinpath(*relative.parts)
            data = tar.extractfile(member).read(member.size + 1)
            if len(data) != member.size:
                raise ValueError('archive member size mismatch')
            if name != manifest_name:
                row = files.get(str(relative))
                if (not isinstance(row, dict) or row.get('bytes') != len(data)
                        or row.get('sha256') != hashlib.sha256(data).hexdigest()
                        or row.get('mode') != oct(member.mode)):
                    raise ValueError('archive payload differs from manifest')
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open('xb') as output:
                output.write(data)
            target.chmod(member.mode)
    if not (destination / 'bin/flussonix').is_file() or not (destination / 'web/index.html').is_file():
        raise ValueError('archive lacks daemon or admin UI')
    return manifest


def smoke(destination):
    state = destination.parent / 'state'
    state.mkdir(mode=0o700)
    password = secrets.token_hex(24)
    env = dict(os.environ, FLUSSONIX_ADMIN_USER='admin',
               FLUSSONIX_ADMIN_PASSWORD=password,
               FLUSSONIX_PEER_KEY=secrets.token_hex(32),
               FLUSSONIX_SRT_PLAY_PASSPHRASE=secrets.token_hex(24))
    command = [str(destination / 'bin/flussonix'), '--listen', '127.0.0.1:0',
               '--rtsp-listen', '127.0.0.1:0', '--srt-play-listen', '127.0.0.1:0',
               '--config', str(state / 'config.json'), '--media-dir', str(state / 'media'),
               '--web-dir', str(destination / 'web'), '--ffmpeg', '/usr/bin/ffmpeg']
    process = None
    try:
        with (state / 'daemon.log').open('wb') as log:
            process = subprocess.Popen(command, cwd=state, env=env,
                                       stdout=log, stderr=subprocess.STDOUT,
                                       start_new_session=True)
            startup = None
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline and process.poll() is None:
                try:
                    startup = json.loads((state / 'daemon.log').read_text().splitlines()[0])
                    break
                except (FileNotFoundError, IndexError, json.JSONDecodeError):
                    time.sleep(0.1)
            if startup is None:
                raise RuntimeError('packaged daemon did not start')
            if (startup.get('srt_play_encrypted') is not True
                    or not str(startup.get('rtsp_listen', '')).startswith('127.0.0.1:')
                    or not str(startup.get('srt_play_listen', '')).startswith('127.0.0.1:')):
                raise RuntimeError('packaged optional listeners did not bind')
            http = 'http://' + startup['listen']
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            for _ in range(50):
                if process.poll() is not None:
                    raise RuntimeError('packaged daemon exited during health check')
                try:
                    with opener.open(http + '/health', timeout=1) as response:
                        if response.status == 200:
                            break
                except OSError:
                    pass
                time.sleep(0.1)
            else:
                raise RuntimeError('packaged daemon health failed')
            with opener.open(http + '/admin/', timeout=2) as response:
                body = response.read(1024 * 1024 + 1)
                if (response.status != 200 or len(body) > 1024 * 1024
                        or b'<html' not in body.lower()):
                    raise RuntimeError('packaged admin UI failed')
            if process.poll() is not None:
                raise RuntimeError('packaged daemon exited after admin request')
            return {'health': 200, 'admin': 200, 'rtsp': True, 'srt_encrypted': True}
    finally:
        if process is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive', type=Path, required=True)
    parser.add_argument('--revision', required=True)
    args = parser.parse_args()
    try:
        with tempfile.TemporaryDirectory(prefix='flussonix-release-smoke-') as temp:
            extracted = Path(temp) / 'package'
            extracted.mkdir(mode=0o700)
            manifest = verify_and_extract(args.archive, args.revision, extracted)
            checks = smoke(extracted)
            print(json.dumps({'status': 'ok', 'revision': args.revision,
                              'version': manifest['version'],
                              'binary_sha256': digest(extracted / 'bin/flussonix'), **checks}))
    except (OSError, ValueError, KeyError, RuntimeError, tarfile.TarError) as error:
        print(f'release archive smoke failed: {error}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
