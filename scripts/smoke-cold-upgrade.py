#!/usr/bin/env python3
"""Exercise a cold upgrade and rollback between two extracted Linux packages."""

import argparse
import base64
from contextlib import contextmanager
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        raise RuntimeError('smoke request redirected outside its local endpoint')


def package(path):
    root = Path(path).resolve(strict=True)
    for relative in ('bin/flussonix', 'web/index.html'):
        item = root / relative
        if item.is_symlink() or not item.is_file():
            raise ValueError(f'{root}: missing regular {relative}')
    return root


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


class Lab:
    def __init__(self, directory, media=False):
        self.root = directory
        self.state = directory / 'state'
        self.state.mkdir(mode=0o700)
        with socket.socket() as reserved:
            reserved.bind(('127.0.0.1', 0))
            self.port = reserved.getsockname()[1]
        password = secrets.token_hex(24)
        self.token = secrets.token_urlsafe(24)
        self.auth = 'Basic ' + base64.b64encode(f'admin:{password}'.encode()).decode()
        self.env = {'PATH': os.defpath, 'LANG': 'C', 'HOME': str(directory),
                    'TMPDIR': str(directory), 'FLUSSONIX_ADMIN_USER': 'admin',
                    'FLUSSONIX_ADMIN_PASSWORD': password,
                    'FLUSSONIX_PEER_KEY': secrets.token_hex(32)}
        self.opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}), NoRedirect())
        self.ffmpeg = shutil.which('ffmpeg', path=os.defpath) if media else None
        self.ffprobe = shutil.which('ffprobe', path=os.defpath) if media else None
        if media and (not self.ffmpeg or not self.ffprobe):
            raise RuntimeError('media smoke requires independent FFmpeg and FFprobe')

    def request(self, path, method='GET', body=None, authenticated=False):
        headers = {'Authorization': self.auth} if authenticated else {}
        data = None if body is None else json.dumps(body).encode()
        if data is not None:
            headers['Content-Type'] = 'application/json'
        request = urllib.request.Request(
            f'http://127.0.0.1:{self.port}{path}', data=data, headers=headers,
            method=method)
        with self.opener.open(request, timeout=5) as response:
            payload = response.read(1_000_001)
            if len(payload) > 1_000_000:
                raise ValueError('smoke response exceeds 1 MiB')
            if response.headers.get_content_type() == 'application/json' and payload:
                payload = json.loads(payload)
            return response.status, payload

    @contextmanager
    def daemon(self, release, phase):
        with (self.root / f'{phase}.log').open('wb') as output:
            process = subprocess.Popen([
                str(release / 'bin/flussonix'), '--listen', f'127.0.0.1:{self.port}',
                '--config', str(self.state / 'config.json'),
                '--media-dir', str(self.state / 'media'),
                '--web-dir', str(release / 'web'),
                '--ffmpeg', self.ffmpeg or '/owned-cold-upgrade-no-media-process'],
                cwd=release, env=self.env, stdout=output, stderr=subprocess.STDOUT,
                start_new_session=True)
            try:
                for _ in range(100):
                    if process.poll() is not None:
                        raise RuntimeError(f'{phase}: daemon exited before readiness')
                    try:
                        if self.request('/health')[0] == 200:
                            break
                    except (urllib.error.URLError, TimeoutError):
                        time.sleep(0.1)
                else:
                    raise RuntimeError(f'{phase}: health deadline exceeded')
                yield
            finally:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait(timeout=8)

    def saved_config(self, create=False):
        prefix = '/streamer/api/v3'
        if create:
            require(self.request(f'{prefix}/templates/upgrade_smoke', 'PUT',
                                 {'static': False}, True)[0] in (200, 201),
                    'template save failed')
            require(self.request(f'{prefix}/streams/upgrade_smoke', 'PUT',
                                 {'inputs': [{'url': 'testsrc://'}], 'static': False,
                                  'title': 'Upgrade smoke'}, True)[0] in (200, 201),
                    'stream save failed')
        _, template = self.request(f'{prefix}/templates/upgrade_smoke', authenticated=True)
        _, stream = self.request(f'{prefix}/streams/upgrade_smoke', authenticated=True)
        require(template['name'] == stream['name'] == 'upgrade_smoke',
                'saved identity changed')
        require(stream['inputs'][0]['url'] == 'testsrc://', 'saved input changed')
        require(stream['title'] == 'Upgrade smoke', 'saved title changed')
        require(self.request('/admin/')[0] == 200, 'packaged admin entry point failed')

    def subscriber(self, create=False):
        prefix = '/streamer/api/v3/iptv'
        if create:
            require(self.request(prefix, 'PUT', {}, True)[0] == 200,
                    'IPTV enable failed')
            require(self.request(f'{prefix}/packages/upgrade', 'PUT',
                                 {'channels': ['upgrade_smoke']}, True)[0] == 200,
                    'IPTV package save failed')
            require(self.request(f'{prefix}/subscribers/upgrade_user', 'PUT',
                                 {'token': self.token, 'packages': ['upgrade']}, True)[0] == 200,
                    'IPTV subscriber save failed')
        _, pack = self.request(f'{prefix}/packages/upgrade', authenticated=True)
        _, user = self.request(f'{prefix}/subscribers/upgrade_user', authenticated=True)
        require(pack['channels'] == ['upgrade_smoke'], 'IPTV package changed')
        require(user['packages'] == ['upgrade'] and user['token'] == self.token,
                'IPTV subscriber changed')

    def media_probe(self):
        deadline = time.monotonic() + 20
        manifest = None
        while time.monotonic() < deadline:
            try:
                _, body = self.request('/upgrade_smoke/index.m3u8')
                if b'#EXTM3U' in body and b'#EXTINF' in body:
                    manifest = body.decode('utf-8')
                    break
            except urllib.error.HTTPError as error:
                if error.code not in (404, 425, 503):
                    raise
            time.sleep(0.2)
        require(manifest is not None, 'test stream produced no HLS segment')
        segments = [line.strip() for line in manifest.splitlines()
                    if line.strip() and not line.startswith('#')]
        require(bool(segments), 'HLS playlist has no media URI')
        url = urllib.parse.urlsplit(urllib.parse.urljoin(
            f'http://127.0.0.1:{self.port}/upgrade_smoke/index.m3u8', segments[-1]))
        require(url.hostname == '127.0.0.1' and url.port == self.port
                and url.path.startswith('/upgrade_smoke/') and not url.fragment,
                'HLS segment escaped owned loopback stream')
        _, segment = self.request(url.path + ('?' + url.query if url.query else ''))
        require(isinstance(segment, bytes) and len(segment) >= 1880,
                'HLS segment is missing or too short')
        probe = subprocess.run([
            self.ffprobe, '-v', 'error', '-show_entries', 'stream=codec_name',
            '-of', 'json', '-i', 'pipe:0'], input=segment, capture_output=True,
            timeout=8, check=True, env=self.env)
        streams = json.loads(probe.stdout)['streams']
        require(any(stream.get('codec_name') == 'h264' for stream in streams),
                'packaged stream did not decode as H.264')


def smoke(previous, current, media=False):
    with tempfile.TemporaryDirectory(prefix='flussonix-cold-upgrade-') as name:
        lab = Lab(Path(name), media)
        with lab.daemon(previous, 'previous-first'):
            lab.saved_config(create=True)
            if media:
                lab.media_probe()
        config = (lab.state / 'config.json').read_bytes()
        with lab.daemon(current, 'current-first'):
            lab.saved_config()
            lab.subscriber(create=True)
            if media:
                lab.media_probe()
        iptv = lab.state / 'iptv.json'
        iptv_bytes = iptv.read_bytes()
        require((lab.state / 'config.json').read_bytes() == config,
                'current read changed saved configuration')
        require((lab.state / 'config.json').stat().st_mode & 0o077 == 0,
                'saved configuration is not owner-only')
        require(iptv.stat().st_mode & 0o077 == 0, 'IPTV data is not owner-only')
        with lab.daemon(previous, 'previous-rollback'):
            lab.saved_config()
            if media:
                lab.media_probe()
        require((lab.state / 'config.json').read_bytes() == config,
                'rollback changed saved configuration')
        require(iptv.read_bytes() == iptv_bytes, 'rollback changed IPTV data')
        with lab.daemon(current, 'current-return'):
            lab.saved_config()
            lab.subscriber()
            if media:
                lab.media_probe()
        require((lab.state / 'config.json').read_bytes() == config,
                'return to current changed saved configuration')
        require(iptv.read_bytes() == iptv_bytes, 'return to current changed IPTV data')
    print('cold upgrade and rollback passed; temporary state and daemons removed')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--previous', required=True, type=Path)
    parser.add_argument('--current', required=True, type=Path)
    parser.add_argument('--media', action='store_true',
                        help='also decode packaged HLS output with independent FFprobe')
    args = parser.parse_args()
    smoke(package(args.previous), package(args.current), args.media)


if __name__ == '__main__':
    main()
