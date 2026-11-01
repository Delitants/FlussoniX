"""A cold upgrade must wait for media from the newly started worker."""

import importlib.util
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import patch
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'scripts/smoke-cold-upgrade.py'
SPEC = importlib.util.spec_from_file_location('cold_upgrade_smoke', SCRIPT)
SMOKE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SMOKE)


class ColdUpgradeMediaTests(unittest.TestCase):
    def _fast_deadline(self):
        ticks = iter(range(0, 100, 10))
        return patch.object(SMOKE, 'time', SimpleNamespace(
            monotonic=lambda: next(ticks), sleep=lambda _: None))

    @unittest.skipUnless(shutil.which('ffmpeg') and shutil.which('ffprobe'),
                         'independent FFmpeg and FFprobe are required')
    def test_codec_label_without_decoded_picture_is_rejected(self):
        media = subprocess.run([
            'ffmpeg', '-hide_banner', '-loglevel', 'error', '-nostdin',
            '-f', 'lavfi', '-i', 'testsrc2=size=160x90:rate=10',
            '-f', 'lavfi', '-i', 'sine=frequency=440:sample_rate=16000',
            '-t', '1', '-map', '0:v:0', '-map', '1:a:0', '-c:v', 'libx264',
            '-preset', 'ultrafast', '-g', '10', '-threads', '1', '-c:a', 'aac',
            '-f', 'mpegts', 'pipe:1'], capture_output=True, check=True,
            timeout=10).stdout[:24 * 188]

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == '/upgrade_smoke/index.m3u8':
                    body = ('#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:1,\n'
                            'g' + 'c' * 32 + '_1.ts\n').encode()
                elif self.path == '/upgrade_smoke/g' + 'c' * 32 + '_1.ts':
                    body = media
                else:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory(prefix='flussonix-upgrade-test-') as temp:
                lab = SMOKE.Lab(Path(temp), media=True)
                lab.port = server.server_address[1]
                with self.assertRaisesRegex(RuntimeError, 'decoded H.264 picture'):
                    lab.media_probe()
        finally:
            server.shutdown()
            thread.join(timeout=5)
            server.server_close()

    @unittest.skipUnless(shutil.which('ffmpeg') and shutil.which('ffprobe'),
                         'independent FFmpeg and FFprobe are required')
    def test_restarted_worker_must_serve_new_hls_generation(self):
        media = subprocess.run([
            'ffmpeg', '-hide_banner', '-loglevel', 'error', '-nostdin',
            '-f', 'lavfi', '-i', 'testsrc2=size=160x90:rate=10',
            '-f', 'lavfi', '-i', 'sine=frequency=440:sample_rate=16000',
            '-t', '1', '-map', '0:v:0', '-map', '1:a:0', '-c:v', 'libx264',
            '-preset', 'ultrafast', '-g', '10', '-threads', '1', '-c:a', 'aac',
            '-f', 'mpegts', 'pipe:1'], capture_output=True, check=True,
            timeout=10).stdout
        old = '/upgrade_smoke/g' + 'a' * 32 + '_1.ts'
        fresh = '/upgrade_smoke/g' + 'b' * 32 + '_2.ts'

        class Handler(BaseHTTPRequestHandler):
            manifest_hits = 0
            segment_hits = []

            def do_GET(self):
                if self.path == '/upgrade_smoke/index.m3u8':
                    type(self).manifest_hits += 1
                    segment = fresh if type(self).manifest_hits >= 4 else old
                    body = ('#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:1,\n'
                            + segment.rsplit('/', 1)[-1] + '\n').encode()
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(body)
                elif self.path in (old, fresh):
                    type(self).segment_hits.append(self.path)
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(media)
                else:
                    self.send_error(404)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory(prefix='flussonix-upgrade-test-') as temp:
                lab = SMOKE.Lab(Path(temp), media=True)
                lab.port = server.server_address[1]
                lab.media_probe()
                lab.media_probe()
                self.assertEqual(Handler.segment_hits, [old, fresh])
        finally:
            server.shutdown()
            thread.join(timeout=5)
            server.server_close()

    @unittest.skipUnless(shutil.which('ffmpeg') and shutil.which('ffprobe'),
                         'independent FFmpeg and FFprobe are required')
    def test_rollback_cannot_reuse_first_generation(self):
        media = subprocess.run([
            'ffmpeg', '-hide_banner', '-loglevel', 'error', '-nostdin',
            '-f', 'lavfi', '-i', 'testsrc2=size=160x90:rate=10',
            '-t', '1', '-c:v', 'libx264', '-preset', 'ultrafast',
            '-threads', '1', '-f', 'mpegts', 'pipe:1'],
            capture_output=True, check=True, timeout=10).stdout
        names = ['g' + letter * 32 + '_1.ts' for letter in 'aba']

        class Handler(BaseHTTPRequestHandler):
            manifest_hits = 0

            def do_GET(self):
                if self.path == '/upgrade_smoke/index.m3u8':
                    name = names[min(type(self).manifest_hits, 2)]
                    type(self).manifest_hits += 1
                    body = f'#EXTM3U\n#EXTINF:1,\n{name}\n'.encode()
                elif self.path.rsplit('/', 1)[-1] in names:
                    body = media
                else:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory(prefix='flussonix-upgrade-test-') as temp:
                lab = SMOKE.Lab(Path(temp), media=True)
                lab.port = server.server_address[1]
                lab.media_probe()
                lab.media_probe()
                with self._fast_deadline(), self.assertRaisesRegex(
                        RuntimeError, 'no fresh HLS generation'):
                    lab.media_probe()
        finally:
            server.shutdown()
            thread.join(timeout=5)
            server.server_close()

    @unittest.skipUnless(shutil.which('ffmpeg') and shutil.which('ffprobe'),
                         'independent FFmpeg and FFprobe are required')
    def test_mixed_generation_playlist_is_not_accepted(self):
        media = subprocess.run([
            'ffmpeg', '-hide_banner', '-loglevel', 'error', '-nostdin',
            '-f', 'lavfi', '-i', 'testsrc2=size=160x90:rate=10',
            '-t', '1', '-c:v', 'libx264', '-preset', 'ultrafast',
            '-threads', '1', '-f', 'mpegts', 'pipe:1'],
            capture_output=True, check=True, timeout=10).stdout
        old = 'g' + 'a' * 32 + '_1.ts'
        fresh = 'g' + 'b' * 32 + '_2.ts'

        class Handler(BaseHTTPRequestHandler):
            manifest_hits = 0

            def do_GET(self):
                if self.path == '/upgrade_smoke/index.m3u8':
                    type(self).manifest_hits += 1
                    names = [old] if type(self).manifest_hits == 1 else [old, fresh]
                    body = ('#EXTM3U\n' + ''.join(
                        f'#EXTINF:1,\n{name}\n' for name in names)).encode()
                elif self.path.rsplit('/', 1)[-1] in (old, fresh):
                    body = media
                else:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory(prefix='flussonix-upgrade-test-') as temp:
                lab = SMOKE.Lab(Path(temp), media=True)
                lab.port = server.server_address[1]
                lab.media_probe()
                with self._fast_deadline(), self.assertRaisesRegex(
                        RuntimeError, 'no fresh HLS generation'):
                    lab.media_probe()
        finally:
            server.shutdown()
            thread.join(timeout=5)
            server.server_close()


if __name__ == '__main__':
    unittest.main()
