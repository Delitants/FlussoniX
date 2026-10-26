#!/usr/bin/env python3
"""Read-only live Flussonic source playback through an isolated local daemon."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import select
import secrets
import shutil
import signal
import stat
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, newurl):
        return None


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--source", required=True)
    parser.add_argument("--stream", required=True)
    parser.add_argument("--cluster-key-file", required=True, type=Path)
    parser.add_argument("--timeout", type=int, default=45)
    args = parser.parse_args()
    source = urllib.parse.urlsplit(args.source)
    if (
        source.scheme not in {"m4s", "m4ss", "m4f", "m4fs"}
        or not source.hostname
        or source.username
        or source.password
        or source.query
        or source.fragment
    ):
        parser.error("source must be an M4S/M4SS/M4F/M4FS URL without credentials or query")
    if not args.binary.is_absolute() or not os.access(args.binary, os.X_OK):
        parser.error("binary must be an executable absolute path")
    if args.timeout < 10 or args.timeout > 120:
        parser.error("timeout must be 10–120 seconds")
    if not args.stream or "?" in args.stream or "#" in args.stream:
        parser.error("stream must be a path without query or fragment")
    try:
        key_stat = args.cluster_key_file.stat()
    except OSError:
        parser.error("Cluster key file is unavailable")
    if not stat.S_ISREG(key_stat.st_mode) or key_stat.st_mode & 0o077:
        parser.error("Cluster key file must be regular and owner-only")
    return args


def local_get(opener, url, base, limit):
    target = urllib.parse.urlsplit(url)
    root = urllib.parse.urlsplit(base)
    if (target.scheme, target.hostname, target.port) != (
        root.scheme,
        root.hostname,
        root.port,
    ):
        raise RuntimeError("playlist redirected outside the isolated daemon")
    with opener.open(url, timeout=12) as response:
        data = response.read(limit + 1)
        if len(data) > limit:
            raise RuntimeError("local media response exceeds the probe limit")
        return response.status, data


def read_ready(stream, timeout):
    deadline = time.monotonic() + timeout
    data = bytearray()
    while time.monotonic() < deadline:
        if not select.select([stream], [], [], max(0, deadline - time.monotonic()))[0]:
            break
        chunk = os.read(stream.fileno(), 4096)
        if not chunk:
            break
        data.extend(chunk)
        if len(data) > 65_536:
            raise RuntimeError("local daemon readiness exceeds the probe limit")
        if b"\n" in data:
            return json.loads(data.split(b"\n", 1)[0])
    raise RuntimeError("local daemon did not report readiness within 12 seconds")


def probe(args, root):
    cluster_key = args.cluster_key_file.read_text(encoding="utf-8").strip()
    if not cluster_key:
        raise RuntimeError("Cluster key file is empty")
    viewer_token = secrets.token_urlsafe(24)
    config = {
        "streams": [],
        "templates": [],
        "peers": [],
        "sources": [
            {
                "url": args.source,
                "flussonix_source_kind": "flussonic",
                "cluster_key": cluster_key,
                "flussonix_token_sha256": hashlib.sha256(viewer_token.encode()).hexdigest(),
                "only": [],
            }
        ],
        "auth_backends": [],
    }
    config_path = root / "config.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    config_path.chmod(0o600)
    env = {key: value for key, value in os.environ.items() if not key.startswith("FLUSSONIX_")}
    env.update(
        FLUSSONIX_ADMIN_PASSWORD="owned-vendor-probe-admin",
        FLUSSONIX_PEER_KEY="owned-vendor-probe-peer-key",
    )
    with (root / "daemon.log").open("wb") as log:
        daemon = subprocess.Popen(
            [
                str(args.binary),
                "--listen", "127.0.0.1:0",
                "--role", "cdn",
                "--node-name", "owned-vendor-probe",
                "--config", str(config_path),
                "--media-dir", str(root / "media"),
                "--srt-play-listen", "127.0.0.1:0",
                "--rtsp-listen", "127.0.0.1:0",
            ],
            stdout=subprocess.PIPE,
            stderr=log,
            env=env,
            start_new_session=True,
        )
        try:
            ready = read_ready(daemon.stdout, 12)
            base = "http://" + ready["listen"]
            path = urllib.parse.quote(args.stream, safe="/")
            playlist_url = f"{base}/{path}/index.m3u8?token={viewer_token}"
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
            deadline = time.monotonic() + args.timeout
            statuses = []
            media = None
            while time.monotonic() < deadline and media is None:
                if daemon.poll() is not None:
                    raise RuntimeError("local daemon exited during playback")
                try:
                    status, body = local_get(opener, playlist_url, base, 512_000)
                    statuses.append(status)
                    segments = [
                        line for line in body.decode("utf-8", "replace").splitlines()
                        if line and not line.startswith("#")
                    ]
                    if segments:
                        segment_url = urllib.parse.urljoin(playlist_url, segments[0])
                        segment_status, body = local_get(opener, segment_url, base, 20_000_000)
                        if segment_status == 200 and body:
                            media = body
                except urllib.error.HTTPError as error:
                    statuses.append(error.code)
                time.sleep(0.25)
            if media is None:
                return {"result": "no-media", "scheme": urllib.parse.urlsplit(args.source).scheme, "statuses": statuses[-12:]}, 2
            try:
                local_get(opener, f"{base}/{path}/index.m3u8", base, 512_000)
            except urllib.error.HTTPError as error:
                if error.code not in (401, 403):
                    raise RuntimeError("unauthorized local viewer received unexpected status") from None
                unauthenticated_status = error.code
            else:
                raise RuntimeError("unauthorized local viewer was admitted")
            sample = root / "sample.ts"
            sample.write_bytes(media)
            inspect = subprocess.run(
                ["ffprobe", "-v", "error", "-show_entries", "stream=codec_type,codec_name,width,height", "-of", "json", str(sample)],
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
            decode = subprocess.run(
                ["ffmpeg", "-v", "error", "-xerror", "-i", str(sample), "-t", "2", "-f", "null", "-"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=25,
                check=False,
            )
            streams = json.loads(inspect.stdout).get("streams", []) if inspect.returncode == 0 else []
            passed = bool(streams) and decode.returncode == 0
            return {
                "result": "media" if passed else "decode-failed",
                "scheme": urllib.parse.urlsplit(args.source).scheme,
                "stream": args.stream,
                "statuses": statuses[-5:],
                "unauthenticated_status": unauthenticated_status,
                "segment_bytes": len(media),
                "codecs": streams,
                "decode_ok": decode.returncode == 0,
            }, 0 if passed else 2
        finally:
            try:
                os.killpg(daemon.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            daemon.wait(timeout=5)
            daemon.stdout.close()


def main():
    args = arguments()
    os.umask(0o077)
    root = Path(tempfile.mkdtemp(prefix="flussonix-vendor-live-", dir="/tmp"))
    try:
        result, status = probe(args, root)
        print(json.dumps(result, sort_keys=True))
        return status
    except (OSError, RuntimeError, ValueError, KeyError, subprocess.TimeoutExpired) as error:
        print(json.dumps({"result": "probe-error", "type": type(error).__name__}))
        return 2
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
