#!/usr/bin/env python3
"""Read-only live Flussonic source playback through an isolated local daemon."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
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
    parser.add_argument("--topology", action="store_true", help="also qualify local LB to CDN placement")
    parser.add_argument("--output", choices=("hls", "m4s", "m4f"), default="hls")
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
    if args.output != "hls" and not args.topology:
        parser.error("native output qualification currently requires --topology")
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


def inspect_media(root, media):
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
    return streams, bool(streams) and decode.returncode == 0


def m4s_boxes(data):
    boxes = []
    while data:
        if len(data) < 8:
            raise RuntimeError("truncated M4S box")
        size = int.from_bytes(data[:4], "big")
        if size < 8 or size > len(data):
            raise RuntimeError("invalid M4S box length")
        boxes.append((data[4:8], data[8:size]))
        data = data[size:]
    return boxes


def inspect_m4s_stream(response, deadline):
    buffer = bytearray()
    tracks = []
    media_records = 0
    received = 0
    while time.monotonic() < deadline and received < 8_000_000:
        chunk = response.read(4096)
        if not chunk:
            break
        received += len(chunk)
        buffer.extend(chunk)
        while len(buffer) >= 4:
            size = int.from_bytes(buffer[:4], "big")
            if size < 8 or size > 16 * 1024 * 1024:
                raise RuntimeError("invalid native M4S record length")
            if len(buffer) < size + 4:
                break
            record = bytes(buffer[4:size + 4])
            del buffer[:size + 4]
            boxes = m4s_boxes(record)
            if len(boxes) != 1:
                raise RuntimeError("native M4S record has multiple boxes")
            kind, body = boxes[0]
            if kind == b"MDin":
                tracks = []
                for subkind, track in m4s_boxes(body):
                    if subkind != b"trak":
                        continue
                    handlers = [data for field, data in m4s_boxes(track) if field == b"hdlr"]
                    if len(handlers) != 1 or len(handlers[0]) < 16 or not handlers[0].endswith(b"\0"):
                        raise RuntimeError("invalid native M4S track handler")
                    tracks.append(handlers[0][12:-1].decode("ascii"))
            elif kind == b"Fgop":
                fields = dict(m4s_boxes(body))
                if b"goph" not in fields or not fields.get(b"body"):
                    raise RuntimeError("native M4S GOP is missing header or media")
                media_records += 1
            elif kind == b"FRam":
                fields = dict(m4s_boxes(body))
                if len(fields.get(b"fhdr", b"")) < 32 or not fields.get(b"body"):
                    raise RuntimeError("native M4S frame is missing header or media")
                media_records += 1
            if tracks and media_records:
                return tracks, media_records, received
    raise RuntimeError("native M4S metadata and media did not arrive within the probe limit")


def inspect_m4f_segment(media):
    fields = dict(m4s_boxes(media))
    if b"moov" not in fields or not fields.get(b"mdat"):
        raise RuntimeError("native M4F segment lacks movie metadata or media")
    tracks = []
    for kind, body in m4s_boxes(fields[b"moov"]):
        if kind != b"trak":
            continue
        handlers = [data for field, data in m4s_boxes(body) if field == b"hdlr"]
        if len(handlers) != 1 or len(handlers[0]) < 16 or not handlers[0].endswith(b"\0"):
            raise RuntimeError("native M4F track handler is invalid")
        tracks.append(handlers[0][12:-1].decode("ascii"))
    if not tracks:
        raise RuntimeError("native M4F segment has no tracks")
    return tracks


def parse_m4f_announcement(line):
    parts = line.split()
    if len(parts) != 2 or not parts[0].isascii() or not parts[0].isdigit():
        return None
    match = re.fullmatch(r"(\d{4}/\d{2}/\d{2}/\d{2}/\d{2}/\d{2})-\d+", parts[1])
    return match.group(1) if match else None


def cdn_worker_snapshot(node, stream):
    """Return only operational fields for one owned CDN worker, never its config."""
    for item in node.get("streams", []) if isinstance(node, dict) else []:
        if not isinstance(item, dict) or item.get("name") != stream:
            continue
        stats = item.get("stats")
        stats = stats if isinstance(stats, dict) else {}
        return {"ready": item.get("ready") if isinstance(item.get("ready"), bool) else None,
                **{field: stats.get(field) for field in (
                    "status", "bytes_in", "restart_count", "retry_in_ms",
                    "media_age_ms", "last_error")}}
    return {"present": False}


def private_input_failure(log):
    """Read only fixed, credential-free failure fields from the owned CDN log."""
    try:
        with log.open("rb") as stream:
            stream.seek(0, os.SEEK_END)
            stream.seek(max(0, stream.tell() - 65_536))
            lines = stream.read(65_536).decode("utf-8", "replace").splitlines()
    except OSError:
        return None
    fields = {
        "phase": set(("control_request", "control_response", "control_read", "signal_decode",
                      "segment_request", "segment_response", "segment_read", "segment_decode",
                      "m4s_decode", "bridge_metadata", "bridge_frame", "output_write", "relay")),
        "failure": {"Invalid", "Transient", "Rejected", "Local", "Revoked"},
        "last_private_ack": {"true", "false"},
        "metadata_sent": {"true", "false"},
    }
    for line in reversed(lines):
        if ("flussonix::private_input" not in line
                or "private media input failed" not in line
                or not re.search(r'\bprotocol=(?:"m4f"|m4f)(?:\s|$)', line)):
            continue
        result = {}
        for name, allowed in fields.items():
            match = re.search(r'\b' + name + r'=(?:"([^"]+)"|([A-Za-z_]+))', line)
            value = (match.group(1) or match.group(2)) if match else None
            if value not in allowed:
                break
            result[name] = value
        else:
            status = re.search(r'\blast_http_status=(\d{1,3})(?:\s|$)', line)
            if status and int(status.group(1)) <= 599:
                result["last_http_status"] = int(status.group(1))
                for name in ("last_private_ack", "metadata_sent"):
                    result[name] = result[name] == "true"
                return result
    return None


def worker_lifecycle_events(log):
    """Expose only fixed source/packager stop codes from the owned CDN log."""
    try:
        with log.open("rb") as stream:
            stream.seek(0, os.SEEK_END)
            stream.seek(max(0, stream.tell() - 65_536))
            lines = stream.read(65_536).decode("utf-8", "replace").splitlines()
    except OSError:
        return []
    events = []
    for line in lines:
        if "flussonix::worker_lifecycle" not in line:
            continue
        if "shared input stopped before worker cancellation" in line:
            reason = re.search(r'\breason=(Authority|Closed|Input|Unavailable|Mismatch|Limit|Lagged)(?=\(|\s|$)', line)
            if reason:
                events.append({"event": "shared_input_stopped", "reason": reason.group(1)})
        elif "packager stdout closed before worker cancellation" in line:
            count = re.search(r'\bbytes_in=(\d{1,20})(?:\s|$)', line)
            if count and int(count.group(1)) <= 2**64 - 1:
                events.append({"event": "packager_stdout_closed", "bytes_in": int(count.group(1))})
        elif "worker failure recorded" in line:
            site = re.search(r'\bsite=(\d{1,4})(?:\s|$)', line)
            reason = re.search(r'\breason=(?:"([^"]+)"|([A-Za-z_]+))', line)
            value = (reason.group(1) or reason.group(2)) if reason else None
            if site and value in {"input_closed", "startup_timeout", "input_stalled",
                                  "packaging_failed", "wire_decode_failed"}:
                events.append({"event": "worker_failure", "site": int(site.group(1)),
                               "reason": value})
    return events[-8:]


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
            streams, passed = inspect_media(root, media)
            return {
                "result": "media" if passed else "decode-failed",
                "scheme": urllib.parse.urlsplit(args.source).scheme,
                "stream": args.stream,
                "statuses": statuses[-5:],
                "unauthenticated_status": unauthenticated_status,
                "segment_bytes": len(media),
                "codecs": streams,
                "decode_ok": passed,
            }, 0 if passed else 2
        finally:
            try:
                os.killpg(daemon.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            daemon.wait(timeout=5)
            daemon.stdout.close()


def topology_probe(args, root):
    cluster_key = args.cluster_key_file.read_text(encoding="utf-8").strip()
    if not cluster_key:
        raise RuntimeError("Cluster key file is empty")
    viewer_token = secrets.token_urlsafe(24)
    peer_key = secrets.token_urlsafe(24)
    source = {
        "url": args.source,
        "flussonix_source_kind": "flussonic",
        "cluster_key": cluster_key,
        "flussonix_token_sha256": hashlib.sha256(viewer_token.encode()).hexdigest(),
        "only": [],
    }
    children = []
    logs = []

    def start(role, name, peers):
        config = root / f"{name}-config.json"
        config.write_text(
            json.dumps({"streams": [], "templates": [], "peers": peers, "sources": [source], "auth_backends": []}),
            encoding="utf-8",
        )
        config.chmod(0o600)
        log = (root / f"{name}.log").open("wb")
        logs.append(log)
        env = {key: value for key, value in os.environ.items() if not key.startswith("FLUSSONIX_")}
        env.update(FLUSSONIX_ADMIN_PASSWORD="owned-vendor-probe-admin", FLUSSONIX_PEER_KEY=peer_key)
        if args.output == "m4f":
            env["RUST_LOG"] = "warn,flussonix::private_input=debug,flussonix::worker_lifecycle=debug"
        daemon = subprocess.Popen(
            [
                str(args.binary),
                "--listen", "127.0.0.1:0",
                "--role", role,
                "--node-name", name,
                "--config", str(config),
                "--media-dir", str(root / f"{name}-media"),
                "--srt-play-listen", "127.0.0.1:0",
                "--rtsp-listen", "127.0.0.1:0",
            ],
            stdout=subprocess.PIPE,
            stderr=log,
            env=env,
            start_new_session=True,
        )
        children.append(daemon)
        ready = read_ready(daemon.stdout, 12)
        return "http://" + ready["listen"]

    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def get(url, limit=512_000, headers=None):
        request = urllib.request.Request(url, headers=headers or {})
        try:
            with opener.open(request, timeout=12) as response:
                status, response_headers, body = response.status, response.headers, response.read(limit + 1)
        except urllib.error.HTTPError as error:
            status, response_headers, body = error.code, error.headers, error.read(limit + 1)
        if len(body) > limit:
            raise RuntimeError("local topology response exceeds the probe limit")
        return status, response_headers, body

    def same_origin(url, base):
        target = urllib.parse.urlsplit(url)
        expected = urllib.parse.urlsplit(base)
        return (target.scheme, target.hostname, target.port) == (
            expected.scheme,
            expected.hostname,
            expected.port,
        )

    try:
        cdn = start("cdn", "owned-vendor-cdn", [])
        cdn_root = cdn + "/"
        lb = start(
            "lb",
            "owned-vendor-lb",
            [{"hostname": "owned-vendor-cdn", "api_url": cdn_root,
              "public_payload_url": cdn_root, "private_payload_url": cdn_root}],
        )
        path = urllib.parse.quote(args.stream, safe="/")
        suffix = args.output if args.output != "hls" else "index.m3u8"
        entry = f"{lb}/{path}/{suffix}?token={viewer_token}"
        deadline = time.monotonic() + args.timeout
        statuses = []
        placement = None
        while time.monotonic() < deadline:
            status, headers, _ = get(entry)
            statuses.append(status)
            if status == 302:
                placement = urllib.parse.urljoin(entry, headers["location"])
                break
            time.sleep(0.3)
        if placement is None:
            return {"result": "no-placement", "scheme": urllib.parse.urlsplit(args.source).scheme,
                    "statuses": statuses[-12:]}, 2
        if not same_origin(placement, cdn) or "flussonix_ticket" not in urllib.parse.parse_qs(urllib.parse.urlsplit(placement).query):
            raise RuntimeError("LB placement did not target the owned CDN with a ticket")
        status, headers, _ = get(placement)
        if status != 302:
            raise RuntimeError("CDN did not redeem the placement ticket")
        clean = urllib.parse.urljoin(placement, headers["location"])
        if not same_origin(clean, cdn) or "flussonix_ticket" in urllib.parse.parse_qs(urllib.parse.urlsplit(clean).query):
            raise RuntimeError("CDN clean playback URL is invalid")

        def worker_snapshot():
            try:
                node_status, _, node_body = get(
                    cdn + "/flussonix/api/v1/node",
                    headers={"x-flussonix-peer": peer_key},
                )
                return (cdn_worker_snapshot(json.loads(node_body), args.stream)
                        if node_status == 200 else {"node_status": node_status})
            except (OSError, ValueError, RuntimeError, KeyError):
                return {"node_status": "unavailable"}

        media_bytes = 0
        native_records = 0
        streams = []
        media_ok = False
        m4f_controls = []
        m4f_events = []
        m4f_announcements = 0
        m4f_segments = []
        m4f_lines = 0
        m4f_line_shapes = []
        if args.output == "m4s":
            while time.monotonic() < deadline and not media_ok:
                try:
                    with opener.open(clean, timeout=12) as response:
                        if response.status == 200:
                            streams, native_records, media_bytes = inspect_m4s_stream(response, deadline)
                            media_ok = bool(streams) and native_records > 0
                except urllib.error.HTTPError as error:
                    if error.code != 503:
                        raise RuntimeError("CDN native playback failed") from None
                time.sleep(0.25)
        elif args.output == "m4f":
            while time.monotonic() < deadline and not media_ok:
                event = None
                try:
                    with opener.open(clean, timeout=20) as response:
                        m4f_controls.append(response.status)
                        if response.status == 200:
                            control_started = time.monotonic()
                            event = {"first_line_ms": None, "outcome": "deadline", "lines": 0,
                                     "duration_ms": None}
                            m4f_events.append(event)
                            while time.monotonic() < deadline:
                                raw_line = response.readline(512)
                                if not raw_line:
                                    event["outcome"] = "eof"
                                    event["worker"] = worker_snapshot()
                                    source_failure = private_input_failure(root / "owned-vendor-cdn.log")
                                    if source_failure is not None:
                                        event["source_failure"] = source_failure
                                    lifecycle = worker_lifecycle_events(root / "owned-vendor-cdn.log")
                                    if lifecycle:
                                        event["lifecycle"] = lifecycle
                                    break
                                if event["first_line_ms"] is None:
                                    event["first_line_ms"] = round((time.monotonic() - control_started) * 1000)
                                event["lines"] += 1
                                m4f_lines += 1
                                line_text = raw_line.decode("ascii", "replace").strip()
                                line = line_text.split()
                                if len(m4f_line_shapes) < 3:
                                    m4f_line_shapes.append({"bytes": len(raw_line),
                                                            "words": len(line),
                                                            "first": "numeric" if line and line[0].isascii() and line[0].isdigit() else "other",
                                                            "first_bytes": len(line[0]) if line else 0})
                                # A native M4F relay preserves the upstream sequence; it need not start at 1.
                                segment_time = parse_m4f_announcement(line_text)
                                if segment_time is None:
                                    continue
                                m4f_announcements += 1
                                segment = f"{cdn}/{path}/{segment_time}.m4f?token={viewer_token}"
                                event["outcome"] = "segment_request"
                                status, _, body = get(segment, 20_000_000)
                                m4f_segments.append(status)
                                event["outcome"] = "segment_status"
                                if status == 200 and body:
                                    streams = inspect_m4f_segment(body)
                                    media_bytes = len(body)
                                    native_records = 1
                                    media_ok = True
                                    event["outcome"] = "media"
                                    break
                            event["duration_ms"] = round((time.monotonic() - control_started) * 1000)
                except urllib.error.HTTPError as error:
                    m4f_controls.append(error.code)
                    if error.code != 503:
                        raise RuntimeError("CDN native playback failed") from None
                except TimeoutError:
                    m4f_controls.append("timeout")
                    if event is not None:
                        event["outcome"] = ("segment_timeout" if event["outcome"] == "segment_request"
                                            else "read_timeout")
                        event["duration_ms"] = round((time.monotonic() - control_started) * 1000)
                time.sleep(0.25)
        else:
            media = None
            while time.monotonic() < deadline and media is None:
                status, _, body = get(clean)
                if status == 200:
                    segments = [
                        line for line in body.decode("utf-8", "replace").splitlines()
                        if line and not line.startswith("#")
                    ]
                    if segments:
                        segment = urllib.parse.urljoin(clean, segments[0])
                        if not same_origin(segment, cdn):
                            raise RuntimeError("CDN playlist escaped the owned endpoint")
                        status, _, body = get(segment, 20_000_000)
                        if status == 200 and body:
                            media = body
                time.sleep(0.25)
            if media is not None:
                media_bytes = len(media)
                streams, media_ok = inspect_media(root, media)
        if not media_ok:
            worker = worker_snapshot()
            return {"result": "no-cdn-media", "scheme": urllib.parse.urlsplit(args.source).scheme,
                    "output": args.output, "lb_statuses": statuses[-5:],
                    "m4f_controls": m4f_controls[-8:], "m4f_announcements": m4f_announcements,
                    "m4f_segments": m4f_segments[-8:], "m4f_lines": m4f_lines,
                    "m4f_line_shapes": m4f_line_shapes, "m4f_events": m4f_events[-8:],
                    "cdn_worker": worker}, 2
        unauthorized_lb = get(f"{lb}/{path}/{suffix}")[0]
        unauthorized_cdn = get(f"{cdn}/{path}/{suffix}")[0]
        node_status, _, node_body = get(lb + "/flussonix/api/v1/node", headers={"x-flussonix-peer": peer_key})
        node = json.loads(node_body) if node_status == 200 else {}
        lb_streams = node.get("streams")
        passed = media_ok and unauthorized_lb == 403 and unauthorized_cdn == 403 and lb_streams == []
        summary = {
            "result": "placed-media" if passed else "topology-failed",
            "scheme": urllib.parse.urlsplit(args.source).scheme,
            "stream": args.stream,
            "output": args.output,
            "lb_statuses": statuses[-5:],
            "media_bytes": media_bytes,
            "codecs": streams,
            "media_check": (
                "m4s_records" if args.output == "m4s"
                else "m4f_boxes" if args.output == "m4f"
                else "ffmpeg_decode"
            ),
            "native_records": native_records,
            "unauthenticated_lb": unauthorized_lb,
            "unauthenticated_cdn": unauthorized_cdn,
            "lb_node_streams": len(lb_streams) if isinstance(lb_streams, list) else None,
        }
        if args.output == "m4f":
            summary.update(m4f_controls=m4f_controls[-8:],
                           m4f_announcements=m4f_announcements,
                           m4f_segments=m4f_segments[-8:],
                           m4f_lines=m4f_lines,
                           m4f_line_shapes=m4f_line_shapes,
                           m4f_events=m4f_events[-8:])
        return summary, 0 if passed else 2
    finally:
        for daemon in reversed(children):
            try:
                os.killpg(daemon.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            daemon.wait(timeout=5)
            daemon.stdout.close()
        for log in logs:
            log.close()


def main():
    args = arguments()
    os.umask(0o077)
    root = Path(tempfile.mkdtemp(prefix="flussonix-vendor-live-", dir="/tmp"))
    try:
        result, status = topology_probe(args, root) if args.topology else probe(args, root)
        print(json.dumps(result, sort_keys=True))
        return status
    except (OSError, RuntimeError, ValueError, KeyError, subprocess.TimeoutExpired) as error:
        print(json.dumps({"result": "probe-error", "type": type(error).__name__}))
        return 2
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
