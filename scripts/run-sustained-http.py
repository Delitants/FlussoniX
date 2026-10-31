#!/usr/bin/env python3
"""Finite, loopback-only FlussoniX sustained MPEG-TS delivery qualification."""

import argparse
import base64
import concurrent.futures
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import socket
import stat
import subprocess
import tempfile
import threading
import time
import urllib.request

STREAMS = ('soak_a', 'soak_b')
VIEWERS_PER_STREAM = 4
GAP_BOUNDS_MS = (250, 500, 1000, 1500, 3000, 10000)
MEDIA_CAP = 512 * 1024 * 1024
MIN_FREE = 20 * 1024 * 1024 * 1024
GAP_EVENT_LIMIT = 32
MAX_DAEMON_LOG_BYTES = 1024 * 1024


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def children_of(pid):
    pending, seen = [pid], set()
    while pending:
        current = pending.pop()
        if current in seen:
            continue
        seen.add(current)
        try:
            for task in Path(f'/proc/{current}/task').iterdir():
                try:
                    pending.extend(int(value) for value in (task / 'children').read_text().split())
                except (FileNotFoundError, ProcessLookupError):
                    continue
        except (FileNotFoundError, ProcessLookupError):
            continue
    return seen


def live_scope_members(path, owner_pid):
    members = set()
    for value in (path / 'cgroup.procs').read_text().split():
        pid = int(value)
        if pid == owner_pid:
            continue
        try:
            details = Path(f'/proc/{pid}/stat').read_text()
            if details[details.rfind(')') + 2] != 'Z':
                members.add(pid)
        except FileNotFoundError:
            continue
    return members


def terminate_owned_scope(path, owner_pid=None, grace_seconds=8):
    owner_pid = os.getpid() if owner_pid is None else owner_pid
    for action, seconds in ((signal.SIGTERM, grace_seconds), (signal.SIGKILL, 2)):
        deadline = time.monotonic() + seconds
        while True:
            members = live_scope_members(path, owner_pid)
            if not members:
                return
            for pid in members:
                try:
                    descriptor = os.pidfd_open(pid)
                    try:
                        signal.pidfd_send_signal(descriptor, action)
                    finally:
                        os.close(descriptor)
                except ProcessLookupError:
                    continue
            if time.monotonic() >= deadline:
                break
            time.sleep(0.05)
    if live_scope_members(path, owner_pid):
        raise RuntimeError('owned scope descendants survived cleanup')


def drain_log(source, output, limit, status):
    written = 0
    try:
        while data := os.read(source.fileno(), 16 * 1024):
            available = max(0, limit - written)
            if len(data) > available:
                status['truncated'] = True
            if available and not status.get('write_error'):
                try:
                    output.write(data[:available])
                    written += min(len(data), available)
                except OSError:
                    status['write_error'] = True
    except OSError:
        status['read_error'] = True


def cgroup():
    for line in Path('/proc/self/cgroup').read_text().splitlines():
        if line.startswith('0::'):
            path = Path('/sys/fs/cgroup') / line[3:].lstrip('/')
            if (path / 'memory.current').is_file():
                cpu_quota, period = (path / 'cpu.max').read_text().split()
                memory_limit = (path / 'memory.max').read_text().strip()
                pid_limit = (path / 'pids.max').read_text().strip()
                if (cpu_quota == 'max' or int(cpu_quota) > 2 * int(period)
                        or memory_limit == 'max' or int(memory_limit) > 1024 * 1024 * 1024
                        or pid_limit == 'max' or int(pid_limit) > 512):
                    raise RuntimeError('owned cgroup must limit CPU, memory and processes')
                if live_scope_members(path, os.getpid()):
                    raise RuntimeError('dedicated test cgroup required')
                return path
    raise RuntimeError('bounded cgroup-v2 resource accounting unavailable')


def cgroup_sample(path, elapsed, daemon_pid, media_bytes):
    cpu = dict(line.split() for line in (path / 'cpu.stat').read_text().splitlines())
    events = dict(line.split() for line in (path / 'memory.events').read_text().splitlines())
    return {'elapsed': round(elapsed, 2),
            'memory_bytes': int((path / 'memory.current').read_text()),
            'memory_peak_bytes': int((path / 'memory.peak').read_text()),
            'cpu_usage_usec': int(cpu['usage_usec']),
            'pids': int((path / 'pids.current').read_text()),
            'oom_kills': int(events.get('oom_kill', '0')),
            'daemon_descendants': len(children_of(daemon_pid)) - 1,
            'media_dir_bytes': media_bytes}


def directory_bytes(path):
    total = 0
    for current, _, files in os.walk(path):
        for name in files:
            try:
                total += (Path(current) / name).stat().st_size
            except FileNotFoundError:
                continue
    return total


def api(opener, port, password, path, method='GET', body=None):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        f'http://127.0.0.1:{port}{path}', data=data, method=method,
        headers={'Authorization': 'Basic ' + base64.b64encode(
            f'admin:{password}'.encode()).decode(), 'Content-Type': 'application/json'})
    started = time.monotonic()
    with opener.open(request, timeout=5) as response:
        payload = response.read(1_000_001)
        if len(payload) > 1_000_000:
            raise RuntimeError('metadata response exceeds 1 MiB')
        return response.status, round((time.monotonic() - started) * 1000, 2)


def source_stats(opener, port, password, stream):
    if stream not in STREAMS:
        raise ValueError('source observation is limited to owned streams')
    request = urllib.request.Request(
        f'http://127.0.0.1:{port}/streamer/api/v3/streams/{stream}',
        headers={'Authorization': 'Basic ' + base64.b64encode(
            f'admin:{password}'.encode()).decode()})
    with opener.open(request, timeout=1) as response:
        payload = response.read(1_000_001)
        if response.status != 200 or len(payload) > 1_000_000:
            raise RuntimeError('source stats response invalid or exceeds 1 MiB')
    try:
        data = json.loads(payload)
        stats = data['stats']
        age, received = stats['media_age_ms'], stats['bytes_in']
    except (ValueError, TypeError, KeyError) as exc:
        raise ValueError('source stats payload invalid') from exc
    if type(age) is not int or type(received) is not int or age < 0 or received < 0:
        raise ValueError('source stats counters invalid')
    return age, received


def warm_stream(stream, port, stop):
    until = time.monotonic() + 30
    while time.monotonic() < until and not stop.is_set():
        connection = http.client.HTTPConnection('127.0.0.1', port, timeout=5)
        try:
            connection.request('GET', f'/{stream}/mpegts')
            response = connection.getresponse()
            if response.status == 200 and response.read(188).startswith(b'G'):
                return
        except (OSError, http.client.HTTPException):
            pass
        finally:
            connection.close()
        time.sleep(0.25)
    raise RuntimeError(f'{stream}: warm-up media unavailable')


def gap_bucket(ms):
    for bound in GAP_BOUNDS_MS:
        if ms <= bound:
            return str(bound)
    return '>10000'


def new_viewer_record():
    return {
        'bytes': 0, 'connections': 0, 'statuses': {}, 'errors': {},
        'sync_errors': 0, 'first_byte_max_ms': 0,
        'reconnect_gap_max_ms': 0, 'reconnect_gap_bins': {},
        'media_gap_max_ms': 0, 'last_media_elapsed': None, 'active': False,
        'media_gap_count_over_3000_ms': 0, 'media_gap_events': [],
        'first_media_gap_over_3000_ms': None,
        'last_media_gap_over_3000_ms': None,
    }


def record_gap(record, kind, gap_ms, elapsed):
    if kind != 'media' or gap_ms <= 3000:
        return
    event = {'elapsed_seconds': round(elapsed, 2), 'gap_ms': gap_ms}
    record['media_gap_count_over_3000_ms'] += 1
    if record['first_media_gap_over_3000_ms'] is None:
        record['first_media_gap_over_3000_ms'] = event
    record['last_media_gap_over_3000_ms'] = event
    record['media_gap_events'].append(event)
    if len(record['media_gap_events']) > GAP_EVENT_LIMIT:
        del record['media_gap_events'][0]


def viewer(stream, index, port, started, deadline, stop, state, guard):
    record = state[f'{stream}:{index}']
    last_media = None
    while not stop.is_set() and time.monotonic() < deadline:
        connection = http.client.HTTPConnection('127.0.0.1', port, timeout=8)
        start = time.monotonic()
        first = True
        expected_churn = index == 0
        try:
            connection.request('GET', f'/{stream}/mpegts')
            response = connection.getresponse()
            with guard:
                record['connections'] += 1
                status = str(response.status)
                record['statuses'][status] = record['statuses'].get(status, 0) + 1
            if response.status != 200:
                raise RuntimeError(f'HTTP {response.status}')
            until = min(deadline, time.monotonic() + 30) if expected_churn else deadline
            remainder = b''
            with guard:
                record['active'] = True
            while not stop.is_set() and time.monotonic() < until:
                chunk = response.read1(188 * 64)
                if not chunk:
                    raise RuntimeError('premature EOF')
                now = time.monotonic()
                media = remainder + chunk
                aligned = len(media) // 188 * 188
                errors = sum(media[offset] != 0x47 for offset in range(0, aligned, 188))
                remainder = media[aligned:]
                if aligned == 0:
                    continue
                with guard:
                    record['bytes'] += aligned
                    record['sync_errors'] += errors
                    if first:
                        delay = round((now - start) * 1000, 2)
                        record['first_byte_max_ms'] = max(record['first_byte_max_ms'], delay)
                        if last_media is not None:
                            gap_ms = round((now - last_media) * 1000, 2)
                            record['reconnect_gap_max_ms'] = max(
                                record['reconnect_gap_max_ms'], gap_ms)
                            bucket = gap_bucket(gap_ms)
                            record['reconnect_gap_bins'][bucket] = (
                                record['reconnect_gap_bins'].get(bucket, 0) + 1)
                    elif last_media is not None:
                        gap_ms = round((now - last_media) * 1000, 2)
                        record['media_gap_max_ms'] = max(record['media_gap_max_ms'], gap_ms)
                        record_gap(record, 'media', gap_ms, now - started)
                    record['last_media_elapsed'] = round(now - started, 2)
                first = False
                last_media = now
        except (OSError, http.client.HTTPException, RuntimeError) as exc:
            if not stop.is_set() and time.monotonic() < deadline:
                with guard:
                    key = str(exc) if isinstance(exc, RuntimeError) else type(exc).__name__
                    record['errors'][key] = record['errors'].get(key, 0) + 1
                stop.wait(0.5)
        finally:
            with guard:
                record['active'] = False
            connection.close()


def save(path, report):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8',
                                         dir=path.parent, prefix=path.name + '.',
                                         delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(report, stream, separators=(',', ':'), allow_nan=False)
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--duration', type=int, default=86400)
    parser.add_argument('--sample-seconds', type=int, default=60)
    parser.add_argument('--state', required=True)
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--revision', required=True)
    parser.add_argument('--ffmpeg', default='/usr/bin/ffmpeg', type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r'[0-9a-f]{40}', args.revision):
        parser.error('revision must be a full lowercase Git SHA-1')
    if not 20 <= args.duration <= 86400 or not 5 <= args.sample_seconds <= 300:
        raise SystemExit('bounded duration or sample interval invalid')
    state_dir = Path(args.state).absolute()
    if state_dir.exists() or state_dir.is_symlink():
        raise SystemExit('state directory must be new')
    binary = args.binary.resolve(strict=True)
    if not stat.S_ISREG(binary.stat().st_mode) or not os.access(binary, os.X_OK):
        raise SystemExit('executable binary required')
    ffmpeg = args.ffmpeg.resolve(strict=True)
    if not stat.S_ISREG(ffmpeg.stat().st_mode) or not os.access(ffmpeg, os.X_OK):
        raise SystemExit('executable FFmpeg required')
    with binary.open('rb') as stream:
        binary_sha256 = hashlib.file_digest(stream, 'sha256').hexdigest()
    group = cgroup()
    state_dir.mkdir(mode=0o700)
    media_dir = state_dir / 'media'
    port = free_port()
    password = secrets.token_hex(24)
    env = {'PATH': os.defpath, 'LANG': 'C', 'HOME': str(state_dir),
           'TMPDIR': str(state_dir), 'FLUSSONIX_ADMIN_USER': 'admin',
           'FLUSSONIX_ADMIN_PASSWORD': password,
           'FLUSSONIX_PEER_KEY': secrets.token_hex(32)}
    guard, stop = threading.Lock(), threading.Event()
    signal.signal(signal.SIGTERM, lambda _signum, _frame: stop.set())
    signal.signal(signal.SIGINT, lambda _signum, _frame: stop.set())
    viewer_state = {f'{stream}:{index}': new_viewer_record()
        for stream in STREAMS for index in range(VIEWERS_PER_STREAM)}
    report = {'revision': args.revision[:7], 'binary_sha256': binary_sha256,
              'duration_seconds': args.duration,
              'sample_seconds': args.sample_seconds, 'streams': len(STREAMS),
              'viewers_per_stream': VIEWERS_PER_STREAM,
              'start_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
              'samples': [], 'metadata_checks': {'success': 0, 'errors': {},
                                                   'latencies_ms': []},
              'state': 'starting'}
    process = None
    log_thread = None
    log_status = {'truncated': False, 'write_error': False}
    log_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    with os.fdopen(os.open(state_dir / 'daemon.log', log_flags, 0o600), 'wb', buffering=0) as log:
        try:
            process = subprocess.Popen([
                str(binary), '--listen', f'127.0.0.1:{port}',
                '--config', str(state_dir / 'config.json'),
                '--media-dir', str(media_dir), '--ffmpeg', str(ffmpeg)],
                cwd=state_dir, env=env, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                start_new_session=True)
            log_thread = threading.Thread(
                target=drain_log,
                args=(process.stdout, log, MAX_DAEMON_LOG_BYTES, log_status),
                daemon=True,
            )
            log_thread.start()
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            for _ in range(100):
                if stop.is_set():
                    raise RuntimeError('interrupted')
                if process.poll() is not None:
                    raise RuntimeError(f'daemon exited {process.returncode}')
                try:
                    if api(opener, port, password, '/health')[0] == 200:
                        break
                except OSError:
                    pass
                time.sleep(0.1)
            else:
                raise RuntimeError('health deadline exceeded')
            for stream in STREAMS:
                if stop.is_set():
                    raise RuntimeError('interrupted')
                status, _ = api(opener, port, password,
                                f'/streamer/api/v3/streams/{stream}', 'PUT',
                                {'inputs': [{'url': 'testsrc://'}], 'static': False})
                if status not in (200, 201):
                    raise RuntimeError(f'{stream}: PUT status {status}')
                warm_stream(stream, port, stop)
            if stop.is_set():
                raise RuntimeError('interrupted')
            started = time.monotonic()
            deadline = started + args.duration
            report['state'] = 'running'
            report['listen'] = f'127.0.0.1:{port}'
            save(state_dir / 'checkpoint.json', report)
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(viewer_state)) as pool:
                futures = [pool.submit(viewer, stream, index, port, started, deadline,
                                       stop, viewer_state, guard)
                           for stream in STREAMS for index in range(VIEWERS_PER_STREAM)]
                next_sample = started
                last_media_bytes = 0
                try:
                    while time.monotonic() < deadline and not stop.is_set():
                        now = time.monotonic()
                        if now < next_sample:
                            stop.wait(min(1, next_sample - now))
                            continue
                        if process.poll() is not None:
                            raise RuntimeError(f'daemon exited {process.returncode}')
                        elapsed = now - started
                        scan_started = time.monotonic()
                        last_media_bytes = directory_bytes(media_dir)
                        report['last_media_scan_ms'] = round(
                            (time.monotonic() - scan_started) * 1000, 2)
                        if last_media_bytes > MEDIA_CAP:
                            raise RuntimeError('media directory exceeded 512 MiB cap')
                        if shutil.disk_usage('/').free < MIN_FREE:
                            raise RuntimeError('root free space fell below 20 GiB')
                        if shutil.disk_usage(state_dir).free < 1024 * 1024 * 1024:
                            raise RuntimeError('state filesystem free space fell below 1 GiB')
                        sample = cgroup_sample(group, elapsed, process.pid, last_media_bytes)
                        with guard:
                            sample['viewer_bytes'] = sum(value['bytes'] for value in viewer_state.values())
                            sample['active_viewers'] = sum(value['active'] for value in viewer_state.values())
                            report['viewers'] = json.loads(json.dumps(viewer_state))
                        report['samples'].append(sample)
                        try:
                            status, latency = api(opener, port, password,
                                                  '/streamer/api/v3/streams?limit=1')
                            if status != 200:
                                raise RuntimeError(f'HTTP {status}')
                            report['metadata_checks']['success'] += 1
                            report['metadata_checks']['latencies_ms'].append(latency)
                        except (OSError, RuntimeError) as exc:
                            key = type(exc).__name__ + ':' + str(exc)[:60]
                            errors = report['metadata_checks']['errors']
                            errors[key] = errors.get(key, 0) + 1
                        save(state_dir / 'checkpoint.json', report)
                        next_sample += args.sample_seconds
                finally:
                    stop.set()
                    for future in futures:
                        future.result(timeout=15)
            if time.monotonic() < deadline:
                raise RuntimeError('interrupted')
            report['state'] = 'completed'
            report['elapsed_seconds'] = round(time.monotonic() - started, 2)
            report['daemon_exited_early'] = process.poll() is not None
        except Exception as exc:
            report['state'] = 'failed'
            report['harness_error'] = f'{type(exc).__name__}: {exc}'
        finally:
            stop.set()
            try:
                terminate_owned_scope(group)
                if process is not None:
                    process.wait(timeout=2)
                if log_thread is not None:
                    log_thread.join(timeout=5)
                    if log_thread.is_alive():
                        raise RuntimeError('daemon log drain did not stop')
            except Exception as exc:
                report['state'] = 'failed'
                report['harness_error'] = f'cleanup: {type(exc).__name__}: {exc}'
            report['daemon_log_truncated'] = log_status['truncated']
            if log_status['truncated'] or log_status['write_error'] or log_status.get('read_error'):
                report['state'] = 'failed'
                report['harness_error'] = 'daemon log cap or I/O failure'
            with guard:
                report['viewers'] = json.loads(json.dumps(viewer_state))
            report['end_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
            try:
                if media_dir.exists():
                    shutil.rmtree(media_dir)
                (state_dir / 'config.json').unlink(missing_ok=True)
                (state_dir / 'checkpoint.json').unlink(missing_ok=True)
            except OSError as exc:
                report['state'] = 'failed'
                report['harness_error'] = f'file cleanup: {type(exc).__name__}'
            save(state_dir / 'report.json', report)
    print(json.dumps({'state': report['state'],
                      'elapsed_seconds': report.get('elapsed_seconds'),
                      'samples': len(report['samples'])}), flush=True)
    if report['state'] != 'completed' or report.get('daemon_exited_early'):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
