"""Independent Python authentication recorder in front of an FFmpeg receiver.

No FlussoniX parsing/signing code is reused. Media frames pass through unchanged.
Evidence contains only methods, targets, algorithms and nonce counts, never headers.
"""
import argparse, base64, hashlib, json, re, selectors, socket, time
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--port-file', required=True)
p.add_argument('--events', required=True)
p.add_argument('--receiver', type=int, required=True)
p.add_argument('--profile', required=True)
p.add_argument('--rotate', action='store_true')
p.add_argument('--reject', action='store_true')
p.add_argument('--origin-file')
p.add_argument('--delay-challenge', type=float, default=0)
a = p.parse_args()
listener = socket.socket()
listener.bind(('127.0.0.1', 0))
listener.listen(1)
port = listener.getsockname()[1]
Path(a.port_file).write_text(str(port))
listener.settimeout(20)
local, _ = listener.accept()
listener.close()
remote = socket.create_connection(('127.0.0.1', a.receiver), timeout=10)
local.setblocking(False)
remote.setblocking(False)
s = selectors.DefaultSelector()
s.register(local, selectors.EVENT_READ)
s.register(remote, selectors.EVENT_READ)
buffers = {local: b'', remote: b''}
# Deliberate punctuation tests quoted-string escaping and comma/list handling.
realm = 'owned, "recorder"'
opaque = 'echo\\opaque'
nonce = 'owned-nonce-1'
username, password = 'user name', 'owned:secret'
last_nc = 0
rotated = False
authenticated = False
forwarded_seq = 0
pending = {}
recorded = False
media_frames = 0
media_bytes = 0

def quote(value):
    return '"' + value.replace('\\', '\\\\').replace('"', '\\"') + '"'

def challenge(stale=False):
    if a.profile == 'basic':
        return 'Basic realm=' + quote(realm)
    if a.profile == 'unsupported':
        return 'Digest realm="owned", nonce="n", algorithm=SHA-512, qop="auth-int"'
    if a.profile == 'ambiguous':
        return 'Digest realm="owned", nonce="n", nonce="other"'
    algorithm = 'MD5' if a.profile == 'stale-loop' else a.profile.removesuffix('-legacy')
    result = f'Digest realm={quote(realm)}, nonce={quote(nonce)}, opaque={quote(opaque)}'
    if a.profile != 'MD5-legacy':
        result += f', algorithm={algorithm}'
    if not a.profile.endswith('-legacy'):
        result += ', qop="auth-int, auth"'
    return result + (', stale=true' if stale else '')

def send(sock, data):
    sock.setblocking(True)
    sock.settimeout(3)
    sock.sendall(data)
    sock.setblocking(False)

def event(data):
    with open(a.events, 'a') as out:
        out.write(json.dumps(data) + '\n')

def verify(header, method, target):
    global last_nc
    if a.profile == 'basic':
        return header == 'Basic ' + base64.b64encode(f'{username}:{password}'.encode()).decode()
    if not header.startswith('Digest '):
        return False
    params = {}
    # Independent strict enough oracle: don't accept duplicated signing fields.
    pattern = r'([A-Za-z0-9_-]+)\s*=\s*("(?:\\.|[^"\\])*"|[^, ]+)\s*(?:,\s*|$)'
    tail = header[7:]
    while tail:
        match = re.match(pattern, tail)
        if not match or match[1] in params:
            return False
        value = match[2]
        params[match[1]] = re.sub(r'\\(.)', r'\1', value[1:-1]) if value.startswith('"') else value
        tail = tail[match.end():]
    algorithm = 'MD5' if a.profile == 'stale-loop' else a.profile.removesuffix('-legacy')
    legacy = a.profile.endswith('-legacy')
    if params.get('username') != username or params.get('realm') != realm or params.get('nonce') != nonce or params.get('uri') != target or params.get('opaque') != opaque:
        return False
    if params.get('algorithm', 'MD5') != algorithm:
        return False
    hash_name = 'sha256' if algorithm.startswith('SHA-256') else 'md5'
    def h(text):
        return hashlib.new(hash_name, text.encode()).hexdigest()
    ha1 = h(f'{username}:{realm}:{password}')
    if algorithm.endswith('-sess'):
        if not re.fullmatch('[0-9a-f]{32}', params.get('cnonce', '')):
            return False
        ha1 = h(f'{ha1}:{nonce}:{params.get("cnonce", "")}')
    ha2 = h(f'{method}:{target}')
    if legacy:
        if 'nc' in params or 'qop' in params:
            return False
        expected = h(f'{ha1}:{nonce}:{ha2}')
    else:
        nc = params.get('nc', '')
        if not re.fullmatch('[0-9a-f]{8}', nc) or int(nc, 16) != last_nc + 1 or params.get('qop') != 'auth' or not re.fullmatch('[0-9a-f]{32}', params.get('cnonce', '')):
            return False
        expected = h(f'{ha1}:{nonce}:{nc}:{params["cnonce"]}:auth:{ha2}')
        last_nc = int(nc, 16)
    return params.get('response') == expected

try:
    while True:
        ready = s.select(20)
        if not ready:
            break
        for key, _ in ready:
            sock = key.fileobj
            data = sock.recv(65536)
            if not data:
                raise EOFError()
            buffers[sock] += data
            while buffers[sock]:
                data = buffers[sock]
                if data[0] == 36:
                    if len(data) < 4:
                        break
                    size = 4 + int.from_bytes(data[2:4], 'big')
                    if len(data) < size:
                        break
                    if sock is local:
                        media_frames += 1
                        media_bytes += size - 4
                        assert authenticated and recorded, 'media before authenticated RECORD'
                    send(remote if sock is local else local, data[:size])
                    buffers[sock] = data[size:]
                    continue
                at = data.find(b'\r\n\r\n')
                if at < 0:
                    assert len(data) < 16384
                    break
                head = data[:at].decode('ascii')
                lines = head.split('\r\n')
                headers = {k.lower(): v.strip() for k, v in (line.split(':', 1) for line in lines[1:])}
                size = at + 4 + int(headers.get('content-length', '0'))
                if len(data) < size:
                    break
                frame = data[:size]
                buffers[sock] = data[size:]
                if sock is remote:
                    seq = int(headers['cseq'])
                    original_seq, pending_method = pending[seq]
                    code = int(lines[0].split(' ')[1])
                    if pending_method == 'RECORD' and code == 200:
                        recorded = True
                    if code >= 200:
                        del pending[seq]
                    lines = [line if not line.lower().startswith('cseq:') else f'CSeq: {original_seq}' for line in lines]
                    frame = ('\r\n'.join(lines) + '\r\n\r\n').encode() + frame[at + 4:]
                    send(local, frame)
                    continue
                method, target, version = lines[0].split(' ')
                assert version == 'RTSP/1.0'
                origin = Path(a.origin_file).read_text() if a.origin_file else f'rtsp://127.0.0.1:{port}'
                aggregate = f'{origin}/owned?token=owned-query'
                assert target == aggregate or target.startswith(aggregate + '/trackID=')
                assert password.encode() not in frame and b'user%20name' not in frame
                stale = False
                if method == 'OPTIONS' and a.rotate and not rotated:
                    assert authenticated and verify(headers.get('authorization', ''), method, target)
                    nonce = 'owned-nonce-2'
                    last_nc = 0
                    rotated = stale = True
                valid = not stale and verify(headers.get('authorization', ''), method, target)
                if a.profile == 'stale-loop' and valid:
                    nonce = 'owned-nonce-' + str(int(nonce.rsplit('-', 1)[1]) + 1)
                    last_nc = 0
                    stale = True
                    valid = False
                event({'method': method, 'target': target, 'accepted': valid and not a.reject, 'stale': stale, 'nonce': nonce, 'profile': a.profile})
                if not valid or a.reject:
                    time.sleep(a.delay_challenge)
                    send(local, f'RTSP/1.0 401 Unauthorized\r\nCSeq: {headers["cseq"]}\r\nWWW-Authenticate: {challenge(stale)}\r\nContent-Length: 0\r\n\r\n'.encode())
                    continue
                authenticated = True
                # Receiver authority is different, proving Digest used the gateway's
                # actual request target rather than the publisher loopback endpoint.
                forwarded_seq += 1
                pending[forwarded_seq] = (int(headers['cseq']), method)
                lines[0] = lines[0].replace(aggregate, f'rtsp://127.0.0.1:{a.receiver}/owned')
                lines = [line if not line.lower().startswith('cseq:') else f'CSeq: {forwarded_seq}' for line in lines if not line.lower().startswith('authorization:')]
                frame = ('\r\n'.join(lines) + '\r\n\r\n').encode() + frame[at + 4:]
                send(remote, frame)
except (EOFError, ConnectionError, OSError):
    pass
finally:
    event({'kind': 'summary', 'media_frames': media_frames, 'media_bytes': media_bytes, 'authenticated_record': recorded})
    s.close()
    local.close()
    remote.close()
