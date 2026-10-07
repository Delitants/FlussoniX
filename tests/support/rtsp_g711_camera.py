"""Owned RTSP/1.0 G.711 oracle; no application or vendor packetizer is used."""
import argparse
import base64
import json
import re
import select
import socket
import struct
import time
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

p = argparse.ArgumentParser()
p.add_argument('--codec', choices=['PCMA', 'PCMU'], required=True)
p.add_argument('--samples', type=Path, required=True)
p.add_argument('--ready', type=Path, required=True)
p.add_argument('--proof', type=Path, required=True)
a = p.parse_args()
samples = a.samples.read_bytes()
assert len(samples) >= 8000
listener = socket.socket()
listener.bind(('127.0.0.1', 0))
listener.listen(1)
listener.settimeout(25)
port = listener.getsockname()[1]
a.ready.write_text(str(port))
proof = {'connections': 0, 'transport': None, 'rtp_bytes': 0, 'authorized': 0,
         'query_verified': False, 'versions': [], 'codec': a.codec}
conn = None
udp = []
try:
    conn, peer = listener.accept()
    proof['connections'] += 1
    conn.settimeout(2)
    buf = b''
    playing = False
    dest = None
    channels = (0, 1)
    seq, timestamp, ssrc, packets, octets = 1000, 8000, 0x12345678, 0, 0
    offset = 0
    deadline = time.monotonic() + 25
    next_packet = next_report = time.monotonic()

    def reply(cseq, status=200, headers='', body=b''):
        conn.sendall((f'RTSP/1.0 {status} ' + ('OK' if status == 200 else 'Unauthorized') +
                      f'\r\nCSeq: {cseq}\r\n{headers}Content-Length: {len(body)}\r\n\r\n').encode() + body)

    def send(packet, rtcp=False):
        if dest:
            udp[int(rtcp)].sendto(packet, (peer[0], dest[int(rtcp)]))
        else:
            conn.sendall(struct.pack('!BBH', 36, channels[int(rtcp)], len(packet)) + packet)

    while time.monotonic() < deadline:
        if select.select([conn], [], [], .005)[0]:
            data = conn.recv(65536)
            if not data:
                break
            buf += data
        while buf:
            if buf[0] == 36:
                if len(buf) < 4:
                    break
                size = int.from_bytes(buf[2:4], 'big') + 4
                if len(buf) < size:
                    break
                buf = buf[size:]
                continue
            end = buf.find(b'\r\n\r\n')
            if end < 0:
                break
            header = buf[:end].decode()
            lines = header.split('\r\n')
            method, uri, version = lines[0].split(' ')
            headers = dict(line.split(':', 1) for line in lines[1:])
            headers = {k.lower(): v.strip() for k, v in headers.items()}
            size = end + 4 + int(headers.get('content-length', '0'))
            if len(buf) < size:
                break
            buf = buf[size:]
            proof['versions'].append(version)
            assert version == 'RTSP/1.0'
            cseq = headers['cseq']
            expected = 'Basic ' + base64.b64encode(b'owned-user:owned-camera-secret').decode()
            if headers.get('authorization') != expected:
                reply(cseq, 401, 'WWW-Authenticate: Basic realm="owned-camera"\r\n')
                continue
            proof['authorized'] += 1
            if method == 'OPTIONS':
                reply(cseq, headers='Public: OPTIONS, DESCRIBE, SETUP, PLAY, GET_PARAMETER, TEARDOWN\r\n')
            elif method == 'DESCRIBE':
                assert parse_qs(urlsplit(uri).query)['token'] == ['owned:token']
                proof['query_verified'] = True
                payload = 8 if a.codec == 'PCMA' else 0
                sdp = (f'v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=Owned G711\r\n'
                       f'c=IN IP4 127.0.0.1\r\nt=0 0\r\na=control:*\r\n'
                       f'm=audio 0 RTP/AVP {payload}\r\na=rtpmap:{payload} {a.codec}/8000/1\r\n'
                       'a=control:trackID=0\r\n').encode()
                reply(cseq, headers=f'Content-Type: application/sdp\r\nContent-Base: rtsp://127.0.0.1:{port}/camera/\r\n', body=sdp)
            elif method == 'SETUP':
                transport = headers['transport']
                if 'interleaved=' in transport:
                    channels = tuple(map(int, re.search(r'interleaved=(\d+)-(\d+)', transport).groups()))
                    proof['transport'] = 'tcp'
                    chosen = f'RTP/AVP/TCP;unicast;interleaved={channels[0]}-{channels[1]}'
                else:
                    dest = tuple(map(int, re.search(r'client_port=(\d+)-(\d+)', transport).groups()))
                    assert 'unicast' in transport and 1024 <= dest[0] < dest[1] <= 65535
                    proof['transport'] = 'udp'
                    for _ in range(2):
                        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                        sock.bind(('127.0.0.1', 0))
                        udp.append(sock)
                    chosen = (f'RTP/AVP/UDP;unicast;client_port={dest[0]}-{dest[1]};'
                              f'server_port={udp[0].getsockname()[1]}-{udp[1].getsockname()[1]}')
                reply(cseq, headers=f'Session: owned-session\r\nTransport: {chosen}\r\n')
            elif method == 'PLAY':
                reply(cseq, headers='Session: owned-session\r\nRange: npt=0.000-\r\n')
                playing = True
                next_packet = next_report = time.monotonic()
            elif method == 'TEARDOWN':
                reply(cseq, headers='Session: owned-session\r\n')
                playing = False
                deadline = 0
            else:
                reply(cseq, headers='Session: owned-session\r\n')
        now = time.monotonic()
        if playing and now >= next_packet:
            payload = samples[offset:offset + 160]
            assert len(payload) == 160
            offset = (offset + 160) % len(samples)
            packet = struct.pack('!BBHII', 0x80, 8 if a.codec == 'PCMA' else 0, seq, timestamp, ssrc) + payload
            send(packet)
            proof['rtp_bytes'] += len(packet)
            packets += 1
            octets += len(payload)
            seq = (seq + 1) % 65536
            timestamp = (timestamp + 160) % (2 ** 32)
            next_packet += .020
            if now >= next_report:
                ntp = time.time() + 2208988800
                report = struct.pack('!BBHIIIIII', 0x80, 200, 6, ssrc, int(ntp), int(ntp % 1 * 2 ** 32), timestamp, packets, octets)
                report += struct.pack('!BBHI', 0x81, 202, 3, ssrc) + b'\x01\x05owned\x00'
                send(report, True)
                next_report = now + 1
except (BrokenPipeError, ConnectionResetError):
    pass
finally:
    if conn:
        conn.close()
    for sock in udp:
        sock.close()
    listener.close()
    a.proof.write_text(json.dumps(proof))
