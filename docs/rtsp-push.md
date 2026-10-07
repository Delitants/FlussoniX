# RTSP and RTSPS push

Streams and Templates accept SRT, RTSP and RTSPS in the existing `pushes`
array, with at most four destinations total. In **Input → Push destinations**,
select **Destination protocol**, enter a masked **Destination URL**, and set
**Enabled**, **Connection timeout** and **Retry interval** using
normal fields. RTSPS also exposes **Trusted CA file**. Template inheritance and
an explicit empty stream override retain their existing semantics.

```json
{
  "pushes": [
    {"url": "rtsp://receiver.example/live/channel?password=receiver-secret"},
    {"url": "rtsps://receiver:receiver-secret@secure.example/live/channel", "flussonix_tls_ca": "/etc/flussonix/receiver-ca.pem"}
  ]
}
```

RTSP defaults to port 554; RTSPS defaults to 322. A nonempty stream path is
required. URLs are ASCII, at most 4096 bytes, with valid percent encoding and
no fragment, whitespace or control characters. Query credentials
are retained for the receiving server. Optional `USER:PASSWORD@HOST` credentials
are percent-decoded once and stripped from request targets. The decoded username
is 1–256 printable ASCII characters without a colon; the password is at most
512 printable ASCII characters and can be empty. Encode spaces and URI punctuation
in userinfo. Unicode credentials are outside this profile. `connect_timeout` is 1–30 seconds (default 3);
`retry_timeout` is 1–300 seconds (default 5). `disabled` is boolean and
`comment` is at most 1024 bytes. Other transport options are rejected.

RTSPS always verifies certificate chain, validity and original DNS/IP identity
before sending RTSP. Leave the CA field empty for bundled public roots, or use
an absolute regular PEM trust file on the sending host; custom roots replace
public roots. There is no insecure mode or plaintext fallback. Redirects and
server-directed UDP transport changes are rejected.

Receiver authentication responds to a matching-CSeq 401. The first request is
unsigned; subsequent ANNOUNCE, SETUP, RECORD and OPTIONS requests use the selected
challenge on that connection only. Basic and Digest MD5, MD5-sess, SHA-256 and
SHA-256-sess are supported. Digest accepts legacy challenges without qop or
chooses `auth` from a qop list. It signs the exact upstream absolute request URI,
including its original RTSP/RTSPS authority, query and track control suffix.
Repeated or combined challenges prefer SHA-256 over MD5 over Basic; duplicate
supported algorithms, malformed parameters and unsupported-only offers fail.
An unsupported Digest offer cannot fall back to Basic. Domain parameters never
change the configured destination or authorize a redirect.

Digest stale-nonce renewal requires a changed nonce, `stale=true`, and unchanged
realm, algorithm and qop. It resets the nonce count and generates a fresh random
client nonce. Each control exchange permits at most two challenge retries;
a wrong password or repeated Basic challenge fails immediately after the signed
request. Setup retries retain the same absolute startup deadline. OPTIONS renewal
retains its original five-second response deadline while media continues. A new
connection starts with no cached challenge. Authentication-Info nextnonce and
rspauth negotiation, proxy authentication and international credential encodings
are not implemented. These bounds qualify a receiver authentication profile,
not every vendor recorder's dialect. See [Digest](https://www.rfc-editor.org/rfc/rfc7616)
and [Basic](https://www.rfc-editor.org/rfc/rfc7617) for their definitions.

The native publisher shares the worker's bounded RTP packetizer and creates no
per-destination FFmpeg or encoder. Supported retained media is one H.264 or
single-layer HEVC video track and/or AAC-LC, MPEG-1/2 Layer II and MP3 audio,
with at most eight audio/video tracks total. Audio-only and multiple audio
tracks are supported. The existing worker's copy/CPU/GPU selection applies;
a destination does not initiate another transcode. AAC uses MPEG4-GENERIC;
MPEG audio uses MPA/90000. Embedded captions follow the worker's selected
copy/encode behavior. Separate retained subtitle tracks cannot be represented
by this RTSP profile and fail the destination before connecting, including native
M4F/M4S text that has no MPEG-TS mapping; choosing to filter
original tracks allows supported audio/video output. This does not change the
separate HLS subtitle controls.

Publishing uses ANNOUNCE, one TCP-interleaved SETUP per track, and RECORD.
Relative controls append `/trackID=...` to the exact aggregate URL, including
an existing query, matching the qualified receiving dialect. RTCP sender
reports synchronize tracks, and OPTIONS keeps the session alive. Setup
requires matching CSeq, stable Session and the exact offered distinct channel
pairs. A SETUP response may use `mode=record` or the receiver-side
`mode=receive` alias; playback mode, duplicate options and channel substitutions
remain rejected. Protocol buffers are bounded: 16 KiB headers, 64 headers (only WWW-Authenticate may repeat),
64 KiB bodies, 16 KiB ANNOUNCE and 8 KiB interleaved frames; pending control
requests are capped at 16. Incoming RTCP must use a negotiated RTCP channel.

Each destination retries independently. A stalled destination cannot block
playback or another destination. Each attempt has one startup window of
`connect_timeout + 5` seconds covering metadata, DNS/TCP/TLS preparation,
RTSP setup and first RTP delivery. Connection preparation also retains its
own `connect_timeout` limit within that remaining window. After delivery starts,
RTP must progress within ten seconds; RTCP and control traffic do not reset
this deadline. Cancellation closes the sockets and joins owned
reader/bridge tasks before retry or configuration replacement. Enabled pushes
keep configured on-demand streams active; disabled entries create no connection.
Publication streams still wait for their publisher. Destination edits replace
the shared worker and require viewers to reconnect. Discovered CDN mirrors do
not inherit source push destinations.

`stats.flussonix_pushes` preserves mixed-array indices. RTSP entries report
`protocol`, sanitized scheme/host/port `endpoint`, `status`, `attempts`,
`rtp_bytes` and a fixed `last_error` code. `sending` requires RTP bytes written
through the bridge after accepted RECORD; it proves local transport progress,
not receiver decoding or remote acknowledgement. RTCP/framing bytes are not
counted. Native RTSP entries have `pid: 0` and `fed_bytes: 0`. Diagnostics omit
paths, queries, credentials, trust contents and raw protocol errors.

Independent FFmpeg copy receivers need `-copyinkf` for raw MPEG4-GENERIC AAC:
the depacketizer supplies no keyframe flag, so default stream-copy recording
can discard all AAC access units. Qualification reproduces this behavior
between two unmodified FFmpeg processes and still requires exact codecs and
strict decoding of every requested audio/video track. This receiver setting
is not a universal client compatibility claim.

See [qualification](qualification.md#rtsp-and-rtsps-push-qualification) for
evidence. Basic/Digest viewer or incoming publisher authentication,
proxy authentication, Digest auth-int/userhash/SHA-512 variants, dedicated subtitle RTP,
RTSP balancer redirects, all recorder dialects, long-duration synchronization
and production capacity remain unqualified. No official Flussonic component
is linked, copied or required by this implementation.

## Unicast UDP push

Plain RTSP destinations may select `rtsp_transport: "udp"` using the normal
Destination RTSP transport selector. Omitted or `"tcp"` preserves the existing
TCP default. Templates inherit the same saved destination objects. RTSPS rejects
UDP and continues to carry both control and media through verified TLS.

Each attempt reserves an even/odd local RTP/RTCP pair per retained track, bound
to the control connection's local interface. SETUP must echo those client ports
and supply distinct, unicast even/odd server ports of at least 1024. RTP/AVP and
RTP/AVP/UDP responses are accepted; optional source/destination addresses must
match the connected control peer. Redirects, substituted transports, duplicate
parameters, multicast and colliding remote track pairs fail before RECORD.
Connected UDP sockets enforce the negotiated feedback endpoint. Pre-RECORD
queues are drained through the actual socket with a 64-datagram bound, and
oversized, malformed or overflowing admitted RTCP closes the attempt.

The shared packetizer and worker provide H.264/HEVC, AAC-LC and MPEG Layer II/III.
DTS pacing and a 100 Mbps application-payload token bucket bound bootstrap bursts;
this is an outbound push ceiling, with a 32 KiB burst allowance. RTCP sender
reports and OPTIONS renewal retain the existing deadlines and authentication.
Only successful RTP datagram writes increment `rtp_bytes`; this proves local
transport progress. Cancellation and retry join owned readers and release all
port pairs. No per-destination FFmpeg or vendor component is used.

Qualification covers owned loopback independent FFmpeg receivers, the codec
matrix, Basic/Digest control authentication, strict mapped-track decoding,
transport rejection and port reclamation. NAT traversal, multicast push,
scoped IPv6, WAN loss, all recorder dialects and sustained capacity remain
unqualified. These bounds apply to UDP push; earlier TCP evidence still applies.

The scheduler services due sender reports and OPTIONS before feedback and limits
a consecutive feedback burst to eight frames before servicing ready media. An
idle progress tick reopens feedback service within 100 ms. Both TCP and UDP push
contribute successful outbound RTP/RTCP writes to lifetime node transport
counters, so `--uplink-interface process` includes them in RTSP/media uplink
measurements. `rtsp_push_bytes_out` includes application payload and TCP
interleaving headers; UDP/IP, TCP/IP and TLS record overhead are measured by the
NIC profile rather than these payload counters. Destination `rtp_bytes` remains
RTP-only. Counters survive worker stop/replacement and exclude inbound feedback.
Admitted RTCP checks packet-specific minimum/report lengths, SDES structure and
final-packet padding; RTPFB/PSFB require their sender/media SSRC header.
