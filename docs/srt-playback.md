# Shared SRT listener playback profile

Add an opt-in global playback listener on an explicitly chosen unused UDP
address. For a protected stream, request a five-second, single-use ticket over
HTTPS and connect with the returned tokenless SRT Stream ID. The media listener
must require encryption and the HTTPS and SRT requests must arrive from the
same public IP. Request a fresh ticket after every disconnect. A public stream
can still use `#!::r=NAME,m=request` without a ticket, subject to its viewer
policy. Publishing is rejected.

The SRT Stream ID handshake is not encrypted by the media passphrase. The
legacy `#!::r=NAME,m=request,u=TOKEN` form exposes the reusable viewer token
to an on-path observer and is disabled by default. Enable it only on a trusted
network with `--srt-play-allow-direct-token`. A visible one-use ticket can also
be raced during its short lifetime; it is bound to one stream, listener, route
and observed client IP. See [IPTV transport support](iptv-transport.md).
Names or viewer tokens containing commas cannot currently be represented in
the legacy selection format.
This follows the documented [global playback](https://flussonic.com/doc/fms/play/srt/)
and [SRT token](https://flussonic.com/doc/fms/auth/srt-auth/) conventions.

The first profile uses startup options, like the existing optional RTSP and
HTTPS listeners. Per-stream ports and the vendor's mutable `srt_play` API
object remain pending. Config displays actual listener settings without JSON
or secrets, and stream Output displays the HTTPS ticket request and tokenless
response format.
Default startup enables no SRT listener and changes no production port.

## Ticket setup and client request

Start an HTTPS listener with `--https-listen`, `--https-cert` and
`--https-key`. Start encrypted SRT with `--srt-play-listen`, set
`FLUSSONIX_SRT_PLAY_PASSPHRASE` in the service environment, and advertise a
reachable `srt://host:port` root with `--srt-play-public-url`. A wildcard bind
such as `0.0.0.0:9998` is valid, but it is not a public URL. Config shows
**Ticket playback: Ready** only when the HTTPS issuer, encrypted SRT listener
and public SRT URL are configured.

The client sends its original selection in an HTTPS JSON request body:

```http
POST /flussonix/api/v1/srt-ticket
Content-Type: application/json

{"streamid":"#!::r=news,m=request,u=viewer-token","encryption":"required"}
```

The response contains `address`, `streamid`, `encryption_required: true` and
`expires_in: 5`. Use the entire returned `streamid`, which has the form
`#!::r=news,m=request,flussonix_ticket=<UUID>` and contains no viewer token.
Set the passphrase in the SRT client separately. The response is private and
uncacheable. An LB can issue the same ticket for a native CDN; its management
connection to that CDN must use verified HTTPS. The legacy GET lookup remains
available for integrated clients, but POST keeps tokens out of URL logs.

## Transport and bounds

Use the independent system libsrt 1.5 C API through a small, private Rust
adapter. Load the library only when the listener is enabled; never search
`/opt/flussonic`. Default builds do not require development headers or a
vendor installation. Support Linux IPv4 and IPv6 addresses; incompatible
or missing libraries fail startup before media workers start. Pin the adapter
to the public [1.5.4 C declarations](https://github.com/Haivision/srt/blob/v1.5.4/srtcore/srt.h)
and require a runtime version in the 1.5 series.

Send the existing shared worker's MPEG-TS directly in messages of at most
1316 bytes. No additional FFmpeg process or encode runs per viewer. Delivery starts live;
a late join can wait for the next source keyframe. This profile does not replay
a cached GOP. Keep
nonblocking accept and send operations, a finite SRT send buffer, disabled
linger, bounded pending/active viewers, and the existing bounded worker
broadcast. A slow or lagging receiver closes independently. Send stalls
are bounded to two seconds; absent media progress to ten seconds. Shutdown
cancels and joins every viewer, releasing its socket and worker reference.

Latency is 1–10000 milliseconds (default 120). Viewer slots are 1–4096
(default 128), including authorization requests. An optional global
passphrase is 10–79 printable ASCII bytes; empty means plaintext. Enforce
encryption with AES-128 when configured, with no plaintext fallback. Pass
the secret through `FLUSSONIX_SRT_PLAY_PASSPHRASE`; diagnostics and UI expose
only an encryption boolean. Different per-stream keys remain pending.

## Selection and authorization

Stream ID is at most 512 UTF-8 bytes, starts with `#!::`, and contains
comma-separated unique fields. Ticket playback uses `r`, `m=request` and
`flussonix_ticket=<UUID>` without `u` or opaque fields. The explicit legacy
profile also accepts `u` and ignored opaque `s`/`a` compatibility metadata.
Require a valid configured/discovered stream
name and `m=request` when mode is supplied. Reject duplicate/unknown fields,
controls, malformed UTF-8 and publication modes without starting workers.
Do not URL-decode values a second time after libsrt obtains the Stream ID.

An SRT handshake may complete before application authorization. No media
bytes or worker start is allowed before the existing token/on_play policy
allows the actual peer IP with `proto=srt`. Callback redirects, denial,
outage and LB-role admission all close the connection. Native CDN source
discovery and policy fences remain the same as other outputs. Renewals,
session deletion, stream disable/delete and media replacement terminate
delivery. Client-supplied session metadata never becomes a trusted grant ID.

Count bytes accepted by libsrt in viewer sessions and native SRT egress
metrics. Include SRT listener output in process-based aggregate media/uplink measurements.
Caller pushes are measured separately; interface-based uplink sampling includes all traffic.
These counters do not prove remote acknowledgement. Never report tokens,
raw Stream IDs, passphrases or library error strings.

## Qualification and remaining work

Use independent FFmpeg callers on owner-created ephemeral localhost ports.
Decode H.264/HEVC with AAC, MPEG Layer II and MP3, test encrypted success and
wrong-secret/plaintext denial, concurrent viewers sharing one worker, token
denial, callback renewal/revocation, replacement, slot recovery, shutdown
and native CDN pulls. Test address-family conversion and invalid settings,
including the full Stream ID parser boundary. Browser tests verify the
readable disabled/enabled Config card and tokenless Output instructions.

Dedicated [regional subtitle qualification](srt-subtitles.md) covers encrypted
H.264/HEVC with AAC copy-mode CEA-608/708 and exact DVB/teletext delivery, separate-track filtering
and CEA HLS conversion alongside SRT. Dedicated [source/CDN tests](cluster-srt-subtitles.md)
also qualify private MPEG-TS pulls into encrypted CDN listener playback.
Broader encoder, input and cluster subtitle combinations remain pending.
SRT publication authorization, per-stream keys/ports, complete vendor API
objects, WAN loss/retransmission, sustained throughput and scale remain
explicit further work. Listener playback is not an HTTP LB redirect path.
