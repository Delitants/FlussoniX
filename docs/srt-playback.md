# Shared SRT listener playback profile

Add an opt-in global playback listener on an explicitly chosen unused UDP
address. A caller selects a stream with `#!::r=NAME,m=request,u=TOKEN` in
its SRT Stream ID. `m` can be omitted on this playback-only listener;
publishing is rejected. The `u` value is the existing viewer token.
This follows the documented [global playback](https://flussonic.com/doc/fms/play/srt/)
and [SRT token](https://flussonic.com/doc/fms/auth/srt-auth/) conventions.

The first profile uses startup options, like the existing optional RTSP and
HTTPS listeners. Per-stream ports and the vendor's mutable `srt_play` API
object remain pending. Config displays actual listener settings without JSON
or secrets, and stream Output displays a playback URL with a token placeholder.
Default startup enables no SRT listener and changes no production port.

## Transport and bounds

Use the independent system libsrt 1.5 C API through a small, private Rust
adapter. Load the library only when the listener is enabled; never search
`/opt/flussonic`. Default builds do not require development headers or a
vendor installation. Support Linux IPv4 and IPv6 addresses; incompatible
or missing libraries fail startup before media workers start. Pin the adapter
to the public [1.5.4 C declarations](https://github.com/Haivision/srt/blob/v1.5.4/srtcore/srt.h)
and require a runtime version in the 1.5 series.

Send the existing shared worker's MPEG-TS directly in messages of at most
1316 bytes. No additional FFmpeg process or encode runs per viewer. Keep
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
comma-separated unique fields. Accept `r`, `m`, `u`, and ignored opaque
`s`/`a` compatibility metadata. Require a valid configured/discovered stream
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
metrics. Include SRT in process-based aggregate media/uplink measurements.
These counters do not prove remote acknowledgement. Never report tokens,
raw Stream IDs, passphrases or library error strings.

## Qualification and remaining work

Use independent FFmpeg callers on owner-created ephemeral localhost ports.
Decode H.264/HEVC with AAC, MPEG Layer II and MP3, test encrypted success and
wrong-secret/plaintext denial, concurrent viewers sharing one worker, token
denial, callback renewal/revocation, replacement, slot recovery, shutdown
and native CDN pulls. Test address-family conversion and invalid settings,
including the full Stream ID parser boundary. Browser tests verify the
readable disabled/enabled Config card and token-placeholder output URL.

DVB keep/drop delivery and regional captions use the worker's established
policy; dedicated listener subtitle round trips are qualified separately.
SRT publication authorization, per-stream keys/ports, complete vendor API
objects, WAN loss/retransmission, sustained throughput and scale remain
explicit further work. Listener playback is not an HTTP LB redirect path.
