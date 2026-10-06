# RTSP and RTSPS publication

FlussoniX receives live RTSP/1.0 publication using ANNOUNCE, SETUP and RECORD. A stream must have its sole input set to `publish://`; Streams/Templates expose **Receive a publication** and the existing publisher password and authorization controls. Static configured names can contain path segments. HTTP and RTSP share exclusive publisher ownership and one common FFmpeg worker per active stream. Viewers reuse that worker. No installed Flussonic component is used.

## Enable a listener

Use an unused port appropriate for your node:

```sh
flussonix --rtsp-listen 127.0.0.1:18554
```

For encrypted control and media, provide your TLS material:

```sh
flussonix --rtsps-listen 127.0.0.1:18555 \
  --rtsps-cert /etc/flussonix/server.pem --rtsps-key /etc/flussonix/server.key
```

These examples bind loopback; choose the required interface for external publishers. Listener options accompany the existing management credentials, configuration and media directory options. The receive form displays the actual bound listener addresses. Wildcard listeners use the browser hostname for the displayed URL. Disabled listeners do not produce invented URLs. Generated URLs do not contain stored passwords; add query credentials in the publisher.

## Publish

A standalone FFmpeg example for an owned file:

```sh
ffmpeg -re -i owned-source.mp4 -map 0:v:0 -map 0:a? -c copy \
  -f rtsp -rtsp_transport tcp \
  'rtsp://127.0.0.1:18554/channel?password=YOUR_PUBLISHER_PASSWORD&token=YOUR_TOKEN'
```

The source codecs must fit the receiving profile below. A TLS publisher uses the displayed `rtsps://` URL and must verify the server certificate and name with the appropriate trust store. The qualification fixture uses an independent FFmpeg muxer through an owned Rustls relay that verifies its owned CA and server name before forwarding control/media; this is not a claim that every FFmpeg build forwards custom RTSP TLS trust options.

A query password is checked against the effective stream/template publisher password. Viewer/admin/peer credentials do not bypass this check. `on_publish` receives JSON with `proto: "rtsp"`, socket IP, name, query/token, session ID, received media bytes, duration and monotonic request number. This logical protocol is unchanged on TLS; worker statistics distinguish `rtsp` and `rtsps`. Initial and renewing callback decisions use the existing bounded publisher policy. A changed policy or media configuration revokes pending and active sessions; title-only edits retain them. LB nodes reject receiving publications.

## Receiving profile and bounds

- Interleaved RTP/AVP/TCP only. All announced tracks must negotiate distinct RTP/RTCP channels before RECORD. Playback and publication cannot share a connection. Only live zero/now ranges are accepted.
- At most one H.264 or HEVC video track and up to eight total video/audio tracks. H.264 requires packetization mode 1; HEVC DON reordering is unsupported. AAC-LC uses MPEG4-GENERIC/AAC-hbr; MPEG audio uses MPA (static PT14 or a validated dynamic payload).
- SDP is bounded to 16 KiB. Only codec metadata and bounded controls are accepted. Advertised connection addresses/ports are never used as network destinations: the worker receives regenerated private loopback SDP. Every track retains its validated payload mapping and first valid RTP SSRC. Unsupported codecs, negotiation and external control resources fail explicitly.
- RTSP framing bounds interleaved messages to 8192 bytes; direct UDP packet bounds remain unchanged. RTCP is additionally limited to the existing validated compound profile (2048 bytes). One pending sender report may precede the first RTP packet; it is forwarded only if its sender matches that packet. Decoder feedback returns over the negotiated TCP RTCP channel.
- The shared 64-publisher admission limit includes pending ANNOUNCE sessions. No worker starts until RECORD. Private decoder ports are reserved during preparation, then released just before decoder startup. Readiness observes Linux socket tables without binding probe sockets. Loopback is the same trusted local decoder boundary used by direct RTP input.
- Pending inactivity expires after 30 seconds. Active media silence respects the configured 1–300 second input timeout, independently of control/RTCP traffic. Worker watchdogs also require decoded output. Disconnect, teardown, policy change, malformed media or worker failure cancel and reap only the owning generation.

## Qualification

`tests/rtsp_publication.rs` uses unused local listeners and independently generated FFmpeg publishers. It probes codec multiplicity and at least 20 decoded frames per stream, then strictly decodes the whole recorded shared TS with empty decoder diagnostics. The matrix includes H.264/HEVC paired with AAC/MP2/MP3, AAC-only and two distinct MPEG audio tracks, CPU HEVC with MP2 audio, and verified TLS HEVC/MP3. Admission tests cover wrong credentials, callback denial/renewal, policy mutation during callback and decoder startup, URL/session/channel binding, large bounded TCP packets, malformed packets, SSRC substitution, media stall, exclusive HTTP ownership and reconnect.

```sh
cargo test --locked --test rtsp_publication -- --test-threads=1
```

To retain owned TS clips and metadata, set `FLUSSONIX_RTSP_RECORD_DIR` to a private local directory. The explicitly ignored `vaapi_publication_transcode_strictly_decodes` test requires H.264 VAAPI hardware and a scoped driver environment. It does not qualify GPU HEVC or NVIDIA hardware.

## Remaining directions

UDP publication, dynamic stream creation from publication templates, outbound RTSP/RTSPS push, Basic/Digest publisher or viewer dialects, separate DVB/teletext SDP tracks, long-duration clock/synchronization, mixed-vendor clients and production capacity remain pending. Embedded captions stay on the existing video pipeline; their full publication subtitle matrix is not additional qualification in this stage. Native/HLS/SRT/direct downstream delivery uses the existing shared worker; strict output decoding in this stage is for its common TS, not a new qualification of every downstream protocol.

Protocol references: [RTSP/1.0](https://www.rfc-editor.org/rfc/rfc2326.html), [FFmpeg RTSP](https://ffmpeg.org/ffmpeg-protocols.html#rtsp), [Flussonic configured publication](https://flussonic.com/doc/fms/live/publish/).
