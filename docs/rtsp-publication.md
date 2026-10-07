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

These examples bind loopback; choose the required interface for external publishers. Listener options accompany the existing management credentials, configuration and media directory options. The receive form displays the actual bound listener addresses. Wildcard listeners use the browser hostname for the displayed URL. Disabled listeners do not produce invented URLs. Generated URLs do not contain stored passwords. Configure the publisher password in the publishing client using URL userinfo or the existing password query parameter.

To accept unicast UDP publication, enable the same prebound port pool used by UDP playback:

```sh
flussonix --rtsp-listen 127.0.0.1:18554 --rtsp-udp-ports 24000-24015
```

The range must contain 2–256 ports, begin with an even port, end with an odd port, and stay above 1023. Each published track holds one pair until its session ends; playback and publication compete for the same pool. Exhaustion returns RTSP 453 without starting a worker. The receive form shows UDP availability only when this pool is enabled. `--rtsp-udp-mbps` limits outbound playback traffic, not received publication traffic. External publishers need a usable route and firewall rules for these UDP ports.

## Publish

A standalone FFmpeg example for an owned file:

```sh
ffmpeg -re -i owned-source.mp4 -map 0:v:0 -map 0:a? -c copy \
  -f rtsp -rtsp_transport tcp \
  'rtsp://127.0.0.1:18554/channel?password=YOUR_PUBLISHER_PASSWORD&token=YOUR_TOKEN'
```

For a plain RTSP listener with the pool enabled, replace `-rtsp_transport tcp` with `-rtsp_transport udp`. Each client must send from the exact RTP/RTCP ports it negotiated, on the same IP as its control connection. Address/port rewriting by NAT is unsupported; use TCP across such a boundary.

The source codecs must fit the receiving profile below. A TLS publisher uses the displayed `rtsps://` URL and must verify the server certificate and name with the appropriate trust store. The qualification fixture uses an independent FFmpeg muxer through an owned Rustls relay that verifies its owned CA and server name before forwarding control/media; this is not a claim that every FFmpeg build forwards custom RTSP TLS trust options.

A query password is checked against the effective stream/template publisher password. Viewer/admin/peer credentials do not bypass this check. `on_publish` receives JSON with `proto: "rtsp"`, socket IP, name, query/token, session ID, received media bytes, duration and monotonic request number. This logical protocol is unchanged on TLS; worker statistics distinguish `rtsp` and `rtsps`. Initial and renewing callback decisions use the existing bounded publisher policy. A changed policy or media configuration revokes pending and active sessions; title-only edits retain them. LB nodes reject receiving publications.

## Receiving profile and bounds

- Interleaved RTP/AVP/TCP or explicitly enabled unicast RTP/AVP UDP. All announced tracks must negotiate distinct RTP/RTCP channels or client port pairs before RECORD. A publication cannot mix TCP and UDP tracks, switch transport after initial SETUP, or reuse another track’s client endpoints. UDP requires consecutive even/odd client ports above 1023; multicast and destination overrides are rejected. RTSPS rejects UDP, preserving encrypted media. Playback and publication cannot share a connection. Only live zero/now ranges are accepted.
- At most one H.264 or HEVC video track and up to eight total video/audio tracks. H.264 requires packetization mode 1; HEVC DON reordering is unsupported. AAC-LC uses MPEG4-GENERIC/AAC-hbr; MPEG audio uses MPA (static PT14 or a validated dynamic payload).
- SDP is bounded to 16 KiB. Only codec metadata and bounded controls are accepted. Advertised connection addresses/ports are never used as network destinations: the worker receives regenerated private loopback SDP. Every track retains its validated payload mapping and first valid RTP SSRC. Unsupported codecs, negotiation and external control resources fail explicitly.
- RTSP framing bounds interleaved messages to 8192 bytes; UDP publication accepts datagrams up to 8192 bytes using an additional overflow byte to reject oversized packets. Other direct RTP profiles retain their existing bounds. Foreign IPs/ports and oversized UDP datagrams are discarded without binding media identity, counting bytes or renewing media progress. Malformed admitted media closes the publication. Pre-RECORD UDP queues are drained after decoder startup; more than 64 queued packets per socket cause RECORD to fail closed. Readiness is polled across tracks in rotating order, with cancellation, control and policy timers prioritized. Every receive attempt, including discarded traffic, consumes a cooperative runtime budget so continuously ready queues also yield to other tasks. RTCP is additionally limited to the existing validated compound profile (2048 bytes). Every SR/RR/APP sender, SDES chunk and BYE source in a compound must share the track’s media identity; report-block identities are not treated as senders. One pending sender-report compound may precede the first RTP packet; all its sources must agree before retention, and it is forwarded only if they match that packet. The native relay sends receiver reports every five seconds on the negotiated TCP RTCP channels or UDP RTCP socket/peer, with per-track source identity, sequence/loss/jitter and sender-report timing. These describe relay packet reception. Statistics allow 50 ms of reordering and at most 64 empty sequence markers; media is forwarded directly to the decoder’s jitter handling. Valid private decoder feedback is also forwarded when present.
- The shared 64-publisher admission limit includes pending ANNOUNCE sessions. No worker starts until RECORD. Private decoder ports are reserved during preparation, then released just before decoder startup. Readiness matches the actual decoder process socket descriptors to the Linux socket table without binding probe sockets; reservation inodes are excluded even during process handoff, and other local sockets cannot make RECORD succeed early. Loopback is the same trusted local decoder boundary used by direct RTP input.
- Pending inactivity expires after 30 seconds. Active media silence respects the configured 1–300 second input timeout, independently of control/RTCP traffic. The private SDP decoder uses the same bounded timeout. Worker watchdogs also require decoded output. Disconnect, teardown, policy change, malformed media or worker failure cancel and reap only the owning generation.

## Qualification

`tests/rtsp_publication.rs` uses unused local listeners and independently generated FFmpeg publishers. It probes codec multiplicity and at least 20 decoded frames per stream, then strictly decodes the whole recorded shared TS with empty decoder diagnostics. The matrix includes H.264/HEVC paired with AAC/MP2/MP3, AAC-only and two distinct MPEG audio tracks, CPU HEVC with MP2 audio, and verified TLS HEVC/MP3. The independent UDP matrix also includes eight MPEG audio tracks and CPU HEVC/MP2; wire tests cover opt-in negotiation, disjoint tracks, foreign IPs/ports, oversized/early datagrams, RTCP return endpoints, pool exhaustion/reclamation, pre-RECORD queue overflow and callback byte accounting/renewal denial. Admission tests cover wrong credentials, callback denial/renewal, policy mutation during callback and decoder startup, URL/session/channel binding, large bounded TCP packets, malformed packets, SSRC substitution, media stall, exclusive HTTP ownership and reconnect.

```sh
cargo test --locked --test rtsp_publication -- --test-threads=1
```

To retain owned TS clips and metadata, set `FLUSSONIX_RTSP_RECORD_DIR` to a private local directory. The explicitly ignored `vaapi_publication_transcode_strictly_decodes` test requires H.264 VAAPI hardware and a scoped driver environment. It does not qualify GPU HEVC or NVIDIA hardware.

## Remaining directions

Dynamic stream creation from publication templates, outbound UDP RTSP push, inbound Basic/Digest publisher or viewer dialects, separate DVB/teletext SDP tracks, long-duration clock/synchronization, mixed-vendor clients and production capacity remain pending. Embedded captions stay on the existing video pipeline; their full publication subtitle matrix is not additional qualification in this stage. Native/HLS/SRT/direct downstream delivery uses the existing shared worker; strict output decoding in this stage is for its common TS, not a new qualification of every downstream protocol.

Protocol references: [RTSP/1.0](https://www.rfc-editor.org/rfc/rfc2326.html), [FFmpeg RTSP](https://ffmpeg.org/ffmpeg-protocols.html#rtsp), [Flussonic configured publication](https://flussonic.com/doc/fms/live/publish/).


## Incoming publisher Basic and Digest authentication

The existing effective stream/template **Publisher password** also authorizes
RTSP/RTSPS ANNOUNCE with preemptive Basic or a legacy MD5 Digest challenge.
For example, an independent FFmpeg publisher can use
`rtsp://publisher:YOUR_PASSWORD@HOST:PORT/channel?token=YOUR_TOKEN` instead of a
password query. Percent-encode special userinfo characters. The nonempty ASCII
username (up to256 bytes, without a colon) is a client label, not a configured
user account; the password remains stream-wide and separate from management,
viewer and peer policy. Passwords retain the existing1024-byte limit.

Protected streams without a password query receive RTSP401 with a Digest
challenge. This legacy receiving profile uses MD5 with qop omitted; it does not
advertise or accept auth-int, qop-auth, session algorithms, SHA-256 or userhash.
Basic is accepted preemptively, while the automatic challenge selects Digest.
Use RTSPS to encrypt both control and interleaved media. TLS certificate trust
remains a publisher responsibility.

Each random nonce belongs to one control connection and exact original ANNOUNCE
URI. Successful admission consumes that negotiation; later methods require the
admitted connection and Session. A nonce from another connection, changed URI,
realm/method substitution, malformed/duplicate parameters or wrong password
cannot authorize publication. Three challenges at most and one absolute30-second
negotiation deadline bound failed retries. An explicit wrong query password
retains403; combined password query and Authorization returns400.

Password checks precede SDP admission, callbacks, publisher leases and worker
startup. Each retry reads current effective policy. Existing on_publish callback
and renewal, revocation, exclusive ownership and RECORD startup rules still
apply. Header credentials are absent from callback metadata and diagnostics.
An unprotected stream stays unprotected; supplying a username does not create
an account. No new JSON or account controls are required.

Owned wire clients qualify Basic admission/session binding, inherited passwords,
policy edits, callback denial/renewal and credential privacy. Independent FFmpeg
Digest publishing is strictly decoded through TCP, unicast UDP and a verified
owned TLS relay. Other receiving authentication algorithms, viewer Basic/Digest,
arbitrary camera/recorder dialects and production capacity remain unqualified.
