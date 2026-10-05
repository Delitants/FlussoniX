# RTSP/RTSPS and RTP/SRTP — inbound and outbound

First-release requirement added by the user. Status: v0.8 adds encrypted RTSPS TCP playback and strictly verified RTSPS pull to v0.7 TCP and opt-in unicast UDP playback. The full direction matrix remains required; other cells are designed, not qualified.

## Direction matrix

Direction describes media flow relative to FlussoniX, independently of which side initiates the control connection.

| Protocol | Inbound | Outbound |
|---|---|---|
| RTSP | Pull from an RTSP source; accept RTSP publication | Serve RTSP playback; push publication to another RTSP server |
| RTSPS | Pull and accept publication with TLS-protected control | Serve playback and push publication with TLS-protected control |
| RTP | Receive direct RTP streams, including unicast and multicast | Transmit direct RTP streams to unicast and multicast destinations |
| SRTP | Receive protected media and SRTCP control | Transmit protected media and SRTCP control |

All directions connect to the shared media timeline, source failover, authorization, statistics and CPU/GPU transcoding. Each direction is a release gate; successful input support alone does not satisfy the requirement.

## RTSP and RTSPS

Implement both client and server roles: session setup, SDP, per-track transport selection, playback, keepalive, teardown, and the RTSP 1.0 publication flow using ANNOUNCE/SETUP/RECORD. Support Basic/Digest authentication and the reference's applicable URL-token handling. Match reference status codes, session/CSeq handling, track control URLs and reconnect behavior. [RTSP 1.0](https://www.rfc-editor.org/rfc/rfc2326.html)

RTSP 1.0 interoperability is the initial baseline; record any required RTSP 2.0 capability separately. The installed configuration's `rtsp2://` alias does not by itself prove RTSP protocol version 2.0.

Support UDP RTP/RTCP and TCP-interleaved media in both client/server roles. For RTSPS, validate the TLS peer identity and allow configured trust roots. Interleaved media is protected by its TLS connection; separately transported UDP media requires SRTP if media encryption is selected. The session profile must explicitly describe control protection and media protection. [RTSP 2.0 transport and security](https://www.rfc-editor.org/rfc/rfc7826.html)

A reference peer may expose only some publication/transport combinations. Implement the requested FlussoniX roles, and report tested peer combinations precisely instead of asserting universal reference support.

## RTP and RTCP

Implement shared packetization/depacketization for RTSP-controlled sessions and direct RTP streams. Direct RTP requires an SDP description or explicit payload-type, codec, clock-rate and track configuration; a bare destination address is insufficient for arbitrary dynamic payload types.

Maintain per-source SSRC and sequence state, bounded reorder/jitter buffers, timestamp rollover handling, loss counters and RTCP sender/receiver reports. Map RTP timestamps to the encoded timeline and use sender reports for available wall-clock synchronization. Transmit with bounded pacing and MTU-aware packetization. [RTP/RTCP](https://www.rfc-editor.org/rfc/rfc3550.html)

Initial codec coverage follows the server's H.264/H.265/AAC core; add camera audio payload profiles as explicit tested capabilities. Unicast/multicast configuration includes bind interface, destination, RTP/RTCP ports and multicast scope. Raw RTP does not create an HTTP-style token exchange: authorization must bind an approved configured or negotiated session to its media endpoint.

## SRTP and SRTCP

Use libsrtp behind a narrow Rust adapter for protected RTP and RTCP. Maintain independent inbound/outbound key contexts, packet indices and replay windows; authenticate before releasing received media. Rekeying and restart must prevent reuse of a key/packet-index combination. [SRTP](https://www.rfc-editor.org/rfc/rfc3711.html), [libsrtp](https://github.com/cisco/libsrtp)

The first-release keying plan includes explicitly provisioned keys and SDP Security Descriptions over protected signaling. Add DTLS-SRTP negotiation as a separate interoperability profile with its own acceptance gate; support it before advertising that profile. A full WebRTC signaling/ICE stack remains outside this addition. [SDP Security Descriptions](https://www.rfc-editor.org/rfc/rfc4568.html), [DTLS-SRTP](https://www.rfc-editor.org/rfc/rfc5764.html)

Expose key references, negotiated protection profile and lifecycle status without disclosing key material. Standalone SRTP settings are FlussoniX extensions until an equivalent reference contract is verified. SRT and SRTP have separate transport adapters.

## Reference evidence and API mapping

The installed public schema contains:

- `stream_input_rtsp` with `rtsp://`, `rtsps://`, `rtsp-udp://` and `rtsp2://` patterns.
- RTSP input options `rtp: udp` and `wait_rtcp`.
- `stream_input_rtp` with an `rtp://` pattern.
- `listen_rtsps_config` composed from listener and TLS listener settings.

No SRTP-named component or SRTP text was found in that public schema. This is a documentation gap, not proof that every Flussonic component lacks SRTP. Do not invent a legacy `srtp://` configuration contract or add fictitious REST operations to the reference inventory.

Expose verified RTSP/RTP settings through Streams, Templates and Config. Put additional settings in the existing FlussoniX extension namespace. Auth and Cluster must track session ownership, ports, transport capabilities and reconnect behavior.

## Acceptance

Exercise all direction-matrix cells with independent lab peers, with and without transcoding. RTSP cases cover UDP, interleaved TCP, TLS, credentials, multitrack SDP, on-demand startup, publication and teardown. RTP cases cover unicast/multicast, bounded jitter, packet loss/reordering, clock changes and packet pacing.

SRTP cases cover both directions, SRTCP, agreed keying/profile combinations, rekey/restart, wrong-key rejection and replay handling. Verify that selecting encrypted UDP media never silently produces plaintext output. Measure per-session encryption cost in the fan-out benchmark.

## Implemented v0.6 TCP playback profile

An optional `--rtsp-listen ADDRESS:PORT` starts a separate listener; it is disabled by default. RTSP 1.0 OPTIONS/DESCRIBE/SETUP/PLAY/GET_PARAMETER/TEARDOWN support live TCP-interleaved RTP/RTCP. Explicit decimal-zero and `now` live ranges are accepted; seeking and PAUSE are not implemented. Session and channel pairs are bound to one authorized stream. Queryless track/control URLs retain that connection's authorized identity; an explicit changed query is rejected. Management Basic/Bearer credentials do not authorize viewers.

One packetizer per worker produces H.264 single NAL/FU-A and AAC-LC MPEG4-GENERIC, including fragmented AAC access units. H.264 AVCC length widths 1/2/4, SPS/PPS in SDP, signed composition offsets, clock/sequence wrapping, atomic late-join snapshots and RTP-Info are supported. At most one H.264 and one AAC-LC track are accepted. HEVC, HE-AAC, camera audio variants and vendor-specific SDP/control behavior remain unqualified. RTCP sender reports use shared media/wall-clock mapping and actual per-client packet/payload counts. Malformed codec metadata or access units suppress this RTSP profile; existing native relay is not rewritten.

The shared live queue is bounded to 4096 immutable access-unit or packed-GOP batches/64 MiB, packet length to 1200 bytes, and bootstrap to 32 MiB/100000 records. Producer batches larger than 64 MiB invalidate the RTSP profile. Per-viewer iteration slices shared packet bytes. Bootstrap overflow delays new video joins until a fresh keyframe while established playback continues. Video joins begin at a keyframe; audio-only bootstrap rolls over two seconds. A slow client disconnects on lag or a two-second socket-write timeout. Control headers/body/interleaved input are limited to 16 KiB/64 KiB/8 KiB, at most 64 headers and eight queued requests. There are at most 256 control connections. Initial control, media readiness and established keepalive deadlines are 30, 8 and 60 seconds. Shutdown cancels listeners/clients and bounds connection drain. These are safety bounds, not performance qualification.

Viewer URL tokens and existing on_play callbacks run before worker startup with `proto=rtsp` and the remote IP. Existing local limits, callback renewal, grant revocation and source/worker generation guards apply. Worker or codec replacement ends current RTSP playback; clients must reconnect. RTSP egress has separate telemetry; process-mode capacity uses HTTP plus RTSP media bytes. LB-role playback returns 501; native source discovery/private pulls are reused by CDN output.

Independent tests decode two tracks for two concurrent viewers sharing one worker; decode native M4S/M4F source pulls through CDN RTSP output; and decode RTSP input repackaged as HLS. The RTSP stream-copy input adapter allows initial audio packets without key flags (`-copyinkf:a`), as FFmpeg MPEG4-GENERIC depacketization can omit those flags. The input adapter remains independently installed FFmpeg. No official Flussonic package is loaded.

UDP/multicast, RTSP ANNOUNCE/RECORD or outbound publication, Basic/Digest viewer credentials, RTSPS, direct RTP/SRTP, pause/seek and RTSP load-balancer redirects remain separate implementation gates. Interleaved RTP does not establish direct-RTP compatibility. Continuous-session behavior across source changes, camera interoperability, B-frame end-to-end roundtrips, TLS, GPU and production-scale load remain to be qualified.


## Implemented v0.7 UDP playback profile

The v0.6 codec, authentication, worker, bootstrap and control bounds still apply. An explicit `--rtsp-udp-ports FIRST-LAST` enables RTSP-controlled unicast RTP/RTCP; otherwise UDP SETUP returns 461. Every port is prebound to the RTSP listener IP before workers start. The range is inclusive, 2–256 ports, even first/odd last, all at least 1024. Occupied pools fail startup; exhaustion returns 453 while TCP remains available. Pool sockets remain unconnected so wildcard listeners can reuse them across interfaces; each send uses the negotiated endpoint. Reuse and pre-PLAY retargeting must observe empty kernel queues within a bounded 65-read budget per socket; a larger queued burst rejects that attempt, and a retry continues draining without accepting stale traffic. Each negotiated track owns a consecutive pair until teardown, control EOF, cancellation or shutdown. Before PLAY, repeating SETUP for the same track can change its client pair without acquiring another lease. TCP and UDP cannot mix in one session.

UDP offers accept RTP/AVP or RTP/AVP/UDP with explicit unicast, consecutive even/odd client ports >=1024, and optional PLAY mode. Multicast, destination/source overrides, mux, RECORD, ambiguous alternatives and unknown parameters are rejected. The destination is always the authenticated TCP control peer's IP. Replies advertise client/server ports, source address and SSRC. Incoming RTCP must come from that exact negotiated endpoint, fit 8192 bytes, and be a structurally valid compound RR plus SDES/CNAME reporting the negotiated SSRC; stale, malformed and foreign reports do not renew the session. No per-viewer receive task survives the connection.

Shared RTP records retain decode timestamps outside the unchanged wire bytes. Each UDP session holds at most one pending packet and paces it using decode time plus a token bucket: `--rtsp-udp-mbps` is finite 1–10000 Mbps (default 100) with a 32 KiB burst. Control, grant revocation, worker replacement and shutdown remain responsive during paced waits and RTCP flood. Successful application datagram bytes increment viewer accounting and total RTSP output; `rtsp_udp_bytes_out` reports the UDP subset and is never added twice to capacity. UDP send deadlines remain two seconds. Scheduling more than 30 seconds ahead closes the session as a timestamp discontinuity; the regular connection check also detects queue eviction while a packet is pending. This is payload pacing, not an Ethernet/IP bandwidth guarantee or a loss-recovery protocol.

RTSP input defaults to TCP. The normal input form selects TCP or UDP; UDP is saved as `{"url":"rtsp://...","rtp":"udp"}` and inherited through templates. Changing the URL to another protocol clears the option. Independent FFmpeg UDP clients decode H.264 and AAC together; UDP input is repackaged to independently decoded HLS, and native M4S/M4F private CDN pulls feed UDP output without an additional encoder.

RTSPS, direct RTP, SRTP, RTSP publication/push, multicast, Basic/Digest viewer authentication, RTSP LB redirects, retransmission and migration/large-scale performance qualification remain pending.


## v0.8 encrypted TCP profile

The optional RTSPS listener uses Rustls/ring TLS 1.2/1.3, with separate PEM chain/key CLI flags. Parsing, chain size (1..16 certificates), key matching and port binding complete before static worker startup. TLS handshakes consume the existing 256-connection-per-listener permits and expire after eight seconds. Cancellation interrupts the handshake and drains listener tasks. All existing RTSP framing, per-session channel/path/token binding, viewer authorization, revocation, media readiness, write deadlines and queue bounds apply. Authorization retains `proto=rtsp` for callback compatibility. No client certificate is required. UDP transport returns 461 on TLS; plaintext RTSPS URI requests are rejected. RTSP URI aliases inside an already encrypted connection support the input bridge.

RTSPS input validates the actual forwarded TLS connection against bundled Mozilla roots or an explicitly configured replacement CA store. CA PEMs are absolute regular-file paths, bounded to 1 MiB and 128 certificates; malformed, missing, untrusted, expired and wrong-name certificates fail closed. Original DNS/IP identity supplies Rustls verification and SNI where applicable. Connect plus handshake expire after ten seconds, before any application request or credential is forwarded. The worker reserves a single-client loopback endpoint, then performs remote setup in its abortable task. No application bytes are forwarded until verification succeeds. After setup, acceptance expires after eight seconds if no decoder arrives; the total pending endpoint lifetime is bounded by setup plus acceptance deadlines. Fixed copy buffers apply backpressure. Its guard aborts on failed startup or dropped prepare, and worker shutdown aborts and joins the bridge. Input statistics retain `rtsps`, while total RTSP egress includes encrypted playback media.

The independent FFmpeg decoder checks encrypted playback interoperability; its RTSP demuxer does not expose private TLS trust options through ordinary input CLI flags. Product input always performs verification in Rustls. Local owned tests use independent OpenSSL certificate generation and FFmpeg 7.1.1 (GnuTLS); peer versions are recorded separately in qualification. No official Flussonic runtime, codec library, asset or Erlang code is linked, copied or required.

Qualified codec scope remains H.264 and AAC-LC. ANNOUNCE/RECORD/publication, push, Basic/Digest viewer authentication, separately encrypted UDP/SRTP, direct RTP/SRTP, RTSP LB redirection, exact vendor authority/digest dialects and scale/GPU/migration qualification remain open. Upstream 3xx redirects are rejected rather than followed. A bounded frame guard validates RTSP responses (16 KiB headers, 64 KiB bodies, at most 64 unique headers) and interleaved packets (8 KiB), preserves body/media bytes and rejects ambiguous lengths before forwarding control bytes. Setup failures retain normal worker retry metadata and advance ordered fallbacks; remote waits do not hold the global worker map. URI authority-sensitive third-party servers may require a future adapter; successful local/source/CDN playback does not establish universal RTSPS dialect compatibility.

## HEVC playback increment

The shared packetizer now serves one H.264 or HEVC video track, with an optional AAC-LC audio track, over existing RTSP TCP/unicast UDP and RTSPS TCP listeners. Native copy input supplies original HEVC access units; no HEVC encoder is required for this playback path. SDP carries H265/90000 and original VPS/SPS/PPS. Single-layer HEVC uses single-NAL packets or fragmentation units in decode order, with no DONL; packet size stays at1200 bytes. PTS supplies the RTP clock while DTS supplies pacing. See [RFC7798](https://www.rfc-editor.org/rfc/rfc7798).

HEVC configuration is capped at64 KiB, access units at16 MiB and4096 NALs. Length widths1/2/4 are supported. Forbidden bits, zero temporal IDs, nonzero layer IDs, nested RTP payload types, malformed lengths and incomplete parameter sets fail the RTSP profile before an access unit is published. Existing queue/bootstrap, source generation, authorization, revocation, UDP lease and shutdown limits remain applicable.

Owned M4S Main8-bit B-frame sources qualify video-only TCP, HEVC/AAC TCP and UDP, and HEVC/AAC through a verified RTSPS bridge into an independent FFmpeg decoder. All decoded picture hashes belong to the independently decoded12-picture source fixture; audio is independently decoded too. Source-token and viewer-token checks remain separate, and denied viewers start no worker or source request. Repeated requests share one source connection/worker. HEVC session revocation closes plaintext/TLS media and releases viewer ownership; immediate TLS revocation may close without close_notify.

This increment does not qualify Main10, layered HEVC, long-duration A/V synchronization, HEVC encoding or non-native native-wire origination, parameter-set adaptation without reconnection, MPEG audio RTP, RTSP publication/push, direct RTP/SRTP, mixed-vendor sessions, GPU or production capacity. M4F shares the native-copy hub but is not a new live HEVC RTSP source qualification in this increment.
