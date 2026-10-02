# RTSP/RTSPS and RTP/SRTP — inbound and outbound

First-release requirement added by the user. Status: v0.6 implements the TCP playback profile below and exercises an independent FFmpeg RTSP pull roundtrip. The full direction matrix remains required; other cells are designed, not qualified.

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

The shared live queue is bounded to 4096 records/16 MiB, packet length to 1200 bytes, and bootstrap to 32 MiB/100000 records. Video joins begin at a keyframe; audio-only bootstrap rolls over two seconds. A slow client disconnects on lag or a two-second socket-write timeout. Control headers/body/interleaved input are limited to 16 KiB/64 KiB/8 KiB, at most 64 headers and eight queued requests. There are at most 256 control connections. Initial control, media readiness and established keepalive deadlines are 30, 8 and 60 seconds. Shutdown cancels listeners/clients and bounds connection drain. These are safety bounds, not performance qualification.

Viewer URL tokens and existing on_play callbacks run before worker startup with `proto=rtsp` and the remote IP. Existing local limits, callback renewal, grant revocation and source/worker generation guards apply. Worker or codec replacement ends current RTSP playback; clients must reconnect. RTSP egress has separate telemetry; process-mode capacity uses HTTP plus RTSP media bytes. LB-role playback returns 501; native source discovery/private pulls are reused by CDN output.

Independent tests decode two tracks for two concurrent viewers sharing one worker; decode native M4S/M4F source pulls through CDN RTSP output; and decode RTSP input repackaged as HLS. The RTSP stream-copy input adapter allows initial audio packets without key flags (`-copyinkf:a`), as FFmpeg MPEG4-GENERIC depacketization can omit those flags. The input adapter remains independently installed FFmpeg. No official Flussonic package is loaded.

UDP/multicast, RTSP ANNOUNCE/RECORD or outbound publication, Basic/Digest viewer credentials, RTSPS, direct RTP/SRTP, pause/seek and RTSP load-balancer redirects remain separate implementation gates. Interleaved RTP does not establish direct-RTP compatibility. Continuous-session behavior across source changes, camera interoperability, B-frame end-to-end roundtrips, TLS, GPU and production-scale load remain to be qualified.
