# Elementary RTP with static SDP

Select **Elementary RTP (SDP)** in Streams or Templates for plaintext direct RTP input or destinations. MPEG-TS/PT33 remains the default. Elementary SRTP, SDP negotiation, SAP and WebRTC are pending; selecting a secure elementary combination returns an error. Existing encrypted MPEG-TS/SRTP remains available.

Input requires a literal, concrete IP URL `rtp://IP:PORT` and an absolute **RTP input SDP file** reference. The file must be regular, owned by the daemon user, not writable by group/others, at most 16 KiB and not a symlink. No SDP JSON editing, inline credentials or remote descriptor fetching is needed. The descriptor's first connection and port must match the URL; all tracks use that IP, with distinct RTP ports and their consecutive RTCP ports. Port range is 1024–65520 at the URL. Reserve room through the base port plus 15 for up to eight tracks.

The static RTP/AVP profile accepts one H.264 or single-layer HEVC video track and multiple AAC-LC, MPEG Layer II or MP3 audio tracks, including audio-only streams. There are at most eight tracks. H.264 uses packetization mode 1; HEVC uses single NAL, AP and FU without decoding-order negotiation. AAC uses indexed AAC-LC configuration, fixed 13/3/3 AU headers and noninterleaved complete AUs. MPEG audio uses the RFC 2250 four-byte header and 90 kHz clock. Dynamic types are 96–127; MPEG audio may use static PT14. Separate ports and SSRCs distinguish audio tracks with equal payload types. Unsupported codecs, attributes, cryptography and resource indirection are rejected.

Independent FFmpeg's AAC SDP emitter omits `streamtype`. The parser accepts that omission only after validating AAC-LC configuration and the fixed framing, then supplies audio streamtype 5 to the private decoder. See [FFmpeg's SDP emitter](https://github.com/FFmpeg/FFmpeg/blob/n7.1.1/libavformat/sdp.c) and [RFC 3640](https://www.rfc-editor.org/rfc/rfc3640).

Native sockets validate bounded codec payloads before pinning a media address and SSRC. An optional exact source IP filter and the existing 64-packet jitter/reorder window apply per track. Plaintext RTP source pinning is not cryptographic authentication. IPv4 multicast needs an explicit interface; group-specific binds isolate groups sharing ports. IPv6 unicast is supported, IPv6 multicast is pending.

The worker receives a regenerated SDP containing only validated codec parameters and owned loopback decoder ports. FFmpeg never receives the original addresses, file reference, arbitrary protocol attributes or public sockets. Decoder startup, relay writes and teardown are bounded. This uses the same trusted daemon-host loopback boundary as other private worker bridges. RTCP sender reports reach the private decoder only after matching a pinned media peer and SSRC; valid receiver feedback returns only to that peer's consecutive port.

Each of up to four destinations uses the shared worker's native packetizer, with one RTP/RTCP pair per actual track at base + 2 × track index. There is no per-destination encoder. Random session timestamp origins preserve relative spacing. Related tracks share one RTCP CNAME and a stable media/wall clock mapping, retaining presentation offsets despite send delays. Enabled destinations on the same IP must have non-overlapping reserved ranges (16 ports for elementary, two for MP2T); disabled rows and distinct groups can reuse ports. Rate pacing, datagram bounds, queue-lag detection, cancellation and codec-generation fences apply. Direct RTP/RTCP bytes and IP/UDP overhead contribute to existing uplink telemetry. Kernel sends are not delivery acknowledgements. A receiver-port ICMP refusal is counted as `unreachable_packets`; it does not stop the source or prevent downloading SDP for a late receiver. Other socket errors and queue lag fail the destination visibly.

After active media starts, use **Output → View SDP / Download SDP**. Authenticated GET `/flussonix/api/v1/rtp-sdp/STREAM?destination=0` returns `application/sdp` with `Cache-Control: no-store`. Indices are 0–3. This read does not start a source. Disabled, inactive, changed-target or superseded codec descriptions return conflict instead of stale SDP. A receiver must reload SDP after a worker or codec change. The UI discards descriptions when the worker/generation changes.

For an independent FFmpeg receiver, allow the file/UDP/RTP protocols and open the downloaded SDP. Copy-only remuxing of RTP audio requires `-copyinkf:a`, since depacketizers can omit audio key flags:

```sh
ffmpeg -protocol_whitelist file,udp,rtp -f sdp -i receiver.sdp \
  -map 0 -c copy -copyinkf:a -f mpegts received.ts
```

CPU-transcoded outputs use the same worker path. Internal VAAPI H.264 is opt-in qualified on the available iGPU; hardware HEVC is not qualified on this host. Separate DVB/teletext subtitle tracks need MPEG-TS carriage rather than this AV elementary profile. Embedded CEA carriage follows the existing copy/processing path but this first elementary matrix does not qualify its subtitle combinations.

Qualification uses independent FFmpeg senders and SDP receivers, exact codec/frame checks and strict decode: H.264 and HEVC paired with AAC/MP2/MP3, AAC audio-only, distinct MP2+MP3 tracks, and actual CPU video/audio conversion. Native tests cover malformed/foreign admission, reorder, RTCP, authenticated SDP, zero media epoch, late receivers, stale target/generation, shared workers and reaping. These small localhost fixtures do not establish WAN, sustained throughput or a complete Flussonic direction matrix.
