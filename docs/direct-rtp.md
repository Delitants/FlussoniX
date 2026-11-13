# Direct RTP transport

FlussoniX receives and sends MPEG-TS over RTP, RFC 2250 MP2T, static payload type 33 and a 90 kHz transport clock. This is a native Rust UDP adapter; it does not load or execute Flussonic components. FFmpeg handles the independent media processing pipeline. H.264, HEVC, AAC, MPEG Layer II and MP3 can be carried in TS, including audio-only sources. A separate [elementary RTP/static SDP profile](elementary-rtp.md) now supports native AV payloads and dynamic types; SDP negotiation, SAP and WebRTC remain pending. RTSP remains a separate protocol implementation.

Configure a stream or template's input with `rtp://IP:PORT`. The IP must be literal; port range is 1024–65534, and the next port belongs to RTCP. Optional input settings in `flussonix_rtp` are `source_ip` (exact source IP), `jitter_ms` (0–1000; default 50), `interface` (IPv4 local interface) and `ttl` (1–255; default 16). IPv4 multicast requires an explicit interface. IPv6 unicast is supported; IPv6 multicast is excluded. URLs do not accept credentials, paths, fragments or queries.

Use the normal Input URL field in the admin screen. RTP inputs reveal interface, source-filter and jitter fields. Under RTP and SRTP destinations choose Override destinations, enter each receiver URL and set pacing, interface and TTL as needed. Template destinations inherit as one list; a stream can override the list or select No destinations to clear it. No JSON entry is required.

The API extension `flussonix_rtp_outputs` accepts up to four destinations. Each row contains `url`, optional `disabled`, optional `max_mbps` (1–10000; default 100), and optional `flussonix_rtp` with interface and TTL. Receive-only settings are rejected on destinations. Each enabled destination subscribes to the stream's single processed TS worker. An enabled destination sustains an on-demand worker; a publication still waits for its publisher. A configuration change replaces the worker generation and starts new SSRC/sequence/clock identities.

Output uses at most seven TS packets per datagram and flushes short aligned payloads after 10 ms. A bounded rate pacer, bounded broadcast subscription, cancellation and two-second write limits prevent a blocked destination from retaining a worker indefinitely. Invalid framing or queue overrun fails that destination; it does not silently discard part of a TS packet. Destination failures appear in stream status. An RTP sender has no delivery acknowledgement; packet counters confirm sends, not viewer reception.

A configured MP2T destination remains active when a unicast receiver temporarily refuses UDP traffic. Refused media/report attempts increment `unreachable_packets`; status becomes `receiver_unavailable`, then returns to `sending` on a successful media send. The sender consumes and discards refused packets without saving or retransmitting old media. Attempts remain paced, the socket and SSRC stay stable, and cancellation still closes the destination. Other socket errors, stalled writes, invalid framing and queue overrun remain terminal. Send counters include only successful socket sends; they do not confirm receipt.

Input validates RTP header versions, CSRC/extension/padding bounds, PT33 and complete TS packets. It pins the first valid media source address and SSRC for the generation, enforces an optional IP filter and reorders at most 64 packets. Gaps are released after the configured jitter delay; loss and duplicate counters remain visible. RTCP uses the consecutive port: periodic compound sender/receiver reports and SDES; feedback is sent only to the pinned media peer. Plain RTP does not authenticate a sender: use a source filter, protected network or SRTP.

Original DVB and teletext track preservation follows Original subtitle tracks. Embedded CEA captions follow the existing copy/transcoding path. HLS subtitle conversion, pass-through and filtering remain independently controlled by HLS subtitles; RTP carries the processed TS output, not the separately filtered HLS variant.

Independent FFmpeg integration tests qualify codec identities and decoded frames, not only successful subprocess exit. Live receivers require a keyframe and normal probing time after joining. Qualification captures span six seconds, since a two-second late-join capture can contain audio before usable video parameter sets. A synthetic low-resolution qualification is not a WAN or sustained-throughput benchmark. Internal CPU encoding is tested separately from externally Intel VAAPI-encoded sources; transport acceptance alone is distinct from the separately qualified [internal VAAPI worker](vaapi.md).

Direct destination bytes (including RTP/SRTP headers, RTCP and IP/UDP overhead) are included in process-mode uplink admission measurements as `direct_rtp_egress_mbps`, `direct_rtp_bytes_out` and total `media_egress_mbps`. Interface mode continues to measure the selected NIC. For mixed LAN pulls and public deliveries, choose the appropriate public NIC rather than equating all process traffic with public uplink use.

On the qualification host, an independently extracted Intel iHD driver produced H.264 VAAPI media that passed the direct RTP receive/copy/transmit path and independent strict decode/frame counts. HEVC VAAPI has no usable HEVCMain encoding entrypoint with this host/driver, including low-power mode, and is not GPU-qualified. CPU-generated HEVC is qualified. This is a codec/transport test at 160×96, not a capacity claim; internal encoding has separate [VAAPI qualification](vaapi.md).

A unicast input's interface setting must match its local bind address, or may restrict `0.0.0.0` to a concrete local IPv4 address. Multicast interface values cannot be wildcard, multicast or broadcast addresses. Multicast sender RTCP stays directed to the group; unicast receiver feedback is accepted on its source RTCP port and pins one valid participant (authenticated for SRTP). Multiple multicast receivers can consume media, while feedback counters summarize only that pinned participant.

Multicast inputs bind their configured group address, so distinct groups may share RTP/RTCP ports and unrelated unicast cannot claim a multicast source. The parser checks TS adaptation occupancy and every flag-directed field length before source ownership or decoder writes. SDES feedback requires terminated, bounded, correctly aligned item lists.

## Control-channel recovery qualification

On Linux with libsrtp2 available, run the owned loopback qualification explicitly:

```sh
cargo test --locked --test direct_rtcp_recovery -- --ignored --test-threads=1 --nocapture
```

These two bounded tests keep RTP receiving while closing only the RTCP receiver,
confirm a real pending ICMP error on the sender's own socket, and require a counted
RTCP send refusal followed by media and control recovery. They also check retained
SRTP/SRTCP authentication and replay state, cancellation, and socket release.
They use synthetic null TS packets, so they do not qualify decoded media or throughput.

The sender's report and error-receive branches race to consume the ICMP error.
The tests fail if no send refusal is observed within eight attempts or the
90-second case deadline. They are excluded from default CI because this finite
probe cannot guarantee which branch wins. An observed SRTCP index gap does not
identify the exact index of a refused packet. Fresh-receiver rollover setup
remains subject to the limits in [the SRTP profile](direct-srtp.md).
