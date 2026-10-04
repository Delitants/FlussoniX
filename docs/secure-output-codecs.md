# Secure output and codec contract

Added to the first-release requirements on 2026-10-02 HST. This extends the approved streaming, cluster and transcoding design; it is not a claim that the current preview implements these additions. See [qualification](qualification.md) for measured support.

## Required behavior

FlussoniX must deliver streams securely from its own daemon, without official Flussonic components or a mandatory external TLS terminator. HTTPS covers HLS playlists, TS/fMP4 segments, initialization files, continuous MPEG-TS, M4F signaling/segments and persistent M4S. Secure publication/push must use the same publisher policy as its plaintext counterpart. Secure private source-to-CDN transport and HTTPS LB-to-CDN redirects are required, alongside secure viewer output.

M4F and M4S must receive, relay, originate and publish HEVC/H.265 video, MPEG audio Layer II (`m2a`) and MP3 audio, in addition to H.264 and AAC. Both plaintext and TLS variants must retain codec identity, configuration, timing and original encoded payloads during pass-through. Audio-only, video-only and multiple audio tracks are required. HEVC must also work in every other requested input/output whose container and packetization support it.

Here `m2a` means MPEG audio Layer II, called `mp2` by FFmpeg; MP3 is Layer III. FFmpeg documents `m2a` among its MP2 format aliases. The precise reference M4 codec identifier/configuration remains a fixture verification item; a filename alias does not define native wire grammar. [FFmpeg format aliases](https://ffmpeg.org/ffmpeg-formats.html), [FFmpeg MP2 codec definition](https://www.ffmpeg.org/doxygen/6.1/mpegaudioenc__float_8c_source.html)

## Present boundary

| Capability | v0.10 profile | Required next work |
|---|---|---|
| RTSPS playback | Verified TLS, H.264/AAC-LC, interleaved TCP media | HEVC/MPEG audio packetization; remaining publication/push roles |
| HTTPS stream output | Native TLS listener shares HTTP media/publication/auth routes; HTTPS-only and secure viewer redirect rules | Full private-CA input/cluster trust, secure push, certificate reload/expiry reporting |
| Secure HTTP inputs/private endpoints | Secure aliases exist; full direction/certificate matrix incomplete | Qualify every TLS role, public/private identity and trust configuration |
| M4F/M4S codecs | H.264/AAC end-to-end subset; [native codec library foundation](native-codecs.md) implemented in subsequent source work | Generalized worker bridge and full HEVC/m2a/MP3 metadata/playback in both directions, with native and reference peers |
| CPU/GPU encoding | CPU H.264/AAC exercised; H.264 NVIDIA option hardware-gated | HEVC CPU/GPU profiles and independently selectable audio handling |
| SRTP/SRT protection | Full secure role matrix incomplete | SRTP/SRTCP keying and SRT encrypted roles, independently qualified |

## Secure delivery design

Use the existing Rust/Tokio/rustls stack. Share certificate/key loading and validation with RTSPS rather than routing media through a vendor process. HTTP and HTTPS listeners serve the same application state and authorization handlers; TLS must never introduce a viewer, publisher or peer bypass. Validate certificates/keys and bind every requested listener before starting workers. Bound handshake concurrency, handshake time and shutdown; slow TLS clients must not consume unbounded tasks or block unrelated streams.

The initial HTTPS listener uses startup certificate/key paths, consistent with RTSPS. The Config screen shows the actual listener, TLS status and certificate expiry. It provides labeled fields or accurate startup-only guidance; a saved field must not imply a listener change that has not happened. Private keys and publisher/viewer credentials never appear in discovery or logs. Subsequent certificate replacement must validate new material before activation and drain existing sessions deliberately.

The Cluster screen keeps public delivery, private media and management addresses separate. Configure public HTTPS URLs explicitly for LB redirects; HTTPS-only delivery must exclude plaintext destinations and reject downgrades. Source pulls verify the original hostname/IP and certificate chain, including configured private CAs. TLS identity comes from the advertised endpoint, not a rewritten LAN address. HTTPS playlists and publication URLs must retain the correct scheme, authority and credentials. Forwarded headers are accepted only under an explicitly configured trusted-proxy policy.

RTSPS protects control and interleaved media. SRTP/SRTCP protects datagram media with a separate key/profile contract. SRT encryption uses its own settings. TLS, SRTP and SRT protection do not by themselves implement HLS content encryption or DRM; those remain separately tracked capabilities.

## Transport and codec requirements

Every row includes inbound pull/receive and outbound serving/push where that role exists in the approved direction matrix. A codec claim must name the exact role, container, security mode and tested profile.

| Transport | HEVC requirement | m2a / MP3 requirement |
|---|---|---|
| M4F / M4FS | Native codec metadata, framing, packed segments and preserved encoded frames | Both codecs required, including audio-only streams and sample timing |
| M4S / M4SS | Native persistent framing, bootstrap and packed GOPs | Both codecs required, including audio-only streams and sample timing |
| HLS / HLSS | fMP4 HEVC ingest and playback; preserve HEVC on supported ingest/container paths | Native MPEG audio in compatible packaging; qualify reference endpoints separately |
| TSHTTP / TSHTTPS | MPEG-TS HEVC pull, POST receive, playback and push | MPEG audio framing, track mapping and pass-through |
| SRT | HEVC in its supported media container across encrypted and plaintext roles | MPEG-TS MPEG audio handling and pass-through |
| RTSP / RTSPS | HEVC SDP and RTP packetization across supported client/server roles | Compatible MPEG audio RTP payload mapping and timing |
| RTP / SRTP | HEVC receive/transmit, fragmentation, RTCP and protected media | MPEG audio receive/transmit and protected media |

Use HEVC fMP4 HLS as the interoperable baseline. Flussonic documents HEVC HLS delivery through fMP4. Browser playback must report device/player capability rather than promise universal HEVC or MPEG audio support. Server ingest, relay and independent decoder success are tested separately from the browser player. [Flussonic HLS playback](https://flussonic.com/doc/fms/protocols/hls/)

CPU and supported GPU profiles must expose H.264 versus HEVC explicitly. Audio copy, AAC, MPEG audio Layer II and MP3 are separate choices where the encoder/container permits them. Unsupported profile/device/container combinations fail validation with a clear reason; they must not silently drop audio or convert HEVC to H.264. Template inheritance applies to the independent video/audio settings. Hardware claims require a named GPU/driver/build combination.

## Media implementation boundaries

The current M4 parsers, wire hub, FLV worker adapter and RTP packetizer contain H.264/AAC assumptions. Widening a codec allowlist alone is insufficient. Replace those assumptions with explicit codec descriptions and dispatch while retaining bounded shared buffers and one media worker per active stream.

- HEVC: preserve VPS/SPS/PPS, decoder configuration and parameter-set revisions; validate NAL lengths/types, keyframe/access-unit boundaries, signed composition offsets and Main/Main10 fixture profiles. Container-specific sample entries and Annex B conversions must match the actual output.
- MPEG audio: validate Layer II versus Layer III, version, sample rate, channel mode, frame size and samples per frame. Audio timestamps and duration must use the observed frame header; no AAC AudioSpecificConfig or fixed AAC sample-count shortcut.
- M4F/M4S: independently specify reference codec tags and metadata, packed GOP/sample tables, signed timing, audio-only segment boundaries, late-join bootstrap and track changes before enabling new tags. Preserve original segment/wire bytes where relaying promises identity.
- Worker transport: replace or extend the AVC/AAC-only FLV bridge with an independently specified encoded-frame channel that can carry every required codec. Choose the framing only after checking the available FFmpeg interfaces. Separate binary media from diagnostics; no undocumented enhanced-FLV assumption or new vendor dependency.
- RTP: implement the codec-specific HEVC/MPEG audio payloads and SDP; share this packetizer with the future SRTP adapter. Do not label an HTTP redirect or MPEG-TS tunnel as native RTP support.

## Qualification and order

1. Implement and qualify native HTTPS output/publication, authorization and secure LB/private source routing while retaining the existing H.264/AAC baseline.
2. Establish HEVC/m2a/MP3 native wire fixtures and worker representation; implement M4F/M4S decoding, relay and originating paths. Keep codec framing uncertainty visible until fixtures resolve it.
3. Extend compatible HLS, MPEG-TS, SRT and RTSP/RTP directions, plus explicit CPU/GPU profiles. Track unimplemented directions separately from codec work.
4. Publish measured capability rows and bounds; full first-release acceptance requires the complete requested secure transport/codec matrix.

For M4F and M4S, test H.264 and HEVC with each of AAC, m2a and MP3, plus video-only, each audio-only codec and multiple audio tracks. Repeat for plaintext/TLS and Flussonic-to-FlussoniX, FlussoniX-to-Flussonic, native and chained relay in every required role. Fixtures include B-frames, Main/Main10 where applicable, track/configuration changes, restart, late join, malformed lengths, jitter and source outage. Independently decode outputs and compare encoded sample/segment hashes, DTS/PTS/duration and A/V synchronization. Reference use stays confined to read-only research or explicitly authorized isolated labs.

TLS tests cover valid chains, private CA, wrong hostname, expired/untrusted certificates, mismatched key, missing files, busy ports, slow handshake, disconnect/shutdown and plaintext downgrade. Repeat token denial/revocation, publisher renewal and clean cluster-ticket redemption over HTTPS. Test LAN source pulls and public LB redirects independently. Capacity qualification measures handshake cost and sustained secure fan-out without changing production Flussonic listeners.

This sequence is implementation order, not removal of any first-release requirement. No migration is claimed ready until the measured matrix covers the actual migration servers and stream profiles.

## Configured native private CA increment

Explicit M4FS/M4SS Stream/Template inputs now share the RTSPS absolute PEM trust option, certificate bounds and identity verification. Owned TLS tests exercise HEVC/Layer II native text preservation and both HLS subtitle formats over protected HTTPS, including certificate rejection before application data and origin-scoped redirects. These checks cover configured inputs rather than cluster discovery/management trust or every codec/direction combination. GPU qualification, secure push and the remaining migration matrix stay open.

## Cluster private CA increment

Configured source/peer management HTTPS requests now use optional private roots with identity verification and no redirects. Private source pulls inherit that trust or use a separate media bundle, including HLS, continuous MPEG-TS, M4F and M4S. Owned source/CDN/LB HTTPS tests verify admission, protected delivery, worker coalescing, FFmpeg decoding and exact native M4F segment bodies. This qualifies the configured native cluster flow; secure push, mutual TLS, automatic certificate rotation and broader vendor/codec/direction matrices remain open.


## Direct HTTP input trust increment

Direct HLSS, TSHTTPS and raw HTTPS input now pass through the verified origin-scoped Rust fetcher. Optional private roots persist on Stream/Template inputs and share the existing friendly CA field. Owned copy/CPU H.264/AAC delivery, fMP4 HLS resources, arbitrary TS paths, TLS rejection before HTTP, same-origin redirect bounds and fallback recovery are qualified independently of cluster credentials. Cross-origin authenticated HLS, active certificate/trust reload, GPU and the remaining direction/codec matrix stay open; see [the input profile](https-delivery.md).
