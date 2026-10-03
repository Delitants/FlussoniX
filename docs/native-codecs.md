# Native codec foundation

The native media library now represents H.264, HEVC, AAC, MPEG audio Layer II (`m2a`) and Layer III (`mp3`). This is preparation for end-to-end codec support. The v0.10 daemon's worker bridge and RTSP/RTP profile remain H.264/AAC; HEVC/MPEG input is explicitly rejected by that bridge, and multiple audio tracks are rejected there rather than silently lost. No new runtime preview or codec capability is advertised by this source stage.

## Implemented

- Explicit codec kinds and observed M4S four-byte frame tags (`hevc`, space-prefixed `m2a`/`mp3`). Native handlers carry the unpadded codec name. Native configuration bytes remain opaque and unchanged; MPEG audio does not acquire an invented AAC configuration.
- M4F/M4S metadata, frame and packed-GOP library handling for up to16 distinct tracks, at most one video; multiple native audio tracks. Metadata handler/codec kinds, frame codec/kind/ID, configurations and record sizes are bounded and checked before relay. The M4F packer budgets the complete 32 MiB container, including opaque configurations and sample tables, before copying payloads; sample durations outside the native 32-bit table fail instead of truncating. Encoded sample payloads and signed decode/composition timing survive native roundtrips.
- HEVC decoder-configuration and length-prefixed sample inspectors: bounded arrays/NALs, VPS/SPS/PPS, widths1/2/4, IRAP detection and Annex B reconstruction. These validate framing and do not replace a full HEVC decoder. Native frame/GOP flavor remains supplied by the producer; inspected IRAP status is a separate result.
- MPEG frame inspection: Layer II versus III, version, sample rate, channel mode, bitrate, exact frame size and sample count. Layer III MPEG2/2.5 uses576 samples; MPEG1 Layer III and Layer II use1152. Final native MPEG sample duration is derived from its header. Free-format and reserved combinations are rejected. [Independent FFmpeg header reference](https://ffmpeg.org/doxygen/7.1/mpegaudiodecheader_8c_source.html)
- HEVC participates in video bootstrap/GOP boundaries; audio does not bypass it. Audio-only originators close M4F segments at the first subsequent sample at least two seconds after segment start and reset their bootstrap there. Segment bytes are cached before notification. Queue/bootstrap/cache limits remain unchanged.
- Native M4F composition tables now originate with version zero and signed offsets. The tested installed26.04.1 reference reads that form and ignores version one; this differs from ISO-BMFF's versioned composition semantics. The native decoder accepts signed offsets in both version-zero and earlier FlussoniX version-one tables. This behavior is confined to the native M4F module and does not alter standard MP4 packaging.

## Measured evidence and limits

Owned12-packet HEVC Main8-bit B-frame fixture and MPEG audio packets were packed/unpacked, reconstructed into elementary streams, and decoded with independent FFmpeg:12 HEVC pictures, five repeated Layer II packets and five repeated Layer III packets. Payloads and DTS/composition values were compared before decoding. Synthetic headers additionally test framing combinations; they do not establish decoded profile coverage or A/V synchronization.

A disposable local Erlang process using the installed reference accepted independently generated HEVC/m2a/mp3 metadata, frames and a packed segment, preserving codec identity, sample payloads and decode/composition times. FlussoniX independently decoded reference re-encoded metadata, frames and packed media with the same comparisons. The three-sample oracle fixture is library interoperability evidence, not live mixed-vendor streaming or a migration test. Reference code/disassembly/output stays in ignored research files and is absent from product tests/builds.

Native metadata currently retains the earlier Track layout: ID, codec and opaque configuration. Full sample-rate/channel/clock metadata qualification, Main10, parameter-set changes during playback, multiple live audio tracks, live mixed-vendor paths and complete codec/container/security combinations remain pending. An empty MPEG configuration can be represented; that does not establish a complete reference playback metadata contract.

The next media stage must replace the single AVC/AAC FLV worker channel with a specified representation that carries all requested codecs, multiple tracks, configuration revisions and decode/composition clocks. Container remuxing, HEVC RTP, secure publication/push, CPU/GPU profiles and full migration qualification remain required by [the codec contract](secure-output-codecs.md). The current installed v0.10 instances are kept running during this source-stage work.

Source-stage validation:222 Rust tests passed, zero failed and one opt-in test ignored;18 new codec cases included. Formatting and all-target Clippy passed. The existing15-case browser suite passed against an owned current-source daemon with HTTP/HTTPS enabled, and the temporary daemon was stopped after testing. Public GitHub CI remains the source publication gate.

## Review and qualification decisions

The final independent review's aggregate-segment size finding was reproduced and fixed. An additional timing-gap finding was reproduced and fixed. Both regressions and the complete Rust suite passed after the fixes.

Only the RTSP test fixture retries its reserve/release/bind handoff when an independent FFmpeg client takes a released UDP port, up to eight fresh ranges; production port binding still fails on an occupied configured port. Browser qualification uses a fresh owned configuration and media directory for each run, because retained test-created names caused a repeated run to fail. Neither correction relaxes product errors or test timeouts.

The following decisions govern this source stage:

- Existing authorization covers isolated development, tests and source publication. If the scope needs revision, the published source can be corrected without changing installed preview services.
- Native codec primitives precede the replacement worker; no new runtime version or deployment is made. Live HEVC/MPEG profiles remain pending.
- Internal wire construction and hub metadata publication return errors instead of inventing AAC identities. Internal Rust callers must handle those errors; HTTP APIs are unchanged.
- Native signed version-zero composition tables are qualified against reference26.04.1, with earlier FlussoniX version-one decoding retained. Other migration versions need timing qualification.
- Track retains codec, ID and opaque configuration. Complete sample-rate/channel/clock metadata needs further playback qualification.
- A feature branch is staged for exact-commit CI before main publication; a failed staging check requires a correction before integration.
- Gaps beyond the native 32-bit sample duration are rejected. A future worker may need to split or restart unusually discontinuous streams.
- Live HEVC/MPEG/RTP, Main10, CPU/GPU and security combinations remain required future qualification. Those migration cases are not supported by this stage's evidence.
- HEVC framing/IRAP inspection is separate from producer-supplied native flavor. Full picture and malformed producer key-flag validation require further work.
- The bounded UDP test retry fails after eight collisions. Fixture failures therefore remain visible.
- Repeated browser runs isolate saved test data. Their qualification covers a fresh instance, not an upgrade of retained fixtures.

Deferred minor: the existing admin status strip contains a hard-coded `v0.9.0` label. The health endpoint reports the actual installed/runtime version; replacing the cosmetic label remains follow-up UI work.
