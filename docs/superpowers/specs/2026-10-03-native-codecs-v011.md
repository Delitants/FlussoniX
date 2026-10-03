# Native codec foundation

Continue the approved independent streaming implementation. This architectural stage establishes native HEVC/MPEG audio representation before replacing the AVC/AAC worker bridge. Existing isolated services and production Flussonic remain unchanged. Execute inline under standing development/publication authorization; no repeated approval menus. No vendor code, binaries or generated disassembly enter builds/tests/releases.

## Codec contract

Introduce explicit H.264, HEVC, AAC, MPEG audio Layer II (`m2a`) and Layer III (`mp3`) identities, content kinds and four-byte native frame tags. HEVC uses `hevc`; MPEG audio tags have one leading space. Handler names remain unpadded codec names with a terminating NUL. Retain opaque bounded native configuration bytes without manufacturing AAC configuration for MPEG audio. Permit up to 16 distinct native tracks, at most one video and multiple audio tracks; duplicate IDs and unsupported codecs fail before publication. Handler kind must match codec kind.

Provide independently implemented bounded HEVC decoder-configuration and length-prefixed access-unit inspection/conversion. Preserve original configuration/sample bytes. Validate version, length widths (1/2/4), array/NAL bounds, NAL headers, parameter-set types and presence, trailing data and 16 MiB access-unit limits. Distinguish IRAP picture presence from metadata-only NAL units; this inspection does not replace codec decoding or claim Main10 playback support.

Provide MPEG audio header/frame inspection: Layer II versus III, MPEG versions 1/2/2.5, rate, channels, bitrate, frame length, sample count and 90 kHz duration. Reject reserved/free-format combinations, truncated or concatenated frames and codec/layer disagreement. MP2 uses 1152 samples; MP3 uses 1152 for MPEG1 and 576 for MPEG2/2.5. Native last-sample durations use observed MPEG headers instead of AAC's duration shortcut. Preserve input DTS/composition offsets and payloads.

## Native relay behavior

Extend M4F pack/unpack and M4S metadata/frame/GOP handling to all five identities without changing existing H.264/AAC wire. HEVC is video for bootstrap and GOP boundaries. Audio-only originators close segments at the first subsequent sample at least 180000 ticks (two seconds) after segment start, without waiting for video. Reset/bootstrap byte bounds and segment-before-signal ordering stay enforced. Audio samples are independently decodable sync samples. Existing original record/segment relay identity remains unchanged.

The old FLV bridge rejects HEVC/m2a/mp3 and excess tracks explicitly before exposing new-codec media through an unsupported worker. Do not silently turn them into AAC. RTP remains its measured H.264/AAC profile and rejects unsupported configurations; HEVC RTP is a later required stage.

## Verification and scope

Owned synthetic FFmpeg HEVC/MPEG audio fixtures support native roundtrip, elementary reconstruction and independent decoding. Disposable local Erlang oracle checks owned native metadata/frames/segments in both directions; reference binaries are research-only and tests remain vendor-independent. Unit/integration tests cover split records, codec/kind mismatch, configuration changes, signed offsets, multi-audio bounds, malformed lengths, durations, HEVC late joins and audio-only segment visibility. Run formatting, Clippy, full Rust and existing browser suite; one fresh final reviewer. Publish source changes only after exact GitHub CI passes.

This stage is a codec foundation, not end-to-end HEVC/MPEG streaming or migration readiness. Worker representation, all secure transport directions, HEVC RTP, CPU/GPU settings and live mixed-vendor profiles remain mandatory next work. Do not version or redeploy a runtime preview solely for an internal codec foundation.
