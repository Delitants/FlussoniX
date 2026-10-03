# Native codec worker ingress

Current source connects native M4F/M4S encoded tracks to the independent FFmpeg worker through a bounded MPEG-TS pipe. Copy-mode native input supports HEVC, H.264, AAC-LC, MPEG Layer II (`m2a`) and MPEG Layer III (`mp3`), with up to16 distinct tracks and one video. All native audio tracks are mapped to the worker. The installed v0.10 preview instances retain their previous worker; this source stage makes no new runtime release or deployment claim.

## Implemented and measured

- Independent TS PAT/PMT with CRC, per-PID continuity, PCR, Annex B AVC/HEVC configuration/access units, ADTS AAC and unchanged MPEG audio frames. Separate signed composition timing and decode timing become explicit PES PTS/DTS; timestamps wrap at33 bits.
- Configurations are bounded at1 MiB and samples at16 MiB. Unknown IDs, backwards per-track DTS, negative resulting PTS, malformed NAL/audio framing and oversized ADTS fail before mutating muxer state. Existing M4F HTTP fetch cap remains16 MiB.
- Native TS probing is bounded to1 MiB and one second of media analysis; a three-second open stream produces HLS rather than waiting for the default five-second probe window.
- Initial native metadata is validated before relay. Repeated identical metadata is accepted. Changed metadata terminates this worker generation and enters existing recovery, rather than leaving FFmpeg on an incompatible track layout. Stable configuration transitions and seamlessly adding/removing tracks require further work.
- AAC-LC accepts indexed sample rates, channel configurations1..7, two-byte ASC and the exact optional SBR-disabled sync extension `56 e5 00` used by independent FFmpeg. Explicit rates, PCE, enabled SBR/PS and other extensions are rejected.
- Copy-mode native workers relay original M4S records and M4F segment bytes and omit the redundant FLV output. TS HLS and MPEG-TS output remain independent of a native copy worker's optional fMP4 slave failure. A missing fMP4 playlist is unavailable, not a supported playback profile.

Owned HEVC Main8-bit B-frame samples, MP2 at48 kHz and MP3 at22.05 kHz were muxed together and independently decoded with FFmpeg. ffprobe compared every video sample's PTS/DTS, all three codec identities, table CRC/continuity/PCR and33-bit wrap. A separate PES check verified a negative composition offset. These short repeated synthetic fixtures do not establish Main10, long-duration A/V sync, throughput or migration readiness.

Owned plaintext HTTP M4F and M4S inputs produced TS HLS containing all three codecs, independently decoded without decoder diagnostics. Original M4F segments and M4S bootstrap sample bytes/timing were compared with the source. Repeated viewers reused one worker; stopping it reaped FFmpeg. Changed and invalid initial metadata were rejected before publishing the invalid layout. Existing H.264/AAC cluster, authentication and recovery tests remain required regression gates.

## Limits and decisions

This changes native ingress and copy relay. It does not add non-native HEVC/MPEG native origination, HEVC RTP, HEVC transcoding profiles, Main10, parameter-set transitions, a full audio metadata/clock contract, all codec/container combinations or every secure input/output direction. Transcoded native input retains the existing H.264/AAC profile; multiple-audio native origination after transcoding is not qualified. Full requirements remain in [the secure-output codec contract](secure-output-codecs.md).

MPEG-TS was chosen over FLV (codec/multiple-track restriction), Matroska SimpleBlock (no explicit DTS field), and a larger new NUT implementation. Existing development/publication authorization covers isolated inline work and exact-commit CI staging before main publication; any scope correction can be a later source revision. The source stage precedes runtime release qualification, so installed codec profiles remain unchanged. The exact disabled-SBR AAC extension is retained for baseline compatibility; other extensions require qualification. Metadata changes require recovery rather than silent adaptation; streams with changing layouts need another qualified adapter stage. Optional fMP4 failure isolation is confined to native copy workers; unsupported combinations can leave that output unavailable while TS continues.
