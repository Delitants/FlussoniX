# Codec-aware native worker startup

## Intent and scope

Continue the next pending native worker task: remove duplicate AAC/unfiltered fMP4 slaves and qualify mixed AAC/MPEG audio. Retain independent native relay, authenticated source/CDN pulls, recovery, HLS epochs and one FFmpeg process per worker. This is a source stage; installed v0.10 instances and production Flussonic remain unchanged.

## Design

An M4F/M4S worker is registered immediately as starting, with PID zero until FFmpeg exists. Its background runner starts one authenticated native HTTP pull into a 64 KiB duplex pipe. Validated initial metadata is delivered through a oneshot before writing TS tables. Parsing and relay reuse that same connection; no preflight probe or second GET. The existing decoder and segment limits remain in force. A bounded pipe provides backpressure rather than retaining a second copy of the live stream.

The runner waits for metadata outside the global worker registry lock, under the configured input timeout and cancellation token. It chooses the fMP4 AAC bitstream filters using numeric mapped output stream indices, then spawns FFmpeg and copies the pipe into its stdin. Mapping remains video first, then every audio in advertised order; track IDs are not stream indices. Non-native and publication startup preserve their synchronous spawn behavior and immediate publication stdin.

For native copy, one fMP4 HLS slave uses `bsfs/N=aac_adtstoasc` for each AAC output and no such filter for MPEG audio. TS HLS/stdout and native relay remain independent of optional fMP4 failure. Remove the private `fmp4_aac` directory and metadata-dependent file mapping. Native transcode keeps the existing H.264/AAC profile and does not use copy filters.

All native setup, input, copying and process tasks are cancelled and joined/reaped before the worker completion signal. Failed metadata, timeout or spawn enter existing recovery without an extra viewer. Duplicate viewers reuse the pending worker. Stopping pending startup releases the HTTP connection and pipe without spawning FFmpeg; registry operations do not await remote metadata. Metadata changes remain a generation failure.

## Alternatives

Retaining two private fMP4 sinks duplicates video writes and cannot carry mixed audio. Probing with a separate HTTP request consumes another source session and risks losing initial records. A bounded TS pipe and metadata handshake reuse the existing parser, bound buffering and preserve the single source connection, at the cost of an extra copy task and deferred native PID availability.

## Qualification

Owned M4S and M4F inputs must independently decode mixed AAC/MP3 and HEVC/MP2/MP3 fMP4 packets, including nonsequential IDs and metadata order different from mapped output order. Verify all-AAC, audio-only and video-only paths, original native bytes, one HTTP control request, one FFmpeg child, and absence of a duplicate fMP4 directory. Delayed/invalid metadata must not spawn a child; stop and unrelated startup remain bounded. Timeout/recovery, changed metadata, publication/authentication/cluster and all existing tests remain gates. FFmpeg absence/spawn error must complete cleanup. No capacity, long-duration, Main10 or migration-readiness claims.

## Subtitle follow-on requirement

Subtitle implementation is a separate pending subsystem described in `docs/subtitle-design.md`. This startup task does not advertise subtitle runtime support. HLS conversion and non-HLS preservation must be independently selectable, with North American CEA-608/708 and European teletext/DVB paths, synchronized rendition segments and no silent subtitle loss. Bitmap DVB requires OCR for selectable text; the user preference question remains open while unrelated startup work proceeds.
