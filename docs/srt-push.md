# SRT caller output profile

FlussoniX sends the shared worker's MPEG-TS to independently configured SRT
listeners. Configure `pushes` on Streams or Templates, using objects with `url`
and optional `streamid`, `passphrase`, `latency`, `connect_timeout`,
`retry_timeout`, `disabled` and `comment`. This is a bounded subset of the
[Flussonic SRT push interface](https://flussonic.com/doc/fms/play/srt-push/)
and the published v3 schema. Other push options and protocols fail validation.

The URL must be `srt://HOST:PORT`, with an empty path or `/`. Caller mode only.
Supported query keys are `streamid`, `passphrase`, `latency`, `connect_timeout`
and `mode=caller`; a field and the same query key cannot both be present.
Literal `#!::` stream IDs in the query are supported as well as percent encoding.
Stream IDs pass through unchanged, including the documented
`#!::r=destination,m=publish` form; no vendor identity is fabricated.
The UI supplies normal fields and masks passphrases and saved stream IDs.

At most four destinations per stream. Latency is 1–10000 milliseconds
(default 120), connection timeout 1–30 seconds (default 3), retry interval
1–300 seconds (default 5). Stream IDs are at most 512 UTF-8 bytes, passphrases
10–79 ASCII bytes or empty for plaintext. No controls are allowed in either.
Encryption always enforces matching secrets with AES-128; disabling enforcement,
other key sizes, listener/rendezvous output, retry limits and MPEG-TS PID/service
customization remain pending. Documented vendor zero/unlimited timeouts are
rejected in this bounded profile. Unknown and duplicate query keys are rejected.
Validation errors contain no destination secrets.

Each enabled destination owns one copy-only FFmpeg remux process, using the
configured independent FFmpeg with libsrt support. It receives the worker's
bounded live TS broadcast, maps every retained track and never encodes again.
HEVC, AAC, MPEG Layer II and MP3 use the worker's existing output profiles.
Separate subtitle tracks follow **Original subtitle tracks**; SRT does not
convert them to WebVTT. Embedded captions follow the worker's copy/encode
behavior. Exact TS byte/PID preservation is not promised after remuxing.

A slow consumer cannot block the shared worker. Broadcast overflow, a failed
process or absent output progress kills and reaps that destination before
retrying. Initial progress has the connection timeout plus five seconds for
probing; after first output, progress must continue within ten seconds. Stop, source closure, stream disable/delete or configuration
replacement cancels and joins all destination processes. Destination edits
replace the shared worker in this first profile and require continuous viewers
to reconnect. Enabled pushes keep an on-demand configured stream active without
viewers; disabled pushes do not. Publication inputs still wait for a publisher.
Push configuration is local and is not inherited by discovered CDN mirrors.

`stats.flussonix_pushes` reports the destination index, sanitized host/port,
state, current process ID, attempts, input bytes fed, output bytes muxed and a
fixed failure code. `sending` requires FFmpeg output progress, not merely input
queued. Neither byte count proves remote reception or SRT acknowledgement.
These are native diagnostics, not a claim of vendor push counter parity.
Passphrases, query strings, stream IDs and raw FFmpeg errors are never included.

Local qualification exercises independent localhost receivers, encrypted
success and wrong-secret denial, H.264/HEVC with all three audio codecs,
simultaneous healthy/unreachable destinations, receiver restart, disabled
destinations, on-demand activation, config replacement and process cleanup.
Owned DVB publication tests verify retained descriptors and subtitle PES at
the SRT receiver, and filtering when separate tracks are disabled. Dedicated [regional subtitle qualification](srt-subtitles.md) also covers encrypted
copy-mode CEA-608/708 and exact DVB/teletext carriage, separate-track filtering
and CEA HLS conversion alongside SRT. Broader encoder, input and cluster
subtitle combinations remain pending. Receiver handshake logs also verify exact Stream IDs, including a 512-byte
UTF-8 ID and punctuation-bearing passphrases from query and normal fields.
Decoded values are passed as separate FFmpeg output options to avoid older
versions' missing URL decode and encoded query-buffer truncation. Owned fault
processes qualify stalls and cancellation, not media interoperability. No production
listener is changed. Internet loss/retransmission, sustained throughput and
all vendor SRT dialects remain unqualified.

FFmpeg's [protocol reference](https://ffmpeg.org/ffmpeg-protocols.html#srt)
uses microseconds for latency and milliseconds for connection timeout; the
adapter translates the public milliseconds/seconds units explicitly.
