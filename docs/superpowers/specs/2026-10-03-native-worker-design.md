# Native codec worker ingress

The migration goal requires independent HEVC/m2a/MP3 streaming, inbound and outbound, secure transports and multiple audio tracks. This stage connects native M4F/M4S input to the existing single FFmpeg worker and preserves unchanged native relay media. It does not qualify new non-native origination, HEVC RTP, Main10, HEVC encoders or all security directions.

## Approach

Use an independent bounded MPEG-TS muxer as the worker pipe. Compared with FLV it supports the requested codecs and multiple audio. Matroska SimpleBlock does not explicitly carry separate DTS; NUT would require a larger new wire implementation. TS preserves 90 kHz DTS/PTS and is already understood by independently installed FFmpeg. No dependencies or vendor components are added.

## Contract

`worker_ts::Muxer::new(&[Track])->Result<Muxer,String>` validates 1..16 distinct tracks, at most one video. `tables(&mut self)->Vec<u8>` emits PAT/PMT. `frame(&mut self,&Frame)->Result<Vec<u8>,String>` emits repeated tables at 100 ms decode-time intervals, PES and 188-byte TS packets, continuity/PCR and modulo-33-bit timestamps. Frames/configurations remain bounded at 16 MiB/1 MiB; malformed samples, unknown IDs, backwards per-track DTS and negative PTS fail before mutating muxer state. The first metadata initializes it; identical repeated metadata is accepted, changed metadata ends this worker generation rather than silently reusing an old demux layout.

H.264 avcC SPS/PPS and HEVC hvcC VPS/SPS/PPS are converted to Annex B and repeated on key samples; video samples include access-unit delimiters. AAC-LC indexed rates/channel configurations use ADTS; explicit-rate/extension/PCE ASC is rejected until qualified. MPEG audio headers determine actual rate/layer/frame size. All audio tracks are mapped for native input. Existing non-native mapping and transcoding profiles remain.

The copy-mode native worker omits the redundant FLV output because native bytes already populate the hub. Standard TS HLS and MPEG-TS output remain required; fMP4 HLS may fail independently when FFmpeg rejects a codec/container combination, and must not terminate TS delivery. No missing playlist is advertised as supported. Transcoded native inputs retain the existing H.264/AAC output profile.

## Qualification

Owned HEVC B-frame/MP2/MP3 fixtures independently decoded through the pipe. Check exact PTS/DTS, CRC/table identity, continuity/PCR, malformed sample rejection and failure atomicity. Native HTTP M4F/M4S inputs must yield independently decoded TS HLS, retain original relay bytes, reuse one worker, preserve multiple audio, and stop/reap cleanly. Existing H.264/AAC cluster/RTSP/auth tests must stay green. Full Rust/fmt/Clippy, frontend build/browser and exact GitHub CI gate source publication. Runtime remains v0.10 until the new live paths have separate release qualification; production Flussonic is untouched.
