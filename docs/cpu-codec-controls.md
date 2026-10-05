# CPU codec controls

Independent video/audio encoding in Streams and Templates. Existing numeric `vb` and encoder behavior remains supported. Add CPU `libx265`; audio `acodec`: `copy`, `aac`, `mp2a`, `mp3`; numeric `ab` in kb/s. The latter names follow Flussonic audio controls; this is a qualified subset, not its complete transcoder schema. MP3 is an additional supported choice.

No transcoder means copy AV. Legacy transcoder objects (including `{}` and vb-only) mean H.264/AAC 96. Explicit video copy means audio copy unless an audio choice overrides it. Audio-only settings without encoder/vb keep video copied. Encoding audio defaults AAC 96, Layer II 192, MP3 128 kb/s. Audio encoders output 48 kHz and at most stereo; copy preserves the original format. AAC accepts integer 32..512 kb/s; Layer II 32,48,56,64,80,96,112,128,160,192,224,256,320,384; MP3 32,40,48,56,64,80,96,112,128,160,192,224,256,320. Validate raw and template-merged profiles before saving; retain configured bitrates when copying so existing video-copy overrides of template bitrates remain valid. Synthetic test sources require encoding raw AV; copy/default falls back to H.264/AAC for testsrc only.

Resolve once per worker; use the same full-AV-copy predicate for original native relay, AAC bitstream filters and worker stdout decoding. A single encoded source feeds all outputs. CPU HEVC uses bounded x265 pools/frame threads and low-latency 8-bit 4:2:0. No new GPU profile or GPU execution qualification. Native text transcoding remains explicitly unsupported when originals are retained. Existing subtitle policy remains.

UI: independent inheritance/reset per dropdown; preserve the other codec and bitrate; no JSON. Use template/current profile bitrate when unchanged, choose valid new default when audio codec changes. Summaries identify both codecs.

Sources: [Flussonic audio options](https://flussonic.com/doc/fms/transcoder/internals/).

Qualification: six CPU video/audio combinations and M4S/M4F input with independently encoded audio or video. Native frame and packed output, TS-HLS and fMP4 HLS are independently decoded; original VCL payloads remain byte-identical for audio-only conversion. MPEG Layer II in MP4 has a generic MPEG-audio descriptor which FFprobe labels MP3; demuxed sample headers prove Layer II and normal FFmpeg decode succeeds. This does not qualify browser Layer II playback. Native/audio copy fMP4 filters depend on audio copy, while original native relay depends on full AV copy.
