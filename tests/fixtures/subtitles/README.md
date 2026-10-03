# Independently authored subtitle transport fixtures

`tests/support/subtitle_fixture.rs` generates original synthetic H.264/AAC with
system FFmpeg/libx264, then adds separately authored MPEG-TS PSI/PES:

- DVB subtitle descriptor: English, normal DVB subtitles, composition page 1,
  ancillary page 2. Payload is a legal empty acquisition page and end-of-display
  set. It tests preservation of page clear/composition packets, not OCR quality.
- Teletext descriptor: German, subtitle page 888 in magazine eight. Header and
  row packets carry Hamming/parity-coded text `EUROPE TELETEXT`.

No commercial server/media artifacts are used. Tests independently inspect these
tracks with ffprobe and compare encoded payloads and semantic descriptors after
worker remuxing. PID numbers are allowed to change.
