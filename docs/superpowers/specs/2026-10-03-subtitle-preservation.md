# Original subtitle preservation runtime

This is the first runtime slice of `docs/subtitle-design.md`. Deliver original European DVB bitmap and teletext tracks on MPEG-TS fan-out without breaking either HLS variant or native AV outputs. Qualify embedded North American 608/708 caption bytes through native H.264/HEVC and worker copy paths. WebVTT decoding, OCR, caption stripping, HLS caption announcements and separate native subtitle tracks remain follow-on work and must not be advertised as implemented.

## Contract

- Independent software and owned fixtures only; no runtime, build or test dependency under /opt/flussonic.
- `flussonix_subtitle_tracks` is a native extension on Streams and Templates: `preserve` or `drop`. Omission retains the existing default `drop` for separate tracks. Template inheritance, explicit override and null removal work normally. Reject other values, objects and unqualified conversion aliases.
- This policy concerns separate subtitle tracks on shared MPEG-TS output. It does not strip caption SEI embedded in video. Name the UI control **Original subtitle tracks**, with **Keep in MPEG-TS output**, **Drop separate tracks**, and the inherited/default choice. Explain that selectable HLS conversion is pending. Never offer a working-looking Convert choice.
- When preserve is selected, map subtitle streams and copy their encoded payload into the worker TS fan-out. In both copy and CPU video transcode modes, preserve DVB and teletext language, DVB composition/ancillary page IDs and teletext page descriptors. Do not claim identical PID numbers: remuxing may assign new ones. Native separate subtitle representations and arbitrary subtitle codecs are not qualified by these tests.
- HLS TS, HLS fMP4 and FLV wire muxers receive video/audio only; subtitle mapping must not kill an incompatible muxer. Embedded video caption data remains intact in copy mode. Keep the single source session and existing one-encode tee architecture.
- Policy changes join the worker signature and replace the generation. Source discovery carries the policy to CDN resolution without adding credentials or inputs. Report effective track policy in worker stats and show a readable UI summary.
- No change to viewer auth, secure transport, production services or installed previews. Authorized local isolated tests and GitHub publication use the existing workflow.

## Qualification

Build owned H.264/AAC TS with valid DVB subtitle/teletext PMT descriptors and PES payloads; independently confirm their codec identities with ffprobe. Capture actual worker MPEG-TS, reconstruct PES and compare subtitle bytes and descriptors, for copy and CPU transcode. Verify drop omits both tracks, both HLS variants stay AV-only/readable, and a policy edit replaces the worker. Test native framing and independent worker TS bridge preservation of GA94 caption packets carrying both 608 and 708 bytes for H.264 and HEVC. A payload-survival test is not decoder/player qualification and must be named accordingly.

Browser tests must save a template policy, inherit it, override it, remove the override, and retain the policy during an unrelated edit without JSON entry. Full Rust and browser suites, formatting, Clippy and build must pass; fresh whole-branch review and same-head CI before publishing. Record decoder/OCR/transport limitations explicitly.
