# Selectable HLS captions implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Deliver real selectable 608 WebVTT captions with existing HLS playback authorization.
**Architecture:** Independent Rust transport/SEI/608 decoder consumes a private copy output from the same FFmpeg source session. Bounded generation-owned cue state renders TS/fMP4 HLS masters and subtitle segments from actual AV clocks and playlists.
**Tech Stack:** Rust/Tokio, FFmpeg, React, Playwright.
**Spec:** docs/superpowers/specs/2026-10-03-hls-captions.md

## Global Constraints
- No official Flussonic or CCExtractor runtime/build/test dependency.
- Up to four distinct configured 608 channels; 708/teletext/OCR conversion remains unavailable.
- Existing original-track policy and disabled conversion behavior remain unchanged.
- One source session; AV must progress through malformed captions, silence and decoder failures.
- Installed preview and production services remain unchanged.

## Review Focus
- Sparse or malformed caption input and continuity loss must not retain stale displayed text (Task 1 reset/parity/size tests).
- HEVC framing, PTS wrap and nonzero mux offsets must align captions (Tasks 1 and 2 owned clock cases).
- AV playlists sliding beyond initial segments must retain generation clock anchors and bounded cues (Task 2 sliding test).
- Authorization revocation and grouped names must protect every subtitle resource (Task 2 router test).
- Template inheritance and unrelated edits must not silently override caption configuration (Task 3 browser test).

### Task 1: Native bounded caption decoder and typed configuration
**Files:** src/captions.rs, src/caption_transport.rs, src/config.rs, src/lib.rs, tests/hls_captions.rs.
**Interfaces:** Produces `captions::configuration(&Value)->Result<Vec<Service>,String>`, `captions::Decoder::new(services)`, `Decoder::push(channel,pair,pts)`, `Decoder::snapshot`, and `caption_transport::Transport::push(bytes,decoder)`. Cue times use unwrapped 90 kHz source PTS; decoder exposes first video PTS.
- [x] Write failing tests for strict config limits, inherited rows, exact pop-on/paint-on/roll-up text and timing, all four channels, parity/duplicate controls, wrap, H.264/HEVC registered SEI and continuity/oversize reset.
- [x] Run `cargo test --locked --test hls_captions`; Expected: FAIL because decoder/config are absent.
- [x] Implement bounded independent decoder, transport parser, strict validation and module exports. Keep private input parsing separate from display semantics.
- [x] Run the same test command; Expected: PASS.
- [x] Commit `feat: decode bounded native CEA-608 caption state`.

### Task 2: Live worker and authorized HLS renditions
**Files:** src/caption_hls.rs, src/caption_filter.rs, src/media.rs, src/server.rs, tests/hls_captions_delivery.rs, tests/hls_subtitle_policy.rs, tests/support/caption_fixture.rs.
**Interfaces:** Consumes Task 1 decoder; produces generation-owned caption state/clock anchors and `caption_hls::render` bytes for flat master, AV, caption playlist and VTT filenames in each HLS variant. Uses existing Engine read and server auth routes.
- [x] Write failing real worker tests using owned known-word timed captions: copy and CPU extraction, TS/fMP4 clocks, empty segments, live AV progress, policy replacement and original-track independence. Router tests cover grouped stream token propagation and revocation. Add sliding playlist/clock/overlap unit cases.
- [x] Run `cargo test --locked --test hls_captions_delivery`; Expected: missing master/renditions or captions.
- [x] Add private video TS copy sink to existing source process, bounded drain/decoder ownership, clock watcher, stats, signature/discovery, HLS rendering and VTT content type. Tasks cancel/join before generation completion. Preserve all existing AV slaves.
- [x] Run delivery tests and full `cargo test --locked`; Expected: green, only existing external opt-in ignored.
- [x] Commit `feat: serve authorized selectable HLS caption renditions`.

### Task 3: Friendly controls and qualification
**Files:** web/src/forms.tsx, web/src/main.tsx, web/tests/admin.spec.ts, docs/subtitle-design.md, docs/compatibility.md, docs/qualification.md.
**Interfaces:** Consumes validated caption rows and stats; produces inheritance mode plus channel/language/name rows in existing approved form layout.
- [x] Add browser test for template rows, stream inheritance, explicit filter/pass modes, restore inheritance and unrelated edit; expect no JSON field.
- [x] Run targeted browser test against owned unused-port daemon; Expected: missing controls.
- [x] Implement friendly controls and summaries with honest regional capability help.
- [x] Run full browser suite, web build, fmt, Clippy and full Rust suite; Expected: all green.
- [x] Document measured behavior and remaining limits; commit `feat: expose selectable caption controls`.

## Self-review
Every first-stage requirement maps to one of these tasks. Regional conversion beyond 608 remains explicit future work; this is a working decoder/rendition slice of the broader subtitle contract. Inline execution and same-head publication are already authorized. The primary remaining tradeoff is one additional held-back segment for synchronization.
