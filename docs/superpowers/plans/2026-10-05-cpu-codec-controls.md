# CPU codec controls implementation plan

> For agentic workers: use superpowers:executing-plans inline with one final whole-branch review.

**Goal:** Independently choose CPU video/audio codecs through friendly controls and qualify delivered outputs.
**Architecture:** Shared internal transcoder profile validation/resolution and FFmpeg argument generation; media consumes its full-copy decision.
**Tech stack:** Rust/Tokio, FFmpeg, React/TypeScript, Playwright.
**Spec:** docs/cpu-codec-controls.md

## Constraints
No vendor runtime dependencies; preserve legacy defaults, production preview config and independent inheritance; GPU unqualified.

## Review focus
Audio-only native relay, copied audio with encoded video, template bitrate incompatibility, synthetic raw-source copy requests, configuration round-trip/reset.

## Task 1: profiles and worker output
- [x] Add failing configuration/inheritance tests and real CPU output tests; observe red.
- [x] Implement internal profile, validation and media selection; observe green.
- [x] Qualify CPU H.264/HEVC with AAC/Layer II/MP3 across native/HLS containers and native audio-only conversion.
## Task 2: friendly controls
- [x] Refine Superdesign draft with selected public UI context.
- [x] Add failing browser tests for codec choices/independent inheritance.
- [x] Implement dropdowns/conditional bitrates/summaries, observe green.
## Task 3: publication
- [ ] fmt, clippy, full Rust then browser.
- [ ] Whole-branch review, exact candidate CI, publish authorized main, safely refresh preview with unchanged config.
