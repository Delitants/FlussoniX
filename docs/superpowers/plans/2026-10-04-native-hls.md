# Native text HLS Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans inline; no implementation delegation. Steps use checkbox syntax for tracking.

**Goal:** Deliver explicitly selected native text as authorized HLS WebVTT.

**Architecture:** Tap the native input before output filtering, observe its video PTS, and feed bounded text cues into the existing generation-owned HLS state. Keep output clock alignment, caching and authorization in the existing HLS path.

**Tech Stack:** Rust/Tokio, existing FFmpeg AV packaging, React forms; no new dependency.

**Spec:** `docs/native-hls-design.md`

## Global Constraints

- No official Flussonic component required for daemon, build or tests.
- Native track IDs 1..4294967295; maximum four renditions.
- Text bound 16 KiB and 64 lines; cue duration at most 120 seconds; history at most 4096 cues, 120 seconds and 1 MiB.
- Explicit errors preserve AV delivery. Existing native preserve/drop policy remains independent.

## Review Focus

- Initially quiet and sparse text must not stall AV or corrupt caption clocks.
- Selected invalid UTF-8, timing and oversized text must fall back to AV.
- Full-width native identifiers must not collide with broadcast selectors.
- Clear samples and CR/LF/blank lines must not leak text into silence or create extra cues.
- Viewer denial and disable/restart must apply to cached subtitle resources.

### Task 1: Conversion and delivery

**Files:** `src/captions.rs`, `src/caption_hls.rs`, `src/m4_ingest.rs`, `src/media.rs`, `src/dvb_ocr.rs`, `tests/native_hls.rs`, `web/src/forms.tsx`, `web/tests/admin.spec.ts`, documentation.

**Interfaces:** Existing `Service` serialized selection gains `native_track`; internal channel identifiers widen to u64. Native HLS state observes a track inventory and frame before native output filtering. Public ingest defaults remain unchanged.

- [x] Write and run failing configuration tests for native IDs, mixed selectors and serialization.
- [x] Add native selector validation, bounded cue processing and same-session ingest tap.
- [x] Add real M4F/M4S frame/GOP fixture delivery tests for TS and fMP4 HLS, copy/CPU, silence, clear, filtering, malformed text and authorization; run RED before implementation of each behavior.
- [x] Add friendly selector fields and verify persistence/inheritance through browser tests.
- [ ] Run fmt, all-target Clippy, locked Rust suite and browser suite; obtain independent final review.
- [ ] Publish verified code, update owned local preview on port 49285, smoke authorization and unchanged configuration, archive private evidence.
