# Subtitle preservation implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep original DVB/teletext tracks on MPEG-TS output and qualify embedded caption payload preservation.
**Architecture:** One FFmpeg demux/encode maps optional subtitle streams into the shared tee. AV-only slaves explicitly select video/audio; the TS fan-out copies original subtitle payloads. Typed policy follows existing configuration inheritance and discovery.
**Tech Stack:** Rust/Tokio, FFmpeg, React, Playwright.
**Spec:** docs/superpowers/specs/2026-10-03-subtitle-preservation.md

## Global Constraints
- Independent software and owned fixtures only; no runtime, build or test dependency under /opt/flussonic.
- No change to viewer auth, secure transport, production services or installed previews.
- Do not advertise WebVTT decoding, OCR or separate native subtitle tracks as implemented.

## Review Focus
- Template null removal must restore inheritance: Task 1 config regression and Task 3 browser round trip.
- More than one subtitle track must survive without breaking fMP4: Task 2 DVB plus teletext same-program fixture.
- Caption SEI payloads must survive framing: Task 2 both 608/708 in H.264 and HEVC.
- Transcoding must not transcode bitmap or teletext into an incompatible subtitle codec: Task 2 explicit copy and CPU runs.
- CDN must inherit the output policy and policy edits must replace workers: Task 1 discovery and Task 2 lifecycle assertions.

### Task 1: Validated output policy
**Files:** src/config.rs, src/media.rs, src/server.rs, tests/subtitle_policy.rs.
**Interfaces:** Produces `flussonix_subtitle_tracks` string preserve/drop consumed by worker mapping and forms; includes policy in media_signature, stats and discovery.
- [ ] Write config tests for defaults, invalid values, template inheritance/override/null reset, signature and authenticated discovery (no publisher secrets).
- [ ] Run `cargo test --locked --test subtitle_policy`; Expected: FAIL on currently rejected option or missing signature/discovery field.
- [ ] Add strict validation, signature inclusion, effective stats and discovery field.
- [ ] Run the same command; Expected: PASS.
- [ ] Commit `feat: validate and propagate subtitle track policy`.

### Task 2: Actual payload preservation
**Files:** src/media.rs, tests/subtitle_preservation.rs, tests/support/subtitle_fixture.rs, tests/fixtures/subtitles/README.md.
**Interfaces:** Consumes Task 1 policy; produces optional copied subtitle mapping and AV-only incompatible tee slaves.
- [ ] Write owned TS generation and independent ffprobe qualification; failing worker capture tests require DVB/teletext bytes/descriptors for copy and CPU, drop exclusion, HLS TS/fMP4 readability, replacement on policy edit. Add native caption SEI payload tests for both codec families and both regional packet types.
- [ ] Run `cargo test --locked --test subtitle_preservation`; Expected: missing original tracks in worker output (caption tests may already pass, preserving an existing property).
- [ ] Map `0:s?` with `-c:s copy` only for preserve; give HLS/fMP4/FLV tee slaves explicit AV selection. Keep shared live TS sink inclusive. Do not add another source session or decode.
- [ ] Run the same command and `cargo test --locked`; Expected: PASS, only existing external opt-in ignored.
- [ ] Commit `feat: preserve broadcast subtitle tracks in MPEG-TS fan-out`.

### Task 3: Friendly controls and release evidence
**Files:** web/src/forms.tsx, web/tests/admin.spec.ts, docs/subtitle-design.md, docs/compatibility.md, docs/qualification.md.
**Interfaces:** Consumes Task 1 field; produces inherited select control and readable saved/stat summary.
- [ ] Write browser test saving template preserve, stream inherit, override drop, remove override and unrelated edit; assert persisted/effective values and no JSON inputs.
- [ ] Run targeted browser test against owned loopback daemon; Expected: missing control.
- [ ] Add control using existing approved form styles, help text describing separate tracks vs embedded captions and pending HLS conversion. Do not redesign the existing product or offer unavailable options.
- [ ] Run full browser suite, fmt, Clippy, full Rust suite and web build; Expected: all green.
- [ ] Document measured preservation and remaining limits; commit `feat: expose friendly subtitle preservation controls`.

## Self-review
The spec is a bounded first runtime stage of the approved larger subtitle contract. All first-stage requirements map to these three tasks; decoder/OCR and per-output caption stripping are explicitly outside this stage. Field names agree across tasks. Each review-focus condition has a real persistence or payload test.
