# Worker output MPEG-TS implementation plan

> **For agentic workers:** Execute inline with superpowers:executing-plans; independent final review only, no implementation delegation.

**Goal:** Qualify independent encoded TS-to-native reconstruction needed by HEVC/MPEG transcoding.
**Architecture:** A bounded incremental decoder consumes worker transport and emits existing Track/Frame events. Transport/PES ownership and codec conversion live in separate focused modules; runtime integration follows after qualification.
**Tech Stack:** Rust, existing PSI/codec inspectors, owned FFmpeg fixtures.
**Spec:** docs/superpowers/specs/2026-10-04-worker-output-ts.md

## Global constraints

No vendor components.16 tracks/one video,64KiB configuration,16MiB PES/sample,32MiB pending samples,4096 NALs,4096-byte PSI. Preserve clocks/payloads; errors are terminal and explicit. Keep the preview unchanged during this source stage.

## Review focus

- Audio PES can contain several frames or split a frame; preserve each sample and cumulative timestamps.
- Initial audio can precede video configuration; publish complete metadata before media and bound waiting.
- Timestamp wrap/B-frame signed offsets must preserve common AV timing.
- Unsupported PMT layouts and configuration changes must fail, never merge identities silently.
- Arbitrary chunk boundaries, duplicate payload packets and adaptation-only continuity need deterministic behavior.

## Task1: Decoder library and codec conversion

Files: src/worker_output.rs, src/worker_output/audio.rs, src/worker_output/video.rs, src/lib.rs; tests/worker_output.rs.
Interfaces: Decoder::push/finish -> Result<Vec<Event>,String>; Event::Info(Vec<Track>), Event::Frame(Frame). Use independent strict PSI assembly, worker_ts::Muxer qualification and native codec inspectors. Existing caption PSI silently drops invalid sections and is intentionally unchanged.
- [x] Write RED roundtrip, initialization-order, exact clocks/bytes, audio aggregation/splits and transport corruption tests; verify expected unsupported decoder behavior.
- [x] Implement bounded transport/PES ownership and codec dispatch; run packet tests GREEN and meaningful independent decoder checks.
- [x] Add independent FFmpeg-produced profiles and malformed-limit regression coverage. Confirm complete sample/video picture coverage rather than weak nonempty checks.
- [ ] Format, all-target Clippy, fresh full Rust/browser qualification, immutable final review and exact-head GitHub CI. Fix demonstrated Important/Critical issues with RED/GREEN before publication.
- [ ] Publish qualified source to main, archive private records, remove only owned worktree, preserve existing preview and saved configuration. Document active worker wiring and encoder UI as next work.
