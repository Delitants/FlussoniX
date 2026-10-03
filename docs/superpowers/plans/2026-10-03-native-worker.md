# Native worker ingress implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement inline task-by-task.

**Goal:** Connect native HEVC/MPEG audio and multiple tracks to independent FFmpeg through MPEG-TS.
**Architecture:** Bounded TS muxing at native ingest, original media retained by hub, one existing worker packages compatible outputs.
**Tech Stack:** Rust/Tokio; existing FFmpeg, no dependencies.
**Spec:** docs/superpowers/specs/2026-10-03-native-worker-design.md

## Global Constraints

1..16 tracks/one video; configurations1 MiB and samples16 MiB. Timestamp clock90000, TS188 bytes, PAT PID0, PMT PID4096, media PIDs256+index. PSI repeat9000ticks. No vendor dependencies; no live runtime upgrade in this source stage. Metadata changes end worker generation.

## Review Focus

- Failed frames/config changes must not partially update state or relay metadata.
- Negative composition offsets and 33-bit timestamp wrap preserve playback timing.
- Multiple MPEG audio and HEVC must not be lost to FLV output or first-audio-only mapping.
- AAC unsupported ASC and oversized ADTS must fail explicitly.
- Native worker cancellation/outage must reap FFmpeg and preserve authentication ordering.

### Task 1: Bounded MPEG-TS worker muxer

Files: src/worker_ts.rs, src/lib.rs, src/rtp.rs; tests/worker_ts.rs.
Interfaces: Muxer::new(&[Track])->Result<Muxer,String>; tables(&mut self)->Vec<u8>; frame(&mut self,&Frame)->Result<Vec<u8>,String>. Reuse bounded avcC parser with crate visibility. HEVC inspectors and MPEG frame inspect are consumed unchanged.
- [ ] Write RED owned HEVC+m2a+mp3 independent ffprobe/FFmpeg decode, exact PTS/DTS and multiple-track identities; audio-only, table/PCR/continuity, wrap, backwards/unknown/negative/oversized/malformed failure atomicity, unsupported ASC tests. Expected missing module.
- [ ] Implement muxer and run cargo test --locked --test worker_ts; fmt/Clippy. Expected all green without vendor files. Commit and task-done.

### Task 2: Native ingest and worker integration

Files: src/m4_ingest.rs, src/media.rs, tests/codecs.rs, tests/native_worker.rs, docs/native-worker.md, docs/native-codecs.md.
Interfaces: ingest uses Muxer, validates initial/repeated metadata before hub publication; changed metadata fails worker. Existing pull signature retained. FFmpeg input becomes mpegts and native all-audio mapping; disable redundant copy-mode FLV, fMP4 slave isolated on codec failure.
- [ ] Write RED owned HTTP M4F/M4S HEVC+MPEG input to decoded HLS/multiple audio and native byte retention; rejected metadata must not relay; cancellation reaps one worker. Expected bridge rejection/no HLS.
- [ ] Integrate; run worker_ts/native_worker/codecs/cluster_integration, full Rust/fmt/Clippy/build/browser. Expected all green with truthful source-only limits. Commit.
- [ ] One immutable whole-branch fresh review, one Important/Critical RED→GREEN fix pass. Stage branch and require exact GitHub CI before publishing main; archive owned evidence/worktree. Keep installed v0.10 services unchanged. Expected clean synchronized main.
