# Codec-aware native startup implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Qualify mixed native AAC/MPEG fMP4 through a single codec-aware worker output.

**Architecture:** A validated metadata oneshot and bounded TS pipe defer native FFmpeg startup outside the registry lock. The existing authenticated parser feeds the same connection and original native relay. Numeric per-stream AAC filters configure one fMP4 slave.

**Tech Stack:** Rust, Tokio, reqwest, independent FFmpeg, owned fixtures.

**Spec:** `docs/superpowers/specs/2026-10-03-native-startup-design.md`

## Global Constraints

- No official Flussonic build/test/runtime dependency; production instances remain untouched.
- One native control connection, one FFmpeg process, 64 KiB TS staging pipe.
- Existing 16-track, 1 MiB configuration, 16 MiB M4F fetch and 32 MiB M4S buffer limits remain.
- Native setup uses configured input timeout and cancellation; no remote metadata wait under registry lock.
- No new runtime release/deployment or capacity claim.

## Review Focus

- Audio-only and metadata order unlike output mapping: AAC filters must target the mapped indices.
- Stop during pending setup: no orphan task, HTTP connection or late child spawn.
- Metadata failure or missing FFmpeg: completion signal and existing recovery must remain available.
- Truncated startup/changed layout: no invalid metadata relay or stale fMP4 output.
- Publication and non-native streams: stdin, PID, epoch and auth behavior must remain unchanged.

### Task 1: Metadata handshake on the existing pull

**Files:** Modify `src/m4_ingest.rs`; create `tests/native_startup.rs`.

**Interfaces:** Produces `pull_ready<W: AsyncWrite + Unpin>(input: &str, key: Option<&str>, output: &mut W, hub: Option<&Hub>, metadata: oneshot::Sender<Vec<Track>>) -> Result<(), String>`. Retains public ChildStdin `pull` wrapper. Metadata is validated and sent once before first TS write.

- [ ] Write owned HTTP tests: a tiny blocked output receives valid metadata before table backpressure; invalid config closes the metadata channel and relays nothing; exactly one control request; both M4S and M4F reuse initial records. Existing pull cannot supply readiness, so the new interface fails to compile.
- [ ] Run `cargo test --test native_startup`; Expected: RED for missing readiness interface.
- [ ] Refactor shared generic pull implementation and optional metadata sender; preserve parser/transport/relay ordering and limits.
- [ ] Run `cargo test --test native_startup --test native_worker --test worker_ts`; Expected: all pass.
- [ ] Commit and run task-done with the same command; Expected: passing task ledger entry.

### Task 2: Deferred worker lifecycle and single fMP4 output

**Files:** Modify `src/media.rs`, `src/wire.rs`, `tests/native_worker.rs`, `tests/native_startup.rs`, `docs/native-worker.md`; add `docs/subtitle-design.md`.

**Interfaces:** Consumes Task 1 `pull_ready`; Worker PID becomes atomic, zero only while native metadata is pending. Native runner waits outside registry lock, chooses output filters from advertised tracks in mapped order, owns and joins its input/copy tasks, and reaps child before signaling done. Public fMP4 file paths retain their existing names.

- [ ] Replace mixed fMP4 error expectation with decoded packets for all three tracks; cover M4F and M4S, audio-only/video-only and reversed metadata order/nonsequential IDs. Assert one control request and no `fmp4_aac` directory.
- [ ] Add blocked-metadata startup tests: ensure returns promptly with PID zero; unrelated synthetic input starts; cancellation closes source and stop returns promptly; timeout and bad FFmpeg complete failure/recovery; repeated viewers share worker. Expected: RED with current synchronous child spawn/profile error.
- [ ] Run `cargo test --test native_startup --test native_worker`; Expected: failures above.
- [ ] Implement deferred native runner with 64 KiB pipe and bounded metadata wait; preserve non-native immediate spawn and publication stdin. Apply numeric AAC filters only in native copy; remove duplicated private sink/read mapping. Add accurate subtitle follow-on contract, not inactive UI options.
- [ ] Run `cargo test --test native_startup --test native_worker --test publication --test cluster_integration --test recovery`; Expected: all pass.
- [ ] Run fmt, Clippy, full Rust suite and web build; Expected: green. Stage exact source head on GitHub for CI, including browser tests; Expected: success before main publication.
- [ ] Commit and task-done with targeted tests; Expected: passing task ledger entry.
- [ ] Perform exactly one fresh whole-branch review; fix Important/Critical findings with RED/GREEN and full suite; ledger deferred minors and rulings. Publish qualified source, preserve owned evidence, remove only this worktree. Installed previews remain v0.10.
