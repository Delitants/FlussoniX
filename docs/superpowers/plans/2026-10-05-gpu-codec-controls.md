# GPU codec controls implementation plan

> For agentic workers: use superpowers:executing-plans inline, followed by one independent final whole-branch review.

**Goal:** Select NVIDIA H.264/HEVC with independent audio and report bounded encoder readiness honestly.
**Architecture:** Shared once-per-daemon lazy GPU checks, independent of media worker locking; validated profiles select the matching readiness result.
**Tech stack:** Rust/Tokio, FFmpeg, React/TypeScript, Playwright.
**Spec:** docs/gpu-codec-controls.md

## Global constraints
No vendor runtime dependencies, no private diagnostics, preserve saved preview configuration and existing inheritance. Each probe is bounded to five seconds and checks one frame. No GPU output qualification claim without real hardware.

## Review focus
Concurrent capability requests; cancelled/timed-out children; unavailable GPU replacing a live CPU worker; stale routes during readiness checks; GPU caption conversion rejection for both encoders.

## Task 1: profile and readiness
Files: src/gpu.rs, src/lib.rs, src/transcoder.rs, src/media.rs, src/server.rs, tests/config.rs, tests/gpu_readiness.rs.
Interfaces: Engine::gpu_capabilities() -> serde_json::Value; Profile::gpu_encoder() -> Option<&str>. `gpu_profiles` is an array of encoder/codec/status objects.
- [x] Write failing configuration and worker/API tests for HEVC, inheritance, unavailable and slow probes, coalescing and sanitization. Run focused tests; expected missing HEVC support/readiness.
- [x] Implement cached bounded process checks and worker admission before worker lock; preserve route recheck. Run focused tests; expected pass.
- [x] Exercise real FFmpeg readiness on this host; record unavailable status and preserved CPU operation. Commit.

## Task 2: friendly controls
Files: web/src/forms.tsx, web/tests/admin.spec.ts, .superdesign ignored cache.
Consumes: gpu_profiles and hevc_nvenc from task 1.
- [x] Update saved Superdesign draft by deterministic edit, preserving visual direction.
- [x] Write browser regression for NVIDIA HEVC persistence, audio independence, inheritance and readiness labels; expected absent option.
- [x] Add choice and readable capability statuses; run focused browser test; expected pass. Commit.

## Task 3: qualification and publication
- [ ] Run fmt, warnings-denied clippy, full Rust then full browser; all pass.
- [ ] Review entire branch independently; reproduce/fix Important findings with RED/GREEN.
- [ ] Push feature; exact-head CI succeeds before authorized main fast-forward/push.
- [ ] Rebuild and refresh owned preview with unchanged config; archive evidence and clean owned worktree.
