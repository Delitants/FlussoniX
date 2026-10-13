# Vendor daemon playback implementation plan

**Goal:** Activate explicit vendor profiles in guarded daemon HTTP playback.
**Architecture:** One caller-owned App runtime uses the existing native authorizer, registry, packaging and HLS maintenance. Deterministic source namespaces and bounded serialized cold inventory precede viewer admission. Native resolution/recovery is fenced against reserved vendor namespaces.
**Spec:** ../specs/2026-10-13-vendor-daemon-playback.md
**Constraints:** No production changes/vendor components; preserve preview configuration/credentials; own and reap all test subprocesses.

- [x] Write behavioral HTTP routing tests; observe absent routing fail.
- [x] Implement source selection/cache and lifetime owner; integrate HTTP shared responses, native supervisor fences and CLI task join.
- [x] Verify HLS/continuous outputs, auth/redirect/local precedence, concurrency, stale edits and cancellation, including real FFmpeg packaging.
- [ ] Run related regression suites, fmt/clippy, pin/build/smoke candidate and independent immutable review, including active vendor session reauthorization.
- [ ] Publish and qualify full exact-head CI. Preview promotion waits for qualification.
