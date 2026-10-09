# Native TLS cluster recovery qualification

> **For agentic workers:** Use superpowers:executing-plans inline for this approved increment.

**Goal:** Qualify automatic equivalent-origin recovery through four real daemons, verified native TLS, origin GPU encoding and LB playback.

**Architecture:** Two equivalent protected origins, one CDN and one LB run on OS-selected loopback HTTPS ports. Stop only the owned primary origin; observe CDN recovery using read-only node telemetry, then independently decode protected HLS from the replacement generation. Origin encoding is retained upstream; the CDN copies native media.

**Tech Stack:** Rust integration tests, Tokio subprocesses, independent OpenSSL CAs, system FFmpeg/FFprobe, optional Intel H.264 VAAPI.

**Spec:** ../specs/2026-10-02-origin-failover-v05.md; this plan adds transport/hardware qualification to the existing behavior.

## Global Constraints

- Preserve preview PID/configuration/credentials/assets and production services.
- No vendor runtime/build dependency; use owned fixtures and unused ports.
- No viewer/media request, management write or explicit reconciliation during recovery observation.
- No seamless, mixed-vendor, HEVC hardware encoding or capacity claim.
- Clean up owned daemons and encoders even on failed assertions.

## Review Focus

- Cached media cannot prove recovery: require a new CDN PID, fresh bytes and a changed on-disk playlist before playback, then bind HTTP playlist/segment bytes to that observed generation.
- Shared worker behavior: concurrent viewers retain the same copy PID.
- Authentication: anonymous playlist/segment denial and ticket replay rejection before and after recovery.
- TLS trust: independent CAs are explicitly trusted; no insecure verification switches.
- Teardown: signal only owned processes, detect child leaks even after successful daemon exit.

### Task 1: Real-daemon cluster qualification

**Files:** Create tests/cluster_native_recovery.rs and tests/support/cluster_daemon.rs; update docs/qualification.md, docs/cluster-loadbalancing.md and .github/workflows/ci.yml.

**Interfaces:** Consume ConfigStore::put, CLI HTTPS startup JSON and peer-authenticated GET /flussonix/api/v1/node. Produce one ordinary CPU regression and two opt-in GPU native TLS cases.

- [x] Build four-daemon harness and CPU M4SS regression; run it and inspect failures before any product changes.
- [x] Verify the regression fails with temporary background source refresh disabled; restore production source and run GREEN.
- [x] Run opt-in H.264 VAAPI M4SS and M4FS cases; record origin driver mappings, CDN copy arguments, strict decode counts and observation latency.
- [x] Run related cluster tests, fmt and clippy. Expected: all pass.
- [x] Obtain one fresh read-only review; fix Important/Critical issues with targeted negative controls.
- [ ] Publish the reviewed branch and require exact-head full Rust/browser CI before fast-forward publication to main.
- [ ] Restore original shared daemon artifact, verify preview identity/config/assets, archive evidence and remove owned test worktree.
