# Complete origin blackout recovery implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan inline. Steps use checkbox syntax for tracking.

**Goal:** Restart an on-demand CDN pull after all origins temporarily disappear while existing authorized playback demand remains valid.

**Architecture:** Keep policy authority separate from transient route availability. Derive bounded recovery demand from existing PlaybackAuth entries, and preserve its activity clock through guarded Engine startup. Reuse existing discovery and publication fences.

**Tech Stack:** Rust/Tokio/Axum, FFmpeg, owned HTTPS daemon fixtures.

## Task 1: Authorization and lifecycle

Produces: `PlaybackAuth::recovery_demand` (recent allowed playback by stream), `Engine::recover_demand_guarded` (seeded actual activity), guarded mirror-only recovery in `App::reconcile`.
Consumes: current Policy authority, available mirror routes, existing engine startup fence.

- [x] Add tests for valid demand versus controls, expiry, callback validity, revocation and policy changes; observe meaningful RED.
- [x] Add blackout recovery integration RED using current source fixtures.
- [x] Preserve policy only on transient known-origin unavailability; maintain denial and config invalidation.
- [x] Implement demand query and engine demand-clock seeding; guard route and authorization at startup and after startup.
- [x] Run auth/lifecycle/source regressions and commit tested implementation.

## Task 2: Real-daemon qualification and release

Produces: ordinary CPU and optional GPU native-TLS complete-blackout tests, qualified documentation and GitHub candidate.
Consumes: Task 1 recovery contract; existing daemon TLS/media decode helpers.

- [x] Restore owned origins at their original listener addresses; do not reconfigure CDN or issue viewer requests during recovery observation.
- [x] Prove media stopped during blackout and fresh-generation protected HTTP media decodes after autonomous restart; exactly one shared CDN pull and clean shutdown.
- [x] Pin no restart after expiry/removal/denial and stale queued-start guards with deterministic unit/integration coverage.
- [x] Run related regressions, fmt and warnings-denied Clippy; commit.
- [x] Request one fresh read-only whole-branch review; fix Important/Critical findings in one TDD pass.
- [x] Run full exact-head CI; publish and upgrade the preview using preserved configuration/environment/assets and rollback binary. Verify health and process cleanup.

## Execution ledger

Preflight: existing authority/entry locks use authority -> entries -> state; recovery follows the same order. Existing daemon fixtures permit address reuse only after owned child shutdown. No independent recovery map or vendor dependency. User has already authorized development, testing and GitHub publication; no repeated approval required.

Implementation candidate: `4808d53`; 128 library and 57 related integration tests pass after one review fix pass. Meaningful RED/GREEN evidence covers blackout restart, local precedence, changed policy, callback-retry deadline, stale control promotion/byte accounting, denial/unique preemption, management Stop and queued restart/admission fencing.

Final review found two Important authorization/lifecycle gaps, both fixed: canceled activity generations and explicit operator Stop. One Minor remains deferred: startup guards scan the whole authorization cache; candidate-specific validation is the next performance optimization. No measured capacity claim is made. Serial final hardware repetition, full exact-head CI and preserved preview upgrade remain release gates, with observations recorded outside the tracked tree to avoid changing the commit being qualified.
