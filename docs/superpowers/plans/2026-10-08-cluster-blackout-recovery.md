# Complete origin blackout recovery implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan inline. Steps use checkbox syntax for tracking.

**Goal:** Restart an on-demand CDN pull after all origins temporarily disappear while existing authorized playback demand remains valid.

**Architecture:** Keep policy authority separate from transient route availability. Derive bounded recovery demand from existing PlaybackAuth entries, and preserve its activity clock through guarded Engine startup. Reuse existing discovery and publication fences.

**Tech Stack:** Rust/Tokio/Axum, FFmpeg, owned HTTPS daemon fixtures.

## Task 1: Authorization and lifecycle

Produces: `PlaybackAuth::recovery_demand` (recent allowed playback by stream), `Engine::recover_demand_guarded` (seeded actual activity), guarded mirror-only recovery in `App::reconcile`.
Consumes: current Policy authority, available mirror routes, existing engine startup fence.

- [ ] Add tests for valid demand versus controls, expiry, callback validity, revocation and policy changes; observe meaningful RED.
- [ ] Add blackout recovery integration RED using current source fixtures.
- [ ] Preserve policy only on transient known-origin unavailability; maintain denial and config invalidation.
- [ ] Implement demand query and engine demand-clock seeding; guard route and authorization at startup and after startup.
- [ ] Run auth/lifecycle/source regressions and commit tested implementation.

## Task 2: Real-daemon qualification and release

Produces: ordinary CPU and optional GPU native-TLS complete-blackout tests, qualified documentation and GitHub candidate.
Consumes: Task 1 recovery contract; existing daemon TLS/media decode helpers.

- [ ] Restore owned origins at their original listener addresses; do not reconfigure CDN or issue viewer requests during recovery observation.
- [ ] Prove media stopped during blackout and fresh-generation protected HTTP media decodes after autonomous restart; exactly one shared CDN pull and clean shutdown.
- [ ] Pin no restart after expiry/removal/denial and stale queued-start guards with deterministic unit/integration coverage.
- [ ] Run related regressions, fmt and warnings-denied Clippy; commit.
- [ ] Request one fresh read-only whole-branch review; fix Important/Critical findings in one TDD pass.
- [ ] Run full exact-head CI; publish and upgrade the preview using preserved configuration/environment/assets and rollback binary. Verify health and process cleanup.

## Execution ledger

Preflight: existing authority/entry locks use authority -> entries -> state; recovery follows the same order. Existing daemon fixtures permit address reuse only after owned child shutdown. No independent recovery map or vendor dependency. User has already authorized development, testing and GitHub publication; no repeated approval required.
