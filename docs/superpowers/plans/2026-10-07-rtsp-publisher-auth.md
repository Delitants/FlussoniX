# Incoming RTSP Publisher Authentication Implementation Plan

> For agentic workers: use superpowers:executing-plans to implement inline.

**Goal:** Authenticate configured RTSP/RTSPS publications with standard headers.
**Architecture:** A private publication authentication module verifies credentials
against the existing effective policy. The receiving loop owns bounded challenge
retries before allocating sessions or workers.
**Tech Stack:** Rust, Tokio, MD5, Base64, independent FFmpeg, React/Playwright.
**Spec:** ../specs/2026-10-07-rtsp-publisher-auth.md

## Global Constraints

- No official Flussonic runtime/build/test dependency; owned unused loopback ports.
- Preserve saved configuration, credentials, existing callbacks and workers.
- Three challenges and one absolute 30-second negotiation deadline.
- Existing query passwords retain admission behavior; combined credentials fail.
- One shared worker, only after RECORD; cleanup every owned media process.

## Review Focus

- Configuration/password change during challenge must fail under old credentials.
- Header/query ambiguity and malformed quoted parameters must not bypass policy.
- Nonces cannot be reused on a different connection, URI or method.
- Callbacks still deny valid passwords and never receive header credentials.
- Cancellation/deadlines bound unauthenticated retries without publisher leases.

### Task 1: Incoming authentication, UI guidance and independent qualification

**Files:** src/rtsp/publication.rs, new publication/auth.rs;
tests/rtsp_publication.rs; web/src/forms.tsx; publication/qualification docs.
**Consumes:** existing Snapshot/Policy and bounded Request framing.
**Produces:** header-authenticated publication using existing session ownership.

- [ ] Add socket RED tests for Basic admission and Digest challenge/retry.
- [ ] Verify they fail for missing behavior; implement strict bounded verification.
- [ ] Add adversarial admission, inheritance/callback/revocation cases.
- [ ] Independently publish and strictly decode TCP/UDP/verified TLS media.
- [ ] Update friendly help and documentation; run affected suites, fmt/Clippy,
  normal build and browser checks. Run full exact-head CI.
- [ ] Fresh read-only whole-range review; resolve important findings with RED/GREEN.
- [ ] Publish only after final exact-head CI succeeds; preserve preview state,
  archive proof and stop owned fixtures.
