# Shared HLS sessions implementation plan

> Execute inline with superpowers:executing-plans; use test-driven development.

**Goal:** retain admitted HLS demand across requests and own bounded scheduled renewal.
**Architecture:** an opt-in session owner wraps the existing shared registry and
packaging APIs; one caller-owned loop controls retention and staged renewal.
**Tech Stack:** Rust, Tokio, existing native PlaybackAuth, owned HTTP/FFmpeg tests.
**Spec:** docs/superpowers/specs/2026-10-13-shared-hls-sessions.md

## Global constraints

No official vendor dependency, production changes, automatic route activation,
credential logging, detached tasks, or first-viewer authorization anchors.
Use the accepted preview binary unchanged during builds/tests. Standing user
approval covers isolated development, testing and GitHub publication.

## Task 1: Session ownership and lifecycle

- Write owned packaging regressions for sequential requests, one retained native
  session, actual idle expiry, denied/stale/non-HLS requests, output mismatch,
  concurrent capacity reservations, shutdown and late startup.
- Run RED, then add `media/shared/sessions.rs` and internal native session identity
  access. Bound retained and in-flight sessions. Package every admitted request;
  retain the newest handle per native session. Gate installation on the run epoch.
- Verify GREEN and commit.

## Task 2: Scheduled renewal

- Add owned tests for source grouping, continuation past old deadlines, failed
  refresh/expiry, no retained demand and abort cleanup.
- Add metadata-only `SharedInputs::current_candidate` and fenced renewal info,
  then one bounded FuturesUnordered maintenance loop, no spawned tasks.
- Verify related source/auth/packaging suites, format, clippy and build. Smoke the
  candidate on an unused port. Review the exact branch head, fix important issues,
  publish it and require exact-head full CI before release.

## Review focus

Authorization/session identity, capacity races, lock order, no cache authorization
bypass, cancellation/drop ordering, idle fairness and renewal limits, failed
refresh authority, stale generation resurrection and process cleanup.
