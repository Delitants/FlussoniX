# Recovery authorization performance plan

> Use superpowers:executing-plans for inline implementation and one fresh read-only whole-branch review.

Spec: `docs/superpowers/specs/2026-10-08-recovery-auth-performance.md`.

## Task 1: Candidate-scoped checks

Produces: internal `RecoveryDemand` carrying activity and cached session references; `PlaybackAuth::recovery_allowed(name, &RecoveryDemand) -> bool`.
Consumes: existing authority, entries, State and recovery eligibility rules.

- [x] Add a guard contention test: capture demand, hold an unrelated session's State lock, and require the candidate check to complete before that lock is released. Establish meaningful RED with the old full-map algorithm behind the new interface.
- [x] Add literal authorization-boundary cases after capture and a retired/replaced-entry case. Missing, mismatched and control-only demand must fail; any current eligible captured session suffices. Observations must not renew activity.
- [x] Extract one eligibility predicate and capture existing cache identities/references once. Revalidate cache membership and current authority/state only for the candidate, retaining lock order. Expected: the contention test and authorization tests pass.

## Task 2: Integrate and qualify

Produces: guarded App recovery using `Option<&RecoveryDemand>`; verified publication and preserved preview.
Consumes: Task 1 candidate checks and activity timestamp.

- [x] Pass the same candidate to all three guards; seed Engine activity from the original demand timestamp. Update queued recovery tests without weakening assertions.
- [x] Run fmt, warnings-denied Clippy, library/auth/source/recovery suites and the ordinary CPU real-daemon blackout regression. Expected: all pass, fixtures exit cleanly.
- Release gate: request one fresh whole-branch review; fix Important/Critical findings in one TDD pass. Ledger deferred minors.
- Release gate: run full exact-head CI, fast-forward main, publish and upgrade preview with rollback and preserved settings/environment/assets. Verify health, GitHub head, artifact/PID metadata and no owned fixture processes.

## Review focus

Captured references must not bypass Stop, revoked/expired/canceled state, a changed policy or cache eviction. Hash identity lookup must also validate pointer identity to reject replacement. No repeated whole-cache scan may remain in candidate guards. Snapshot enumeration still occurs once per pass, and no clock is renewed by any observation.

## Execution decisions

Standing user authorization covers isolated development, local testing, GitHub publication and the qualified preview upgrade. The prior approved next step is this optimization; no repeated design approval is needed. Full CI is the broad project-suite gate; targeted local checks avoid duplicating the long full media suite on this slow host. Hardware behavior is unchanged, so CPU blackout is the appropriate new daemon regression; previous hardware results are not claimed as newly repeated.

Candidate verification: 132 library tests and 57 related integration tests pass (four opt-in hardware cases excluded); formatting, warnings-denied all-target Clippy and whitespace checks pass. No FFmpeg/FFprobe fixtures remain. The review, exact-head CI and preview publication gates are recorded by commit in private `.runtime/recovery-auth-performance-records` and the public GitHub workflow, rather than changing this candidate after qualification.
