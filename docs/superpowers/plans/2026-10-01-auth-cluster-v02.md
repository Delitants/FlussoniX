# Playback sessions and measured uplink implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a tested next preview with renewable/revocable playback sessions and real interface-aware load balancing.
**Architecture:** A focused playback authorization module owns decisions, single-flight checks and cancellable grants. The HTTP layer consumes grants, a separate sampler owns Linux telemetry, and the existing supervisor drives renewal/sampling.
**Tech Stack:** Rust 1.99/Tokio/Axum/Reqwest; existing FFmpeg workers and React UI. No new service dependency.
**Spec:** docs/superpowers/specs/2026-10-01-auth-cluster-v02.md

## Global Constraints

- Existing test installations remain isolated from production Flussonic.
- Owned media, allocated local ports and private ignored credentials only.
- Tokens/raw queries never appear in management session responses.
- Global distributed session limits remain open.
- Missing/reset/stale counters and initial warmup are unknown, never zero-load evidence.
- Preserve the full remaining transport/mixed-vendor/GPU scope.

## Review Focus

- Reauthorization racing a config save or manual deletion cannot restore revoked access; Task 2 race regressions.
- Multiple same-identity requests and different-stream requests share decisions but enforce authoritative local user limits; Task 1 concurrent callback/limit tests.
- Revocation closes TS/M4 bodies even while a producer is silent, while peer pulls remain separate; Task 2 live-body tests.
- Lost, reset or obsolete interface samples exclude an edge instead of making it look idle; Task 3 fixture/admission tests.
- Source-policy portability and metadata-only edits retain correct identity and do not bypass a changed backend; Task 2 policy tests.

### Task 1: Structured policy and callback session engine

**Files:** create src/playback_auth.rs, tests/playback_auth.rs; modify src/config.rs, src/lib.rs, src/server.rs.
**Interfaces:** produces Policy::from_config(cfg, root), ViewerRequest, PlaybackAuth::authorize(policy, request) -> AuthOutcome, cancellable Grant with bytes/activity; consumes existing effective configuration and separate peer credentials.
- [ ] Write tests for valid object policy / rejected unsupported fields, required callback parameters and UUID, concurrent first authorization, X-Max-Sessions across streams and cached redirect.
- [ ] Run scripts/cargo-local test --test playback_auth --test config. Expected: new contract assertions fail against the preview.
- [ ] Implement typed policy and bounded single-flight registry, explicit allow/deny/redirect/unavailable outcomes, standard headers and identity keys; adapt initial HTTP authorization to grants without changing worker ownership.
- [ ] Run scripts/cargo-local test --test playback_auth --test config. Expected: PASS plus all existing config cases.
- [ ] Commit the independently testable callback/config behavior.

### Task 2: Renewal, revocation and session API

**Files:** modify src/playback_auth.rs, src/server.rs, src/main.rs; test tests/playback_auth.rs and tests/api.rs.
**Interfaces:** consumes Task 1 grants; produces PlaybackAuth::renew_due, invalidate/revoke, session snapshots and stream reauth operations; body fan-out observes grant cancellation.
- [ ] Write regressions for default/header renewal, explicit deny, backend outage retaining a prior decision but denying a new identity, DELETE/edit permissions, reauth envelope, inactive expiry and changed-policy/manual-revoke races.
- [ ] Run scripts/cargo-local test --test playback_auth --test api. Expected: renewal/revocation assertions fail before integration.
- [ ] Implement bounded renewal scheduling, cancellation-aware live HTTP bodies, mutation invalidation and GET/DELETE/reauth APIs. Preserve grants through metadata-only edits and portable object policy on source discovery.
- [ ] Run scripts/cargo-local test --test playback_auth --test api --test cluster_integration --test shutdown. Expected: PASS including existing wire topology and child cleanup.
- [ ] Commit session lifecycle behavior.

### Task 3: Measured interface telemetry

**Files:** create src/telemetry.rs, tests/telemetry.rs; modify src/main.rs, src/server.rs, src/lib.rs and cluster tests.
**Interfaces:** consumes daemon HTTP byte counter; produces sampled optional CPU/RAM/uplink and source/interface/age metadata; admission consumes only valid numeric measurements.
- [ ] Write fixture tests for auto route/name validation, bytes/time conversion, unrelated-process traffic, initial warmup, reset, missing and stale counters; test unknown metrics exclude routing/admission.
- [ ] Run scripts/cargo-local test --test telemetry --test auth_cluster. Expected: missing sampler or behavior assertions fail.
- [ ] Implement --uplink-interface and independent sampling. Keep process egress separate. Adjust topology launch helpers to wait for valid warmup samples rather than assuming zero load.
- [ ] Run scripts/cargo-local test --test telemetry --test auth_cluster --test cluster_integration. Expected: PASS and the same source/CDN/LB media delivery.
- [ ] Commit telemetry behavior.

### Task 4: Qualification, review and publication

**Files:** modify web/src/main.tsx, README.md, docs/qualification.md, versions; extend browser tests only for material new behavior.
**Interfaces:** consumes Tasks 1–3 API fields; UI describes live telemetry source and session controls in existing views.
- [ ] Add auth-tab session controls backed by the actual API and label interface versus process egress; verify browser interaction against an allocated-port test instance.
- [ ] Run fmt, clippy -D warnings, cargo test, web build/browser tests. Expected: all PASS; opt-in live source qualified separately.
- [ ] Build static x86_64 musl release, run one fresh whole-branch review against this plan/spec, fix Important/Critical findings with RED/GREEN regressions.
- [ ] Qualify only isolated copies on remote unused test ports; check production Flussonic remains unchanged. Record exact supported policy/telemetry behavior and remaining limitations.
- [ ] Merge the verified branch, publish GitHub preview and observe fresh-runner CI. Expected: published coherent artifact and green checks; secrets/vendor files excluded.
