# Equivalent-origin failover implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans inline, task-by-task.

**Goal:** Recover CDN/LB stream routes through explicitly equivalent origins without weakening viewer policy.
**Architecture:** Bounded native metadata fetcher plus serial-fenced sticky mirrors; existing media Engine performs replacement with preserved demand/HLS windows.
**Tech stack:** Existing Rust/Tokio/reqwest/FFmpeg and React/Playwright; no new vendor components.
**Spec:** docs/superpowers/specs/2026-10-02-origin-failover-v05.md

## Global constraints

- Optional native identities:1..128 ASCII letters/digits/dot/underscore/hyphen; eight sources per failover group.
- Four concurrent metadata requests;750ms each;3s total;1MiB JSON; no redirects.
- Same group/content identity/normalized viewer Policy required for an existing route switch. Disabled/invalid/404 known authority fails closed.
- Local precedence, source-specific peer keys/private paths, actual demand/config fences and original wire relay preserved.
- Never mutate or signal production/demo services. Standing user authorization covers isolated test copies and GitHub publication.

## Review focus

- A permissive replica cannot bypass disabled/changed/invalid authority.
- Hung management endpoints cannot prevent usable equivalents within bounded discovery.
- Late source responses cannot republish obsolete routes/policies after save or switch.
- Concurrent viewers coalesce one worker; shared demand/grants survive allowed changes.
- Missing content identity and differing peer keys cannot become accidental equivalence/credential forwarding.

### Task 1: Config contract and bounded directory fetch

**Files:** src/config.rs, new src/source_directory.rs, src/lib.rs; tests/config.rs, new tests/source_directory.rs.
**Interfaces:** query(&Client,&Value,&str,&str)->Result<Value,LookupFailure>; bounded source identity validation.
- [ ] Write/run RED validation and HTTP fixtures (hang, size, redirect, malformed, encoded path/key).
- [ ] Implement identity validation/group cap and independent bounded fetch helper.
- [ ] Run config/directory tests GREEN; commit/ledger.

### Task 2: Sticky authority-fenced source switching

**Files:** src/server.rs, src/source_directory.rs; new tests/source_failover.rs.
**Interfaces:** Clone Mirror with availability/publication serial/switch count; selected/fallback resolution and recovery-triggered discovery; safe runtime stats.
- [ ] Write/run RED actual source/CDN tests for media/API outage, equivalence mismatch/disabled, sticky route, per-source keys/local precedence and stale async lookup.
- [ ] Implement candidate discovery, serial/root revision fences, fail-closed authority and background switching after cooldown; retain worker demand.
- [ ] Run failover/cluster/auth/recovery regressions GREEN with actual decode; commit/ledger.

### Task 3: Friendly identities and cluster operational UI

**Files:** web/src/forms.tsx, web/src/main.tsx; web/tests/admin.spec.ts.
**Interfaces:** Content identity inheritance, source failover group form and active-pulls table from node stats.
- [ ] Write/run RED identity form persistence/inheritance.
- [ ] Add labeled controls, validation and source/transport/state/switch table; preserve existing design.
- [ ] Browser tests/build GREEN; commit/ledger.

### Task 4: Review, isolated qualification and publish

**Files:** versions/locks, README, qualification/compatibility/cluster docs and owned ignored helpers.
- [ ] Full Rust/browser/fmt/clippy/build; one fresh branch review and RED/GREEN fixes.
- [ ] Exact static candidate on isolated test nodes; temporary resource-limited replica on unused port; real background failover/auth/media/browser checks; production PID/listener preservation and configuration restoration.
- [ ] Whitelist/audit standalone package; fast-forward/push main under standing authorization; fresh vendor-absent CI; publish/download/check v0.5 release/tag.
- [ ] Complete ledger, archive owned scratch/temporary helpers and remove owned worktree only; preserve user test services.
