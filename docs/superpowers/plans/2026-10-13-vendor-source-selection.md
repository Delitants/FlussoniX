# Vendor source selection implementation plan

**Goal:** Persist explicit vendor source type and local viewer policies, with atomic validation and exclusion from native discovery.

**Architecture:** Share strict normalized-source projection between ConfigStore and the opt-in coordinator. Gate both native directory I/O and its source schedulers. Keep automatic daemon vendor routing disabled.

**Spec:** ../specs/2026-10-13-vendor-source-selection.md

**Constraints:** Independent Rust implementation; no production changes; preserve native behavior, preview credentials/configuration, and all source/authorization fences.

- [x] Add behavioral tests for persisted policy, configuration transaction failures, strict coordinator preparation, source kind transitions and absence of native HTTP traffic. Observe failures before implementation.
- [x] Implement shared strict projection, source-only selection/policy fields, bounded vendor configuration and native directory/scheduler guards.
- [x] Run config, native source and vendor regression suites, formatting and clippy; build an isolated candidate.
- [ ] Pin/smoke the candidate, reap test processes, review the immutable increment, resolve findings, publish the branch and obtain full exact-head CI qualification. Keep preview unchanged until qualified.
