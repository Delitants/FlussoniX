# Vendor processing templates implementation plan

> Execute inline with superpowers:executing-plans; one independent review of the final branch.

**Goal:** Apply selected Media template processing to guarded vendor HTTP playback.
**Architecture:** A strict local template projection feeds existing shared packaging. Exact config revisions fence selection and edits. Configured registry attachment owns conversion state per input generation, while the existing HLS owner retains admitted demand.
**Tech stack:** Rust/Tokio, FFmpeg, React/TypeScript, Playwright.
**Spec:** ../specs/2026-10-13-vendor-output-templates.md

## Constraints and review focus

- Preserve native defaults, explicit embedding APIs and viewer/source fences.
- Output projection is exactly five fields; no upstream URL, policy or credentials.
- Fresh conversion state on worker replacement; canonical state for concurrent attachments.
- Referenced-template removal/invalid edits fail atomically; valid edits retire old output.
- All fixture daemons, FFmpeg and probes must be owned and reaped; preserve accepted preview.

## Task 1: Persist and resolve processing references

Files: new `src/vendor_output.rs`; `src/config/vendor_sources.rs`,
`src/source_directory/flussonic_config.rs`, `src/lib.rs`;
`tests/vendor_output_templates.rs` and `tests/vendor_source_selection.rs`.

- [x] Write and observe failing tests for saved `flussonix_output_template`, reopen/coordinator preparation, source-only/native rejection and missing/invalid/deleted references with unchanged disk/revision.
- [x] Implement `vendor_output::resolve(root: &Value, source: &Value) -> Result<Value, String>` and `validate(settings: &Value) -> Result<(), String>`, using existing transcoder/subtitle/caption validation and exact field projection.
- [x] Add the source field gate and validated reference removal in complete private-client projection. Preserve group-policy rejection of output references.
- [x] Run focused configuration/coordinator regressions.

## Task 2: Apply processing under shared generation ownership

Files: `src/source_directory/modern_coordinator/admission/shared.rs`,
`src/media/shared/sessions.rs`, `src/vendor_playback/response.rs`;
`tests/vendor_daemon_playback.rs`.

- [x] Add owned HTTP tests showing selected CPU conversion changes delivered codec and decodes, subtitle preservation/filtering, English/German HLS conversion, concurrent viewer reuse, edit revocation and authorized cached segments.
- [x] Add crate-private `SharedInputs::attach_configured(admission, settings)` through the existing locked attachment path. Construct fresh native conversion state for a new worker, reuse matching current worker state, reject mismatches.
- [x] Add `SharedHlsSessions::package_configured(engine, admission, settings)` through the existing reservation/package/retention path; preserve public explicit-options API.
- [x] Resolve template settings using `ConfigStore::at_revision(candidate.config_revision(), ...)` before admission; use configured attachment for all vendor HTTP outputs.
- [x] Run daemon, shared input/packaging/HLS/renewal, caption and source/edit regressions; check all child cleanup.

## Task 3: Friendly template selection and qualification

Files: `web/src/vendorSourceFields.tsx`, `web/src/forms.tsx`,
`web/tests/vendor-sources.spec.ts`, vendor operator docs.

- [x] Update the existing Superdesign draft using only public UI context and fictional data.
- [x] Observe browser RED for Processing template selection, readable summary, direct/staged persistence and explicit clearing on native conversion.
- [x] Implement the selector using existing templates props; retain the reference until explicitly cleared. Show only projected processing in the summary.
- [x] Verify browser build and related vendor/native cases on an unused local port.
- [ ] Run final relevant Rust checks, fmt/clippy/build; pin artifacts and smoke exact candidate; obtain immutable independent review; publish branch/full exact-head CI. Preview promotion waits for qualification.
