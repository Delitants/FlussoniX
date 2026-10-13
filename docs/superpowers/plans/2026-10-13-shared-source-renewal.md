# Shared source lease renewal

Use the existing shared-input and packaging APIs. Add explicit, caller-owned
`SharedInputs::renew(handle, lifetime)`; no automatic routing or detached timer.
Equivalent direct-source streams retain their generation, connection, packager
and independent viewer grants. Ordinary inventory refresh still invalidates
immediately. Renewal must complete before both old and proposed deadlines.

1. Add owned HTTP regressions in `tests/vendor_shared_renewal.rs`: uninterrupted
   renewal, fresh viewer joining, stale standalone admission, changed/removed
   rows, failed/cancelled/expired refresh, concurrent attempts, ordinary refresh,
   config/identity/install changes, shutdown and exact generation retirement.
   Add a real FFmpeg continuity regression to `vendor_shared_packaging.rs`.
2. Add opaque staged discovery in `flussonic_config/modern/lease/renewal.rs`.
   Reserve a separate newest renewal turn without replacing the published lease;
   observe complete config/pages/config outside locks; prepare route decisions,
   recheck old/new deadlines, and publish all decisions under one source lock.
3. Store a grant-independent mutable route per shared worker. Current checks use
   config -> coordinator -> source -> route -> member locks. Preserve per-viewer
   revocation; immutable standalone admissions retain their original ticket.
4. In `admission/shared/renewal.rs`, fence registry shutdown epochs, update only
   currently installed equivalent workers, and retire missing/ineligible/changed
   candidates. Match observed ID, source/profile, literal name, canonical local
   policy, kind and selected-static status. Do not revive expired/closed workers.
5. Verify focused tests, related source/admission/packaging suites, full CI,
   formatting and clippy. Request immutable-head review, fix important findings,
   publish branch. Keep the qualified preview until exact-head release checks pass.

No live vendor, capacity, GPU or failover qualification is implied by this step.
