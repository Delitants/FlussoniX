# Native RTSP cluster routing implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans. Execute inline with TDD and one fresh final review.

**Goal:** Native secure RTSP302 placement through measured CDN selection and bound reservations.
**Architecture:** Existing selection/admission ownership extended with compact cached RTSP telemetry; CDN authorization remains independent and precedes ticket consumption/media acquisition.
**Tech Stack:** Rust/Tokio/Axum, React/Playwright, independent FFmpeg fixtures.
**Spec:** docs/superpowers/specs/2026-10-07-rtsp-cluster-routing.md

## Global constraints
- No official Flussonic component or production changes.
- Native pool<=64; snapshot cache1second, network concurrency8, body<=2MiB, timeout3seconds.
- Existing select hard ceilings/weighted policy; projected2Mbit/s per RTSP request; reservations5seconds.
- Actual TLS socket never downgrades; no peer credentials in viewer URL.
- Friendly UI; no JSON inputs. Preserve API/config/environment/listeners on release.

## Review focus
- Peer configuration changes/revocation during delayed placement: no stale redirect.
- Wrong protocol/token/stream ticket use: no bypass or destruction of a legitimate reservation.
- Large/malformed/stale management replies: bounded memory, no stale fallback, no credentials forwarded.
- Listener/role/self-route mismatches: no HTTP-as-RTSP, secure downgrade or direct loop.
- Concurrent viewers/LBs: chosen edge owns final capacity; cached load does not authorize media.

### Task1: Validated friendly public listener addresses
Files: src/config.rs, web/src/forms.tsx, tests/rtsp_cluster.rs, web/tests/admin.spec.ts.
Interfaces: config peers produce optional flussonix_rtsp_url / flussonix_rtsps_url string fields; Task2 consumes validated listener roots.
- [ ] Add config test `public_rtsp_endpoints_validate_and_persist_without_mutating_failed_edits`: valid root schemes save/reopen; query/path/userinfo/unsafe/malformed/zero-port/wrong collection/scheme rejected without state mutation.
- [ ] Run cargo test --test rtsp_cluster public_rtsp_endpoints; Expected RED unsupported field on valid save.
- [ ] Add validated optional peer-only fields using existing strict destination validator.
- [ ] Add browser test `cluster peer RTSP delivery fields validate save edit and clear`: friendly inputs, wrong scheme inline error, API persistence, masked key/no textarea.
- [ ] Expose friendly fields and server-equivalent endpoint validation; npm build and selected owned-daemon browser test. Expected pass.
- [ ] Commit config/UI deliverable; record evidence and interface ruling.

### Task2: Native measured RTSP placement and CDN ticket admission
Files: src/server/rtsp_balancer.rs, src/server.rs, src/server/rtsp_access.rs, src/rtsp.rs, src/media.rs, tests/rtsp_cluster.rs, tests/rtsp.rs, docs/rtsp-cluster-routing.md, docs/qualification.md, docs/compatibility.md, README.md.
Interfaces: App::rtsp_admit(viewer:ViewerRequest, source:&url::Url, secure:bool)->Result<Admission,u16>; App owns rtsp_balancer::Registry; Reservation adds kind and bitrate; HTTP default kind retained. Engine::rtsp_ready_names()->Vec<String> fresh supported worker names. App::load_node()->Value shared node telemetry. Peer GET rtsp-routing compact JSON, peer POST admit optional protocol/token_hash.
- [ ] Real-socket native LB redirect test `native_lb_routes_before_media_and_preserves_credentials`: valid token/query routes, bad token creates no RPC/reservation/worker. Expected RED oldLB501.
- [ ] Implement cached bounded compact control snapshot, existing-policy ranking with per-node bandwidth cost, candidate role/listener/self/unsafe checks, short protocol/token-bound admission and bounded retry.
- [ ] Extend admit ledger and HTTP kind checks; sum outstanding reservation bandwidth under lock. RTSP source resolution before trusted reservation but no media/auth callback.
- [ ] Authorize LB/CDN before placement/consume; strip internal ticket qs; retain same-connection session URI and normal worker/policy fences.
- [ ] Add and run tests for token/stream/transport/protocol ticket mismatch, replay/expiry, concurrent capacity, stale/drain/malformed/cache polls, admission retry, config/revocation while paused and independent private-M4S audio/video decode/local reuse; TLS verified routing/no downgrade and HTTP/callback regressions. Expected all pass/no owned children.
- [ ] Update prior LB501 regression to absent-capacity503; document precise estimate/scale/client limitations and accurate capability status.
- [ ] Run fmt/clippy/lib/config/rtsp/rtsp_auth_redirect/rtsps/auth_cluster/cluster_integration/cluster_tls, new suite, normal build and web build. Expected0 failures; owned FFmpeg/FFprobe gone.
- [ ] Commit final candidate; fresh read-only whole-range review, resolve blockers with RED/GREEN evidence; exact-head full CI required.
- [ ] Publish authorized main/preview from qualified head, verify binary/assets/config/env/listeners/health; archive evidence and remove only owned worktree. Expected clean root and no media test processes.

Pre-flight: Task1 produces exact peer field names consumed by Task2. HTTP defaults remain unchanged; RTSP additive kind cannot consume HTTP tickets. No incompatible interface conflict. Authorization and actual transport are owners of access and security; cache is advisory.
Execution ruling: inline and isolated under standing development/publication authorization; no redundant approval gates. Spec and plan reviewed for coverage, signatures and bounds before product edits.
