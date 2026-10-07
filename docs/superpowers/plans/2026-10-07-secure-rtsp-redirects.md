# Verified RTSPS redirects implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans. Execute inline with TDD and one fresh final whole-range reviewer.

**Goal:** Decode native TLS LB → CDN → private-source input while verifying every hop.
**Architecture:** Intercept bounded pre-session 301/302 inside the owned bridge; verify next TLS socket, redirect FFmpeg only to a new owned loopback listener. One root task owns the chain.
**Tech Stack:** Rust/Tokio/rustls, native cluster fixtures and system FFmpeg.
**Spec:** docs/superpowers/specs/2026-10-07-secure-rtsp-redirects.md

## Global constraints

- Absolute strict RTSPS destinations, no userinfo/fragment/port0/downgrade; no inherited tokens.
- Four redirects; twenty-second initial routing deadline; TCP/TLS10seconds; unused listener8seconds.
- Header16KiB/64 unique keys, body64KiB, interleaved8192bytes. No redirects after SDP/Session/media.
- No vendor components, production changes or detached hop tasks. Preserve direct credentialed behavior and worker retry ownership.

## Review focus

- Redirected TLS identity/trust fails before token/auth application bytes.
- Invalid Location/CSeq/body framing never leaks a remote response to FFmpeg.
- Established sessions cannot be moved by a late redirect; credentialed inputs cannot delegate secrets.
- Slow peers, unused listeners and cancellation release every chain resource.
- Native CDN consumes its bound ticket and still independently authorizes/reuses its private source.

### Task: Verified bridge chain and decoded native topology

Files: src/tls_input.rs, src/tls_input/redirect.rs if needed, src/rtsp.rs, src/rtsp/redirect.rs, tests/tls_input.rs, tests/tls_redirect.rs, tests/rtsp_cluster.rs, docs/rtsp-cluster-routing.md, docs/qualification.md, docs/compatibility.md.
Interfaces: Bridge::prepare/start/local_url/close unchanged; reuse crate-visible RTSP destination validator. Internal bounded response reader returns a validated pre-session redirect without forwarding it. Supervisor verifies destination and produces an owned local Location before returning control to FFmpeg.

- [x] Add `verified_redirect_uses_owned_loopback_and_delivers_final_tls_response`; run exact and record RED old bridge closes without redirect.
- [x] Implement bounded owned chain, shared strict URI validator, certificate verification before local redirect, cycle/hop/deadline limits and post-session rejection.
- [x] Add real TLS rejection/credential/framing/lifecycle cases. Update earlier blanket-redirect test to assert no remote redirect escape and no application bytes before trust, retaining plaintext zero-connect check.
- [x] Add `configured_rtsps_relay_decodes_native_lb_cdn_source_chain`: CA bundle for LB/CDN, verified private HTTPS M4S source, actual configured worker, strict independent audio/video decode, LB workers0/CDN1/source1/shared M4S pull1, denied-token no worker. Run RED then GREEN.
- [x] Run fmt/clippy, lib/tls_input/rtsps/rtsp_cluster/rtsp_auth_redirect/rtsp/cluster_tls/playback_auth, normal build and existing UI build; stop/reap all owned fixtures.
- [ ] Document precise qualified scope. Commit immutable candidate, fresh read-only whole-range review and full exact-head CI; resolve any blocker with RED/GREEN evidence.
- [ ] Publish authorized main/preview only after gates; verify hashes/assets/config/full environment/protected listeners/health and no test processes. Archive evidence and remove only owned worktree.

Pre-flight: existing destination validator enforces raw URI safety; bridge ownership and worker API remain unchanged. The fresh endpoint causes FFmpeg to repeat its own handshake and consume the CDN ticket once, rather than replaying a native admission internally. Single deliverable; setup/docs belong to its qualification. Execution and publication use standing authorization without redundant approval handoffs.
