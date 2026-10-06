# RTSP publication implementation plan

> Execute inline using executing-plans and test-driven-development under the user's continuing development and publication authorization.

**Goal:** Receive live RTSP and encrypted RTSPS publications on configured publish:// streams with shared worker ownership and existing publisher authorization.
**Spec:** docs/superpowers/specs/2026-10-06-rtsp-publication.md
**Architecture:** Hand an initial ANNOUNCE on an unused RTSP connection to a separate bounded publisher session using the existing reader and writer. Reuse publisher admission snapshots and callback renewal; add an explicit SDP publication worker mode. A native validated private RTP bridge supplies the same FFmpeg worker as HTTP and downstream delivery.
**Stack:** Rust/Tokio/rustls, standalone FFmpeg, existing React forms.

## Global Constraints

- Work in .worktrees/rtsp-publication on codex/rtsp-publication, baseline 82f6d90. Preserve the running preview executable, assets and configuration before builds. Use unused loopback listeners only.
- No vendor runtime dependency, production host changes, credential publication or unrelated cleanup.
- Shared publisher permit limit, exactly one worker per active stream, generation-specific cleanup, finite queues and timeouts.
- Every feature or bug fix has observed behavior RED then GREEN. Full exact-head CI is required before integration. One fresh whole-branch review after all tasks; one fix pass for Important/Critical findings.

### Task 1: Receive and authorize RTSP publications through the shared worker

**Files:** src/rtsp.rs, src/rtsp/protocol.rs, src/rtsp/publication.rs, src/rtsp/publication/sdp.rs, src/media.rs, src/server/publication.rs, src/server.rs, src/direct_rtp/elementary/{packet,readiness,mod}.rs, tests/rtsp_publication.rs
**Interfaces:** Produces an ANNOUNCE/SETUP/RECORD receiving path for existing RTSP/RTSPS listeners, a guarded SDP publication worker entry and shared admission helpers. Consumes existing publish:// configuration, Policy, media signatures, decoder SDP/packet validation and worker generation fences. No configuration schema change.
**Completion contract:** Real control tests cover admission before worker creation, password and callback denial, disabled/LB/missing/non-publisher streams, URL/session/channel binding, codec bounds, idle revocation, stalled media, conflict and disconnect/reconnect. Independent FFmpeg publishes H264/AAC to the real listener and shared TS independently decodes. Existing playback and HTTP publication tests pass.

1. Add real listener tests expecting successful ANNOUNCE and no worker before RECORD; run cargo test --locked --test rtsp_publication.
   Expected: fails at missing publication (501), never at test setup.
2. Implement the native SDP profile, record-mode transport parser, guarded worker mode and publisher state machine. Share the existing callback snapshots rather than duplicate policy semantics. Add boundary cases before the corresponding implementation.
   Expected: protocol/admission tests pass and no unauthorized worker exists.
3. Add independent FFmpeg input/strict shared TS decode test, observe RED, implement the bridge and readiness/lifecycle behavior, run targeted suite.
   Expected: meaningful decoded video/audio frames, one worker, clean teardown and reconnect; existing playback and HTTP publication tests pass.
4. Commit implementation and tests.
   Expected: clean committed task diff.
5. Run task-done with cargo test --locked --test rtsp_publication --test rtsp_protocol --test rtsp --test publication -- --test-threads=1.
   Expected: all targeted tests pass.

### Task 2: Qualify media, TLS and friendly publication controls

**Files:** tests/rtsp_publication.rs, tests/rtsps.rs or dedicated publication TLS test, src/server.rs, src/main.rs, web/src/components/ConfigurationFields.tsx and form parents, web/tests/admin.spec.ts, README.md, docs/COMPATIBILITY.md, docs/testing/rtsp-publication.md
**Interfaces:** Consumes Task 1's receiving path and codec profile; produces independently decoded media qualification, actual listener capability metadata and friendly URL fields. Existing HTTP Publication URL label remains usable.
**Completion contract:** HEVC, AAC, MP2/MP3, multi-audio/audio-only and CPU profile shared TS strictly decode; encrypted owned TLS input decodes with certificate verification and plain/TLS downgrade rejection; active callback renewal denies and reaps; config changes during admission revoke. UI capabilities show actual enabled addresses and hide unconfigured URL fields; browser test passes. H264 VAAPI is explicit opt-in if available, no HEVC GPU claim.

1. Write qualification and listener/UI tests first; run each before the relevant behavior addition.
   Expected: cases testing already-shared codecs may pass as qualification evidence; new listener metadata/UI fails until wired.
2. Wire listener metadata into capability response and friendly receive-publication URLs. Qualify independent publisher media and secure input; correct actual defects with failing reproduction first.
   Expected: real decoded outputs and friendly URLs match listener addresses, existing controls preserved.
3. Update feature/profile limitations and standalone usage documentation; run fmt, clippy all targets warnings denied, UI build and targeted browser tests.
   Expected: clean checks, no vendor dependency.
4. Commit changes; run task-done with cargo test --locked --test rtsp_publication -- --test-threads=1.
   Expected: all publication qualification tests pass.
5. Run one fresh whole-branch review; fix Important/Critical findings once with RED/GREEN. Push the branch and require successful exact-head complete CI before merging/publishing main and replacing only the owned preview with rollback available. Archive private evidence before cleaning only this worktree/plan workspace.
   Expected: full Rust/browser/fmt/clippy/build checks pass on the final commit; preview configuration hash preserved and own PID/binary match release.

## Review Focus

- Policy races: config mutation while initial or renewing callback is pending, and byte accounting or RTCP/control floods hiding media silence.
- Generation ownership: HTTP/RTSP conflict, aborted startup, stale teardown and immediate reconnect cannot kill a replacement worker or leak resources.
- Protocol binding: percent-encoded names, authority/query/control substitutions, session IDs and colliding or unconfigured interleaved channels.
- Untrusted SDP and RTP: advertised remote network resources never used, codec/packet bounds, SSRC substitution and sender reports before valid media.
- Qualification and UI truth: independent decoding includes the common worker TS with actual codec multiplicity, TLS verification is real, and URLs reflect enabled listeners rather than assumed ports.
