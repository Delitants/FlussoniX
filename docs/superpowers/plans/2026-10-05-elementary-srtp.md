# Elementary SRTP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax.

**Goal:** Add authenticated encrypted elementary input and output with friendly static SDP/key references.
**Architecture:** Each lane owns separate inbound/outbound libsrtp2 context loaded from one bounded generation key snapshot. Input decrypts before validation and private decoder relays; output rewrites only transport identity/clock and encrypts shared packetizer payloads.
**Tech Stack:** Rust/Tokio/libsrtp2, independent FFmpeg, React/Playwright.
**Spec:** docs/superpowers/specs/2026-10-05-elementary-srtp.md

## Global Constraints

- Independent system FFmpeg and libsrtp2 only; no official Flussonic runtime components, installation changes, or production/CDN service modifications.
- Work in an owned isolated worktree. Preserve existing root preview/configuration/credentials until exact-head qualification, then update only owned preview49285 under existing publication authorization.
- AES_CM_128_HMAC_SHA1_80; owner-only absolute regular key files containing30 base64-decoded bytes. Load once per transport generation into bounded per-track contexts; erase temporary buffers. At most8 tracks/4 destinations.
- Secure static SDP requires RTP/SAVP; plaintext requires RTP/AVP. Reject profile mismatch, inline crypto keys, negotiation/remote resource attributes and unsupported cryptography. Downloaded SDP contains no key or key path. This is out-of-band keying, not SDES negotiation.
- Authenticate/decrypt before structural admission, peer/SSRC pinning or decoder relay. Reject plaintext, wrong keys, tamper/replay. Source filters still apply before crypto. Authenticated malformed candidates cannot seize peer ownership or lose rollover/replay history.
- Public SR/RR uses SRTCP; the decoder receives only regenerated plaintext SDP and authenticated plaintext media/control on trusted owned loopback pairs. Protect validated private feedback before public transmission. Load key and initialize all crypto before opening public sockets.
- Each secure destination/track generation assigns a fresh unique random SSRC and its own sequence starting at0; preserve payloads and marker/timestamp spacing. Do not reuse shared packetizer SSRC/sequence as a restarted encryption epoch. Existing sessions cross sequence rollover; uncoordinated receiver late joins after rollover remain unsupported.
- Preserve common destination media/wall clock, related CNAME, actual generation-fenced SDP, bounded pacing/cancellation/loss/queue semantics and existing MP2T/plaintext behavior. Account ciphertext/trailer/IP/UDP egress bytes.
- UI offers elementary on secure URLs, labeled SDP/key fields and preserved template overrides. Changing secure to plaintext removes key reference while retaining elementary profile; switching to MP2T removes input SDP. No raw keys or editable JSON fields.


## Review Focus

1. A key file replaced during initialization must not give related lanes inconsistent generation keys; atomic snapshot helper and unsafe-key-before-socket test owned byTask1.
2. Authenticated malformed RTP before pin and near rollover must retain replay/ROC state but cannot seize the socket; malformed-candidate/rollover test owned byTask1.
3. Media and SRTCP feedback on related ports must have separate directions and bounded SSRC state; encrypted SR/private RR and wrong-source/invalid-feedback tests owned byTask1/Task2.
4. Restarted/late-added destinations using the same key cannot reuse shared worker nonce identities; multi-destination/restart/rollover wire tests owned byTask2.
5. Downloaded SDP or management errors must never reveal keys or imply plaintext downgrade; secure descriptor auth/fence and UI/no-secret tests owned byTask2.

### Task 1: Authenticated elementary input

**Files:** src/direct_rtp/{config,crypto,stats}.rs; src/direct_rtp/elementary/{input,sdp}.rs; tests/elementary_{sdp,io,media}.rs; new tests/elementary_srtp_input.rs; docs/elementary-srtp.md.
**Interfaces:** consumes Settings.secure/key_file, existing crypto::Session, Session::decoder_sdp, per-lane relay. Produces crypto::track_sessions(path:&Path, senders:&[Option<u32>])->Result<Vec<(Session,Session)>, &'static str> (bounded8; one key snapshot; None transmitter pins first trusted private feedback SSRC). Existing crypto::sessions/new unchanged externally. Native Input::bind/run signatures unchanged. SDP parser accepts exactly settings.secure?RTP/SAVP:RTP/AVP.
- [ ] Write config acceptance/mismatch tests; secure two-track admission/feedback fixture asserts no plaintext, wrong key, replay/tamper/foreign rejection, valid max-size packet, malformed candidate rollover and cancellation/release. Run `cargo test --locked --test elementary_srtp_input --test elementary_sdp -- --test-threads=1`; Expected: secure configuration rejected before implementation.
- [ ] Implement bounded single-key snapshot direction-specific crypto helper; load all per-track contexts before public binds. Permit secure elementary config with existing key requirements. Parser enforces secure transport matching and keeps private decoder SDP AVP/key-free. Relay decrypts RTP/SR before structural parse/pin, discards invalid candidate preserving history, protects valid decoder feedback. Cipher input bound permits1600-byte plaintext+10-byte tag, truncation cannot pin.
- [ ] Add independent secure-source media test using real FFmpeg senders and existing actual Engine worker playback; owner-only SDP sanitizes inline keys to key-free SAVP and config key reference. Strict native/TS decode validates H264/HEVC+AAC/MP2/MP3, audio-only/multiple audio, CPU conversion. Run `cargo test --locked --test elementary_srtp_input --test elementary_sdp --test elementary_srtp_media --test direct_srtp --test direct_srtp_io -- --test-threads=1`; Expected: all new/source and old secure suites pass.
- [ ] Document input and operational key/epoch limits; commit Task1. Run task-done contract `cargo test --locked --test elementary_srtp_input --test elementary_sdp --test elementary_srtp_media -- --test-threads=1`.

### Task 2: Encrypted destinations and operator controls

**Files:** src/direct_rtp/elementary/output.rs; src/server.rs; web/src/{forms,rtpFields,elementarySDP}.tsx; web/tests/admin.spec.ts; tests/elementary_{output,api}.rs; new tests/elementary_srtp_{output,media}.rs; docs/elementary-srtp.md; README.md.
**Interfaces:** consumes Task1 crypto::track_sessions helper and strict SAVP input. Produces existing actual SDP API with key-free SAVP descriptors, fresh per-lane secure transport identities, authenticated feedback/ciphertext accounting; no management key export endpoint.
- [ ] Write native secure output wire test: libsrtp deciphered payload, encrypted SR/common clocks/CNAME, independent destination SSRCs and seq0, plaintext/malformed/tampered RR rejection before pin, rollover/restart/media generation fences/socket release/lag/late receiver. Run `cargo test --locked --test elementary_srtp_output -- --test-threads=1`; Expected: current output plaintext/auth fails.
- [ ] Implement fresh SSRC/sequence rewriting only for secure lanes; initialize per-lane crypto before sockets/SDP; protect media and SR, authenticate valid feedback before pin; preserve fences and count encrypted wire bytes. Generate key-free SAVP, keep plaintext AVP unchanged. Run secure output and existing elementary output/API/direct secure regressions; Expected: pass.
- [ ] Extend independent media matrix to secure source→secure destination, private receiver key injection only in600 file, strict codec/decode/frame checks; CPU conversion and H264 VAAPI opt-in use same shared worker. Run `cargo test --locked --test elementary_srtp_media -- --test-threads=1`; Expected: secure matrix pass; opt-in actual iGPU separately passes under scoped independent driver environment.
- [ ] Write browser test for secure elementary input/output labeled key+SDP, URL/profile transitions/inheritance/no key leakage and SAVP download; Expected: current secure selector missing/profile reset. Implement friendly controls and key-free receiver setup explanation; update legacy plaintext-switch assertion to preserve elementary rather than downgrade, but explicit MP2T selection remains covered. Build UI, run targeted owned browser tests; Expected: pass with no editable JSON.
- [ ] Document receiver-local out-of-band setup, rollover/key replacement limits and qualification; update stale README current capability. Run fmt/Clippyalltargets and task contracts; commit Task2; task-done `cargo test --locked --test elementary_srtp_output --test elementary_srtp_media --test elementary_output --test elementary_api -- --test-threads=1`.

## Release

Generate immutable whole-branch package; one required fresh reviewer, all Important fixes one observed RED/GREEN pass, ledger every declined judgment/minor. Push candidate and require whole exact-head CI. Publish qualified main/update only owned preview with identical config/credential hashes and rollback; archive hashed private evidence/exhaustive rulings; clean only owned merged worktree.
