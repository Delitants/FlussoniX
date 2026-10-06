# Direct RTP and SRTP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Native direct MP2T RTP and authenticated/encrypted SRTP receive/transmit, integrated with shared workers and friendly forms.
**Architecture:** Rust UDP transport owns framing, jitter/pacing, RTCP and lifecycle; system libsrtp2 protects packets. Input writes validated TS into the existing FFmpeg stdin path; output subscribes to the shared processed TS broadcast. CPU and external iGPU fixture encoders provide independent media tests.
**Tech Stack:** Rust/Tokio/libloading/libc, system libsrtp2, FFmpeg/FFprobe, React/Playwright.
**Spec:** docs/superpowers/specs/2026-10-05-direct-rtp-srtp.md

## Global constraints

- Owned ports/media only; root preview/config and vendor services protected.
- MP2T PT33,90kHz; <=7x188 TS bytes per outbound packet; <=4 outputs; RTP/RTCP ports1024..65534/consecutive.
- Jitter0..1000ms/default50,<=64 pending; rate1..10000/default100Mbps; multicast IPv4 requires explicit interface; TTL1..255/default16.
- SRTP AES_CM_128_HMAC_SHA1_80,30-byte key+salt,128-packet replay window; authenticated peer pinning and no downgrade.
- Key references only, opened non-symlink/regular/owner-only/absolute; no plaintext key logs/SDP.
- Inline implementer; fresh final review only. Authorization includes both tasks and publication; no inter-task pause.

## Review focus

- Malformed RTP headers/extensions/padding or TS packets: never feed ambiguous bytes into the media worker.
- Source spoofing and RTCP reflection: only configured/pinned authenticated media peer can own reception or receive feedback.
- Worker replacement/shutdown and blocked UDP/stdin: all sockets/tasks release promptly without global startup lock stalls.
- Key/profile error and replay: fail closed before bytes reach decoder; no secret disclosure or plaintext fallback.
- Template and configuration changes: transport identity/options affect worker generation, output enablement sustains demand, legacy SRT remains intact.

### Task 1: direct RTP input/output and forms

Files: create src/direct_rtp/{mod.rs,config.rs,packet.rs,input.rs,output.rs}; modify src/{lib.rs,media.rs,config.rs,server.rs}; create tests/direct_rtp.rs and tests/direct_rtp_media.rs; create web/src/rtpFields.tsx, modify web/src/forms.tsx; add browser cases to existing suite; docs/direct-rtp.md.
Interfaces: Settings::input(&Value)->Result<Option<Settings>,String>; outputs(&Value)->Result<Vec<Output>,String>; enabled(&Value)->bool; Input::bind(&Settings)->Result<Input,String>, Input::run(ChildStdin,CancellationToken)->Result<(),String>; output State::run(Receiver<Bytes>,CancellationToken), State::stats()->Value. Packet parser and bounded Reorder expose validated ordered TS payloads. SRTP adapter slot is optional and cannot downgrade.
- [x] Write public config/framing/reorder failing tests; run targeted cargo test and preserve RED.
- [x] Implement strict config and packets/reorder; run GREEN including malformed/overflow/source bounds.
- [x] Write failing independent input-to-HLS/output-to-FFmpeg tests and lifecycle/worker identity checks; integrate UDP input and output; run GREEN with all requested codec combinations and short CPU-transcoded cases.
- [x] Write browser failing friendly field/inheritance/persistence tests; implement controls and true capabilities/stats; run browser GREEN.
- [x] Document implemented MP2T profile and limits, qualify external Intel VAAPI fixtures, commit task1 and ledger evidence.


Task1 qualification: 6 native IO, 3 packet, 1 config, 7 telemetry and independent CPU/copy codec matrices passed; H264 VAAPI RTP fixture strict decoded; HEVC VAAPI unavailable on this host/driver. Two friendly form browser cases passed. Shared regional subtitle qualification is carried into Task2 as a ledgered dependency on its crypto adapter.

### Task 2: SRTP/SRTCP and final release

Files: create src/direct_rtp/crypto.rs; tests/direct_srtp.rs and tests/direct_srtp_media.rs; modify transport config/input/output and fields; CI install independent libsrtp2-1; docs/direct-srtp.md.
Interfaces: crypto::Session::new(key:[u8;30],sender:Option<u32>)->Result<Session,String>; protect/unprotect(&mut Vec<u8>,rtcp:bool)->Result<(),String>; availability()->bool. Session owns opaque library state, key is erased after create; no Clone/concurrent use.
- [x] Write failing key-file, packet confidentiality/auth/replay/SRTCP and ABI layout tests; preserve RED, implement narrow adapter, run GREEN.
- [x] Add independent FFmpeg encrypted inbound/outbound tests for codec/CPU/iGPU fixture profiles, wrong secret, tampering, replay, plaintext exclusion, key generation change, cancellation; run GREEN and existing RTP regressions.
- [x] Finish friendly secure fields/capabilities/CI runtime dependency/docs; validate no key leakage and vendor independence.
- [ ] Run fmt, warnings-denied Clippy, relevant suites and browser checks; commit both tasks; one fresh whole-branch review, apply justified fixes with RED/GREEN and ledger rulings.
- [ ] Push candidate and verify full exact-head CI; publish main, verify owned preview config and binary provenance, archive evidence, remove only this owned merged worktree.

Release gates and reviewer decisions are recorded in the owned qualification ledger. They are finalized after exact-head CI and publication, without retroactively changing the tested commit solely to update plan checkboxes.
