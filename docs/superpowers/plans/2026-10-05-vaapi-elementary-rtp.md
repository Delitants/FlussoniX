# Internal VAAPI and elementary RTP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Internal Intel VAAPI transcoding and direct elementary RTP/SDP input/output integrated with shared workers and friendly forms.
**Architecture:** Extend validated FFmpeg worker profiles/readiness for VAAPI; native direct transport owns endpoint admission/framing/reorder/RTCP, independent FFmpeg SDP input handles depacketization, and existing RTSP packetizers supply shared elementary output.
**Tech Stack:** Rust/Tokio/libc, independent FFmpeg/libVA, React/Playwright.
**Spec:** docs/superpowers/specs/2026-10-05-vaapi-elementary-rtp.md

## Global constraints

- Owned Linux ports/config/media/driver environment only, no production/vendor changes; independent components only.
- VAAPI h264_vaapi/hevc_vaapi, software decode/NV12 upload, no CPU fallback; renderD1..3digits, default128; low_power false; vaapi_rc cqp(default)/cbr and qp24(default),0..51,CQP only; CQP forbids explicit vb, CBR forbids qp; readiness5seconds, bounded16-entry coalesced cache.
- Elementary plaintext RTP/AVP only; SRTP elementary fails closed, encrypted MP2T remains. Maximum8 tracks,4 outputs; baseport through base+15 owned; SDP16KiB, no crypto/indirection.
- Source/SSRC pin after valid payload, optional IPfilter;64 reorder; bounded pacing/write/cancellation/generation fences; direct wire bytes included in uplink telemetry.
- Existing GPU caption-conversion exclusion remains; elementary separate subtitles need MP2T. Hardware/short fixtures do not qualify WAN/scale.

## Review focus

- Wrong/missing/changed render device or pending GPU check: no CPU fallback, stale routes cannot replace a newer worker, no global lock held during hardware work.
- Hostile SDP references/attributes or RTP payloads: no arbitrary file/network access, unbounded allocations, malformed source ownership or codec spoofing.
- Multiple tracks/groups sharing ports or late startup/cancellation: exact track routing, group isolation and complete owned task/socket cleanup.
- Changed codec generation/destination policy during pacing: no packets or SDP from a superseded generation, queue lag fails visibly.
- Template protocol/encoder changes: dependent fields cleared/inherited properly; secure profile never silently becomes plaintext; MP2T/SRT/RTSP unchanged.

### Task 1: VAAPI shared worker and controls

Files: src/{transcoder,gpu,media,server}.rs; web/src/forms.tsx; tests/{vaapi_profiles,vaapi_readiness,vaapi_media}.rs; web/tests/admin.spec.ts; docs/vaapi.md.
Interfaces: Profile::vaapi()->Option<(&str,bool)> and Profile::prepare(&mut Command) for global device setup; apply output options; Checks::require_profile(ffmpeg,&Profile) validates hardware readiness; Engine::vaapi_capabilities()->Value default-device report. Existing GPU/public_error vocabulary stays sanitized.
- [ ] Add failing config/inheritance/option-clearing and readiness error/timeout/stale-generation tests; preserve RED. Implement device/mode validation, bounded cache/profile-identical checks and command preparation; run GREEN including old NVENC/CPU regressions.
- [ ] Add opt-in independent actual internal VAAPI delivery/consumer-sharing/replacement tests and CPU comparison; run H264 on owned driver environment, retain clear unsupported HEVC result; exact codecs, decoded frames and cleanup required.
- [ ] Add failing friendly form/default/inherited/custom device/mode tests; implement fields/summaries/capabilities, compile and run browser GREEN. Document implemented profile/limits and commit Task1.

### Task 2: elementary RTP/SDP receive/transmit

Files: src/direct_rtp/elementary/{mod,sdp,packet,input,output}.rs; src/direct_rtp/{config,mod,sockets}.rs; src/{media,server}.rs; web/src/{rtpFields,forms,main}.tsx; tests/{elementary_sdp,elementary_io,elementary_media}.rs; docs/elementary-rtp.md; browser cases.
Interfaces: Settings gains profile/sdp_file; Input dispatches MP2T or elementary with common stats/run(stdin,cancel). SDP::parse(bytes,Settings)->Session with up to8 validated Track objects, Session::decoder_sdp(loopback_ports)->String. Elementary State::run(Arc<Worker>,cancel) consumes existing wire.rtp.play_snapshot, State::stats()->Value includes actual SDP for current generation. Reuse Pair and public packetizer, no per-destination encoding.
- [ ] Add failing strict SDP reference/grammar/network/codec/ports and payload/source-pinning cases; preserve RED. Implement canonical SDP, bounded payload validation, owned public pairs/private relay and common media input dispatch; run GREEN with cancellation, malformed/foreign packets and port cleanup.
- [ ] Add failing native elementary sender/RTCP/generation/lag/egress tests; implement shared packetizer destinations/actual SDP API with bounded pacing and fences; run GREEN including multiple tracks/groups/destinations and existing MP2T/SRTP tests.
- [ ] Add independent FFmpeg SDP media tests for input/output, codec/audio-only matrices and CPU; test available VAAPI-generated worker elementary output; strict decode/frame/codec assertions. Add friendly profile/SDP-reference/runtime-download/inheritance browser cases RED->GREEN. Document limits and commit Task2.
- [ ] Format, Clippy, compiled UI, meaningful local qualification; one fresh whole-branch review with exact spec/plan/focus and rulings, one RED->GREEN fix pass for accepted Important/Critical findings, no second review. Exact-head full CI before publication.
- [ ] Publish qualified main and update owned preview preserving credentials/configuration; archive hashed private evidence/ledger and remove only this owned merged worktree. Final outcomes recorded in stage ledger without a doc-only checkbox commit.
