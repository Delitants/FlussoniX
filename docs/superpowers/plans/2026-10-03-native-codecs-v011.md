# Native codec foundation implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Establish independent HEVC/MPEG audio native wire and relay primitives.
**Architecture:** Explicit codec identity and bounded inspectors feed native parsers/packers and hub boundaries. The AVC/AAC worker adapter keeps a clear compatibility guard until its replacement.
**Tech Stack:** Existing Rust/Tokio/bytes; independently installed FFmpeg for owned fixtures.
**Spec:** docs/superpowers/specs/2026-10-03-native-codecs-v011.md

## Global Constraints

No vendor build/runtime/test dependency; research-only disposable reference processes. 16 distinct native tracks, at most one video. Existing 16 MiB M4S record, 32 MiB parser/bootstrap/segment limits and 8 segment/64 MiB cache remain. HEVC config at most1 MiB, access units16 MiB; no dependency changes. Current end-to-end codec profile stays H.264/AAC.

## Review Focus

- MPEG2/2.5 Layer III duration must not use1152 or AAC samples.
- Metadata-only HEVC records must not count as an IRAP picture.
- New audio codecs must not bypass video bootstrap when HEVC is present.
- A third native track must not silently disappear into the single-audio FLV worker.
- Duplicate/kind-mismatched metadata and malformed codec lengths must fail without payload amplification.

### Task 1: Explicit codecs and bounded media inspectors

Files: create src/codec.rs, src/hevc.rs, src/mpeg_audio.rs and tests/codecs.rs; modify src/lib.rs. Owned fixtures under tests/fixtures/codecs with provenance.
Interfaces: Codec::parse(&str)->Result<Codec,String>, Codec::is_video()->bool, Codec::tag()->[u8;4]. hevc::Configuration::parse(&[u8])->Result<Configuration,String>, Configuration::annex_b(&self)->Vec<u8>, Configuration::access_unit(&self,&[u8])->Result<AccessUnit,String>. mpeg_audio::inspect(Codec,&[u8])->Result<Header,String>; Header exposes sample_rate, channels, samples, frame_bytes and duration_90k().
- [ ] Write failing tests for identities, owned HEVC config/sample Annex B reconstruction and IRAP, malformed/trailing/oversized NALs, MPEG1/2/2.5 layer/rate/length/sample counts and reserved/free-format/truncated frames. Expected missing modules then green.
- [ ] Implement the above bounded interfaces; run cargo test --locked --test codecs and fmt/Clippy. Expected all cases pass without vendor components. Commit.

### Task 2: Native metadata, packed media and shared boundaries

Files: src/m4s.rs, src/m4f.rs, src/wire.rs, src/m4_ingest.rs; tests/m4s.rs, tests/wire_relay.rs, tests/codecs.rs; docs/native-codecs.md, docs/secure-output-codecs.md.
Interfaces: consume Task1 Codec/inspectors; retain Track/Frame public layout. Track::kind()->Result<Codec,String> and validate_tracks(&[Track])->Result<(),String> enforce native metadata bounds. Existing pack/unpack/Decoder signatures remain. FLV helpers return errors on unsupported codecs; ingest validates bridge suitability before hub relay.
- [ ] Write RED native HEVC/m2a/mp3 metadata/packed-GOP roundtrips, exact tags/config/payload/signed times, multi-audio limits/kind mismatch, observed MPEG tail durations, HEVC bootstrap/reset and audio-only two-second segments. Write RED legacy bridge rejection before native publication. Expected old codec rejection/wrong tags/missing audio segments.
- [ ] Implement native dispatch, handler checks and shared segment closure; retain bounds and original relay bytes. Run targeted codecs/m4s/wire_relay tests. Expected all green. Commit.
- [ ] Independently decode owned reconstructed elementary fixtures and verify native oracle metadata/frames/segments in disposable research processes. Record exact evidence and truthful worker/RTP limits. Run full Rust/fmt/Clippy/UI build/browser suite. Expected green.
- [ ] Perform one fresh immutable whole-branch review; fix Important/Critical findings RED→GREEN and run required checks. Publish source on main only after exact CI success; archive owned records/worktree. Leave v0.10 test services running. Expected clean synchronized main and no new runtime codec claim.
