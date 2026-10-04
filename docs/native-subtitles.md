# Native M4F/M4S text subtitles

The native adapters now accept the observed generic `subtitle` codec: a `text` track handler and M4S `subt` frames with content type 4. This is an independent implementation. No official Flussonic component is required by the daemon, build or tests.

## Controls and delivery

Use **Original subtitle tracks → Keep in compatible outputs** on a Stream or Template to retain native text in copy-mode M4F/M4S input and output. **Drop separate tracks** removes the text tracks and their sample payloads. Template inheritance and source discovery use the existing `flussonix_subtitle_tracks` extension. The default remains drop. These controls do not establish exact compatibility with legacy subtitle configuration fields.

Native copy preserves track IDs, codec configuration, opaque track fields, encoded bodies, start/end timing and empty clear samples. M4F segment copy retains the original segment bytes when preservation is selected. M4S frame input keeps its wire records and carries opaque track metadata into generated M4F segments. Filtering compacts sample data and rewrites audio/video chunk offsets while retaining unrelated track fields and sample tables. Audio/video codec changes and explicit M4S declaration changes restart the worker rather than joining incompatible generations. Repeated identical M4S declarations are accepted across quiet segments. Segment inventories may omit silent text tracks; cue/quiet/cue transitions retain the existing AV muxer, continuity counters and clock.

Video/audio packaging continues through the existing native-to-TS bridge. Generic native text is never classified as an audio stream or mapped into TS audio or elementary RTP. A worker requires audio or video; a subtitle-only source is not a qualified worker profile.

## Explicit limits

Opaque native text is **not converted to WebVTT and is not delivered as a separate HLS subtitle track**. Detected sources report **Native text tracks**, **Native text output**, and **Native text in HLS** in stream status. The HLS indication is **Not supported**, or **Off** when HLS filtering is selected. Audio/video HLS remains available. The existing HLS selector applies to the separately qualified embedded CEA and broadcast TS formats; it does not turn unknown native payloads into text.

Preserving native text during CPU/GPU transcoding fails with `native_subtitle_transcode_unsupported`. When initial metadata declares text, rejection precedes packager startup. If a previously quiet M4F source first reveals text later, the worker stops with the same error; audio/video may have begun packaging before that discovery. Select drop to transcode audio/video without these tracks. CPU audio transcoding with drop is exercised; GPU native-subtitle retention is not implemented or qualified.

This generic codec must not be treated as a mapping for DVB bitmap or teletext PES. For regional broadcast subtitles, use the [MPEG-TS cluster path](cluster-subtitles.md) and the qualified [CEA, teletext and DVB processing](subtitle-design.md). Parsing the generic native container does not establish payload format semantics, subtitle player compatibility, automatic language interpretation, regional native mappings or full Flussonic interoperability.

## Qualification

Owned fixtures contain two independent text tracks, UTF-8 bodies, clear samples and MPEG audio Layer II. M4F, M4S frames and M4S packed GOPs exercise preservation and removal. Four additional M4F/packed-GOP regressions start with quiet text, then alternate cues and silence; they first reproduced AV worker termination and now require all five segments to arrive through one source session. Tests compare encoded bodies, timing, metadata and identifiers, check that removed text is absent from native sample data, and decode real audio/video HLS segments with independent FFmpeg. Existing AAC elementary RTP remains usable with native text declared.

HTTP delivery tests require viewer authorization before a source pull, verify token checks on cached native assets and revoke delivery when a stream is disabled. Source requests retain their own query credentials; native peer keys are origin scoped. Existing transport TLS handling is unchanged; this stage does not add a dedicated text-over-TLS qualification.

Read-only, disposable library checks against the locally installed reference version 26.04.1 accepted independently generated metadata, text/clear frames and M4F cue timing. Reference re-encodings were also decoded by FlussoniX. Reference inputs/outputs and inspection artifacts are private research records, not shipped fixtures or dependencies. CI uses only owned fixtures and verifies that `/opt/flussonic` is absent. These checks do not qualify production concurrency, migration servers or every vendor payload variant.
