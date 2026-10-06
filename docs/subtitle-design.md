# Subtitle conversion and preservation contract

Status: original separate DVB/teletext track preservation and selectable plain-text CEA-608/708 WebVTT are implemented. HLS offers original TS pass-through, conversion and filtering. Selected Level 1 Latin teletext conversion is also implemented. Optional DVB bitmap OCR conversion is implemented; enhanced/non-Latin teletext and advanced DVB objects remain pending. See [CEA-708 qualification](cea708-qualification.md) and [teletext qualification](teletext-qualification.md) and [DVB qualification](dvb-qualification.md) for the measured subsets.

## User requirement

Streams and Templates need friendly controls to convert subtitles to an HLS-supported format, or keep their original representation for compatible non-HLS outputs. Cover both North American and European broadcast formats, inbound and outbound, with the same viewer authorization and secure transport as video/audio. A stream may simultaneously serve HLS and TS/SRT consumers; HLS conversion must not strip originals from the other outputs.

## Processing families

| Source | HLS conversion | Preservation |
| --- | --- | --- |
| CEA/CTA-608 | Decode selected CC1..CC4 services to timed WebVTT | Retain embedded caption data in compatible video/container outputs |
| CEA/CTA-708 | Decode selected SERVICE1..SERVICE63, including window/text state, to WebVTT | Retain embedded service packets; do not call 608 fallback full 708 support |
| DVB teletext | Decode selected subtitle pages, character sets and page changes to WebVTT | Retain teletext PID, PMT descriptors and language/page identifiers in TS-based outputs |
| DVB bitmap subtitles | Decode composition/page/region/palette state, then OCR to timed WebVTT | Retain subtitle PES, composition/ancillary page identifiers, descriptors and original bitmap payload |
| Existing text subtitles | Convert supported text tracks to WebVTT with timing/language retained | Copy the original codec only where the output container permits it |

CEA data can also be carried in-band by HLS and announced as closed captions; WebVTT conversion is a selectable additional rendition, not a reason to discard the original. HLS subtitle renditions use WebVTT initially. IMSC1 support is a separate qualification, not a promise to wrap DVB bitmap data in a text container. [Apple describes HLS caption and subtitle delivery](https://developer.apple.com/library/archive/referencelibrary/GettingStarted/AboutHTTPLiveStreaming/about/about.html).

DVB bitmap-to-text conversion requires OCR; plain codec remuxing is insufficient. The default design is selectable WebVTT using an independent OCR engine and installed language data. Burn-in is a separate optional transcode operation, not equivalent to selectable subtitles. The user was asked which bitmap conversion they prefer; preserve that answer when implementation begins. [Flussonic distinguishes bitmap recognition](https://flussonic.com/blog/changelog/version-26-08) and [teletext extraction with continued TS passthrough](https://flussonic.com/doc/mcaster/subtitles/).

## UI and configuration

Streams and Templates expose two independent controls:

- **HLS subtitles:** Convert to selectable WebVTT / Pass through original subtitles / Off.
- **Other outputs:** Keep original subtitles / Off.

Show detected subtitle services/pages as rows with format, language, display name, enabled state and relevant service/page selector. For bitmap conversion expose OCR language selection and capability availability. Do not require editing JSON. Keep WebVTT extraction additive to original passthrough; global caption stripping cannot implement this requirement.

API fields and migration aliases must be checked against the target Flussonic versions before claiming exact compatibility. Legacy `cc.extract` adds text alongside embedded captions according to [the vendor's migration notes](https://flussonic.com/doc/sapsan/closed-captions/); do not reinterpret it as global removal. These current Sapsan/Mcaster documents provide behavior references, not a substitute for a qualified legacy Media Server API contract.

Read-only inspection of this host's `schema-v3-public.json` and `schema-v3-private.json` under `/opt/flussonic/lib/web-1/priv/` found the following migration candidates. Only field names, types and behavior were observed; no schema file is copied into the project or used by its build/tests:

- Stream `dvbocr`: `add` retains DVB and adds WebVTT; `replace` replaces the original with text. The mixed HLS/TS use case needs additive processing or genuinely per-output replacement.
- Deprecated input `subtitles`: `accept`, `drop`, `ocr_add`, `ocr_replace`; the schema points to stream `dvbocr` as its replacement. Deprecation annotations do not establish removal in a particular running version.
- MPEG-TS `teletext_page`: integer 100..899, including magazine selection; page 888 is a common default rather than a universal assumption.
- Input `closed_captions`: string-to-string rules on MPEG-TS, SRT, M4F, M4S and copy inputs; rule value semantics still need observation.
- Video track caption metadata: language/name, with private fields for standard 608/708 and channel/service identity. Preserve these distinctions when constructing HLS manifests.

These schema observations inform follow-on migration work. The independent generic native text subset is now qualified separately in [native subtitle delivery](native-subtitles.md); it does not implement these legacy field semantics.

## Runtime and delivery

One source session feeds video, audio and subtitle processing. Parse announced languages/service descriptors and dynamically discovered services, with stable track identifiers across recovery. Keep subtitle cue state bounded per service, explicit size/service limits, timeout behavior and per-track backpressure. Recognition must not stall AV delivery. Report errors and OCR availability/quality in stream statistics; never quietly report conversion success while dropping a track.

Segment WebVTT on the AV HLS generation grid, with correct MPEGTS timestamp mapping, overlapping cues, empty segments during silence, discontinuities, timestamp wrap and recovery. Include renditions in TS-HLS and fMP4-HLS master playlists. Serve every rendition playlist and segment through the existing token/session checks and HTTP/HTTPS endpoints. CDN metadata/cache and source pull must carry the language, service, timing and output policy without a second source subscription.

Preservation applies only when the target protocol/container carries that representation. TSHTTP/TSHTTPS, SRT carrying TS and RTP carrying TS need subtitle PIDs/descriptors; elementary audio/video RTP does not carry arbitrary DVB PES. Generic native M4F/M4S text copy and filtering are implemented as described in [native subtitle delivery](native-subtitles.md). Mapping regional broadcast PES into native subtitle tracks remains unqualified. Copy-mode caption SEI must survive H.264/HEVC framing; CPU/GPU transcoding needs explicit caption retention or extraction, verified per encoder. Unsupported combinations must return an actionable capability error.

## Implementation and qualification sequence

1. Own regional fixtures and inspect the legacy API/native metadata contract read-only. Add typed subtitle policy, capability validation, statistics and Stream/Template controls when real paths are available.
2. Preserve CEA SEI and DVB subtitle/teletext PIDs/descriptors in compatible outputs; qualify TS/SRT and source-to-CDN round trips, copy and transcode separately.
3. Implement CEA-608/708 and teletext decoding to WebVTT, HLS segmenting/manifests/auth, multiple languages, Unicode, roll-up/pop-on/window state and silence/recovery tests.
4. Implement bounded DVB bitmap decoding and independent OCR integration, language dependencies and confidence reporting. Test bitmap updates/clear cues, language glyphs and slow/failing OCR without AV interruption.

Acceptance needs actual playback/cue contents and preserved original bytes/identifiers, not only a configuration save or advertised manifest. No official Flussonic binary/library is a product dependency. Record fixture provenance, independent decoder/player verification and remaining limits before release.

## First runtime stage: original separate tracks

Streams and Templates now accept `flussonix_subtitle_tracks`: `preserve` copies separate subtitle tracks into MPEG-TS fan-out; `drop` omits them. Omission keeps the pre-existing drop behavior. The friendly **Original subtitle tracks** select supports template inheritance and explicit override. This extension is not a claim of exact legacy subtitle API compatibility. Worker stats report the effective policy and native discovery carries it.

The converted HLS variants and native worker AV channel receive only video/audio, so DVB/teletext tracks do not make incompatible containers fail. Copy-mode embedded captions stay in video; the separate-track drop setting does not strip caption SEI. Owned DVB clear-page and teletext page 888 fixtures retain encoded payloads, languages and page identifiers through copy and CPU video transcoding. Remuxing may change PID numbers. Native H.264/HEVC framing tests retain both 608 and 708 packet bytes; this does not yet prove decoded caption timing, encoder retention or complete regional functionality.

At that initial stage, selectable conversion, service detection, OCR, caption stripping and regional cluster round trips remained pending; the subsequent stage below implements selected 608 conversion and targeted HLS filtering. Native text copy/filtering and the MPEG-TS regional cluster path are now documented in the later qualification stages; the DVB stage below adds optional OCR. In particular, the current AV-only HLS source pull does not carry separate DVB/teletext tracks from source to CDN. Dedicated [SRT copy-mode output tests](srt-subtitles.md) now qualify encrypted H.264/HEVC with AAC copy-mode CEA-608/708 and DVB/teletext carriage with independent HLS policy. The [source/CDN-to-SRT profile](cluster-srt-subtitles.md) additionally qualifies native MPEG-TS pulls into encrypted CDN listener playback for H.264/HEVC/AAC copy mode. Broader SRT encoder/input/cluster combinations, RTP subtitle delivery and GPU caption retention remain unqualified. The subsequent Convert option uses the real decoder and authorized rendition path described below.

Sparse subtitle qualification: continuously paced publications with declared but absent subtitle packets, and with only initial cues followed by silence, keep AV progressing and publish both HLS variants in copy and CPU modes. Preservation caps common tee and nested live-TS interleaving at 100 ms; this is a mux buffering budget, not an end-to-end latency promise. Four new regressions first reproduced startup timeouts before the fix and then passed.

## HLS mode and selectable 608 stage

The friendly **HLS subtitles** control on Streams and Templates supports inheritance and three explicit modes:

| Mode | Delivered HLS | Other outputs |
| --- | --- | --- |
| Pass through original subtitles | Explicit pass-through retains DVB/teletext descriptors and PES in TS-HLS with copy/CPU encoding, and embedded 608/708 with copy video. Original-format player support is required. fMP4 cannot carry separate DVB/teletext tracks; they are not advertised as selectable text renditions. | Existing policy remains independent. |
| Convert to selectable WebVTT | Adds configured CC1..CC4, digital services1..63 selected teletext pages100..899 and optional DVB bitmap recognition plain-text renditions to TS and fMP4 HLS. Does not remove original video captions. | Original subtitle tracks may still be preserved. |
| Filter out HLS subtitles | Omits conversion and clears GA94 caption process/count/valid flags in delivered H.264/HEVC media. Byte lengths, unrelated SEI and encoded video/audio remain intact. | Does not strip captions from shared TS/native media. |

`flussonix_hls_subtitles` accepts `passthrough`, `convert` or `drop`. Missing mode infers conversion from nonempty `flussonix_hls_captions`, otherwise pass-through, preserving earlier behavior. The selected mode can override inherited service rows without overwriting them; restoring inheritance removes local mode and rows. `flussonix_hls_captions` holds up to four distinct rows with language/name and exactly one of `channel`1..4, `service`1..63, `teletext_page`100..899 or `dvb_page`0..65535 with required `ocr_language`. Mode/service fields are native extensions, not verified legacy aliases. Conversion needs at least one selected service; empty rows previously used to disable conversion remain supported.

An independent bounded Rust decoder reads private copied source video before CPU encoding through the same FFmpeg input session. It handles 608 display state, parity, duplicate controls, all four channels, basic/special/extended characters and timed erase. H.264 and HEVC registered SEI framing, B-frame presentation reordering and 33-bit timestamp wrap are tested independently. Real live copy/CPU H.264 delivery is qualified separately. Styles are reduced to plain text; 708 fallback bytes are not full 708 decoding.

WebVTT uses actual AV clock anchors and sequence grids, empty silent segments and immutable one-second cue slices with generation-scoped IDs. Conversion holds back one AV segment and requires timestamp headroom before freezing cue files. All masters, AV/subtitle playlists and VTT files reuse current viewer auth, token rewriting and revocation. Decoder lag/size/clock errors and private socket failure report failure and fall back to progressing AV. Filtering fails closed for unsupported/malformed containers instead of returning captions. HLS filtering currently parses each delivered segment; this stage has no sustained scale or cache performance claim.

GPU conversion, real HEVC caption player delivery, enhanced/non-Latin teletext decoding, advanced DVB objects, automatic source-service discovery and separate native subtitle relay remain open. MPEG-2 caption conversion/filtering is not available. Original-track controls do not imply global embedded-caption stripping across every protocol.


Explicit `passthrough` uses the independent MPEG-TS segment path for TS-HLS; the omitted default keeps its earlier embedded-caption behavior. The same FFmpeg input/encode supplies raw subtitle PES. All three interleaving queues are bounded for absent/silent services. A generation-owned task publishes an atomic six-segment live playlist, retains two prior finalized segments plus the current open segment, and maps the segment mux's local counter to the public 64-bit sequence epoch. Replacement invalidates old filenames and carries discontinuity history. fMP4 remains AV-only for separate tracks; filtering omits separate tracks and disables supported embedded caption flags. Raw TS carriage does not imply browser decoding of DVB bitmaps or teletext. [FFmpeg's segment mux](https://ffmpeg.org/ffmpeg-formats.html#segment_002c-stream_005fsegment_002c-ssegment) supplies finalized MPEG-TS files without its HLS mux's WebVTT subtitle routing.

Post-review regressions cover captions after one and two complete 33-bit timestamp periods, stale-display clearing on truncated registered SEI and oversized GA94 counts, regional TS-HLS copy/CPU payload preservation, output-policy independence, absent packets, silence after initial cues, bounded retention and generation replacement. These are owned fixtures, not a claim of a 26-hour production soak or complete regional conversion.

## Native CEA-708 service stage

Configured digital services1..63 now use the same independent extraction, source clock and HLS rendition path, with up to four combined608/708 selections. Friendly forms choose a format and channel/service; existing608 fields and URLs remain compatible. Independent packet/window decoding handles delayed commands and bounded source-gap recovery. Owned copy/CPU H.264 media supplies two separate digital languages without608 fallback and agrees with pinned independent Shaka text/time output. H.264/HEVC transport framing and wrap are qualified independently; exact styles/position/effects, alternate P16 encodings, real HEVC browser output and GPU conversion remain unqualified. See [the qualification and limits](cea708-qualification.md). The following teletext stage adds selected Latin pages; The DVB stage below adds optional bitmap recognition.


## Native teletext page stage

Selected announced DVB teletext subtitle pages100..899 share the existing source clock, bounded cue history, authenticated TS/fMP4 WebVTT and copy/CPU extraction session. Friendly format/page controls preserve template inheritance, typed selector isolation and independent pass-through/filter policies. The native decoder handles boxed Level 1 Latin text, seven national subsets, serial/parallel magazines, partial updates, subpages, erase/inhibit, continuity and PMT ownership changes. Enhanced/alternate signaling clears affected conversion and reports degradation. Four combined CEA/teletext renditions are supported; Discovery remains separate work; the following stage adds bitmap recognition. See [qualification and limits](teletext-qualification.md) and [decisions](teletext-decisions.md).

## DVB bitmap recognition stage

The shared subtitle carrier binds configured DVB composition/ancillary pages and reconstructs coding0 interlaced bitmap display sets in bounded native caches. Optional independent Tesseract jobs fill retained source intervals before HLS publication; clear/update/timeout closes intervals even while recognition is pending. Canceled/reset/rebound tokens cannot revive cues. Identical accepted or pending images extend intervals; failed recognition may retry at a source refresh.

Rows require `dvb_page`0..65535 and `ocr_language` (one to four installed model names joined by `+`). Stable URLs use `dvbN`; existing CC/digital/teletext wire fields and URLs remain unchanged. Public Rust Service/Cue identities are nowu32. Friendly UI fields and recognition status are available in Streams/Templates. Original TS track preservation and HLS pass-through/filter remain independent. See [the measured profile](dvb-qualification.md). Full styling/color, enhanced objects, automatic discovery, capacity qualification, GPU conversion and regional native subtitle carriage remain open.

## Generic native text stage

Copy-mode native M4F/M4S now preserves or filters the observed generic text codec through the same original-track control. HLS conversion of its opaque payload and preservation during transcoding remain unsupported and explicitly reported. See [native subtitle delivery](native-subtitles.md) for the format, evidence and limits.

Generic native UTF-8 text now has an explicit HLS track selector and independent conversion path. See [native subtitles](native-subtitles.md) for controls, clock alignment, limits and qualification.
