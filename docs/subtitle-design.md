# Subtitle conversion and preservation contract

Status: original separate DVB/teletext track preservation is implemented for the shared MPEG-TS output; selectable HLS conversion and OCR remain pending. Embedded 608/708 payload survival is tested at native framing boundaries, not decoder or player semantics.

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

- **HLS subtitles:** Convert to selectable WebVTT / Keep supported embedded captions / Off.
- **Other outputs:** Keep original subtitles / Off.

Show detected subtitle services/pages as rows with format, language, display name, enabled state and relevant service/page selector. For bitmap conversion expose OCR language selection and capability availability. Do not require editing JSON. Keep WebVTT extraction additive to original passthrough; global caption stripping cannot implement this requirement.

API fields and migration aliases must be checked against the target Flussonic versions before claiming exact compatibility. Legacy `cc.extract` adds text alongside embedded captions according to [the vendor's migration notes](https://flussonic.com/doc/sapsan/closed-captions/); do not reinterpret it as global removal. These current Sapsan/Mcaster documents provide behavior references, not a substitute for a qualified legacy Media Server API contract.

Read-only inspection of this host's `schema-v3-public.json` and `schema-v3-private.json` under `/opt/flussonic/lib/web-1/priv/` found the following migration candidates. Only field names, types and behavior were observed; no schema file is copied into the project or used by its build/tests:

- Stream `dvbocr`: `add` retains DVB and adds WebVTT; `replace` replaces the original with text. The mixed HLS/TS use case needs additive processing or genuinely per-output replacement.
- Deprecated input `subtitles`: `accept`, `drop`, `ocr_add`, `ocr_replace`; the schema points to stream `dvbocr` as its replacement. Deprecation annotations do not establish removal in a particular running version.
- MPEG-TS `teletext_page`: integer 100..899, including magazine selection; page 888 is a common default rather than a universal assumption.
- Input `closed_captions`: string-to-string rules on MPEG-TS, SRT, M4F, M4S and copy inputs; rule value semantics still need observation.
- Video track caption metadata: language/name, with private fields for standard 608/708 and channel/service identity. Preserve these distinctions when constructing HLS manifests.

These observations inform the follow-on implementation; they do not imply the current AV-only native parser supports the subtitle tracks.

## Runtime and delivery

One source session feeds video, audio and subtitle processing. Parse announced languages/service descriptors and dynamically discovered services, with stable track identifiers across recovery. Keep subtitle cue state bounded per service, explicit size/service limits, timeout behavior and per-track backpressure. Recognition must not stall AV delivery. Report errors and OCR availability/quality in stream statistics; never quietly report conversion success while dropping a track.

Segment WebVTT on the AV HLS generation grid, with correct MPEGTS timestamp mapping, overlapping cues, empty segments during silence, discontinuities, timestamp wrap and recovery. Include renditions in TS-HLS and fMP4-HLS master playlists. Serve every rendition playlist and segment through the existing token/session checks and HTTP/HTTPS endpoints. CDN metadata/cache and source pull must carry the language, service, timing and output policy without a second source subscription.

Preservation applies only when the target protocol/container carries that representation. TSHTTP/TSHTTPS, SRT carrying TS and RTP carrying TS need subtitle PIDs/descriptors; elementary audio/video RTP does not carry arbitrary DVB PES. Native M4F/M4S subtitle track and metadata representation needs independent reference observation and codec qualification; existing AV-only parsers cannot be presented as subtitle passthrough. Copy-mode caption SEI must survive H.264/HEVC framing; CPU/GPU transcoding needs explicit caption retention or extraction, verified per encoder. Unsupported combinations must return an actionable capability error.

## Implementation and qualification sequence

1. Own regional fixtures and inspect the legacy API/native metadata contract read-only. Add typed subtitle policy, capability validation, statistics and Stream/Template controls when real paths are available.
2. Preserve CEA SEI and DVB subtitle/teletext PIDs/descriptors in compatible outputs; qualify TS/SRT and source-to-CDN round trips, copy and transcode separately.
3. Implement CEA-608/708 and teletext decoding to WebVTT, HLS segmenting/manifests/auth, multiple languages, Unicode, roll-up/pop-on/window state and silence/recovery tests.
4. Implement bounded DVB bitmap decoding and independent OCR integration, language dependencies and confidence reporting. Test bitmap updates/clear cues, language glyphs and slow/failing OCR without AV interruption.

Acceptance needs actual playback/cue contents and preserved original bytes/identifiers, not only a configuration save or advertised manifest. No official Flussonic binary/library is a product dependency. Record fixture provenance, independent decoder/player verification and remaining limits before release.

## First runtime stage: original separate tracks

Streams and Templates now accept `flussonix_subtitle_tracks`: `preserve` copies separate subtitle tracks into MPEG-TS fan-out; `drop` omits them. Omission keeps the pre-existing drop behavior. The friendly **Original subtitle tracks** select supports template inheritance and explicit override. This extension is not a claim of exact legacy subtitle API compatibility. Worker stats report the effective policy and native discovery carries it.

Both HLS variants and FLV receive only video/audio, so DVB/teletext tracks do not make incompatible containers fail. Copy-mode embedded captions stay in video; the separate-track drop setting does not strip caption SEI. Owned DVB clear-page and teletext page 888 fixtures retain encoded payloads, languages and page identifiers through copy and CPU video transcoding. Remuxing may change PID numbers. Native H.264/HEVC framing tests retain both 608 and 708 packet bytes; this does not yet prove decoded caption timing, encoder retention or complete regional functionality.

Selectable HLS conversion, service detection/announcements, OCR, native separate subtitle tracks, caption stripping and subtitle cluster round trips remain pending. In particular, the current AV-only HLS source pull does not carry separate DVB/teletext tracks from source to CDN. SRT/RTP subtitle delivery and GPU caption retention have not been qualified. No Convert option is exposed until its real decoder and authorized rendition path works.
