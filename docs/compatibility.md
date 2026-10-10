# Compatibility contract

## Meaning of compatibility

Compatibility is measured per **reference version × feature × transport direction × codec × authorization mode**. There is no current claim of exact equivalence.

A route inventory is only a starting point. Acceptance covers methods, paths, percent encoding, query parsing, request bodies, omitted/null/default values, HTTP statuses, headers, response shapes, timing-sensitive state transitions and visible runtime effects. Generated clients alone cannot prove this.

The local public management schema exposes 139 operations on 74 paths; its info version is `1.2.3`, while the installed package is 26.04.1. The downloaded public schema identifies itself as `26.03-499` and has the same method/path set but a different schema population. Pin file hashes and behavior fixtures instead of relying only on `info.version`.

## First-release management surface

All paths below are relative to `/streamer/api/v3`.

| Area | Reference operations |
|---|---|
| Streams | GET `/streams`; GET/PUT/DELETE `/streams/{name}`; POST `/streams/{name}/stop` |
| Templates | GET `/templates`; GET/PUT/DELETE `/templates/{name}` |
| Config | GET/PUT/POST `/config`; GET `/config/stats`; associated first-release operational fields |
| Cluster | GET `/cluster/peers`, GET/PUT/DELETE `/cluster/peers/{hostname}`; corresponding `sources` routes |
| Cluster balancers | Bundled private-schema `/cluster/balancers` operations: explicitly versioned extension, not assumed public |
| Authorization | GET `/auth_backends`; GET/PUT/DELETE `/auth_backends/{name}` |
| Sessions | GET `/sessions`; GET/DELETE `/sessions/{id}`; POST `/sessions/reauth` |
| IPTV compatibility | `/iptv`, `/iptv/packages`, `/iptv/subscribers` and their item operations |
| Operational visibility | Applicable monitoring, configuration statistics and event integration |

The demo demonstrates built-in IPTV authorization. Include this compatibility mode, while keeping its demonstration configuration separate from actual migration requirements. Public IPTV API routes are in the installed schema.

Some UI behaviors depend on private routes, such as explicit input selection. Record them separately from public operations and decide each feature's first-release implementation deliberately.

### Semantics that must be implemented explicitly

- Stream PUT performs a partial update; `null` removes/disables a feature and `$reset: true` replaces explicit configuration. Apply schema-specific rules for arrays and nested objects.
- GET returns effective configuration with defaults/templates applied. `config_on_disk` represents explicit saved configuration and must not be confused with runtime state.
- Preserve collection envelopes, cursors, default ordering, nested `select`, filters, search and sorting. Do not invent offset pagination.
- Multi-segment stream names require raw-path-aware routing. Test encoded separators, Unicode, spaces and encoded source URLs against the reference.
- POST `/config` validates JSON or text without saving or activating it; syntax diagnostics include the reference's location/path fields.
- Observe stop versus disabled behavior, on-demand startup, template-created streams, source failover and duplicate publication in the lab.
- Represent unsupported options as explicit compatibility failures during import/validation. Never silently accept a configuration whose requested behavior cannot run.

Reference: [management API](https://flussonic.com/doc/api/reference/). Detailed route metadata comes from the bundled public and private schemas recorded in the evidence inventory.

### Implemented collection field selection

Authenticated GET `/streams` and `/templates` support comma-separated `select`
fields, including dotted object paths such as
`select=name,title,transcoder.vb,stats.status,config_on_disk.template`.
Selection applies to each returned row after effective configuration, search,
ordering and pagination. It does not change `estimated_count`, `next`, `prev`
or `timing`, save configuration, or start a worker. Without `select`, existing
full responses are preserved. Item GET is unchanged. Cluster peer/source
collections additionally support the profile below; other collections are unchanged.

Selecting a whole object or array preserves that value. Dotted paths traverse
objects; array element projection and scalar traversal are unsupported and
omitted. Unknown fields are omitted; an existing object with no matching child
is returned as `{}`, except for the required names below. An explicitly empty
selector returns `{}` per template
and only the required `name` per stream. Stream rows always retain `name`;
selected `config_on_disk` objects also retain their `name`, matching the
reference schema's required identity fields. Wildcards and JSON
path syntax are not supported. Overlapping parent/child selectors resolve in
query order: a later parent selects the whole value; a later child narrows it.

The contract is grounded in the installed 26.04.1 schema, read-only inspection
of its collection selector, and the public
[API design principles](https://flussonic.com/doc/fms/api/flussonic-api-design/#limiting-the-field-set-of-the-result).
Unknown/non-object paths are handled without reproducing reference exceptions.
The tests and implementation use no vendor components. Reference default
ordering and full vendor cursor parity remain separate tasks.

### Cluster collection field selection

Authenticated GET `/cluster/peers` and `/cluster/sources` also accept the same
comma-separated `select` paths, for example
`select=hostname,private_payload_url` or `select=drain,flussonix_source_group`.
Selection runs after the existing search, ordering and positional pagination,
preserving counts, navigation and timing. Without `select`, full rows remain
unchanged. Item GET ignores `select` as before. Both view and edit management
credentials can read selected rows; projection does not replace authorization.

Empty or unknown-only selections return `{}` per row; cluster rows have no
automatically retained identity. Scalar child paths are omitted. Whole values,
object traversal and overlapping paths follow the selector rules above. Only
fields present in the current saved row can be selected; projection does not
synthesize peer telemetry, reference source fields or runtime state. Selecting
`hostname` does not include endpoints or `cluster_key`; explicitly selecting a
saved field retains its existing management visibility. No configuration is
saved and no media worker is started by collection reads.

This extends field selection over the existing native cluster representation.
Sources still use the native `hostname` identity and endpoint fields; reference
URL-keyed source configuration, static `only` activation, source `prefix` mapping,
cluster sorting/filtering/cursor parity and complete cluster schema compatibility
remain separate requirements. The native `except` blacklist is described below.

### Implemented scalar collection filtering

Authenticated collection GET `/streams` and `/templates` accept scalar filters
before search, ordering, counts, pagination and `select`. Implemented fields are
`name`, `title`, `comment`, `template`, `position`, `static`, `disabled`, and the
native transcoder profile's `transcoder.encoder`, `transcoder.vb`,
`transcoder.ab`. Streams additionally support those paths under `config_on_disk`
and `stats.status`, `stats.online_clients`, `stats.alive`. Filters read actual
returned values: an absent runtime `alive` is not synthesized as `false`.
Streams use effective inherited configuration; Templates use saved fields.

`field=value1,value2` performs typed list membership; different predicates are
combined with AND. `_lt`, `_lte`, `_gt`, `_gte` compare values of the field's
scalar type, and `_ne` excludes a value. Numeric comparisons preserve integer
precision. Booleans accept only `true`/`false`; invalid integer or boolean values
return HTTP 400 after authentication. `_like` on strings is a case-sensitive
literal substring match, including Unicode: `%`, `_`, and regex characters have
no special meaning. Non-string `_like` is rejected. `_is=null` and
`_is_not=null` test absence, JSON null, or the reference's text sentinels `null`
and `undefined`; missing values never satisfy equality, ranges or substring
matching. Missing values do satisfy `_ne`.

Unknown fields and unsupported paths are ignored. Object/array filtering, array
indices, other collections/items, complete reference field coverage, enum and
format validation, conflicting parent/child predicates, and exact vendor error
payloads remain unqualified. Repeated identical query keys retain the existing
last-value behavior. Filters do not mutate configuration or start media workers.
Pagination now uses the [value-based cursor profile](#implemented-value-based-collection-cursors); full vendor cursor parity remains pending.

This independent profile is grounded in read-only inspection of the installed
26.04.1 collection/schema modules, the official
[collection query design](https://flussonic.com/doc/fms/api/flussonic-api-design/#filtering-collections)
and [open-source handler](https://github.com/flussonic/openapi_handler).
No vendor schema, bytecode or source is needed to build, test or run these filters.

### Implemented composite scalar collection sorting

Authenticated GET `/streams` and `/templates` accept comma-separated scalar
sort fields, for example `sort=title,-position` or
`sort=transcoder.vb,-stats.online_clients`. Each field is ascending unless
prefixed with `-`; the first differing field decides the order. Dotted paths
traverse objects in the returned effective stream or saved template row.
Ordering precedes pagination and `select`, so fields omitted from the response
can still determine order. Filters, search and collection envelopes retain
their existing behavior. Default ordering remains ascending `name`. Ascending
`name` breaks ties unless explicitly requested, including explicit `-name`.

Integers compare numerically without loss of signed/unsigned 64-bit precision.
Strings use case-sensitive UTF-8 lexical order; booleans sort `false` before
`true`. Mixed scalar types follow the inspected reference's ordering: missing,
integer, string, floating-point number, boolean. JSON null and the strings
`null`/`undefined` count as missing; an empty string is present. Descending
reverses that field's complete ordering, including missing values.
The top-level identity `name` always remains literal text, including valid names
`null` and `undefined`, preserving existing default and identity tie ordering.

Unknown paths, object/array values, array indices, scalar traversal and empty
terms contribute no ordering; subsequent fields and identity still apply.
Duplicate fields retain query order. No wildcards, JSONPath, leading `+`, or
whitespace normalization are provided. Item GET and other collections retain
their existing behavior. Sorting neither saves configuration nor starts media.

This independently implemented scalar profile follows the official
[sorting design](https://flussonic.com/doc/fms/api/flussonic-api-design/#sorting-collections)
and read-only inspection of the installed 26.04.1 collection comparator and
[public handler](https://github.com/flussonic/openapi_handler). Vendor ordering
of complete arrays/objects, implicit position/default-key parity and sort-key
cursors across changing snapshots remain unqualified as vendor parity. The
[independent value-based cursor profile](#implemented-value-based-collection-cursors)
now continues the supported scalar order across insertions and deletions.

## Implemented value-based collection cursors

Streams and Templates collections return opaque `next` and `prev` tokens.
URL-encode a token as the next request's `cursor`; retain the collection, sort,
filter and search arguments. `limit` and `select` may change. Omitted sort and
explicit `sort=name` share the same context. Counts describe the current filtered
collection before the cursor and page limit. Empty pages have null navigation.
Item GET and other collections keep their existing contracts.

The independent native token is standard Base64 of a URL query containing one
`$flussonix_cursor` value. Its versioned JSON contains a SHA-256 digest of the
collection, canonical sort specification and filter/search arguments, direction,
and typed scalar boundary values, including identity. Integers retain signed and
unsigned 64-bit precision; floating-point values use finite IEEE-754 bit patterns.
The token is opaque continuation state, not an authorization credential or a
signed snapshot. Management authorization applies on every request.

Forward pages contain rows strictly after the saved boundary. Backward pages
contain the nearest preceding rows in normal sort order. The boundary remains
usable if its row is deleted; adding or deleting earlier rows does not shift a
forward continuation. All current filters and sorting run before boundary
comparison; projection runs afterward. Each request sees the current collection.
Changing a row's sort keys, filter membership or runtime statistics can move it
across a boundary; snapshot isolation and exactly-once traversal during such
changes are not promised. New rows before an already-passed boundary are not
included in its forward continuation.

Inbound legacy `$position_gt=<nonnegative integer>` tokens remain accepted,
including safe exhaustion at the platform's maximum integer. For identity-only
`name` or `-name` sorting, reference `name_gt` / `name_lt` bounds are accepted;
backward bounds include `$reversed=true`. An optional reference `$position_gt`
or `$position_lt` is validated and treated as redundant with the unique name.
For example, Base64 of `$position_gt=2&name_gt=a1` continues ascending names
strictly after `a1`. These incoming reference/legacy tokens lack native context
binding. New responses always return the native profile, even after legacy input.

Malformed Base64, URL escapes or UTF-8; duplicate inner query keys; unsupported
reference compound bounds; native schema/version/context/key mismatches; and
nonfinite or nonscalar boundary values return HTTP 400 after authentication.
Tokens are limited to 24,000 encoded bytes and 16,384 decoded URL-query bytes.
If an outgoing sort boundary exceeds either limit, the request returns HTTP 400
with guidance to choose smaller scalar sort fields. Tokens should be echoed,
not constructed or edited by clients. Changing even equivalent filter spellings
may require restarting from the first page because argument values are bound.

This profile follows the official
[cursor API design](https://flussonic.com/doc/fms/api/flussonic-api-design/#cursors)
and read-only reference inspection, without a vendor build/runtime dependency.
Full vendor compound-cursor serialization, implicit position/default ordering,
and reference filtering quirks remain unqualified. Vendor clients that decode or
construct other cursor dialects require further interoperability qualification.

## Cluster and load-balancing contract

The first release must support the user's **LB → CDN → source over LAN** topology, with local-stream reuse and on-demand pulls. Preserve peers, sources, public/private/API addresses, source filters and the four reference balancer modes. [Detailed design and routing matrix](cluster-loadbalancing.md)

Keep native uplink/CPU/RAM selection and admission reservations in a separate FlussoniX policy namespace. Reference compatibility requires fixtures for stream visibility, redirect path/query/token handling, affinity, failure responses and bitrate units; field names alone do not prove units. HTTP redirect capability does not establish transparent redirection for RTSP, SRT or RTP.

The implemented [native HTTP/RTSP pressure profile](native-cluster-pressure.md)
uses the busiest normalized uplink/CPU/RAM resource, bounded ready preference,
actual reserved Mbps and per-node viewer bandwidth estimates. Its fixed defaults
are distinct from the reference balancer modes; full mode parity remains pending.

## Protocol scope

| Family | First-release behavior | Compatibility details |
|---|---|---|
| HLS / HLSS | Pull HLS over HTTP/HTTPS; serve live HLS over HTTP/HTTPS; push manifests/segments to compatible destinations | Preserve `hls://` and `hlss://` input/push syntax, redirects, track selection, MPEG-TS/fMP4 manifests, sequence and discontinuity behavior |
| TSHTTP / TSHTTPS | Pull continuous MPEG-TS over HTTP/HTTPS; serve `/{name}/mpegts`; receive and send HTTP MPEG-TS publication | Preserve `tshttp://` and `tshttps://`; reference push uses a continuous HTTP POST |
| M4F / M4FS | Receive and originate segment-based inter-server delivery, pull/push, cluster use | Signal channel, separate segment transfer, source timeline, metadata and original payload identity |
| M4S / M4SS | Receive and originate persistent inter-server delivery, pull/push | HTTP setup, frame boundaries, metadata changes, reconnects and redirect behavior |
| SRT | Pull, publish, push and playback using the reference URL/stream-ID conventions | Caller/listener and required rendezvous cases, encryption, latency, stream IDs, authorization and reconnects |
| RTSP / RTSPS | Inbound pull and incoming publication; outbound playback serving and push publication | Client/server session state, SDP, Basic/Digest/token auth, UDP and TCP-interleaved media, TLS |
| RTP | Direct inbound receive and outbound transmit, unicast and multicast | RTP/RTCP, SDP/payload mappings, codec packetization, jitter, pacing and clock synchronization |
| SRTP | Inbound receive and outbound transmit with SRTCP | Explicit keying/protection profiles, independent sender/receiver state, rekey and replay handling |

RTSP/RTSPS and RTP/SRTP support is mandatory in **both directions**. See the [detailed transport contract](rtsp-rtp-support.md) for the direction matrix, keying plan and reference evidence. RTSPS control encryption and SRTP media encryption are separately configured and tested.

HLSS and TSHTTPS are the secure scheme variants in Flussonic configuration. End-user HTTPS delivery uses ordinary HTTPS URLs. The installed schema also recognizes selected HTTP/HTTPS aliases, SRT1/SRT2 aliases and HLS2 variants; implement those required by the profile, with explicit reporting for any deferred variant.

Standalone secure output is mandatory: the daemon must serve HLS, MPEG-TS, M4F and M4S over TLS, with the same authorization as plaintext, secure public redirects and independently verified private source connections. M4F/M4S must support HEVC, MPEG audio Layer II (`m2a`) and MP3 in both directions; HEVC is required across all other requested inputs/outputs that support it. See the [secure-output and codec contract](secure-output-codecs.md) for the implementation boundaries and acceptance matrix. These requirements extend beyond the current H.264/AAC preview.

M4F and M4S are **not equivalent to generic fMP4 packaging or a .m4s filename extension**. A standards-only HLS/DASH implementation cannot satisfy inter-server compatibility.

HLS input fetches require bounded playlist/segment state, live sliding-window handling, discontinuities, changing init data, multitrack/variant selection and relative URL resolution. Distinguish HLS encryption from authentication; encrypted input support is a separately recorded capability. First playback paths include `index.m3u8`, `index.ts.m3u8`, `index.fmp4.m3u8` and track playlists; LL-HLS is an explicit extension rather than an implied promise.

## M4F/M4S implementation plan

Documented M4F behavior includes source/edge time alignment, stable segment ordering, preservation of payloads, segment availability notifications and origin DVR metadata. M4S is documented as a persistent inter-server protocol and supports redirects on publishing. [Cluster restreaming](https://flussonic.com/doc/fms/cluster/restreaming/), [stream pushing](https://flussonic.com/doc/fms/play/push/)

Static inspection of the local 26.04.1 modules adds evidence:

| Observation | Evidence | Status |
|---|---|---|
| M4F reader has HTTP setup and a separate segment fetch path | `m4f_reader`: signal connection, HTTP requests, segment pre-unpacking calls | Statically observed; runtime not tested |
| M4F includes a protocol-version response header | Reader contains version-header handling | Statically observed; version negotiation rules incomplete |
| M4S reader starts with HTTP and switches to framed delivery | `m4s_reader`: HTTP parsing, packet framing, decode calls | Statically observed; complete wire contract incomplete |
| Both paths depend on shared media/container decoding | `m4stream` references `dvr_m4f` pack/decode | Statically observed; complete container format unresolved |

Static outputs stay in temporary reference workspaces. Do not mistake assembly extraction for recovered original source or successful interoperability.

Build the adapters in this order:

1. Specify request/response setup, URI translation, headers, timeouts, version behavior and authorized cluster handshakes.
2. Define transport framing, message types, initialization, track revisions, timestamps, discontinuities, segment identifiers and terminal states.
3. Implement bounded incremental parsers and exact opaque segment relay before introducing repackaging.
4. Decode enough media structure to feed HLS, TSHTTP, SRT, RTSP/RTP and transcoding without losing source timing.
5. Implement the opposite server role and push/publish handling.
6. Validate the mixed-node direction matrix and error/reconnect behavior.

The exact cluster credential transformation, message grammar, all codec metadata and source discovery payloads are **open verification items**. Do not substitute a guessed HTTP auth scheme, a homemade M4F container or generic fragmented MP4.

### Required interoperability matrix

For each M4F and M4S secure/plain variant, exercise:

| Source / publisher | Receiver | Must demonstrate |
|---|---|---|
| Flussonic | FlussoniX | Pull and push reception; media, metadata and authorization |
| FlussoniX | Flussonic | Pull serving and push publication; accepted payloads |
| FlussoniX | FlussoniX | Native transfer plus restart and failover |
| Flussonic → FlussoniX → Flussonic | Chained relay | Preserved identity/timing where pass-through requires it |

Use controlled streams in a separate lab. Cover audio-only/video-only, H.264 and HEVC paired with AAC, m2a and MP3, B-frames, multiple audio tracks, codec revisions, discontinuities, TLS, normal rejection, intentional source outage, reconnect and late join. The supplied demo is not the place to run load, mutation or failover tests.

Compare transport bytes for unchanged relayed segments; compare track/frame/timestamp semantics after legitimate container conversion; compare codec settings and decoded quality after transcoding. Full native DVR archive writing/reading is a separate compatibility dimension.

## Authentication

Keep management, viewer, publisher and peer credentials separate.

**Management:** support `edit_auth` and `view_auth` capabilities and documented Basic/Bearer behavior. The installed schema labels bearer format as JWT but describes the token as base64 username/password. This conflict is a specific reference-test item; do not replace the legacy format with JWT merely because of the label.

**Playback and publication:** preserve query-token and protocol-specific token extraction, configurable session identity keys, callback parameter encoding, session renewal, termination, denial caching and maximum-session behavior. The public callback schema describes GET play checks and POST publish checks; callback URLs themselves are configured by the operator. [Authorization Backend API](https://flussonic.com/doc/api/authorization/)

Support `X-AuthDuration`, `X-UserId`, `X-Max-Sessions` and applicable legacy `X-Unique` behavior. Preserve redirects and protocol-specific rejection results. Test deprecated fields against the pinned server instead of trusting deletion dates in a separately versioned schema.

**Configured backends:** match rule precedence and parallel-backend result combination. An explicit denial and an unavailable backend are different outcomes. Keep the selected reference profile's `allow_default` behavior. [Authorization configurator](https://flussonic.com/doc/fms/auth/configurator/)

**Session lifecycle:** cache decisions per identity and use single-flight authorization for concurrent first-segment requests. Reauthorize on the configured schedule rather than once per media object. The documented baseline retains a previous decision on backend outage while new sessions without approval are denied unless configured fallback rules apply; verify exact timeouts and expiry interactions. [Authorization behavior](https://flussonic.com/doc/authorization/)

**RTSP/RTP sessions:** authorize RTSP playback/publication before enabling media and preserve the reference's credential/token semantics. Direct RTP/SRTP uses an approved configured or negotiated endpoint/session; media packets do not carry an HTTP-style authorization callback. Bind the endpoint and, for SRTP, the key context to that decision.

**IPTV:** implement subscriber/package authorization, existing token acceptance, expiration and channel permissions, plus the supporting management/playlists contracts. Do not infer a database layout or token algorithm from the demo's `iptv://` scheme alone.

For horizontal operation, define session-limit scope and ownership explicitly. Account for reconnect races, duplicate requests and node failure; telemetry counts are not authoritative admission reservations. Any native cluster credentials or session tickets must remain outside the legacy wire adapter unless peers explicitly support them.

## Implemented v0.3 subset

The current preview adds observed packed AVC/AAC M4S GOP ingest, original-wire/segment relay without transcoding, and explicit native HLS/M4S/M4F LAN source selection. These add to the subset qualified in [qualification](qualification.md); they do not complete the mixed-vendor matrix above. The UI replaces raw configuration inputs with labeled forms while retaining the same API payloads. The installed reference is reverse-engineering evidence only; product build, tests and runtime are independent.

## Implemented v0.4 recovery subset

The independent supervisor adds bounded configured-input retry/fallback and media-stall detection, background local/CDN recovery and generated HLS restart identities. `flussonix_input_timeout` and recovery stats are native extensions. This does not establish the reference's precise retry timing, seamless recovery, origin equivalence or new transport-direction support. Existing peer/viewer credentials and policy publication remain distinct.

## Implemented v0.5 native origin failover

Explicit source-group/content identities and exact normalized policy matching add bounded sticky fallback among configured replicas. These extension names and failure rules are native FlussoniX behavior; they are not a claim of matching undocumented Flussonic source groups, retry timing or cluster credentials. An authoritative denial is never bypassed by another replica. Whole-cluster blackout still requires a later playback request after source rediscovery. Existing M4/HLS qualified subsets and missing protocol roles remain unchanged.

## Implemented v0.6 RTSP TCP playback

An optional separate listener serves the bounded RTSP 1.0 H.264/AAC-LC TCP-interleaved playback profile with URL-token/callback authorization and existing native CDN private pulls. Independent FFmpeg decoding and RTSP-input-to-HLS tests are recorded in [qualification](qualification.md). This adds a specific output direction; UDP/publication/push, RTSPS, direct RTP/SRTP, Basic/Digest viewer credentials and exact vendor dialects remain open. Protocol direction coverage is not inferred from interleaved RTP or HTTP redirects.


## Implemented v0.7 RTSP unicast UDP

The opt-in UDP playback profile shares the TCP packetizer, worker and authorization while pacing datagrams to the TCP control peer. A finite prebound port range and application-payload rate limit are required; input `rtp:"udp"` is available through normal Streams/Templates forms. See [profile, bounds and remaining directions](rtsp-rtp-support.md#implemented-v07-udp-playback-profile). Exact vendor dialects, publishing/push, RTSPS, direct RTP/SRTP and RTSP LB redirects remain unqualified.

## Implemented v0.10 HTTPS profile

The optional standalone HTTPS listener serves existing H.264/AAC media, publication, API and admin routes with shared authorization and real socket client IPs. HTTPS-only startup skips HTTP binding. HTTPS viewer redirects exclude plaintext public CDNs/callback targets. Native codec and remaining direction boundaries remain unchanged. See [HTTPS delivery](https-delivery.md) and [qualification](qualification.md); full secure mixed-vendor roles and HEVC/m2a/MP3 are still pending.

## Original subtitle track preservation stage

`flussonix_subtitle_tracks` (`preserve` / `drop`, omitted default `drop`) is a native Stream/Template extension with a friendly inherited control. The shared MPEG-TS fan-out preserves original DVB subtitle and teletext PES and semantic PMT descriptors when selected. Owned fixture tests compare exact encoded payloads, language, DVB composition/ancillary page identifiers and teletext magazine/page through copy and CPU H.264 transcoding. Both HLS variants stay AV-only and readable. PID renumbering is allowed by remuxing. Policy edits replace the worker generation; authenticated discovery carries the policy without publisher credentials.

Native H.264/HEVC M4S/M4F framing and native-to-TS bridge tests preserve GA94 bytes containing both 608 and 708 packets; these are payload-survival tests, not subtitle decoding/player qualification. The DVB fixture is an acquisition clear-page, not an OCR image-quality test. Teletext contains an independently authored subtitle header and visible text row. There is no official Flussonic component dependency.

At the original preservation stage, selectable WebVTT and CEA/teletext decoding were pending; later stages below add selected conversion. Still pending: automatic service discovery, complete subtitle API compatibility, separate native subtitle tracks and source-to-CDN regional subtitle round trips. Current AV-only HLS source pulls omit separate subtitle PIDs. GPU caption retention, SRT/RTP subtitle delivery and exact presentation semantics remain unqualified. Dropping separate tracks does not strip embedded captions.

## HLS subtitle mode and native 608/708 conversion

Streams/Templates expose friendly inherited pass-through / selectable WebVTT / filter-out controls via native extensions `flussonix_hls_subtitles` and `flussonix_hls_captions`. These are not claimed legacy API aliases. Selected CEA-608 CC1..CC4, CEA-708 digital services1..63 announced Level 1 Latin teletext pages100..899 and selected DVB composition pages0..65535 with optional OCR convert to authorized TS/fMP4 HLS WebVTT with one held-back AV segment; configured languages/names, silent segments, stable cue slices, generation replacement and grouped token/revocation paths are covered. Copy/CPU caption extraction shares one source session with AV and remains independent of original DVB/teletext preservation.

HLS filtering disables embedded GA94 caption process/count/valid flags in delivered H.264/HEVC segments, preserving lengths and unrelated SEI. Shared live MPEG-TS originals remain available. Explicit pass-through also retains original DVB/teletext descriptors and PES in TS-HLS with copy/CPU encoding, independently of other TS output policy. The player must decode the original format; fMP4 does not carry separate DVB/teletext tracks. Omitted mode keeps the prior embedded-caption default. Conversion provides selected plain-text608/708, Level 1 Latin teletext and optional DVB bitmap recognition; see [the digital service profile](cea708-qualification.md) and [teletext profile](teletext-qualification.md) and [DVB OCR profile](dvb-qualification.md). Enhanced/non-Latin teletext, advanced DVB objects, real HEVC caption playback, GPU conversion, separate native subtitle tracks, full subtitle migration aliases and sustained performance remain pending. See [subtitle contract](subtitle-design.md) and [qualification](qualification.md).

## DVB bitmap recognition preview

Selected composition pages0..65535 use independent bounded bitmap decoding and optional Tesseract OCR, with per-service recognition models and four combined608/708/teletext/DVB rows. Friendly Stream/Template controls preserve inheritance, reject mixed or duplicate selectors and expose recognition confidence/failure without JSON inputs. Copy/CPU dual-language TS/fMP4 WebVTT, exact source timing, original payload preservation, mixed teletext isolation, grouped auth/revocation, and missing/slow/oversized OCR AV progress are covered by owned fixtures. Real browser playback selects English and German external text tracks with embedded decoding disabled.

The1.5second queue/process deadline and daemon-wide two-process cap bound lag/cost; queue pressure or confidence below60 drops affected recognition while preserving source interval ends and advancing AV. Reset/rebind cancels tokens and reaps stale children. This is a measured coding0 interlaced SDR bitmap profile, not full broadcast or migration qualification. See [qualification, deployment and limits](dvb-qualification.md) and [decisions](dvb-decisions.md).


## RTSP publication receiving profile

Configured `publish://` streams, including template inheritance, receive RTSP/RTSPS ANNOUNCE, per-track SETUP and live RECORD through the existing optional listeners. Publication uses interleaved TCP or opt-in unicast UDP, with H.264/HEVC, AAC-LC and MPEG Layer II/III; RTSPS remains encrypted TCP. The shared HTTP publisher password and `on_publish` callback apply; changes to effective media settings or publisher policy revoke pending or active ownership. The callback protocol is `rtsp` on plain and TLS connections. Capability and node metadata contain actual enabled listener addresses.

Independent publisher and strict decoder qualification covers the common worker TS, audio-only, distinct MPEG audio tracks, CPU HEVC/MPEG audio and a verifying TLS relay; H.264 VAAPI has an explicit opt-in local hardware test. See [profile, commands and limits](rtsp-publication.md). Earlier RTSP sections describe their stage-specific coverage; this receiving qualification supersedes their publication exclusions only for this profile. UDP receiving additionally binds the control peer and each negotiated RTP/RTCP port, shares the prebound playback pool, discards pre-RECORD queues, and returns RTCP through the negotiated transport. Independent UDP qualification covers the codec matrix, eight audio tracks, CPU transcoding, foreign endpoints, port reclamation, renewal denial, queue overflow and control responsiveness during foreign traffic. Dynamic publication, outbound UDP push, separate SDP subtitle tracks, incoming Basic/Digest authentication, mixed-vendor publication and production capacity remain unqualified.

## RTSP and RTSPS push increment

The existing v3 `pushes` subset now accepts four total mixed SRT/RTSP/RTSPS
destinations in Streams/Templates. Native ANNOUNCE/SETUP/RECORD publishing
shares the existing RTP packetizer, verifies RTSPS identity and roots, and
retains query-based receiver credentials and supports Basic/Digest URL
credentials for the bounded receiver profile. Friendly protocol-specific fields,
inheritance and explicit empty overrides are available. Unsupported options,
unsupported authentication profiles, UDP substitution, redirects and retained separate
subtitle tracks fail explicitly. Runtime counters are sanitized native
diagnostics, not vendor push-stat parity. See the [exact push profile](rtsp-push.md).
This supersedes older outbound RTSP pending statements for the bounded TCP
profile; complete API, vendor recorder dialects and migration compatibility
remain to be qualified.


## RTSP UDP input URL alias

`rtsp-udp://` is accepted as an input URL in Streams and Templates, preserving
inheritance and persistence. It selects the existing unicast UDP pull profile;
`rtp:"udp"` may also be present. The friendly transport selector recognizes both
forms. Selecting TCP changes only the alias scheme to `rtsp://` and removes the
UDP option. Credentials and queries retain their original encoding; summaries
continue masking secrets. Unsupported transport values, direct RTP settings
and TLS CA settings are rejected before replacing the saved configuration.

This closes the input-scheme gap advertised in the installed reference schema.
Qualification uses owned FlussoniX sources and independently installed FFmpeg,
without invoking official components. It does not establish mixed-vendor camera
interop, UDP publication/push, receiving Basic/Digest policy, `rtsp2`,
`wait_rtcp`, multicast RTSP or production capacity.

## RTSP2 camera input

Streams and Templates accept `rtsp2://` as a camera input alias. The independent
FFmpeg adapter receives `rtsp://`; only the scheme changes, preserving encoded
credentials, paths and queries. Saved configuration and worker input statistics
retain the original alias. This is RTSP/1.0, not RTSP protocol version 2.0.
TCP is the default; `rtp:"udp"` selects unicast UDP. The friendly input form
recognizes both transports without rewriting the camera alias. Plaintext
`rtsp2` rejects TLS CA and direct RTP settings before changing saved state.

For the currently selected `rtsp2` input, absent effective `transcoder.acodec`
defaults to AAC. Video retains the existing profile, normally copy. Audio uses
48 kHz stereo at 96 kb/s unless an effective audio bitrate is configured. This
bounded camera profile **encodes all source audio**, including audio already in
AAC; it does not selectively transcode G.711 while copying other audio codecs.
An explicit stream or inherited template `acodec` wins, including `copy`,
`aac`, `mp2a` and `mp3`. Copying G.711 does not make it compatible with the shared
HLS/MPEG-TS worker. Defaults follow each selected fallback input independently;
canonical `rtsp`, `rtsp-udp` and `rtsps` retain their existing audio defaults.
The form displays the primary input's default and explains the per-input rule.

Owned PCMA/PCMU camera fixtures qualify default AAC conversion on TCP and UDP,
source Basic authentication and encoded query preservation, independently
decoded HLS, explicit MP3/MPEG Layer II overrides and shared-worker reuse.
Configuration and browser checks cover inheritance, persistence, transport
changes, secret masking, independent audio controls and clearing foreign TLS
options. This adds no vendor runtime dependency. Camera dialect parity,
`wait_rtcp`, receiving Basic/Digest policy, RTSP 2.0, multicast RTSP and
production-scale migration qualification remain separate gaps.

## Outbound unicast UDP RTSP push

The native `pushes` RTSP profile accepts `rtsp_transport: "udp"`; TCP remains the default and RTSPS rejects plaintext UDP. Per-track owned sockets bind the control peer and negotiated server ports, pace shared native RTP, and carry bounded RTCP. Invalid negotiation closes before RECORD; retries/cancellation release the pairs. Streams/Templates expose a normal transport selector with inheritance. See [UDP push contract and qualification limits](rtsp-push.md#unicast-udp-push). This supersedes older UDP push exclusions only for this bounded unicast profile.

## RTSP multitrack playback

RTSP TCP, opt-in unicast UDP and verified RTSPS TCP share an eight-media-track
packetizer. The qualified copy-mode native profile includes HEVC plus mixed
AAC/MP2/MP3, eight MP2 audio tracks, and HEVC plus seven AAC tracks. Selected
SETUP tracks retain their IDs, negotiated transport and existing viewer policy.
UDP feedback from each track receives bounded round-robin service; invalid or
foreign traffic does not renew authorization or suppress another track's turn.
See [playback profile, evidence and limits](rtsp-rtp-support.md#multitrack-playback).

## RTSP authorization callback redirects

Initial DESCRIBE on standalone/CDN nodes accepts backend-selected absolute
RTSP/RTSPS destinations, with RTSP 302/Location before worker/session startup.
Cached decisions retain token/template policy and revocation; actual TLS control
connections reject plaintext destinations on every response. HTTP redirect
behavior stays separate. This is callback-selected routing; [native adaptive RTSP LB reservations](rtsp-cluster-routing.md) are implemented as a separate profile. See [contract and qualification](rtsp-auth-redirects.md).

## Native RTSP cluster routing

The native LB profile implements measured RTSP/RTSPS selection, compact cached telemetry, bound five-second tickets and independent CDN authorization. Friendly peer fields configure public RTSP/RTSPS listener roots. Verified TLS never downgrades; independent FFmpeg decodes plaintext LB redirects through a shared private M4S source pull. Configured RTSPS workers also qualify the secure native LB/CDN chain through verified owned input bridges. Sustained capacity and mixed-vendor parity remain unqualified. See [profile and limits](rtsp-cluster-routing.md).

## Verified RTSPS input redirects

Configured inputs follow bounded pre-session native TLS redirects through owned loopback bridges. Every destination verifies identity and trust before application data. Username/password inputs retain their configured credentials only within the original normalized hostname and effective port; changed origins fail before connecting. Owned Basic and Digest MD5/qop-auth camera-style fixtures independently decode H.264/AAC after redirect. See [contract, qualification and limits](secure-rtsp-redirects.md).


## HTTP Basic input and publication

Configured HTTP(S) MPEG-TS publishers and upstream HTTP-family inputs also
support a bounded [HTTP Basic profile](http-basic.md). Receiving publication
uses the existing password policy; viewer auth and peer credentials remain
separate. Missing publisher credentials return401 with a challenge, while
incorrect legacy query passwords retain403. This is not HTTP Digest or a
per-user publisher account system.

## Incoming RTSP publisher header credentials

Configured RTSP/RTSPS publications accept preemptive Basic and MD5 Digest
with qop-auth or legacy omitted-qop responses using the effective stream/template publisher password. The username
is a publisher label, not a management account. Initial401 challenges support
bounded same-connection retries before callbacks or worker startup; original
query-password behavior remains available. Independent FFmpeg qualification
covers observed qop-auth and decoded H.264/AAC on TCP, UDP and verified TLS. See
[the receiving authentication profile](rtsp-publication.md#incoming-publisher-basic-and-digest-authentication).
Viewer Basic/Digest, additional incoming algorithms and complete vendor API/auth
parity remain open.


### Cluster source exclusions

Native source rows accept `except` as an array of exact stream names or subtree patterns such as `region/*`. Matching is case sensitive: `region/*` blocks `region/news` and `region/deep/news`, but allows `region` and `regional/news`. Spaces and Unicode names are literal. A source accepts at most 1,024 patterns, each at most 256 UTF-8 bytes including `/*`; other wildcard forms and invalid stream paths are rejected atomically. Omit the field or use `[]` to allow all names. A management merge patch with `except: null` removes the override.

Exclusions apply to that source before metadata requests and during cold discovery and replica selection. Other allowed sources can provide the same stream, subject to existing content identity, group and viewer policy checks for failover. Locally configured streams retain precedence. Updating a source through the management API invalidates its cached routes and viewer authorization; reconciliation stops stale pulls. In-flight metadata from the previous configuration revision cannot reinstall the excluded route. The source editor has labeled Add/Remove exclusion rows and client validation.

This implements the blacklist portion of the [reference source behavior](https://flussonic.com/doc/fms/cluster/restreaming/). `only` is a static activation policy in the reference, not an allowlist that hides other streams, and remains pending. Source `prefix` mapping and full legacy source discovery/interoperability remain pending; the URL identity profile below adds bounded source CRUD. Source rows continue to use native `hostname`, management/private endpoints and the existing authenticated native metadata protocol.


### URL-keyed cluster source configuration

Native sources additionally accept the reference `url` primary key through `/streamer/api/v3/cluster/sources/{url}`. Encode the entire source address as a path component. A URL source uses a lowercase `m4f://`, `m4fs://`, `m4s://` or `m4ss://` server root with an optional port and trailing slash. The address is a literal, case-sensitive identity preserved as supplied, up to 4,096 ASCII bytes. Credentials, stream paths, queries, fragments, percent escapes, empty/zero ports and malformed addresses are rejected. Existing `hostname` rows remain valid; a source cannot carry both identities.

For example, PUT `/streamer/api/v3/cluster/sources/m4s%3A%2F%2Forigin.example%3A8080` with `{}` creates a source whose `url` is `m4s://origin.example:8080`, native `api_url` is `http://origin.example:8080/`, and native `flussonix_transport` is `m4s`. Secure variants infer HTTPS and retain certificate/identity verification. Explicit native management/private endpoints and transport settings override those defaults; their actual schemes determine TLS. The API returns and persists the inferred native fields. Null-removing an endpoint/transport override or resetting a row re-applies its address defaults. A conflicting body URL is rejected; the request identity stays fixed.

The same normalization applies to config validation, replacement and startup imports. Validation does not save; startup does not rewrite the file. Source updates retain configuration revision fencing, viewer authority invalidation and worker reconciliation. URL identities appear in source lists, selected fields, selected-origin telemetry and replica switch accounting. The source editor provides a Source identity selector and a fixed Source address field, alongside existing endpoint controls and staged Config editing.

This closes the bounded native source identity/CRUD gap against the [reference API schema](https://flussonic.com/doc/api/reference/). It does not implement vendor source inventory/metadata messages, `prefix` mapping, static `only` activation, group configuration or full source/schema interoperability. Installed Flussonic components remain reference material only.
