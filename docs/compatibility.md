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

## Cluster and load-balancing contract

The first release must support the user's **LB → CDN → source over LAN** topology, with local-stream reuse and on-demand pulls. Preserve peers, sources, public/private/API addresses, source filters and the four reference balancer modes. [Detailed design and routing matrix](cluster-loadbalancing.md)

Keep native uplink/CPU/RAM selection and admission reservations in a separate FlussoniX policy namespace. Reference compatibility requires fixtures for stream visibility, redirect path/query/token handling, affinity, failure responses and bitrate units; field names alone do not prove units. HTTP redirect capability does not establish transparent redirection for RTSP, SRT or RTP.

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
