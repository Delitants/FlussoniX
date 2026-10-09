# Preview qualification

This milestone is an executable subset of the product design, not complete Flussonic parity. Baseline reference: installed Flussonic 26.04.1. The native cluster uses FlussoniX peer authentication and source discovery; a mixed Flussonic cluster is not qualified.

## Evidence

- Rust tests exercise deep partial/reset/null merge, template inheritance across restart, validation without mutation, disk-write failure rollback and rejection of unsupported options.
- Management tests exercise edit/view roles, Basic and base64 Bearer credentials, multi-segment stream names, auth before any media worker, and preserving unrelated workers on metadata changes.
- Real FFmpeg tests demonstrate a single shared worker for concurrent subscribers, generated HLS, and child reaping on stop.
- The HTTP topology test runs independent source, CDN and LB servers on OS-allocated ports. It verifies unauthorized requests do not pull a source, the public redirect uses a single-use admission ticket, concurrent CDN requests coalesce, protected segments remain protected, and delivered MPEG-TS contains H.264.
- Both independently generated M4F and M4S outputs were pulled over HTTP by a second FlussoniX instance and decoded by FFmpeg. M4F sample-table tests preserve timestamps/composition offsets and reject truncation.
- An owned M4F segment produced by the Rust adapter was fed to the installed Flussonic decoder in an isolated Erlang process: 144 frames decoded. This is a container check, not proof of every Flussonic playback/cluster behavior.
- The user-authorized live M4S source produced H.264/AAC HLS; FFmpeg decoded the delivered segment. Its URL and token are retained only in ignored private test configuration.
- The authorized production M4F signal endpoint returned live notifications. Downloads of its `.m4f` segments with the supplied viewer token returned HTTP 403, so that source's M4F ingestion is not qualified. No production configuration was changed to work around it.
- Browser tests cover stream creation/persistence, config validation without saving, Templates and Cluster views, and preserving template inheritance after a title edit.
- Review regressions cover bounded SIGTERM shutdown with an open MPEG-TS response, portable named source auth backends, clean CDN playlist reload URLs, strict auth policy validation and keeping live bodies in admission accounting.

## Limits

M4F currently supports up to two uniquely identified H.264/AAC tracks with one contiguous chunk each, 90 kHz normalization, at most 100,000 decoded samples and 32 MiB of aggregate decoded payload per segment. M4S supports observed MDin/FRam AVC/AAC frames and Fgop packed AVC/AAC segments. HEVC, subtitles, SCTE/ad metadata semantics and full legacy control behavior remain unqualified. Unknown records can be forwarded live as opaque bytes without executing them. Generated audio-only M4F output and subsecond GOP timestamp-path reuse remain unqualified. These modes must be qualified separately before migration.

CPU encoding uses libx264 plus AAC. NVIDIA configuration is exposed, but no GPU success is claimed without a real supported device and decoder test. v0.6 adds the explicitly tested RTSP TCP playback profile below. FFmpeg adapters and this profile do not establish full RTSP or SRT serving/publishing parity. RTSPS, direct RTP/SRTP and push roles are not implemented.

HLS and M4 windows are bounded; live subscribers use bounded shared queues and disconnect on lag. There is a 256-worker implementation limit. This build has not been benchmarked for production stream/viewer counts. v0.2 samples Linux CPU/RAM and aggregate TX bytes on the selected interface independently of management requests. HTTP media egress is reported separately; explicit process mode is available. Unknown/warmup/reset/stale metrics exclude new admissions. Balancing applies resource headroom and authoritative CDN reservations; it does not promise perfect uplink prediction.

API compatibility is a subset. Collection default ordering is by name; `sort=-name` and literal substring `q` are implemented, while full projection/filter/sort semantics, configuration-text formats and many operations remain open. API credentials/listeners/resource limits are startup settings rather than the complete vendor config contract. Unknown saved options fail validation. Viewer session counts expire after 30 seconds of inactivity when no continuous response remains open; global cross-node session ownership remains open.

For testing, keep management endpoints on a trusted interface or reach them through SSH forwarding. The daemon currently serves HTTP, so HTTPS delivery requires a separately configured TLS terminator. The dedicated test instances must use separate directories, accounts, units and unused ports; never replace or restart existing Flussonic services.

## CDN test deployment

The user authorized installations on cdn4-uk.ott.pink and cdn5-uk.ott.pink. Read-only inspection verified Ubuntu 24.04 x86_64, independent `/usr/bin/ffmpeg`, unused TCP 18210, and a shared private network. Installed isolated prefix/unit: `/opt/flussonix-test` / `flussonix-test.service`. Release artifacts are built for x86_64 Linux with musl so no host libc upgrade is required.

Qualification completed 2026-10-01 HST on the same x86_64 musl binary at both nodes (SHA256 `b3678fb1dde7a4856cc37bfd1a1f9e78802f4187a5557708da358a2487465e87`). cdn4 ran the source role, receiving the authorized Flussonic M4S AVC/AAC stream over HTTPS. cdn5 ran the CDN role and its FFmpeg input was verified to use cdn4's private address, 172.16.0.7. The local LB ran on 127.0.0.1:18211 and queried management through SSH forwards (18214/18215). It returned a 302 to cdn5; ticket redemption returned a clean reload URL. Delivered 1920x1080 H.264/AAC MPEG-TS passed FFprobe and an actual FFmpeg decode. Anonymous entry and segment requests were denied, and a later playlist reload succeeded.

A Chromium HLS player followed the LB/CDN redirect flow through the SSH forwards and sustained playback beyond the initial playlist: 11 manifest loads, 10 buffered fragments, no fatal HLS errors. The test temporarily set the LB's public delivery endpoint to the CDN SSH forward and restored the cdn5 host endpoint afterward. Direct public access to test port 18210 was unavailable from the development host; no firewall or production listener was changed. Use SSH forwarding to access the remote test UI.

The dedicated systemd units run as `flussonix-test`, with Nice 10, CPUQuota 100%, MemoryMax 512 MiB, a separate writable runtime directory and control-group cleanup. They are started for testing, not enabled at boot. Production Flussonic process IDs and port 80/443 listeners were checked before and after; neither production service was restarted or reconfigured. These checks are bounded functional qualification, not a throughput benchmark.

Production stream tokens, API passwords, peer keys, key files and source URLs are excluded from this repository.

## v0.2 authorization and telemetry

The callback/session tests exercise structured template policy, UUID and protocol fields, concurrent-request coalescing, user limits across streams, cached redirect without worker startup, edit/view session API permissions, denial caching, live body cancellation, renewal retaining the session ID, backend-outage behavior and configuration/manual-revoke races. A real daemon renews a held MPEG-TS connection without another playback request and closes it on backend denial. Inactive sessions expire while live bodies retain their client slots. Ordered identity keys use an independent hash; byte-for-byte vendor session hashing is not claimed.

Telemetry fixtures exercise default-route selection, explicit interface validation, rate conversion including other-process TX traffic, separate HTTP bytes, first interval, stale samples, counter reset and missing/reappearing counters. The topology test waits for a valid sampling interval rather than treating startup as idle. Browser qualification includes actual session reauthorization/deletion and verifies subsequent playback is denied.

Object `on_play` currently supports URL, literal session keys and max sessions only. Geography/domain/soft-limit/query-key policies, publisher auth and full session history/projection/pagination are not implemented. Renewal runs in bounded batches; outages retain an existing decision with a ten-second retry. Admission and user limits are authoritative locally; cluster-wide revocation and global user limits require further work. Redirect auth backends must return absolute HTTP(S) locations. Remote-source policy changes are reconciled when discovery refreshes and by polling active mirrors after the ten-second cache expires. The five-second supervisor processes up to 64 due streams in parallel batches of 16; large installations need further scheduling/scale qualification. Source discovery has a three-second total timeout; failure to refresh invalidates viewer policy until valid rediscovery. There is no distributed invalidation bus. Versioned policy snapshots fence stale requests, and unchanged root config saves preserve discovered sessions.

Fresh review identified five Important lifecycle races. Each was reproduced as a failing regression and fixed: stale policy snapshots (including a missing initial session record), manual deletion followed by reauth, active remote policy changes, late discovery after source removal, and unchanged root saves. Additional concurrent user-limit and silent M4F/M4S revocation tests passed.

## v0.2 release qualification

The release candidate static binary SHA256 is `57f2bd1f0d457532d0c40fdf3864e974c46c611c35ebb65c859adf054585fc64`. It was installed into both existing isolated units on TCP 18210. The source/CDN/local-LB topology passed again with this binary: protected redirects, canonical playlist reload, private source pull, 1920×1080 H.264/AAC probing and actual FFmpeg decode. Chromium completed 11 manifest loads and 11 buffered fragments with no fatal HLS errors. The public CDN endpoint was restored after SSH-forward browser qualification.

A temporary backend on the source's private interface qualified structured source-policy portability, stable UUIDs and scheduled renewals on the remote CDN. Explicit backend denial terminated the existing TS response, subsequent playback returned cached 403, and peer source pulls created no viewer sessions. The temporary backend was stopped and its stream/backend configuration removed afterward.

Both nodes auto-selected their public default-route interface `ens19`. A measured interval reported 115.8 Mb/s aggregate source TX versus 8.7 Mb/s HTTP media output, and 183.5 Mb/s CDN TX versus zero HTTP media output, demonstrating that other-process traffic contributes to admission load. These are functional measurements, not throughput benchmarks.

Local qualification passed 52 Rust tests, fmt and clippy with warnings denied, plus five browser tests against the release candidate on the unused local port 18220. One live-source test remains opt-in; the authorized live M4S source was qualified through the remote topology. The native-guard and unrelated-metadata-save regressions also passed. Production Flussonic remained at PID 1346636 on cdn4 and PID 211384 on cdn5, with the same port 80/443 listeners.

## v0.3 operator forms and original M4 relay

The UI uses labeled forms for streams, templates, node endpoints, processing and viewer policy; configuration JSON is no longer an input requirement. Config supports staged edits/removal, validate without saving, apply and discard. Browser tests verify template inheritance, actual persisted bitrate/backend/endpoint values, source transport choice, masked peer keys, sessions and staged configuration. Existing identities are read-only to avoid accidental duplication. An explicit `copy` encoder overrides inherited transcoding; removing an override continues inheritance.

The independent M4S parser accepts the observed packed-GOP wrapper with UTC, DTS, sequence, duration and complete M4F payload. It rejects nonfinite timing, duplicate required fields and excessive box counts. Native M4 inputs without transcoding feed their original wire hub directly; FFmpeg separately produces HLS/TS. Tests preserve original M4F payloads/UTC paths and M4S authored record extensions, then decode HLS with independently installed FFmpeg. This does not establish complete mixed-vendor server/publisher interoperability.

Shared wire queues cap both record count (256) and bytes (16 MiB). Bootstrap caps at 32 MiB and waits for a new keyframe after overflow; segment cache caps at 8 entries / 64 MiB. M4F HTTP ingest limits fetched segment bytes to 16 MiB, caches before announcing, and skips the latest 16 duplicate segment paths. Native peer requests do not follow HTTP redirects, preventing peer-key forwarding. Stop/replacement cancels input and delivery and reaps the packaging child.

Source-only `flussonix_transport` selects HLS/M4S/M4F over the configured LAN endpoint, preserving endpoint prefixes and encoded names; default remains HLS. Secure endpoint URLs choose secure input aliases. Source processing is not reapplied at the CDN. Local topology tests cover all three source transports with token denial, clean redirects, one shared worker and actual media decoding. Worker stats report upstream protocol.

Local checks passed 74 Rust tests (one authorized-source test opt-in) and eight browser cases. Build/test/runtime depend on independent Rust/JavaScript dependencies and FFmpeg, not the installed Flussonic reference. CI asserts the reference package is absent. Optional isolated reference probes inform the wire specification only.

Fresh review reproduced four Important failures. Regression tests now verify that unchanged metadata retains a keyframe bootstrap, overflow withholds dependent live samples and excludes pre-key samples from recovered M4F, and worker cancellation prevents buffered bootstrap delivery. Native HLS peer credentials stay inside a bounded loopback fetcher: redirects are rejected, and playlist URI lines/attributes must resolve to the configured HTTP(S) origin. Nested playlists, keys, initialization maps and byte-range forwarding are tested. URI resolution follows [RFC 8216](https://www.rfc-editor.org/rfc/rfc8216). This native peer restriction deliberately rejects foreign-origin resources; ordinary viewer-token HLS inputs retain their existing FFmpeg behavior.

### v0.3 isolated live qualification

On 2026-10-02 HST the same static x86_64 musl binary (SHA256 `036d7574150810bafaff7a0a7adfa5c4bbe6a5feaa6df843755efc5640d1b270`) was installed into the existing isolated cdn4/cdn5 units on TCP 18210. The v0.2 test binaries and UI directories were retained as rollback copies. The source continued pulling the authorized Flussonic M4S AVC/AAC feed; production configuration was not changed.

The local LB and remote source/CDN topology passed separately with native HLS, M4S and M4F source selection over the source's private LAN endpoint. Every mode demonstrated protected entry/segments, clean ticket redemption/reload, running shared workers, 1920×1080 H.264/AAC probing and actual FFmpeg decoding. Native M4F also demonstrated byte-identical source/CDN segment bodies at the same UTC path. This proves the independent native relay; direct access to the supplied production Flussonic M4F URL remains denied and unqualified.

With M4S retained as the CDN test transport, Chromium sustained LB/CDN playback through the SSH forwards: 11 manifest loads, 9 buffered fragments, no fatal HLS errors. Its temporary loopback public endpoint was restored afterward. Production Flussonic remained at PID 1346636 on cdn4 and PID 211384 on cdn5, with the same port 80/443 listeners. Test units retain their separate resource limits; no production service, listener or firewall was changed.

## v0.4 supervised recovery

The packaging watchdog measures MPEG-TS output progress, including startup. `flussonix_input_timeout` is a stream/template extension (1..300 seconds, default 15), with labeled UI controls and inheritance. Failure closes old continuous bodies and reaps the child; retries advance ordered inputs with 1/2/4/8/16/30-second capped cooldowns. A thirty-second span of successful output progress resets short-failure backoff; the final interval without output does not count. Runtime fields contain sanitized error labels, input index/protocol, restart count, retry delay and media age. No arbitrary FFmpeg errors or credential-bearing URLs enter status fields.

Reconciliation recovers enabled static streams and recently demanded local/CDN streams without another playback request. Background retry preserves actual demand, retires idle on-demand streams before retry, and checks current configuration after asynchronous startup. Barrier-controlled removal/disable tests demonstrate that stale retries cannot revive deleted/disabled streams. Demand timestamps and body counts are shared across generations: held old bodies retain activity, and late departure refreshes the replacement’s grace period. Source discovery and viewer-policy fences remain in force.

Generated TS/fMP4 HLS replacements use a sequence high-water mark seeded once by epoch microseconds and continued from that stream’s old playlist windows, fresh generation names for segments/init, and a discontinuity marker. Clock rollback/exhaustion, forward-clock changes and cross-stream sequence holes are tested; failed workers cannot serve stale manifests as fresh output. A replacement fMP4 init/fragment passed actual FFmpeg decode. Chromium resumed across an owned packaging-worker failure, buffered a new generation and advanced beyond the pre-failure buffer without fatal HLS errors. This demonstrates recovery, not gapless playback. Number monotonicity across daemon restarts under clock rollback is not qualified.

The first candidate passed 88 Rust tests and all ten browser cases. Whole-branch review reproduced a demand-generation race, a hidden Problems status, and an incorrect healthy-backoff reset. Regressions cover both held-body and late-departure demand and exclude terminal stalls from healthy progress. Remote browser qualification also exposed clock/cross-stream sequence holes; deterministic regressions and a narrow review verified the correction. The final candidate’s full checks and isolated release qualification follow below. No Flussonic build/test/runtime dependency is introduced. Full origin-equivalence failover, global session ownership, additional protocol roles, GPU operation and production-scale behavior remain roadmap gates.

### v0.4 final isolated qualification

On 2026-10-02 HST, static x86_64 musl candidate SHA256 `729ba1ec50443c0aa4db3e4c72387fb52287503b29bfed7fcfeeaead979657d0` passed 92 Rust tests (one authorized-source test opt-in), fmt, clippy with warnings denied, web build and all ten Chromium cases. The stronger playback test requires a distinct generation, advancement beyond the old buffered media and five seconds of sustained normal playback. The Problems filter was also checked with an actual retrying worker. No official Flussonic component is needed for these normal checks.

That same binary was installed into both isolated test units on TCP 18210. Native LAN HLS, M4S and M4F modes again passed protected LB redirects/ticket redemption, clean playlist reload, protected segments, H.264/AAC probing and actual FFmpeg decode. M4F retained byte-identical source/CDN segment payloads at their original UTC paths. M4S remains the test CDN's source transport; the authorized production input is unchanged.

A SIGKILL was sent only after confirming that the target FFmpeg process belonged to `flussonix-test.service` and had the dedicated FlussoniX process as parent. Management-only polling demonstrated background CDN replacement without any new media request. Its playlist continued after the old window, changed generation, marked discontinuity and delivered decodable protected media. A separate kill during Chromium playback through the local LB and SSH-forwarded CDN produced 19 playlist loads and 17 buffered fragments, no fatal errors, and 5.003 seconds of playback advancement in the final five-second interval after old buffered media had been passed.

The temporary loopback delivery endpoint was restored. Both nodes retain v0.3 test binaries and UI directories as rollback copies. Production Flussonic remained at PID 1346636 on cdn4 and PID 211384 on cdn5, with unchanged port 80/443 listeners. No production service, configuration or firewall was modified. This qualifies worker recovery in the tested subset; it does not establish gapless playback, daemon-restart timeline continuity, complete protocol/API parity, GPU support or production scale.

## v0.5 equivalent-origin failover

Owned fixtures exercise media-only failure with a live source API, management outage, sticky fallback after preferred-source recovery, different per-source keys and private routes. Real source/CDN apps decode delivered HLS after switching; concurrent first viewers retain one pull. Content, group, absent identity and normalized-policy mismatches are rejected. Disabled/deleted/invalid known authorities fail closed, local configuration retains precedence, and barrier-controlled late lookup/source removal cannot republish obsolete routes. A fresh verification exposed an issued-versus-completed lookup race; lookup tickets now fence publication before network awaits.

The local suite passes 110 Rust tests (one external-source case remains opt-in), fmt and warnings-denied Clippy. All eleven Chromium cases pass, including content identity override/inheritance, friendly validation and source-group persistence; the web build passes. Source statuses expose identity and switch counts without credential-bearing endpoints. No installed Flussonic component is required for these normal tests.

All-origin blackout stops media and requires later playback demand after rediscovery. Continuous-body reconnection, authority refresh intervals and bounded lookup behavior do not establish gapless delivery or production-scale availability. Independent mixed-vendor cluster semantics, global session ownership, remaining protocol directions and GPU qualification remain open. Whole-branch review reproduced four Important failures. New RED/GREEN regressions retain authoritative denial through later API outages, advance failed-origin searches to a third replica, preserve selected workers across delayed authorization and engine-lock waits, and reclaim completed-probe/negative-cache capacity. The full 110-test suite and warnings-denied Clippy pass after the single fix pass. Exact static-candidate CDN evidence follows below.

### v0.5 isolated LAN failover

On 2026-10-02 HST, exact static x86_64 musl binary SHA256 `308dd73ce3e9746cd068f88c8c777758d5ed45c028e5c1f6f0c75a138b34b280` was installed in both existing isolated units on TCP 18210, retaining the v0.4 binary/UI for rollback. An owned temporary source on cdn4’s private address 172.16.0.7:18212 ran as a separate resource-limited transient unit. Primary and backup carried an owned synthetic stream with explicit matching group/content identity and viewer token guard; no production stream was used for failure injection.

After validating its unit/parent, only the CDN test packaging child received SIGKILL. Management-only polling observed background switching to the backup while the primary source API remained alive. Protected delivered media decoded, and the fallback stayed selected after the primary recovered. Stopping only the temporary backup source then caused an API/media outage; the CDN returned to the primary with switch count 2. Chromium followed LB ticket redemption, buffered a distinct generation, advanced beyond the old buffered media and sustained 5.003 seconds of normal advancement over the final five-second interval: 11 manifest loads, 9 buffered fragments, no fatal errors. The active-pulls UI reported the current source and both switches. Anonymous segments remained denied.

The temporary replica, configuration and SSH forward were removed; source/CDN/LB root configurations were restored and verified. Production Flussonic remained at PID 1346636 on cdn4 and PID 211384 on cdn5, with unchanged 80/443 listeners. A first cold qualification attempt timed out before the expected state was observed; instrumented retries observed the intended transitions and the final complete run passed. An intermediate browser helper read the UI before asynchronous loading completed; the helper now waits for the row. These are lab results, not a production-scale availability claim.

The same exact binary also passed the existing authorized live-source regression separately with native LAN HLS, M4S and M4F. Each mode demonstrated protected LB redirects, clean ticket redemption/reload, protected segments, H.264/AAC probing and actual FFmpeg decode. Source/CDN M4F segment bodies remained byte-identical at the original UTC path. M4S was restored as the dedicated CDN source transport; the production input/configuration remains unchanged.

## v0.6 RTSP TCP playback

Owned tests reconstruct fragmented H.264/AAC payloads, validate SDP, composition timestamps, clock/sequence wrap, AVCC widths, malformed metadata/access units, atomic late joins and bounded lag. Socket tests exercise token/callback denial before any worker, channel/session/path/query binding, unsupported UDP/LB roles, revocation and optional-listener SIGTERM. Independent FFmpeg decodes H.264 and AAC for concurrent viewers sharing one worker, native authenticated private M4S/M4F pulls exposed as CDN RTSP, and RTSP input repackaged as HLS. Separate RTSP telemetry contributes to process-mode capacity while HTTP stays separately measured. No vendor component is required for these tests.

Initial real-client testing rejected decimal-zero PLAY ranges; the parser now accepts live decimal zero while rejecting seeks. The RTSP copy roundtrip exposed missing AAC key flags in FFmpeg MPEG4-GENERIC depacketization: audio packets were received but discarded by stream copy, causing nested muxer delays and startup timeout. A targeted audio-copy flag fixes the cause; speculative timestamp and interleave changes were removed. Independent two-track decode now passes. Concurrent tests also exposed generated placeholder codec headers; FLV output now waits for the declared complete track set before publishing native/RTSP metadata.

Full direction-matrix, mixed-vendor RTSP dialect/authentication, UDP/TLS/direct RTP/SRTP, source-change continuity, GPU and production-scale qualification remain open. After fresh review and one fix pass, 136 Rust tests pass (one external-source case opt-in), with fmt, warnings-denied Clippy, the web build and all eleven Chromium cases. Review regressions cover a 5 MiB H.264 access unit, a 5000-AU packed GOP, bootstrap overflow with established delivery continuing and new joins waiting for a fresh keyframe, and SETUP for a valid 251-byte nested stream name. Shared immutable AU/GOP batches replace packet-level queue publication, avoiding self-eviction before healthy subscribers run. Limits are explicit in the profile; no memory/throughput scale claim is made.

### v0.6 isolated RTSP and HTTP qualification

On 2026-10-02 HST the exact static x86_64 musl binary SHA256 `082492cefa7bed63d501e65b7cc6ebd0b7e2a3af3514563433b8382364805d0e` was installed into both existing isolated cdn4/cdn5 test units. HTTP remains on 18210; RTSP listens only on 127.0.0.1:18554. The v0.5 binary, web directory and service unit are retained for rollback. Production Flussonic remained at PID 1346636 on cdn4 and PID 211384 on cdn5, with unchanged 80/443 listeners.

Owned token-protected synthetic streams supplied the RTSP checks. Independent FFmpeg decoded 125 video and 235 audio frames over five seconds from each source and CDN output; anonymous DESCRIBE was denied. The CDN reused its authenticated private M4S pull. A separate owned source worker ingested its loopback RTSP output and produced HLS with both tracks independently decoded. The fixtures and temporary RTSP SSH forwards were removed. No production stream was used for RTSP failure injection or altered.

The same binary on both nodes and the local LB passed the separate authorized live-source regression with native LAN HLS, M4S and M4F. Protected LB redirects, clean ticket redemption/reload, anonymous segment denial and H.264/AAC decoding passed in every mode. Source/CDN M4F segment bodies remained byte-identical at their original UTC path. M4S was restored as the dedicated CDN source transport. The authorized production input/configuration is unchanged.

The staged standalone package also started with an independent system PATH, served its compiled UI assets and delivered independently decoded RTSP video/audio. Whitelist and credential audits passed; the package contains no vendor BEAM/Erlang assets or private lab secrets. These results qualify the exercised preview subset, not full migration, vendor/API parity, UDP/TLS/publish, direct RTP/SRTP, GPU or production-scale readiness.


## v0.7 RTSP unicast UDP playback

The full suite passes 161 Rust tests with zero failures and one authorized-source case opt-in, plus fmt and all-target warnings-denied Clippy. The built UI passes all 12 Chromium cases, including real persistence of the TCP/UDP RTSP selector, TCP default restoration and clearing UDP when the URL becomes HLS. Two existing cases initially timed out during concurrent compilation/UI asset replacement; both passed a targeted rerun, then a fresh complete static-preview run passed all twelve. Normal tests require no official Flussonic component.

Fresh whole-branch review reproduced wildcard socket route pinning, more stale RTCP than the bounded drain could clear, and excessive timestamp scheduling that concealed subscriber lag. Focused regressions failed first, then passed after the single fix pass. Pool sockets now stay unconnected while every send targets the negotiated control peer; a temporary route probe selects the advertised wildcard source address. Reuse/retarget must prove the queue empty within at most 65 reads per socket or reject that attempt; a failed retarget retains the previous endpoint. A schedule more than 30 seconds ahead closes the UDP session, and the ordinary connection tick checks queue eviction during pending waits. IPv6 source-port/family and actual failed-retarget destination checks pass. These bounds favor session termination and retry over silently stalled or cross-session playback.

### v0.7 isolated LAN qualification

On 2026-10-02 HST exact static x86_64 musl binary SHA256 `3539a469a5fce2096757c2f3e141fec8bdda267d8073dd1704ce96bb5f92f61a` was installed in both existing isolated test units. The v0.6 binary, web directory and unit remain available for rollback. For qualification only, control and the 32-port UDP pool used private interfaces 172.16.0.7/172.16.0.8 on TCP 18554 and UDP 42000–42031. Independent FFmpeg ran on the other LAN host: source UDP and TCP and CDN UDP/TCP each decoded 125 video and 235 audio frames over five seconds. Both native M4S and M4F authenticated private pulls supplied CDN UDP output. Anonymous DESCRIBE returned 403 before synthetic worker startup.

An owned RTSP input with `rtp:"udp"` produced HLS independently decoded as 50 video and 94 audio frames. Source UDP output increased by 531909 application bytes during this roundtrip, distinguishing it from the default TCP adapter. Final source/CDN UDP counters were 1380462/1403989 bytes. Synthetic fixtures and their workers were removed, the CDN source transport restored, and both dedicated RTSP listeners/pools returned to loopback. HTTP remains on the existing test port 18210. Production Flussonic PIDs remained 1346636 on cdn4 and 211384 on cdn5 with unchanged 80/443 listeners. No production configuration, stream or process was altered.

The staged standalone binary serves its compiled UI and independently decodes UDP and TCP playback (75 video/141 audio frames each over three seconds) with a system PATH. Credential, dependency-license and artifact-whitelist audits pass; no official Flussonic runtime/component or private lab credential is packaged. This qualifies the exercised H.264/AAC-LC preview subset. RTSPS, direct RTP/SRTP, publishing/push, Basic/Digest viewer authentication, RTSP LB redirection, full API/vendor parity, GPU and production-scale/migration qualification remain open.

The same exact binary and local LB also pass the separate authorized live-source regression with native private HLS, M4S and M4F. Each mode passes protected LB routing, clean ticket redemption/reload, anonymous segment denial and H.264/AAC decode. Source/CDN M4F bodies remain byte-identical at the original UTC path. M4S is restored as the dedicated CDN transport; the original production input/configuration stays unchanged.


## v0.8 RTSPS development checks

The pre-review suite passes 176 Rust tests (zero failed, one authorized external-source case opt-in), fmt and all-target warnings-denied Clippy. All 13 Chromium cases and the compiled UI build pass. Owned tests independently decode encrypted H.264/AAC playback and verified RTSPS input repackaged as HLS; they exercise authorization before worker startup, revocation, UDP/plaintext rejection, stalled handshake/accept deadlines, startup failure, original protocol stats and socket cleanup. Wrong-name, untrusted and expired certificates receive zero application data. A normal CA path field persists, reloads, clears and is removed when changing protocols; CI generates its own temporary CA. Exact artifact and isolated LAN qualification are recorded below after final review.

After the fresh review findings and one fix pass, 181 Rust tests pass with zero failures and one opt-in ignored. Focused RED/GREEN regressions reject unverified redirect reconnects, keep setup failure retry/fallback metadata, and prove unrelated streams/stop remain responsive during stalled handshakes. Additional cases reject malformed/oversized response framing while preserving opaque bodies and interleaved bytes. fmt and all-target Clippy remain clean. The reviewer tool interruption and root's handling are disclosed in [v0.8 decisions](v08-decisions.md).

### v0.8 exact standalone and isolated LAN qualification

On 2026-10-02 HST static x86_64 musl binary SHA256 `118602dd8254a29534719ece1bb596bbc67947b89314f9ba2e38b6b33ba9605b` passed the compiled UI and all 13 Chromium cases. A system-PATH standalone smoke decoded TCP, unicast UDP and TLS playback as 75 video and 141 audio frames each over three seconds; a separate strict Python/OpenSSL client verified its certificate/IP identity. Credential, license-graph and artifact-whitelist audits pass: no official Flussonic component, vendor asset, Erlang module, private test key/certificate or lab credential is packaged. Direct Rustls/roots dependencies promote already-resolved libraries without changing the licensed dependency graph.

The same binary runs in the existing limited source/CDN test units on cdn4/cdn5. Only for qualification, unused TLS port 18555 listened on private LAN addresses 172.16.0.7/172.16.0.8. Independent peer FFmpeg 6.1.1 decoded source TLS and CDN TLS via private M4S and M4F pulls as 125 video and 235 audio frames each over five seconds. A strict independent client verified source IP identity and anonymous DESCRIBE returned 403 before worker startup. A FlussoniX RTSPS input on cdn5 verified cdn4 against its owned private CA, reported `input_protocol:rtsps`, and produced HLS independently decoded as 50 video and 94 audio frames. This exercises actual verified remote input, not a separate certificate preflight.

Owned synthetic streams/workers were removed, source transport restored to M4S, and TLS listeners returned to loopback. The final local preview also independently decodes UDP and TLS as 75 video/141 audio frames, with strict TLS identity and anonymous-denial checks. Both hosts retain v0.7 binary/web/unit rollback copies. Production Flussonic PIDs stayed 1346636/211384 with unchanged 80/443 listeners; production streams/configuration/processes were not altered. The dedicated lab uses owned temporary certificates valid for 14 days; users supply their own certificates for the optional TLS listener, and no test key/certificate is shipped.

The final binary and local load balancer also regress all three private native HLS/M4S/M4F paths against the already authorized live test source: protected redirects, clean ticket redemption/reload, anonymous segment denial and H.264/AAC decode pass. Original M4F payload bytes and UTC segment path are preserved. These results qualify the exercised preview subset; full API/vendor parity, publishing/push, direct RTP/SRTP, GPU, performance/scale and migration readiness remain open.


## v0.9 HTTP MPEG-TS publication qualification

The final candidate passes 196 Rust tests, zero failures and one opt-in authorized-source test ignored, plus fmt, all-target warnings-denied Clippy, the compiled UI build and all 14 Chromium cases. Tests cover independent publisher policy/template inheritance, denied requests without body consumption or workers, exclusive admission, stale startup/callback fences, 64 pending callbacks, renewal metadata/denial, malformed/partial/large uploads, stalls, cancellation, reconnect and private native pulls. Copy and CPU cases independently decode both HLS variants. Normal checks require no installed Flussonic component.

Fresh whole-branch review confirmed native peer discovery exposing publisher credentials and identified a template endpoint that did not exist. Both received failing regressions before one fix pass. Discovery now allowlists playback policy, media identity and display metadata, excluding publisher password/callback, upstream inputs and raw saved configuration. Receive templates give setup guidance and inheriting stream forms display their actual publication URL. No review finding remains deferred; [implementation decisions](v09-decisions.md) record the judgments and profile limits.

### Exact standalone and isolated source/CDN checks

On 2026-10-02 HST exact static x86_64 musl binary SHA256 `87ac0c4a4be95dc4bf93202c17915a875acf79961e9cc4988f1aa417aaaa3426` passed all 14 browser cases. The staged package served its compiled assets with a system PATH and decoded TCP, UDP and TLS playback as 75 video/141 audio frames over three seconds. A separate strict client verified certificate/IP identity. Credential, Rust/npm resolved dependency graph, license and artifact-whitelist audits pass. No official Flussonic component, vendor runtime/source/asset, private lab credential or test certificate/key is shipped; independently installed FFmpeg/FFprobe remain dependencies.

That same binary runs in the existing dedicated source/CDN test services on cdn4/cdn5, HTTP 18210 and loopback RTSP/RTSPS 18554/18555 with UDP 42000–42031. Owned H.264/AAC-LC MPEG-TS was published from this development host. Incorrect password and duplicate publisher checks passed; native discovery withheld publisher secrets. Protected LB redirects and clean CDN ticket redemption passed. Source TS/fMP4 HLS and source TCP/UDP/TLS playback independently decoded at least 75 video/140 audio frames over three seconds. Both authenticated private LAN M4S and M4F pulls supplied CDN HLS and TCP/UDP/TLS playback with independently decoded video and audio. Original M4F source/CDN segment bytes and UTC path were identical.

Changing the processing configuration closed the old publisher, left the stream waiting without a restart and permitted a new publication. The dedicated source child was verified to use libx264 under the test service. Its CPU HLS/fMP4 output independently decoded 74 video/141 audio frames and RTSP 75 video/141 audio frames over three seconds. Owned streams, producer and extra publication tunnel were removed; the CDN source transport was restored to M4S. Existing v0.8 binaries/web directories are retained for rollback. Local preview and LB also run the exact candidate on their existing ports.

Initial lab harness attempts used Python’s default form content type instead of video/mp2t and multiplexed publication upload with API requests over one WAN SSH connection. The server correctly rejected the wrong content type. Quiet API roundtrip measured559ms against the unchanged750ms lookup bound; a separate owned publication tunnel allowed the complete qualification to pass. Unique fixture names avoid stale test-policy caches. Product lookup/media deadlines were not increased. These results do not qualify WAN management lookup latency or sustained performance/scale.

Production Flussonic PIDs remained1346636/211384 and 80/443 listeners were unchanged during installation; production configuration, streams and processes were not altered. This qualifies configured HTTP receiving in the exercised preview subset. Full publisher policy/session parity, HTTPS receiving listener, RTSP/SRT publication/push, direct RTP/SRTP, additional codecs, GPU hardware, full vendor/API/mixed-cluster parity and migration/scale readiness remain open.


The same exact binary and local LB also pass the separate authorized live-source regression in native private HLS, M4S and M4F modes. Protected redirects, clean ticket redemption/reload, anonymous segment denial, H.264/AAC probing and independent FFmpeg decoding pass in each mode. Original M4F segment bytes and UTC path are preserved. M4S is restored as the dedicated CDN source transport; the authorized production input/configuration is unchanged.


The first GitHub CI run stopped at a pre-existing UDP test-fixture handoff collision before media startup. A new regression failed when released fixture ranges were reused; the shared helper now issues disjoint monotonic ranges outside the default ephemeral range. All affected RTSP/UDP/shutdown tests and the fresh full 196-test suite pass. This changes test allocation only; the product binary SHA256 and deployed qualification remain unchanged.


A later CI run passed all Rust checks and exposed the new browser case reading saved state before its asynchronous PUT completed. Adding a controlled 250 ms save delay reproduced the exact failure locally. The case now awaits a successful PUT and closed editor before each persistence assertion; all 14 cases pass with that delay. No product timeout, browser retry or runtime change was introduced.

## v0.10 standalone HTTPS profile

The development branch adds a native TLS1.2/1.3 HTTP/1.1 listener and HTTPS-only startup, sharing the existing router, worker map and authorization. Exact branch checks:204 Rust passed,0failed,1opt-in ignored;15 browser cases passed, including Config listener status. fmt and all-target warnings-denied Clippy pass. Eight new real TLS tests cover trust/identity rejection, bad material/busy startup ports before workers, concurrent idle/plaintext handshakes, five-second handshake expiry, cancellation, actual callback IP versus spoofed headers, secure LB/backend redirect rules, HLS/fMP4 independent decode, M4F payload identity, M4S media and HTTPS publication callback renewal/drop.

The exact static musl binary SHA256 is `107c9c2091a127aea80ebb88740d74273894f7385ba9bbce9d8a10e50f23db3e`. The standalone artifact was checked with independent system FFmpeg, owned TLS identity/chain verification, served UI assets, unchanged dependency-license graph and credential/file-whitelist audits. HTTPS TS HLS and fMP4 HLS decoded74 video/141 audio frames over3seconds. HTTPS continuous TS decoded49/141 after initial mid-GOP joining; plaintext comparison decoded30/141. Existing RTSP TCP/UDP and RTSPS artifact checks decoded75/141 each.

This exact binary/UI was installed only in the dedicated local/source/CDN/LB previews. Source/CDN verified HTTPS HLS and fMP4 each decoded73/141 over3seconds; continuous TS checks used5seconds to observe recovery after an original B-frame GOP boundary and decoded80/235 and116/235 respectively. Only bounded initial missing-PPS/slice/reference warnings were accepted for this declared continuous-TS late-join behavior; separate plaintext checks decoded both tracks. No product timeout/retry/framing workaround was added.

HTTPS M4F signaling and segments preserved original payload hashes versus plaintext; HTTPS M4S delivered native wire. Protected viewer denial, HTTPS LB redirects, capacity-ticket removal and local HTTPS MPEG-TS publication/independent fMP4 decode passed. The publication fixture was deleted and producer reaped. Production Flussonic PIDs1346636 on cdn4 and211384 on cdn5 and listeners80/443 stayed unchanged during dedicated installation; PIDs and deployed binary hashes were rechecked after qualification. Original dedicated source/CDN private LAN transport/configuration remains in place; the owned LB public test endpoint now uses HTTPS through the owned forwarding tunnel.

The static candidate passed all15 browser cases; Config status and publication-origin inheritance additionally passed over HTTPS. The browser lab pins only its owned leaf public key, while its API requests use the owned CA; independent TLS transport/negative certificate tests remain strictly verified. Lab TLS uses owned certificates on loopback18443, local LB18444 and forwarded18224/18225; it does not establish public production-host certificate provisioning. The CA was corrected to include keyCertSign/cRLSign after Python3.13 strict validation rejected the original fixture; verification was not disabled.

GitHub CI and freshly downloaded published-asset checks remain publication gates. No expanded codec, full mixed-vendor role, hardware, sustained-scale or migration-readiness claim is made. See [HTTPS delivery](https-delivery.md) and [decisions](v010-decisions.md).

## Original subtitle track preservation stage

`flussonix_subtitle_tracks` (`preserve` / `drop`, omitted default `drop`) is a native Stream/Template extension with a friendly inherited control. The shared MPEG-TS fan-out preserves original DVB subtitle and teletext PES and semantic PMT descriptors when selected. Owned fixture tests compare exact encoded payloads, language, DVB composition/ancillary page identifiers and teletext magazine/page through copy and CPU H.264 transcoding. Both HLS variants stay AV-only and readable. PID renumbering is allowed by remuxing. Policy edits replace the worker generation; authenticated discovery carries the policy without publisher credentials.

Native H.264/HEVC M4S/M4F framing and native-to-TS bridge tests preserve GA94 bytes containing both 608 and 708 packets; these are payload-survival tests, not subtitle decoding/player qualification. The DVB fixture is an acquisition clear-page, not an OCR image-quality test. Teletext contains an independently authored subtitle header and visible text row. There is no official Flussonic component dependency.

At the original preservation stage, selectable WebVTT and CEA/teletext decoding were pending; later stages below add selected conversion. Still pending: automatic service discovery, complete subtitle API compatibility, separate native subtitle tracks and source-to-CDN regional subtitle round trips. Current AV-only HLS source pulls omit separate subtitle PIDs. GPU caption retention, SRT/RTP subtitle delivery and exact presentation semantics remain unqualified. Dropping separate tracks does not strip embedded captions.

Sparse subtitle qualification: continuously paced publications with declared but absent subtitle packets, and with only initial cues followed by silence, keep AV progressing and publish both HLS variants in copy and CPU modes. Preservation caps common tee and nested live-TS interleaving at 100 ms; this is a mux buffering budget, not an end-to-end latency promise. Four new regressions first reproduced startup timeouts before the fix and then passed.

## HLS subtitle selection and CEA-608 conversion stage

Native `flussonix_hls_subtitles` selects original TS-HLS pass-through, plain-text 608 WebVTT conversion, or HLS filtering; omitted mode preserves prior behavior. Friendly Stream/Template controls expose CC1..CC4, language and display name without JSON, distinct names/channels, inherited overrides and restoration. Separate original DVB/teletext preservation remains independent.

Independent owned H.264 registered T35 fixtures display **USA 608** at source-local 1.160..3.000 seconds, then **LIVE**, followed by silence. The independent FFmpeg decoder agrees with the first pop-on cue. Real copy and CPU workers publish TS/fMP4 selectable renditions, silent VTT segments, stable overlapping one-second display slices and generation-scoped IDs. Owned mixed DVB/teletext fixtures simultaneously retain descriptors and exact PES payloads in shared TS. Token propagation/revocation, grouped paths, MIME type, moving playlists, live AV progress, replacement and old-generation removal are tested.

Chromium/Hls.js reads external English WebVTT in TS and fMP4 HLS and observes its first cue at approximately 1.181 seconds (source/encoded video versus audio origin offset included), more than 10 decoded frames and advancing playback without fatal player errors. Separate real player cases show original embedded captions in pass-through and no caption cues in filtered TS/fMP4 playback. Targeted H.264/HEVC suppression retains exact lengths and unrelated SEI; independently decoded AV remains readable and shared TS retains its originals. Unsupported malformed filtering fails closed. Known audio-only HLS filtering is a byte-preserving no-op; unavailable caption video and private output failure cannot stop audio/video.

Native decoder tests cover all four channels, exact display/erase timing, pop-on/paint-on/roll-up, basic/special/extended characters, parity, repeated retransmissions, true field-two controls, row bounds, continuity reset, reordered presentation, timestamp wrap and memory limits. HEVC is qualified at framing/filter boundaries, not real caption player delivery. No official Flussonic or external caption executable/library is required. A release decoder probe was rejected for incorrect timing/empty output; no probe dependency entered the product.

Initial stage limits: conversion was plain-text CEA-608, with approximately one additional HLS segment of latency. At that stage708 decoding and teletext conversion, DVB bitmap OCR, source-service discovery, GPU extraction, separate native subtitle tracks, regional cluster relay, exact legacy aliases, real HEVC caption player delivery and sustained performance/scale remain pending. Filtering is specific to HLS and parses bounded delivered segments; original policy on other protocols is independent.

Final local completion checks:304 Rust tests passed,0failed,1existing external opt-in skipped;22 browser cases passed. Formatting, all-target Clippy with warnings denied and frontend build pass. Fresh review found three Important issues; each was reproduced and fixed with a fresh full green suite. Publication requires GitHub CI on the identical commit before fast-forwarding main. See [decisions and tradeoffs](hls-subtitle-decisions.md).


Post-review TS-HLS qualification adds original DVB/teletext carriage for explicit pass-through. Owned copy and CPU tests compare descriptors and exact encoded PES; a separate test drops other TS outputs while retaining raw HLS subtitles. Four paced cases cover absent subtitle packets and silence after initial cues in copy/CPU. A 24-second owned source exercises bounded raw segment retention, public 64-bit sequences, generation replacement, discontinuity history and unavailable old filenames. fMP4 raw DVB/teletext remains unsupported and the UI says so. Timestamp epoch tests cover one and two full 33-bit periods; malformed SEI/GA94 truncation closes stale display. These tests do not establish sustained production scale or a 26-hour soak.

## Native CEA-708 service conversion

Digital services1..63 now join CC1..CC4 in up to four combined selectable plain-text WebVTT renditions. Friendly format/service controls preserve template inheritance and strict typed selectors. Owned digital-only H.264/AAC fixtures qualify copy/CPU TS and fMP4 delivery, English/Spanish browser selection with embedded decoding disabled, authentication/revocation, accurate display/hide timing and quiet output. The independent pinned Shaka packet/service/window/text probe agrees on all four fixture cues.

Native framing covers H.264/HEVC and33-bit wrap. Source-clock regressions include delayed DLC/RST behind future reference frames, silent reordered video, and bounded backspace across horizontal/vertical window boundaries. Runtime timing and HLS readiness advance at the safe DTS frontier while retaining the first presentation timestamp as anchor. No official Flussonic or external caption decoder becomes a dependency.

The [qualified subset and limits](cea708-qualification.md) explicitly exclude exact styling/position/effects, alternate P16 encodings, non-LTRwhole-word wrapping, real HEVC browser output, GPU conversion, discovery and full regional migration/scale. At that stage teletext and DVB OCR were pending; the next section qualifies selected Latin teletext. The following DVB stage adds optional recognition. See [decisions and tradeoffs](cea708-decisions.md).


## Native teletext page conversion

Announced pages100..899 join selected CEA renditions in authorized TS/fMP4 WebVTT. Owned dual-page H.264/AAC copy and CPU tests check German/French Unicode, source display/update/erase times, escaped text, language isolation, silent/missing packets, original DVB/teletext payloads and token revocation. Literal bounded transport tests cover national subsets, parity/Hamming, serial/parallel pages, partial/subpage updates, PSI/PES fragmentation, PMT reassignment, ambiguity, repeated packets, source reorder/wrap and recovery. Private independent FFmpeg/libzvbi agrees on owned words and display starts; its fixed-duration export does not qualify erase ends.

Friendly page controls support mixed selectors,100..899 validation and inherited restoration without JSON. Browser qualification disables embedded-caption decoding and selects external German/French WebVTT over both HLS variants. The real fixture/publisher uses zero mux delay, frequent PCR, a 100 ms interleaving limit and packet flushing, preserving teletext PTS within the MPEG-TS demuxer's tolerance. No official component or external teletext decoder is a runtime/build/test dependency. Enhanced/non-Latin text, GPU/real HEVC player qualification and regional native subtitle relay remain pending. See [the exact profile](teletext-qualification.md) and [decisions](teletext-decisions.md).

Pre-review teletext stage local checks:354 Rust tests passed,0failed,1existing external opt-in skipped;28 browser cases passed on a fresh isolated daemon with normal FFmpeg. Formatting, all-target Clippy with warnings denied, frontend build and diff checks passed. Whole-branch review and exact-head CI gate publication.

After the fresh review, six regressions reproduced three Important subtitle-integrity issues: SEI deadline ordering, PAT/PMT program ownership and repeated-page transactions. The single fix pass passed360 Rust tests and28 browser cases, with0failures and1existing opt-in skip. No second reviewer or deferred Minor findings.

Qualification waits for both independent HLS variants to reach the quiet tail before checking silent subtitle segments. The CI-reproduced premature fMP4 check was corrected for analog, digital and teletext fixtures; text, timing, silence assertions and existing deadlines remain intact. The resulting fresh full Rust run passed360 tests.

## DVB bitmap recognition preview

Selected composition pages0..65535 use independent bounded bitmap decoding and optional Tesseract OCR, with per-service recognition models and four combined608/708/teletext/DVB rows. Friendly Stream/Template controls preserve inheritance, reject mixed or duplicate selectors and expose recognition confidence/failure without JSON inputs. Copy/CPU dual-language TS/fMP4 WebVTT, exact source timing, original payload preservation, mixed teletext isolation, grouped auth/revocation, and missing/slow/oversized OCR AV progress are covered by owned fixtures. Real browser playback selects English and German external text tracks with embedded decoding disabled.

The1.5second queue/process deadline and daemon-wide two-process cap bound lag/cost; queue pressure or confidence below60 drops affected recognition while preserving source interval ends and advancing AV. Reset/rebind cancels tokens and reaps stale children. This is a measured coding0 interlaced SDR bitmap profile, not full broadcast or migration qualification. See [qualification, deployment and limits](dvb-qualification.md) and [decisions](dvb-decisions.md).

## Native private CA and secure subtitle qualification

Configured M4FS/M4SS inputs accept the same absolute PEM trust file as RTSPS; custom roots replace public roots. Stream/Template inheritance and restart persistence are tested, with non-TLS protocols and invalid trust rejected before saving. Owned private-CA sources deliver HEVC/Layer II and two native text IDs into protected HTTPS native and TS/fMP4 WebVTT outputs. Tests compare retained M4F cue/clear bytes, early-clear timing and rendition isolation, reject unauthorized cold/cached requests and revoke disabled delivery. Wrong trust, wrong hostname/IP and expired certificates reach no source HTTP handler. Secure control/segment redirects reject plaintext and foreign origins; same-origin HTTPS redirects are allowed only without peer credentials. Browser forms expose the labeled trusted CA path, retain it between supported secure schemes and clear it on plain input.

This increment does not qualify private-CA cluster discovery/management trust, GPU subtitle conversion, bitmap/XML native text, native subtitle retention during transcoding, full vendor interoperability or production capacity. No official component is a runtime, build or test dependency.

## Cluster private CA qualification

Owned source, CDN and LB HTTPS instances use independently generated private CAs on OS-assigned unused ports. All four private transports—HLS, MPEG-TS, M4F and M4S—exercise source discovery, measured CDN telemetry, admission and viewer redirects. Tests deny cold unauthorized requests, consume tickets once, retain a clean reload URL, coalesce concurrent viewers into one source/CDN worker, protect cached segments and independently decode delivered video with FFmpeg. M4F also compares source/CDN native segment bodies over HTTPS. A separate private HTTPS endpoint with its own CA exercises the explicit media trust override.

Management identity, wrong/public-only trust and expired chains fail before source HTTP; saved trust changes invalidate old client profiles. Peer management and private HLS/TS redirects do not reach foreign endpoints. Invalid/non-HTTPS trust settings roll back without changing saved configuration, and file settings persist across restart. Friendly Cluster fields validate absolute paths, preserve independent management/media settings, restore the management endpoint default when the optional private URL is cleared, and clear trust on incompatible endpoint changes. Active private pulls carry the receiving node's CA in their generation signature.

The checks require no official Flussonic component and do not qualify production capacity, mixed vendor clusters, mutual TLS, automatic certificate rotation, secure push or GPU operation.


## Direct HTTPS input trust qualification

Owned TLS sources qualify explicit HLSS/TSHTTPS and raw HTTPS pulls without a peer key. Independent FFmpeg decoding checks H.264/AAC copy and CPU delivery, including fMP4 HLS initialization resources, extensionless HLS and an arbitrary continuous TS path. Source handlers reject any peer/management credential header. Untrusted/public-only, wrong-root, wrong-IP and expired certificates reach no source HTTP handler. Direct same-origin redirects retain the final playlist resource base, cycles stop after three requests, and foreign variant/segment/key/map or redirect targets reach no foreign socket. Failed TLS input recovers to a configured synthetic fallback without contacting the rejected source over HTTP. A selected MPEG-TS input returning an HLS body is rejected without following its media URL; explicit demuxer selection is checked with owned loopback fixtures.

Stream/Template settings persist and inherit; malformed trust/URL settings roll back. Browser editors show the normal trusted CA field across the supported secure schemes, save it without JSON, restore public trust on clearing and remove incompatible trust when the protocol becomes plain. Existing peer redirect exclusion and private-CA source/CDN/LB tests remain part of qualification. This increment does not establish cross-origin/header-authenticated HLS, automatic certificate/session rotation, all codec/container profiles, GPU operation, mixed vendor parity or production capacity.

## HEVC RTSP playback qualification

Owned HEVC Main8-bit12-picture B-frame media is independently decoded once as elementary video, then delivered through an owned paced M4S source to the real native copy worker and RTSP listeners. Video-only TCP and HEVC/AAC TCP, UDP and verified TLS tests decode at least50 video pictures over3seconds; every picture hash matches the source set, and all12 distinct source pictures appear. Audio cases decode at least100 audio frames. Audio-first metadata and nonsequential IDs preserve SDP track identity. No vendor files/processes or public endpoints are involved. Denied viewer requests start no source request or worker; later playback uses exactly one source pull and one worker. Both plaintext and TLS HEVC sessions close on revocation and release viewer ownership.

Packet regressions reconstruct byte-identical single-NAL/FU payloads across length widths1/2/4, verify presentation timestamps, final marker and sequence progression, and reject malformed access units/configuration. The initial implementation RED rejected HEVC as an unsupported RTSP codec; the final GREEN adds its bounded packetization profile. The first revocation assertion assumed orderly TLS EOF; source inspection established existing immediate session drop without close_notify, and the corrected check accepts only bounded EOF or the specific TLS UnexpectedEof, with zero remaining viewers. This is a fixture contract correction, not a transport verification bypass. Full checks and immutable review/CI remain publication gates.


## MPEG audio RTSP playback qualification

Owned independently encoded MPEG audio extends the HEVC native-copy playback lab. Nine live combinations cover Layer II and MPEG-2 MP3 over TCP, UDP and verified TLS, both audio-only TCP codecs, and fragmented MPEG-1 MP3 with HEVC over TCP. A 2.5-second delayed join exercises rolling bootstrap and the normal MP3 bit reservoir. Independent FFmpeg decoding checks expected 32/22.05 kHz stereo audio, nonconstant PCM, and all 12 independently hashed HEVC source pictures when video is present. Four additional plaintext/TLS audio-track revocation cases cover both codecs. Denied viewers start no worker/source request; successful playback uses one worker and source connection with positive RTSP egress.

Nine packet regressions check static MPA/90000 SDP without AAC fmtp, byte-identical Layer II/III frame reconstruction including fragmentation, reserved fields/offsets, sequence and timestamp progression/wrap, talkspurt markers, audio/video track identity, bounded opaque configuration, malformed/truncated/concatenated/wrong-layer frames and explicit MPEG-2.5 RTSP exclusion. Six initial tests failed on unsupported RTSP codecs before packetization was added. All fixtures use owned loopback ports and independent FFmpeg; no official Flussonic components, production listeners or public media endpoints are involved. Full Rust/browser checks, immutable review and exact-head CI remain publication gates. This is the bounded playback profile in [RTSP/RTP support](rtsp-rtp-support.md#mpeg-audio-playback-increment), not complete encoding, direction, migration or capacity qualification.


A deterministic delayed-join regression exposed an existing RTP-Info fallback using stream-start time when a keyframe had just cleared cached audio. It now maps absent audio to the current GOP clock, with AAC and both MPEG layers covered; cached video presentation timestamps remain unchanged. FFmpeg 6.1 diagnostic playback is exercised separately from the host's 7.1 checks. The initial CI audio-count failure is retained in the private qualification record; assertion failures now report actual counts and sanitized worker state without lowering media thresholds.

## Direct RTP and SRTP MPEG-TS profile

Native direct MP2T/PT33 receive/transmit adds unicast IPv4/IPv6 and explicit-interface IPv4 multicast, bounded packet reordering and pacing, compound RTCP reports, shared TS destination fan-out and process-mode uplink accounting. [RTP](direct-rtp.md) and [SRTP/SRTCP](direct-srtp.md) document the precise configuration, key-loading contract and operating limits. Streams/Templates use labeled transport fields with whole-list inheritance and clearing, without JSON input. libsrtp2 is an independent optional system runtime dependency for secure transport.

The owned direct RTP AV matrix receiver now uses explicit SDP for MP2T/PT33.
The independent SRTP protocol receiver retains its input-key options. CI exposed
a plaintext receiver startup race: FFmpeg's RTP URL header
probe closes its socket before reopening it through SDP, briefly exposing the
connected sender to ICMP errors. Network traces confirm one RTP bind with the
explicit SDP fixture; all original codec, frame-count and strict-decoding checks
remain. This is a test fixture correction, with no product transport change.
See [FFmpeg's RTP header implementation](https://github.com/FFmpeg/FFmpeg/blob/n7.1.1/libavformat/rtsp.c#L2355-L2463).


Independent FFmpeg receivers qualify H.264 and HEVC copy delivery with AAC, MPEG Layer II and MP3, plus audio-only, and internal CPU H.264/AAC transcoding through RTP and SRTP. Codec identity, decoded frame counts, successful strict decode and empty decoder error output are required. Native UDP tests exercise source/SSRC pinning, malformed frames, wrapping sequences, multicast feedback, occupied ports, sparse flush, fan-out and cancellation. Crypto tests cover confidentiality, RTP/SRTCP authentication and replay, plaintext exclusion, unsafe/missing files, malformed authenticated candidates, retained candidate rollover state and replacement by a new key-file reference. A compiled public 2.7 C-header probe confirms the narrow Rust policy ABI. No proprietary runtime component is loaded.

Owned CEA-608/708 and DVB/teletext fixtures traverse both direct transports with exact descriptor languages and original PES checks, original-track filtering, independent HLS conversion/pass-through/drop settings and strict H.264/HEVC AV decode. An independently authored RFC2250 framer feeds FFmpeg's raw RTP/SRTP protocol: the FFmpeg `rtp_mpegts` muxer's nested TS context otherwise replaces subtitle language metadata with `und` before reception. The fixture correction retains exact metadata assertions; it does not reconstruct missing upstream metadata in the product. Qualification is on fresh SRTP epochs; continued receiver sessions cross sequence rollover, while unsignalled new receivers after rollover fail closed and require coordinated source restart.

Intel GeminiLake UHD600 H.264 VAAPI independently encoded fixtures passed both transport directions and strict delivered-media decode. The separately extracted iHD test driver was confined to owned scratch files; host/vendor installations were untouched. HEVC VAAPI has no usable HEVCMain entrypoint with this host/driver in either default or low-power mode. CPU HEVC is qualified. These tests expose no new internal VAAPI encoder controls and make no hardware throughput or migration-readiness claim. Hardware fixture cases remain explicit opt-in tests; normal CI requires no GPU.

## Internal VAAPI and elementary RTP preview

Internal VAAPI profiles now supervise an independently installed FFmpeg encoder with explicit device, low-power and CQP/CBR choices, shared across delivery. On the available Intel iGPU, internal H.264 constant-quality encoding passed independent native M4S/M4F, TS/fMP4 HLS and elementary RTP decoding with the supported audio choices. GPU/CPU replacement, readiness denial and child teardown are exercised. This host has no usable HEVC encoding entrypoint; CPU HEVC passes. These small owned fixtures do not qualify sustained capacity or hardware recovery. See [VAAPI profile](vaapi.md).

Plaintext elementary RTP/static SDP adds native bounded public admission/reorder, private validated decoder relay, per-track shared native output, RTCP and authenticated actual SDP download. The independent media matrix covers H.264 and HEVC with AAC/MP2/MP3, AAC-only, distinct MP2+MP3 tracks, CPU H.264/AAC-to-HEVC/MP3 and available internal VAAPI H.264-to-MP2. Strict codec/frame/decode assertions accompany native multicast/source isolation, eight-track routing, four destinations, generation/target guards, zero-epoch clock origins, late receivers, lag and cancellation/reaping. Friendly profile/reference/download/inheritance controls have browser coverage, including mobile SDP rendering. Existing CPU, RTP/SRTP and RTSP/HEVC regressions pass locally; full exact-head CI is the publication gate. See [elementary profile and interoperability limits](elementary-rtp.md).

Elementary SRTP/DTLS negotiation, IPv6 multicast, broader subtitle combinations, WAN/loss/scale, complete API parity and mixed-vendor cluster qualification remain pending. No official Flussonic components participate in this implementation or its media tests. The test-only scoped iHD driver environment does not alter global drivers or production services.


## Shared MPEG-TS playback from elementary RTP/SRTP

The elementary media harness independently probes and strictly decodes the shared worker's MPEG-TS recording as well as the independent RTP/SRTP receiver's recording. Exact codec-track multiplicity and at least20 decoded frames per track are required on both outputs; FFmpeg must decode every stream with `-xerror` and no error output. The worker recording is not remuxed or trimmed to hide startup failures. A lagged recording subscriber fails, and independent probe/decode subprocesses have 30-second deadlines with cancellation cleanup.

The controlled qualification fault replaced only the shared worker recording with invalid owned bytes. The previous harness passed despite that corruption; the added worker decode rejected it while the RTP recording remained valid. The normal cases cover H.264/HEVC with AAC/Layer II/MP3, audio-only and separate Layer II/III tracks, plaintext/encrypted directions and CPU conversion. Internal H.264 VAAPI remains an explicitly hardware-gated case; GPU HEVC, WAN loss and sustained capacity remain unqualified. This adds automated coverage to the existing shared-worker delivery path and changes no production API or runtime dependency.

## RTSP and RTSPS push qualification

Owned independent FFmpeg recording receivers exercise all six H.264/HEVC ×
AAC/MP2/MP3 combinations, AAC+Layer II+MP3 audio-only publication, receiver
restart, healthy/unreachable destination isolation and configuration replacement.
A live encrypted SRT+RTSP case strictly decodes both receivers from one worker
and verifies owned process/session cleanup.
Exact codec checks and strict mapped-track decoding require real received
media, including every audio-only track. A longer session crosses the OPTIONS
keepalive interval. An independent FFmpeg-to-FFmpeg reference reproduces the
raw AAC stream-copy key-flag behavior; receivers use `-copyinkf`, rather than
omitting audio checks. CPU encoders are exercised; the opt-in H.264 VAAPI test
requires a qualified render device and driver environment. It passed on the
available internal iGPU with the separately qualified Intel driver environment.

A real trusted TLS bridge carries HEVC/MP3 into the independent receiver.
Untrusted roots, wrong IP identity and expired certificates forward no RTSP
data. A receiving FlussoniX node tests the configured publisher password and
rejects an administrator password. Separate retained DVB subtitles and native
M4F/M4S text fail the destination before connecting; explicit filtering of native
US/European text still delivers strictly decoded audio. Disabled and publication-waiting streams retain
the existing on-demand activation rules. Real sockets exercise cancellation
during stalled TLS and partial RTSP setup. A delayed real MPEG-TS publication
consumes ten seconds before an actual TLS ClientHello; the stalled handshake
still closes within the single thirteen-second startup window and enters retry
backoff. An early-metadata case separately preserves the shorter two-second
connection cap. Both cases verify socket cleanup and no retry after stopping.

Bridge cases cover unchanged path/query/track authority translation, matching
CSeq, redirects, ambiguous lengths, invalid status, UDP/channel substitutions,
channel collisions, pending-request/header/body/frame bounds and media before
RECORD. RTCP alone increments no RTP byte counter. Browser cases exercise
normal protocol controls, masked credentials, TLS CA validation, mixed template
inheritance, transport switching and mobile layout alongside SRT regressions.
These are owned loopback qualifications, with no production listeners or vendor
components. Full Rust/browser/fmt/clippy/build and final exact-head CI remain
publication gates; this does not qualify all recorder dialects, long-duration
synchronization or production capacity.

## Outbound RTSP receiver authentication

An independent Python authentication gateway verifies Basic and Digest using
Python's base64/hashlib implementations, then forwards unmodified interleaved
media to an independent FFmpeg recorder. CSeq mapping isolates the recorder's
sequence space from rejected authentication requests; this is a controlled
receiver profile, not a qualification of every commercial recorder. The gateway
asserts media begins only after accepted authenticated RECORD, counts received
RTP/RTCP independently (zero on every denied session), and checks exact original
aggregate/track URIs, query retention, quoted realm/opaque
escaping and monotonically increasing nonce counts before accepting control.
Basic plus all four MD5/SHA-256 normal/session Digest variants with and without
qop independently record and strictly decode H.264/AAC. Published RFC7616 MD5 and
SHA-256 response vectors provide a separate signing check.

A trusted TLS fixture authenticates the original RTSPS hostname/port and strictly
decodes HEVC/MP3. Untrusted, expired and wrong-name TLS fixtures now contain URL
credentials and still receive zero application data. A nineteen-second recording
survives an OPTIONS stale-nonce renewal without reconnecting. Wrong credentials,
unsupported or ambiguous challenges and repeated fresh stale challenges produce
no RTP and enter bounded backoff. A delayed authenticated setup still expires
inside the original startup budget; stopping while authentication is pending
closes the socket without retry. Authenticated and unsigned destinations share
one worker while a third destination rejects its different password.

The local RTSP suite passes 22 tests with one explicit GPU opt-in skip; all 78
library tests pass. The affected browser flows save and inherit masked userinfo,
retain it when switching RTSP/RTSPS, validate credential encoding and remove
unsupported userinfo when switching to SRT. Framing still rejects duplicate
CSeq/Session/Content-Length; only WWW-Authenticate may repeat. No production host,
stream, configuration or Flussonic component is used by these fixtures. The
supported outbound profile does not imply Basic/Digest viewer or incoming
publisher authentication, auth-int, SHA-512 variants, international credentials,
proxy authentication, Authentication-Info negotiation, vendor dialect parity or
migration/throughput qualification. Exact-head CI remains the publication gate.


## RTSP UDP URL alias increment

Owned loopback sources exercise `rtsp-udp://` both alone and with the redundant
`rtp:"udp"` input option. Independent FFmpeg strictly decodes video and audio
from the resulting HLS segments; the source must also report actual UDP media
egress. The canonical `rtsp://` plus `rtp:"udp"` roundtrip remains covered. The
original alias is retained in worker input statistics, and an escaped token
query reaches source authorization.

Configuration tests preserve template inheritance, restart persistence and the
original URL including credentials and query encoding. Contradictory transport
values, TLS CA configuration on the plaintext alias and direct RTP options must
fail without replacing the prior saved configuration. The local configuration
suite passes 24 cases and the RTSP suite passes 18; formatting, Clippy across all
targets and the normal standalone build pass. Browser regressions cover the
inherited masked summary, selector values, TCP scheme conversion, redundant UDP
options and clearing foreign settings when switching RTSP/RTSPS. Exact-head
full CI and review gate publication.

These fixtures do not use official Flussonic runtime components or production
streams. The alias adds the existing UDP pull profile; it does not qualify UDP
publication/push, receiving Basic/Digest negotiation, all codecs over this alias,
real camera dialects, loss recovery or migration-scale load.

## RTSP2 camera input increment

The owned Python RTSP/1.0 camera uses independently FFmpeg-encoded G.711 A-law
and µ-law samples and a small RTP sender separate from application code. A
canonical RTSP input with explicit AAC first validates the oracle. Default
RTSP2 input must produce independently probed and strictly decoded AAC HLS
on both TCP and UDP, preserving encoded query authorization and camera Basic
credentials. Source proof records actual transport, emitted RTP bytes and wire
version. Repeated pulls reuse one worker and one source connection. Explicit
MP3 and MPEG Layer II profiles must decode to their requested codecs, and the
existing video/audio roundtrip checks copied video with encoded camera audio.
Owned workers and camera children are stopped and reaped even on assertion
failures. No official server, library, production stream or remote CDN is used.

Configuration checks cover template inheritance, restart persistence, invalid
transport, plaintext TLS and direct RTP options without replacing saved state.
The browser verifies masked inherited TCP summaries, AAC/96 kb/s defaults with
copied video, absence of unwanted saved transcoder overrides, explicit MP3/copy
selection, alias retention on transport changes and removing foreign TLS
settings. Full exact-head CI and fresh review gate publication. These checks
qualify the stated camera profile, not receiving viewer Basic/Digest policy,
selective G.711-only conversion, all camera dialects or migration capacity.

## Outbound unicast UDP RTSP push qualification

Owned independent FFmpeg UDP RECORD receivers strictly decode all six
H.264/HEVC × AAC/MP2/MP3 combinations from the shared native packetizer and
worker. Independent Python Basic and SHA-256 Digest control gateways authenticate
ANNOUNCE, each SETUP and RECORD while the independent UDP recorder decodes both
tracks. Credentials remain absent from diagnostics. Existing full TCP/RTSPS push
and authentication regressions pass alongside these new cases.

Socket qualification checks exact local source ports, admitted receiver feedback,
foreign feedback filtering, real pre-RECORD queue draining and both local ports
being reusable after release. A controlled RTSP receiver substitutes a foreign
source, invalid port pair, duplicate server ports, missing ports or colliding
track destinations. Each attempt closes before RECORD, emits zero RTP progress
and returns every allocated local pair. The native receiving-node test exercises
both TCP and UDP: the management password is denied, the publisher password
starts one worker, and stopping UDP push returns every receiver lease.

The local suites pass 84 library, 27 push, 13 UDP playback and seven RTSPS tests;
one push hardware test remains explicitly opt-in. Browser qualification verifies
friendly UDP selection, persisted template settings, TCP default restoration and
removal of UDP settings when choosing secure RTSPS. Production code passed the
new configuration and independent media RED/GREEN cases. The UI RED first caught
the API exclusion, then—after correcting a template-table locator—caught the
missing selector against the previous UI; the new UI passes the same flow.
All fixtures use owned loopback ports and independent components. Full final-head
CI remains the release gate. See [transport bounds and remaining interoperability
limits](rtsp-push.md#unicast-udp-push).

The first read-only review found continuous-feedback priority and missing
process-uplink accounting; that candidate CI was canceled before release. A
behavior-preserving extraction of the existing service turn made its priority
testable. A continuously replenished, bounded 16-frame feedback queue reproduced
media starvation without queue overflow; ready-periodic-control and malformed-RR
cases also failed before correction. Feedback service now yields after eight
frames and due reports/OPTIONS retain priority. An actual UDP push/management
node regression first observed zero process egress, then requires positive RTSP
and total process uplink, strictly decodes the receiver and checks lifetime bytes
remain monotonic after worker stop. Successful RTP and RTCP writes feed the node
counter without counting received feedback. Final-head checks supersede the
earlier candidate, and the preview remains protected until they pass.

A later authentication regression exposed a fixture readiness race: its port
file became visible before its number was written, causing an empty-integer
parse before any authentication request. The independent gateway now publishes
that readiness file with an atomic rename. The failed run and its diagnostic
are retained; successful fixture and full push reruns are required before release.

## RTSP multitrack playback qualification

Actual socket regressions send valid receiver reports on each of eight tracks
and queue invalid reports on seven tracks before a valid report on the eighth.
Both reproduce the old two-socket receive limit before correction. The rotating
receiver must service all tracks while preserving source endpoint, media SSRC,
malformed-report and oversized-datagram checks. No additional receive task is
created per track.

Owned M4S fixtures retain nonsequential IDs and place audio before video in
metadata. Independently encoded source tones differ per audio track. FFmpeg
explicitly maps every output audio stream; strict error-free decoded recordings
check per-stream frame counts, sample rates and tone-energy signatures against separately
decoded source tracks, detecting swapped or duplicated audio. A half-second
mono 48 kHz PCM window must retain at least 80% energy at its expected tone and
match each reference tone energy within 5 percentage points. Profiles
cover HEVC/AAC/MP2/MP3 over TCP, UDP and verified RTSPS, eight MP2 tracks over UDP,
and HEVC with seven AAC tracks over UDP. Denied viewers start neither workers
nor source requests; allowed sequential viewers reuse one native source pull.
Every UDP lease is reacquired after recording. A selected third audio track
checks custom interleaved channels, one-entry RTP-Info, revocation and viewer
ownership cleanup.

The initial generalized test fixture needed a local array binding to outlive an
async call; its compiler diagnostic is retained. The source-audio oracle also
needed FFmpeg’s `mp3` input demuxer for Layer II. A direct late-start decode of
the same saved AAC source reproduced different PCM hashes without this server,
so audio identity uses phase-independent tone energy rather than exact PCM
frame hashes. Failure logs and the owned source experiment are retained. All media fixtures and
processes are owned; no vendor components or production/demo servers are used.
Full final-head CI and read-only review gate publication. Short recordings do
not establish sustained capacity, long-duration sync or all recorder dialects.

## RTSP authorization callback routing

The owned real-socket callback test first reproduces RTSP 403 instead of a
backend-selected redirect. After correction, ten cases check exact original
Location/CSeq, decision caching without worker/viewer ownership, template policy,
token denial, TLS downgrade rejection including cached plaintext decisions and
URI aliases, invalid/missing/oversized destinations, direct self-redirects,
configuration races, pending/cached revocation and unchanged HTTP behavior.
An independent FFmpeg client follows a redirect to a separately token-protected
node and strictly decodes mapped video/audio. That destination rejects the wrong
token before any worker startup. The redirecting node retains no media worker.

The first pending-revocation fixture incorrectly looked up an inactive entry in
the active session collection. It now uses the actual session UUID sent to the
callback; its failure log is retained. Production session visibility is unchanged.
Verified private-CA TLS clients qualify secure response framing and downgrade
guards; secure multi-hop decoding, native adaptive RTSP LB routing and real
vendor/client dialect parity remain separate gates. See [full scope](rtsp-auth-redirects.md).

Final review corrections are qualified by failing regressions before fixes:
raw URI-unsafe characters were previously emitted in Location, and token-free
session keys allowed a warmed RTSP redirect to bypass builtin credentials or an
invalid-first HTTP request to poison the shared cache. The corrected policy
checks every request's builtin token before cache lookup, retaining refresh
checks; raw RTSP destinations require RFC3986 characters and valid percent
escapes. Eleven routing tests pass, including the shared HTTP grant regression.
The earlier candidate CI was deliberately cancelled after review found these
issues; release requires a full successful run of the revised immutable head.

A final minor review case verifies that empty raw userinfo markers (`@` and
`:@`) are rejected even when URL parsing discards their empty credential
fields. An encoded-safe destination containing `@` in query data remains
byte-for-byte intact. The regression failed before the raw-authority check
and all eleven routing cases pass after it.

## Native RTSP cluster routing

Owned loopback source/CDN/LB tests qualify initial authorization before networking, exact query and secret separation, non-destructive protocol/token/stream/transport ticket mismatch, replay and expiry, shared-cache concurrent reservations and CDN capacity rechecking. They cover malformed/stale/drained/saturated/incompatible/self endpoints, cache invalidation and failed refresh, bounded pools, alternate admission, and configuration/revocation during pending placement. Callback redirects take precedence; internal tickets stay out of callback query data. Verified private-CA clients check actual TLS routing, plain URI aliases and no downgrade. Independent FFmpeg strictly decodes mapped video/audio from two LB requests, verifying one CDN worker and one private M4S pull. All fixtures are owned and stopped after tests.

A failed expiry regression exposed allowed control decisions lingering in playback occupancy. Control admission now retains decision caching without playback linger; valid media admission promotes its grant. A separate unit test checks live ownership and promotion. Friendly peer URL validation/save/edit/clear has browser coverage. Full final-head CI and independent review gate release. Two-second decodes do not establish sustained capacity; at this stage secure multi-hop FFmpeg, mixed-vendor dialects and dynamic media cost reservation were pending. The following verified-input stage qualifies configured native TLS workers. See [full profile](rtsp-cluster-routing.md).

Ticket-bearing HTTP requests also use control authorization: rejected cross-protocol tickets and valid cleanup redirects leave no phantom HTTP playback slot. The clean media request retains normal authorization and playback accounting. A failing cross-protocol occupancy assertion qualifies this correction.

A deterministic concurrency regression yields after constructing the live grant and before returning it to the caller. The initial implementation admitted sixteen distinct control viewers under a one-viewer limit. Atomic admission now constructs the live grant under the same lock as capacity checks; that grant owns capacity through handoff and cancellation. Pending policy checks retain their cache entry without owning capacity. The revised regression requires exactly one allowance. Warmed-cache regressions also verify global and callback user limits, unique-user revocation and independent playback-grace timestamps. A full 64-peer pool regression places a healthy CDN last behind 63 stalled probes and verifies successful routing within the overall deadline.

## Verified RTSPS input redirects

The worker-owned bridge verifies each native redirected destination before
constructing a local loopback redirect for FFmpeg. An owned source/CDN/LB plus
configured RTSPS relay independently decodes H.264/AAC for two HLS viewers and
reuses one verified private HTTPS M4S pull. The direct response regression failed against the old
blanket-rejection bridge; the corrected native fixture also failed against that
baseline because it produced no HLS. An initial fixture used an incorrect fMP4
playlist path; that result is excluded from the feature evidence.

Real TLS cases cover identity/trust/expiry before target application data, strict
destinations and framing (including Unicode whitespace rejected before networking), cross-origin credential rejection, late SDP/Session/media
redirects, exact/changing-query cycles, cancellation during handoff and unused
listener cleanup. A stalled incomplete header expires under the initial routing
deadline, while established interleaved media continues beyond it. All fixture
workers and subprocesses are owned and stopped. Whole-head CI and independent
review gate release. See the [exact scope and limits](secure-rtsp-redirects.md).

### Same-origin credentialed RTSPS redirects

Configured username/password now follows only the original normalized hostname
and effective port. Real owned TLS fixtures check escaped userinfo preservation,
target-only path/query, no connection to changed hosts or ports, credential-free
cycle identity and replacement trust/expiry/identity rejection before handoff.
Hostname case and default port 322 share the same authority and cycle identity;
DNS aliases and changed ports do not. A changing-query credentialed chain retains
the configured userinfo through four handoffs, closes the old TLS sockets and
rejects a fifth redirect before another connection.
Independent Basic and Digest MD5/qop-auth camera fixtures require valid initial
authentication before redirect, then authenticate the final URI and decode both
H.264 video and AAC audio twice. Digest rotates its nonce with `stale=true` only
when the cached nonce has a valid password response. Wrong passwords start no
source. All workers and subprocesses are stopped after each case. The original
blanket rejection fails the direct handoff and valid-authentication media cases;
an earlier fixture with incomplete nonce-renewal semantics is excluded from the
product regression evidence. General camera/realm/algorithm interoperability and
cross-origin credential forwarding remain outside this qualification.


## Incoming RTSP publisher header authentication

Owned socket regressions admit Basic credentials without a URL password and
retain stream/Session binding before RECORD. Protected streams issue an MD5
Digest challenge and accept the correct retry on the same connection; another
connection cannot reuse that nonce. An independently installed FFmpeg publisher
strictly decodes H.264/AAC through plain TCP, unicast UDP and an owned verifying
TLS relay into the common shared worker. This is the legacy MD5/no-qop receiving
profile, with Basic accepted preemptively.

Adversarial cases reject management/peer/wrong passwords, malformed Basic/Digest,
method/URI/realm substitution, another algorithm or qop, duplicate parameters,
trailing commas and simultaneous query/header passwords. Inherited template
password edits invalidate the old challenge response; the current password can
still complete that negotiation. Existing callbacks deny otherwise valid
passwords, renew the same session and omit header credentials from metadata.
Three401 challenges close failed negotiation; cancellation and a retry midway
through the absolute30-second deadline release the connection without a worker.
The first socket REDs rejected Basic with403 and omitted the Digest challenge;
the challenge-limit RED exposed a fourth-reply off-by-one before correction.

The new targeted socket/media run passes12 tests; two Chromium cases verify
friendly publisher controls, inheritance, masked saved secrets, enabled listener
URLs and updated encoder guidance. Formatting, all-target warnings-denied Clippy
and the compiled UI pass. Full final-head CI and fresh whole-range review remain
publication gates. Viewer Basic/Digest, per-user accounts, additional incoming
Digest algorithms/qop, arbitrary vendor dialects and migration/scale remain open.

## Incoming publisher MD5 qop-auth extension

The receiving challenge now advertises `qop="auth"`. Real wire clients send
independently derived MD5 responses with complete cnonce/nc/qop fields; successful
admission, uppercase hexadecimal count hashing, connection nonce isolation and
rejected second ANNOUNCE are covered. Nineteen malformed/substitution cases cover
partial tuples, auth-int, zero/nonhex/short/long counts, empty/oversized cnonce,
changed cnonce/count/method/URI, duplicate fields and orphan legacy fields. No
worker starts for these pending/failed admissions. Legacy omitted-qop replies
continue to pass their existing wire regression.

The owned transparent fixture observes the independent FFmpeg publisher's
ANNOUNCE header using qop-auth, nc and cnonce without altering the request or
retaining credentials. Published H.264/AAC is strictly decoded through TCP,
unicast UDP and a verifying owned TLS relay. Before implementation the valid
qop wire response returned401, and the media fixture detected legacy authentication;
after implementation all four Digest-focused tests pass. The existing broader
publication suite remains the regression gate for Basic/query passwords,
inheritance, callback renewal, policy revocation, fixed retry/deadline bounds,
ownership and receiving codecs.

This is initial configured publication admission, with a connection/URI-bound
random server nonce consumed by the session transition. It does not authenticate
every later RTSP method separately. Legacy fallback remains enabled for client
compatibility, so qop is not enforced and MD5 is not upgraded. SHA/session
algorithms, auth-int, viewer Basic/Digest, arbitrary vendor clients and sustained
capacity remain unqualified. No vendor build/runtime dependency is introduced.
See [contract and limits](rtsp-publication.md#incoming-publisher-basic-and-digest-authentication).

## Native cluster pressure ranking

HTTP and RTSP/RTSPS share maximum normalized uplink/CPU/RAM pressure rather
than a weighted average. The fixed strict90/90/95percent ceilings remain;
ready streams receive preference only within0.05 of the best pressure, with
stable hostname ties. HTTP now uses actual `reserved_mbps` plus the existing
2Mbps viewer estimate divided by each node's own uplink capacity. Both paths
reject malformed/negative telemetry and checked count/age overflow.

Six selection regressions cover bottlenecks, readiness, ordering and hard
limits. Owned authenticated HTTP/RTSP routing fixtures choose a balanced node
under CPU/RAM or pending-egress pressure, preserving viewer credentials and
using the real CDN admission ledger. Twelve invalid/incomplete HTTP telemetry
cases fail without reservations or workers. The existing RTSP cluster suite
also checks independent decoded private-source playback, verified TLS,
admission refusal/retry, concurrent LBs, cache invalidation and ticket binding.

These are bounded functional fixtures. Production placement quality, sustained
capacity, codec/LAN cost reservation, fairness across equivalent nodes and exact
Flussonic mode parity remain unqualified. No official component is used for
building, testing or running this native profile. See
[policy and limits](native-cluster-pressure.md).

## HTTP Basic upstream and publication profile

The existing upstream URL userinfo and Publisher password configure a bounded
HTTP Basic profile. Native HTTP-family fetchers remove encoded UTF-8 credentials
from resource/decoder URLs and apply sensitive Authorization headers only to the
configured origin. Credentialed plaintext HLS/TS now use the same origin-scoped
bridge as verified HTTPS. Native M4 control and segment requests share the
credentials; header credentials cannot be combined with a native peer key.

Owned fixtures independently decode H.264/AAC for HLS/HLSS, TSHTTP/TSHTTPS,
M4S/M4SS and M4F/M4FS with encoded punctuation and UTF-8 credentials. Wrong
passwords yield no authorized source media; expired TLS fails before HTTP.
Redirect tests require successful source authentication and then zero foreign
contacts. Existing peer-key resource, private-CA and uncredentialed TLS tests
remain the regression gate.

Incoming HTTP(S) publications use effective stream/template passwords from
Basic headers, retaining legacy query credentials and `on_publish` policy.
Password-protected requests without credentials now return401 with a challenge;
wrong legacy query passwords retain403. Mixed/duplicate credentials are rejected
before body polling, callbacks and workers. Owned socket clients exercise
template inheritance and independent decoded delivery over HTTP and verified
HTTPS. See [configuration, encoding bounds and limits](http-basic.md).


## HTTP and HTTPS MPEG-TS push qualification

The [native HTTP publishing profile](http-ts-push.md) adds continuous HTTP/HTTPS POST output to the shared mixed-destination framework. `tests/http_push.rs` uses independently owned loopback HTTP/TLS receivers to check saved configuration and template inheritance, percent-encoded Basic credentials and original query delivery, actual codec probing and strict decode of all six H.264/HEVC × AAC/Layer II/MP3 pairs, isolated status retries and redirect refusal, certificate trust/identity/expiry denial before publishing, and stream-stop cleanup. Regional copy fixtures verify every authored CEA-608/708 command and DVB/teletext descriptor/PES payload, filtering of separate tracks and resulting HLS WebVTT alongside both HTTP outputs.

The nonreading-receiver unit first reproduced an HTTP client background dispatcher retaining publisher sockets after cancellation. The adapter now owns its HTTP/1.1 TCP/TLS halves directly; its regression requires no publisher socket remain before the blocked receiver resumes reading. Other socket tests check continued publishing after interim/early successful acknowledgement, bounded excessive or malformed response heads and no output producer after cancellation. Byte counters mean completed local application-body writes and contribute to HTTP transport telemetry; they are not remote decode acknowledgements. Two browser cases exercise masked normal fields, HTTPS trust, template inheritance, switching and mobile validation without JSON editors.

These tests use no official Flussonic binary, library or production endpoint. CPU and copy media profiles are qualified here; the named Intel GPU profile is recorded below. HLS/M4 push, HTTP/2, proxies, mutual TLS, all recorder dialects, WAN loss, long-duration operation and throughput/production capacity are not claimed. Exact-head full Rust/browser CI and whole-branch review remain release gates.

## HTTP/HTTPS Intel GPU publishing qualification

Owned opt-in tests exercise Intel GeminiLake UHD600/i915 with independent Intel media driver25.3.0 and GMM22.8.1 under isolated process environment. H264VAAPI CQP24, low-power off, 640x360/25fps, and AAC96/MP2 192/MP3 128kbps audio at48kHz produce independently decoded continuous HTTP and verified private-CA HTTPS uploads simultaneously from one shared worker. Running encoder arguments and mapped driver rule out a CPU fallback claim; real request captures verify Basic/path/query. Each output must decode at least50video/80audio frames with changing content and empty decoder errors. Template inheritance and liveGPU→CPU→GPU replacement retain codec identity, close uploads and reap old children. The initial byte-only fixture ended too early (40video/75audio); adding3seconds collection retains those minimum assertions. This is qualification of an existing path with no production behavior change.

Hardware cases remain explicitly ignored in ordinary CI. HEVCVAAPI encoding rejects this host profile; NVIDIA, CBR/low-power/hardware decode/Main10, GPU subtitle conversion, broader hardware and sustained capacity/WAN behavior remain unqualified. The daemon requires its own usable independent libVA setup; no host package, current preview environment or official Flussonic component is used or changed. See [exact command and measured profile](http-ts-push.md#intel-gpu-qualification).

A real unsupportedHEVC replacement test additionally requires existingH264GPU uploads to continue on the same encoder, with no CPUfallback or extra publish connection. Named kernel profile:6.17.0-41-generic.

## Compressed upstream Intel HTTP publishing qualification

Two opt-in `http_push gpu::upstream::` cases exercise independently encoded
8-bit H.264/MP3 and HEVC/Layer II inputs over owned Basic-authenticated, private-CA
verified HTTPS. Software decoding and NV12 upload feed the actual Intel H.264
VAAPI CQP24 encoder on the same GeminiLake UHD600/i915 profile above. Each input
produces AAC, Layer II and MP3 audio with simultaneous plain HTTP and verified
HTTPS publishing; twelve delivered outputs and both encoded sources undergo full
independent strict decoding. Reports retain mapped private driver/GMM paths and
hashes and selected driver variables captured from the running encoder. Worker
reuse, templates, credential isolation, exact request headers/path/query and
encoder/input/output cleanup are asserted. Wrong Basic credentials and an
untrusted CA must yield zero published media and prompt socket/encoder cleanup;
a receiver may see an empty initial POST. TLS rejection precedes upstream HTTP.
A saved-config regression reproduced an older rule incorrectly forbidding Basic
credentials on HLSS/TSHTTPS/HTTPS inputs. Validation now reuses the fetcher's
credential parser; valid credentials persist and inherit, invalid pairs and
fragments are rejected without mutation or credential disclosure. No official
vendor components, host packages or preview environment changes are needed.

This qualifies finite paced compressed sources derived from synthetic pictures
and tones, not hardware decoding or HEVC hardware encoding. Other input families,
Main10/resolution changes, reconnect/failover, broadcast defects, WAN and capacity
remain unqualified for this profile. Ordinary CI explicitly skips hardware cases;
local hardware evidence is separate. See [command and limits](http-ts-push.md#compressed-upstream-decoding).

Observed host run:2 hardware tests passed in80.26 seconds. All12 published outputs
had zero strict decoder errors, with minimum196 video and327 audio frames each.
Both denial cases sent0media and reaped their encoder; each configured receiver
saw one empty POST. Ordinary related checks passed57 tests with5 hardware cases
explicitly ignored. The saved-config regression was observed failing before the
validation fix and passing afterward.


## Native M4 input Intel HTTP publishing qualification

Opt-in `http_push gpu::native::` tests cover plaintext and verified TLS M4S/M4F
source pulls into Intel H.264 VAAPI CQP24 HTTP and verified HTTPS publishing.
H.264/MP3 and HEVC/Layer II compressed sources, each with AAC/Layer II/MP3 output,
produce24 cases and48 strictly decoded outputs on the named GeminiLake profile.
Source original/remux frame counts match; actual pipe input, encoder and mapped
system driver/GMM are checked. Basic/query credentials remain on native control
and segment requests and are distinct from publication identity. Denials start no
media encoder and send no media, though one empty initial POST is allowed while
metadata is pending. Socket/process cleanup and frozen counters are asserted.
Ordinary CI runs the fixture characterization and explicitly ignores the two
hardware cases. Native records use FlussoniX packers, so full vendor dialect
interoperability is not implied. See [commands and exact limits](http-ts-push.md#native-m4-source-decoding).


## Native input publishing recovery qualification

The native HTTP publishing recovery fixtures close an already delivering source
connection or stop the primary source listener. Plain M4S/M4F reconnect cases
use H.264/MP3; verified TLS M4S↔M4F ordered fallback cases use HEVC/Layer II.
The named Intel H.264 CQP24 encoder produces AAC96 with concurrent HTTP and
verified HTTPS publishing. Each generation's delivered outputs are independently
strictly decoded. Tests require the inactive fallback to remain unused before
failure, old source/upload closure and encoder reaping, one replacement worker
for concurrent recovery calls, correct input index/restart count, preserved
configuration and credentials, and frozen old-generation counters. An ordinary
CPU case runs the same fallback contract in CI. These tests qualify the existing
recovery entry point; no production implementation or preview settings change.
See [commands and lifecycle boundaries](http-ts-push.md#native-source-recovery).

Observed local run: the ordinary CPU case passed, and two opt-in Intel tests
passed in73.64 seconds. Four hardware recovery scenarios produced eight encoder
generations and16 independently decoded HTTP/HTTPS outputs, with zero strict
decoder errors and minima of93 video/175 audio frames. The CPU scenario added
four decoded outputs. The first local test run exposed a fixture assumption that
cancellation meant reaping had completed; the corrected test waits on the existing
worker completion state before asserting process exit. Production code is unchanged.


## Automatic daemon native publishing recovery qualification

`gpu::native::supervisor::` runs the actual daemon and its five-second background
supervisor. An on-demand stream with template-inherited HTTP/verified HTTPS
publishing reconnects or switches to its ordered alternate without a new media
request, config save, explicit recovery call or manual reconciliation. Only
read-only management stats are polled. Plain M4S/M4F reconnect carries H.264/MP3;
verified TLS M4S↔M4F fallback carries HEVC/Layer II. Intel H.264 VAAPI CQP24 and
AAC96 output use the same independently installed driver profile above.

Observed local run: two opt-in hardware tests passed in86.45 seconds. Four
scenarios produced eight encoder generations and16 independently decoded
outputs with zero strict errors and minima of93 video/175 audio frames. Source
loss to both replacement captures exceeding250kB measured6.076–6.656 seconds.
The ordinary CPU fallback case passed in20.18 seconds and resumed in7.705
seconds, adding four strictly decoded outputs. These local measurements include
supervisor scheduling, startup and collection; they are not a production SLA or
first-frame timing. A shared receiver trace requires the retired PID absent at replacement POST
admission before any body bytes and detects generation overlap across both
destinations. Old uploads cannot append more media; old/final encoders are reaped, saved config bytes remain identical,
and graceful daemon shutdown releases its port and source/output sockets.

This increment adds tests and documentation for the existing supervisor. No
runtime/UI implementation, preview settings, host packages or official vendor
components change. Automatic cluster discovery/equivalent-origin switching,
repeated faults/failback, seamless output continuity, mixed-vendor native dialects,
WAN/soak/capacity and additional GPU profiles remain unqualified here. Hardware
cases stay ignored in ordinary CI; its CPU regression covers autonomous recovery.
A temporary mutation disabling only background reconciliation failed the CPU
regression at the intended replacement-output timeout; the daemon still started
and initially published. The source was restored before normal checks.
Independent review found that an initial late PID assertion missed ordering at
replacement admission. The trace above corrects it; a live owned-PID negative
control fails at the intended admission assertion. Owned leaked encoders are
cleaned even after a successful daemon exit, and independent source closure is
awaited within a bounded interval.
See [commands and measurement boundaries](http-ts-push.md#automatic-native-source-recovery).

## Automatic native TLS cluster recovery with GPU origins

`tests/cluster_native_recovery.rs` qualifies the existing equivalent-origin
resolver with four real daemons: two sources, one CDN and one LB. Each daemon
uses an HTTPS-only OS-selected loopback listener and an independent trusted CA.
Distinct origin peer keys protect M4SS/M4FS pulls; matching content identity,
source group and viewer-token policy authorize the equivalent replica.

After initial protected LB/CDN playback, the harness gracefully stops the owned
primary source daemon and encoder. Recovery observation makes only read-only
peer-authenticated node GETs. A different CDN PID, over 250kB of new input and a
playlist from a new media generation are required before another playback
request. Origin H.264/AAC encoding remains upstream; CDN video/audio arguments
must both use copy. Anonymous playlists/segments remain denied, LB tickets are
single-use, concurrent reloads retain one CDN worker, and saved configuration
bytes remain identical. Owned daemons and encoder children are cleaned up even
after failed assertions.

Observed local run: the CPU M4SS case and Intel H.264 VAAPI CQP24/AAC96 M4SS and
M4FS cases passed in 74.10 seconds. Six protected HLS outputs were strictly
decoded with zero errors, each yielding 50 video/94 audio frames. Hardware
recovery observations were 7.795 seconds (M4SS) and 8.803 seconds (M4FS); CPU
recovery was 4.366 seconds. These are shutdown-request-to-fresh-generation
observations on synthetic 640×360/25fps,48kHz sources, excluding offline decode;
they are not first-frame timing or a production SLA. Process evidence confirms
system FFmpeg, the independently installed Intel driver and origin GPU encoding
without vendor mappings or repeat encoding at the CDN.

A temporary mutation disabling background source refresh still started and
served the primary, then failed the ordinary CPU regression at its intended
automatic-recovery timeout. Production source was restored before qualification.
The initial fixture run used an incorrect telemetry field and was corrected
against the actual exported `uplink` metric; no product code change was needed.
Hardware cases remain opt-in; ordinary CI runs the CPU contract. Full vendor
dialect interoperability, seamless output continuity, repeated faults/failback,
HEVC hardware encoding/decoding, WAN/soak and capacity remain separate gates.
See [commands and exact measurement boundaries](cluster-loadbalancing.md).

Independent review identified that disk generation alone did not bind the decoded
HTTP response to recovered media. The harness now preserves that observed
identifier, requires every delivered playlist segment to use it, and checks the
downloaded segment hash against the completed file in that generation. Replaying
the saved pre-fault playlist fails at the intended HTTP generation assertion.
An inherited SRT passphrase also reproduced an unrelated CLI startup failure;
owned HTTPS-only fixtures now clear it. CI additionally runs the CPU regression
with a harmless exported SRT setting to retain that environment-isolation check.
