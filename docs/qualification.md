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

M4F currently supports up to two uniquely identified H.264/AAC tracks with one contiguous chunk each, 90 kHz normalization, at most 100,000 decoded samples and 32 MiB of aggregate decoded payload per segment. M4S supports MDin/FRam AVC/AAC frames, not packed GOP mode, HEVC, subtitles, SCTE/ad metadata or full legacy control messages. These modes must be qualified separately before migration.

CPU encoding uses libx264 plus AAC. NVIDIA configuration is exposed, but no GPU success is claimed without a real supported device and decoder test. FFmpeg transport adapters do not establish serving/publishing parity for RTSP or SRT. RTSPS, RTP/SRTP and push roles are not implemented.

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
