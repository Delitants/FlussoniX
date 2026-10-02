# First working copy qualification

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
- Browser tests cover stream creation/persistence, config validation without saving, Templates and Cluster views.

## Limits

M4F currently supports one contiguous chunk per H.264/AAC track, 90 kHz normalization and bounded segments. M4S supports MDin/FRam AVC/AAC frames, not packed GOP mode, HEVC, subtitles, SCTE/ad metadata or full legacy control messages. These modes must be qualified separately before migration.

CPU encoding uses libx264 plus AAC. NVIDIA configuration is exposed, but no GPU success is claimed without a real supported device and decoder test. FFmpeg transport adapters do not establish serving/publishing parity for RTSP or SRT. RTSPS, RTP/SRTP and push roles are not implemented.

HLS and M4 windows are bounded; live subscribers use bounded shared queues and disconnect on lag. There is a 256-worker implementation limit. This build has not been benchmarked for production stream/viewer counts. Metrics use Linux CPU/RAM and this daemon's actual HTTP media egress, not aggregate interface traffic from other processes. Balancing applies resource headroom and authoritative CDN reservations; it does not promise perfect uplink prediction.

API compatibility is a subset. Collection default ordering is by name; `sort=-name` and literal substring `q` are implemented, while full projection/filter/sort semantics, configuration-text formats and many operations remain open. API credentials/listeners/resource limits are startup settings rather than the complete vendor config contract. Unknown saved options fail validation. Viewer session counts use 30 seconds of inactivity; revocation and global cross-node session ownership remain open.

For testing, keep management endpoints on a trusted interface or reach them through SSH forwarding. The daemon currently serves HTTP, so HTTPS delivery requires a separately configured TLS terminator. The dedicated test instances must use separate directories, accounts, units and unused ports; never replace or restart existing Flussonic services.

## CDN test deployment

The user authorized installations on cdn4-uk.ott.pink and cdn5-uk.ott.pink. Read-only inspection verified Ubuntu 24.04 x86_64, independent `/usr/bin/ffmpeg`, unused TCP 18210, and a shared private network. Intended isolated prefix/unit: `/opt/flussonix-test` / `flussonix-test.service`. Release artifacts are built for x86_64 Linux with musl so no host libc upgrade is required.

Remote qualification results will be recorded here after installation. Production stream tokens, API passwords, peer keys, key files and source URLs are excluded from this repository.
