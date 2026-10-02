# FlussoniX

An independently written Rust media server with a React admin interface and a Flussonic v3 API compatibility layer.

**Status: first working copy, for testing. It is not a complete Flussonic replacement or migration-ready release.**

This build implements persisted Streams/Templates configuration, authenticated management, playback authorization, CPU transcoding, shared stream workers, native source/CDN discovery and an adaptive HTTP redirect balancer. M4F and M4S have independent wire adapters for the qualified H.264/AAC subset. Generic fMP4 HLS remains a separate format.

| Feature | Current implementation |
|---|---|
| Admin UI | Streams, Templates, Config and Cluster, backed by real APIs |
| API | `/streamer/api/v3` subset; CRUD, partial updates, reset, inheritance, validation without applying, collection cursors |
| Authentication | Separate edit/view credentials; Basic and legacy base64 Bearer; HTTP `on_play` callbacks; local session limits and auth-duration cache; separate peer key |
| Input | HLS/HLSS, TSHTTP/TSHTTPS, M4S AVC/AAC frame mode, M4F single-chunk AVC/AAC sample tables; FFmpeg RTSP pull and SRT receive adapters |
| Output | HLS with TS or fMP4 segments, HTTP MPEG-TS, M4S frame stream, M4F signal plus live segment window |
| Transcoding | One supervised FFmpeg worker per stream; CPU H.264/AAC; `h264_nvenc` configuration requires NVIDIA hardware and runtime |
| Native cluster | Separate public/private endpoints, source discovery, LAN pull, uplink/CPU/RAM selection, readiness, drain/stale exclusion, expiring capacity reservations |

Required later work includes complete API/schema parity; Flussonic cluster discovery and credential compatibility; M4S packed GOP and additional codec/metadata modes; publisher authentication; RTSP serving/publication/push; RTSPS, RTP/SRTP inbound and outbound; SRT output/push; HTTPS serving or reverse-proxy integration; full transcoder profiles, GPU qualification, DVR, session reauthorization/revocation and scale/failover qualification. Unsupported saved options return errors. See [qualification](docs/qualification.md) for evidence and limits.

## Build and run

Dependencies: Rust (the pinned toolchain), Node.js 22+, and an independently installed FFmpeg/FFprobe with libx264 and AAC. No Flussonic package is needed to build or run.

```bash
cargo build --release --locked
npm ci --prefix web
npm run build --prefix web
export FLUSSONIX_ADMIN_USER=admin
export FLUSSONIX_ADMIN_PASSWORD="$(openssl rand -hex 24)"
export FLUSSONIX_PEER_KEY="$(openssl rand -hex 32)"
./target/release/flussonix --listen 127.0.0.1:18210 \
  --config runtime/config.json --media-dir runtime/media --web-dir web/dist
```

Open `http://127.0.0.1:18210/admin/` and sign in with the generated management credentials. Add `testsrc://` as an owned synthetic source, or configure your authorized input URL. Use the stream detail player to test HLS. Bind the chosen test address with `--listen`; a port conflict fails startup. Stop with SIGTERM or Ctrl-C to reap workers.

The config file is created atomically on the first successful save. API credentials, role, node name, uplink capacity, client limit and drain are startup options; run `flussonix --help`. API config validation does not apply runtime changes. Native extensions live under `/flussonix/api/v1`.

```json
{
  "streams": [{"name":"owned","static":false,"inputs":[{"url":"testsrc://"}]}],
  "templates": [], "peers": [], "sources": [], "auth_backends": []
}
```

Sources and peers use `hostname`, `api_url`, `private_payload_url`, `public_payload_url`, and optionally `cluster_key` (the native peer key). Set `--role source`, `cdn`, or `lb`. The CDN's sources point to the source's API and private media endpoint. The LB's sources discover viewer policy; its peers provide CDN management and public delivery endpoints. A routing ticket reserves capacity and never replaces viewer authentication.

For a protected test stream, use an HTTP `on_play` endpoint or native `flussonix_token_sha256`. The token extension is not a Flussonic authentication algorithm. Every playlist, segment, initialization file and wire endpoint checks viewer policy before returning media. Query credentials are preserved in HLS URIs.

## Verification

```bash
cargo test --locked
cargo clippy --all-targets --locked -- -D warnings
cargo fmt --check
# Against a running test daemon, with its credentials in the environment:
npm test --prefix web
```

Tests use owned synthetic media and OS-allocated ports for the real source/CDN/LB topology. Live-source qualification is opt-in: `FLUSSONIX_M4S_URL` plus `cargo test --test external_media -- --ignored`. Keep authorized URLs/tokens in ignored local environment files. Never commit credentials, test media, vendor extracts or runtime state.

The `scripts/cargo-local` wrapper supports the workspace's isolated development toolchain and falls back to system Cargo. Deployment notes and build qualification are in [docs/qualification.md](docs/qualification.md).

The full product scope and design are recorded in [architecture](docs/architecture.md), [compatibility](docs/compatibility.md), [cluster/load balancing](docs/cluster-loadbalancing.md), [RTSP/RTP](docs/rtsp-rtp-support.md), and the [reviewed UI](docs/ui-review.md). The original supplied admin host is a read-only functionality demo, not a migration source. Reference schemas and isolated decoder checks inform interoperability; proprietary binaries/source/UI bundles are not redistributed or runtime dependencies.
