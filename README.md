# FlussoniX

An independently written Rust media server with a React admin interface and a Flussonic v3 API compatibility layer.

**Status: v0.9 preview, for testing. It is not a complete Flussonic replacement or migration-ready release.**

This build implements persisted Streams/Templates configuration, authenticated management, playback authorization, CPU transcoding, shared stream workers, native source/CDN discovery and an adaptive HTTP redirect balancer. M4F and M4S have independent wire adapters for the qualified H.264/AAC subset. Generic fMP4 HLS remains a separate format.

| Feature | Current implementation |
|---|---|
| Admin UI | Labeled forms for Streams, Templates, Config and Cluster; staged validation/apply; no JSON input required |
| API | `/streamer/api/v3` subset; CRUD, partial updates, reset, inheritance, validation without applying, collection cursors |
| Authentication | Separate edit/view credentials; Basic and legacy base64 Bearer; structured/string `on_play` callbacks; scheduled viewer renewal, revocation and local limits across streams; publication password and POST on_publish renewal; separate peer key |
| Input | HLS/HLSS, TSHTTP/TSHTTPS, M4S AVC/AAC frame and packed-GOP modes, M4F single-chunk AVC/AAC sample tables; HTTP MPEG-TS publication to publish:// streams; RTSP pull, verified RTSPS TCP pull and SRT receive adapters |
| Output | HLS with TS or fMP4 segments, HTTP MPEG-TS, Original M4S frame/GOP relay, generated frame output; M4F signals with original or generated live segments; optional RTSP 1.0 TCP-interleaved or opt-in unicast UDP H.264/AAC-LC playback; optional RTSPS TLS TCP playback |
| Recovery | Startup/media watchdog, capped retries through ordered inputs, background local/CDN recovery, new HLS sequences/segment/init identities after restart |
| Transcoding | One supervised FFmpeg worker per stream; CPU H.264/AAC; `h264_nvenc` configuration requires NVIDIA hardware and runtime |
| Native cluster | Separate public/private endpoints, source discovery, explicit equivalent-origin failover, LAN pull, uplink/CPU/RAM selection, readiness, drain/stale exclusion, expiring capacity reservations |

Required later work includes complete API/schema parity; Flussonic cluster discovery and credential compatibility; additional M4 codec/metadata modes; full publisher policy/session parity; RTSP publication/push and Basic/Digest viewer authentication; RTSPS publication/push and SRTP-protected UDP; direct RTP/SRTP inbound and outbound; SRT output/push; HTTPS serving or reverse-proxy integration; full transcoder profiles, GPU qualification, DVR, distributed session ownership and complete failure/scale qualification. Unsupported saved options return errors. See [qualification](docs/qualification.md) for evidence and limits.

## Publication preview

Choose **Receive a publication** in Streams or Templates. Set an optional **Publisher password** and **Publisher authorization** URL or named backend using normal fields. Publish MPEG-TS with HTTP POST to `http://HOST:PORT/STREAM/mpegts?password=PUBLISHER_PASSWORD&token=PUBLISHER_TOKEN`. Viewer tokens and peer/management credentials remain separate. Empty publication streams wait without starting FFmpeg; one connected publisher owns the worker, and disconnects or changed policy stop it. Reconnects are immediate.

The callback receives documented publication JSON metadata with a stable session UUID, request counter, protocol, socket IP, original query and received bytes. Only HTTP 200 allows; redirects, failures and failed renewals deny. Renewal honors `X-AuthDuration` (1–3600 seconds, default30). This static HTTP H.264/AAC-LC receiving profile feeds HLS, fMP4 HLS, MPEG-TS, native M4F/M4S and the existing RTSP packetizer; CPU transcoding is exercised. HTTPS receiving needs a separately configured TLS terminator. Full publisher policy objects, user/global limits, dynamic prefix publications and other publishing protocols remain pending. See [publication profile and limits](docs/publishing.md).

## RTSPS preview

Enable encrypted playback on an unused port using `--rtsps-listen 127.0.0.1:18555 --rtsps-cert /etc/flussonix/server-chain.pem --rtsps-key /etc/flussonix/server.key`. Certificates and keys must be readable by the service account. Certificate/key parsing and matching complete before any worker starts. Playback uses `rtsps://HOST:18555/STREAM?token=VIEWER_TOKEN` with the existing stream token/callback policy. TLS protects both control and TCP-interleaved RTP/RTCP; UDP SETUP is rejected. The plaintext listener stays separate and optional.

For input, enter an `rtsps://` URL in the normal form. Set **Trusted CA file** to an absolute server-side PEM path for a private CA, or leave it empty for bundled public CAs. Certificate chain, validity and the original DNS/IP identity are always verified before any application data. Its API extension is per-input `flussonix_tls_ca`. The worker uses an owned loopback bridge after verification; no unverified FFmpeg reconnect or plaintext fallback occurs. Upstream redirects are rejected, and failed setup participates in the normal ordered input fallback. The default RTSPS source port is 322. See [the exact profile and remaining directions](docs/rtsp-rtp-support.md).

## RTSP playback preview

Enable a separate unused listener with `--rtsp-listen 127.0.0.1:18554`. Its default is disabled. A configured stream is available at `rtsp://127.0.0.1:18554/STREAM?token=VIEWER_TOKEN`. For an owned synthetic test without a token policy, use `ffmpeg -rtsp_transport tcp -i rtsp://127.0.0.1:18554/owned -t 5 -f null -`.

To enable UDP on unused ports, add `--rtsp-udp-ports 42000-42031 --rtsp-udp-mbps 100`. All ports bind to the RTSP listener address before workers start. The inclusive range must have 2–256 ports, an even first port, an odd last port, and no port below 1024; each negotiated track leases two ports. `--rtsp-udp-mbps` caps application payload per viewer (1–10000 Mbps, default 100) with a 32 KiB burst. Select `-rtsp_transport udp` in an independent FFmpeg client. The control connection remains TCP; datagrams go only to its peer IP. UDP is disabled without the range. The normal **RTSP transport** field selects TCP or UDP for RTSP inputs; UDP persists as `rtp:"udp"`, and TCP uses the default absent option.

This profile serves RTSP 1.0 playback over TCP interleaving or opt-in unicast UDP, with H.264 single NAL/FU-A, AAC-LC MPEG4-GENERIC and RTCP sender reports. It reuses one shared worker and packetizer, URL-token/on_play authorization (`proto=rtsp`), revocation and local admission accounting. Native CDNs can expose RTSP while pulling authenticated LAN HLS/M4S/M4F; the HTTP balancer does not redirect RTSP. Independent FFmpeg clients decode both tracks, and RTSP pull to HLS is exercised. Unsupported codecs return 415; worker or codec replacement closes the session and requires reconnecting. See [tested profile and bounds](docs/rtsp-rtp-support.md#implemented-v07-udp-playback-profile). This does not establish every vendor RTSP dialect or the remaining direction matrix.

## Replica failover

Set the same optional `flussonix_source_group` on equivalent source relationships and `flussonix_content_id` on their streams or templates. The CDN only switches when content identity and normalized viewer policy match. A disabled, deleted or invalid known origin fails closed. A healthy fallback stays selected; media can reconnect during the switch. These are native extensions, editable through friendly UI fields. See [cluster behavior and limits](docs/cluster-loadbalancing.md#implemented-v05-equivalent-origin-failover).

## Preview package

The GitHub prerelease includes an x86_64 Linux static binary, the compiled admin UI and an owned synthetic example configuration. FFmpeg/FFprobe remain host dependencies. After extracting it, set the three `FLUSSONIX_*` environment variables below, then run `./bin/flussonix --config runtime/config.json --media-dir runtime/media --web-dir web`. The default listener is 127.0.0.1:18210.

## Build and run

Dependencies: Rust (the pinned toolchain), Node.js 22+, and an independently installed FFmpeg/FFprobe with libx264 and AAC. No Flussonic package is needed to build, test or run. Its installed binaries, libraries, BEAM modules and web assets are never loaded by the daemon. Optional reverse-engineering tools are reference research only; the normal test suite uses independently generated media and FFmpeg. CI explicitly verifies /opt/flussonic is absent.

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

The admin uses labeled fields, ordered input rows and explicit template inheritance choices. Config stages changes locally: Validate leaves saved state unchanged; Save & apply persists the staged configuration. Node keys and token hashes are masked.

Stream/template `flussonix_input_timeout` sets the media stall timeout in seconds (1–300, default 15). It inherits through templates. Static streams retry continuously; on-demand streams recover within 60 seconds of actual demand, and active continuous bodies retain demand. Retries wait 1/2/4/8/16/30 seconds, capped at 30. Failed TS/M4 bodies close and need reconnecting; HLS replacements have distinct media/init names and a discontinuity. This is configured-input recovery, not seamless migration between unrelated origins.

The config file is created atomically on the first successful save. API credentials, role, node name, uplink capacity/interface, client limit and drain are startup options; run `flussonix --help`. API config validation does not apply runtime changes. Native extensions live under `/flussonix/api/v1`.

```json
{
  "streams": [{"name":"owned","static":false,"inputs":[{"url":"testsrc://"}]}],
  "templates": [], "peers": [], "sources": [], "auth_backends": []
}
```

Sources and peers use `hostname`, `api_url`, `private_payload_url`, `public_payload_url`, and optionally `cluster_key` (the native peer key). Set `--role source`, `cdn`, or `lb`. The CDN's sources point to the source's API and private media endpoint. Source-only `flussonix_transport` selects `hls` (existing default), `m4s` or `m4f`; HTTPS private endpoints select their secure aliases automatically. M4 input without transcoding relays original wire/segment bytes and timestamps. Source transcoding is already applied to the pulled media and is not repeated at the CDN. The LB's sources discover viewer policy; its peers provide CDN management and public delivery endpoints. A routing ticket reserves capacity and never replaces viewer authentication.

For a protected test stream, use an HTTP `on_play` endpoint or native `flussonix_token_sha256`. The token extension is not a Flussonic authentication algorithm. Every playlist, segment, initialization file and wire endpoint checks viewer policy before returning media. Query credentials are preserved in HLS URIs.

Use `--uplink-interface auto` (the default) to sample aggregate TX traffic on the Linux default-route interface, or choose the public delivery NIC by name. `--uplink-interface process` explicitly uses only this daemon’s HTTP and RTSP media bytes. `http_egress_mbps` and `rtsp_egress_mbps` remain separate; `media_egress_mbps` reports their sum. Interface `egress_mbps` remains aggregate NIC traffic. Startup, missing/reset counters and samples older than three seconds are unknown and prevent new cluster admissions. Shared LAN/public NIC traffic is aggregate traffic, not a WAN-only estimate.

`on_play` accepts a URL string or `{"url":"auth://billing","session_keys":["name","proto","ip","token"],"max_sessions":2}` with a configured `auth_backends` entry. Supported keys are literal ordered `name`, `proto`, `ip`, `token`; name/proto are required. Callback sessions use UUIDs and report actual protocol, request number/type, duration and bytes. Default renewal is 180 seconds; `X-AuthDuration` is bounded to 1–3600 seconds. A backend timeout/5xx preserves a previous decision and retries after ten seconds; a new viewer fails closed. `X-UserId`, `X-Max-Sessions`, `X-Unique` and validated HTTP(S) 302 redirects are supported. Limits apply on one node across streams, not globally across a cluster. Unsupported auth options fail validation.

`GET /streamer/api/v3/sessions` and `GET /sessions/{id}` expose local sessions without tokens/raw queries. Edit credentials permit `DELETE /sessions/{id}` (204) and `POST /sessions/reauth?name=STREAM` (`estimated_count`). Deletion cancels continuous TS/M4/RTSP bodies and caches denial for 180 seconds. The stream Auth tab uses these APIs. Metadata-only edits preserve session identity; changes to authorization policy invalidate old grants. Native peer pulls use separate credentials and do not create viewer sessions.

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

The full product scope and design are recorded in [architecture](docs/architecture.md), [compatibility](docs/compatibility.md), [cluster/load balancing](docs/cluster-loadbalancing.md), [RTSP/RTP](docs/rtsp-rtp-support.md), [secure output and HEVC/m2a/MP3 requirements](docs/secure-output-codecs.md), and the [reviewed UI](docs/ui-review.md). Standalone HTTPS delivery and the expanded codec matrix are required upcoming work; the current native M4F/M4S and RTSP output preview remains qualified for H.264/AAC. The original supplied admin host is a read-only functionality demo, not a migration source. Reference schemas and isolated decoder checks inform interoperability; proprietary binaries/source/UI bundles are not redistributed or runtime dependencies.
