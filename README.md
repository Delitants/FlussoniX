# FlussoniX

An independent Linux streaming server with a web admin interface, transcoding and cluster load balancing. Built in Rust, with a Flussonic-compatible API in progress.

**Development preview — not yet ready to replace Flussonic in production.**
FlussoniX does not require any official Flussonic components.

## What you can do

- Manage streams, templates, server settings and cluster nodes through normal web forms.
- Receive and deliver HLS, HTTP MPEG-TS, M4F/M4S, SRT, RTSP/RTSPS and RTP/SRTP using the supported profiles.
- Use HTTPS and encrypted transports, viewer tokens and authorization callbacks, and upstream/publisher Basic authentication.
- Manage IPTV packages and subscribers, issue token-protected playlists and revoke viewers when access changes.
- Transcode video and audio with the CPU or supported Intel/NVIDIA GPUs. The UI reports GPU readiness.
- Preserve, convert or remove subtitles; supported conversion profiles produce HLS WebVTT.
- Build a FlussoniX source/CDN cluster with LAN stream pulls and load balancing based on uplink, CPU and RAM.
- Try [FXLink](docs/fxlink.md), an optional encrypted node-to-node transport. Two-host LAN playback has been checked; WAN loss and production capacity still need testing.

Protocol, codec and API coverage is still incomplete. Mixed Flussonic clusters are under development; [mapped CDN routing](docs/vendor-cdn-placement.md) includes HTTP/RTSP redirects and HTTPS SRT lookup for compatible clients. Explicit Flussonic HTTP source profiles support playback and [equivalent-origin failover](docs/vendor-daemon-failover.md). See [compatibility](docs/compatibility.md) before planning a migration.

## Try it

Use a test server and an available port. Install **FFmpeg and FFprobe** independently; the standard CPU profile needs libx264 and AAC support. Optional GPU and protocol features require their corresponding drivers or libraries.

[Download a Linux preview](https://github.com/Delitants/FlussoniX/releases). Packaged previews may be older than the current source. Extract the package, open a terminal in its directory and set your credentials:

```bash
umask 077
export FLUSSONIX_ADMIN_USER=admin
read -r -s -p "Choose an admin password: " FLUSSONIX_ADMIN_PASSWORD
echo
export FLUSSONIX_ADMIN_PASSWORD
export FLUSSONIX_PEER_KEY="$(openssl rand -hex 32)"
flussonix_data="$HOME/flussonix-data"
mkdir -p "$flussonix_data"
chmod 700 "$flussonix_data"
```

Save your credentials and generated peer key securely. Reuse them after restarts;
FlussoniX nodes in the same cluster use the same peer key. To start the downloaded package:

```bash
./bin/flussonix --listen 127.0.0.1:18210 \
  --config "$flussonix_data/config.json" \
  --media-dir "$flussonix_data/media" --web-dir "$PWD/web"
```

For a verified download and persistent state across upgrades, follow the
[installation and rollback guide](docs/installation.md). Keep your credentials
and peer key for future restarts. For background operation, see the optional
[systemd service guide](docs/service-installation.md).

Open [the admin interface](http://127.0.0.1:18210/admin/) and sign in as `admin` with the password you chose. Use an SSH tunnel or [HTTPS](docs/https-delivery.md) for remote administration; remote HTTP remains available for media, not ordinary management.

In **Media → Streams**, add your input URL or use `testsrc://` for a test picture. Save the stream and open its player to check playback. Settings persist in the configuration file. Press **Ctrl+C** to stop the server and its workers.

For subscriber playlists, set a public HTTP(S) origin at startup, then use **Config → IPTV** to create packages and subscribers. Their tokens live in a separate owner-only `iptv.json` beside `config.json`. Session limits count viewers on the local node; to count viewers across FlussoniX nodes, configure each viewer-serving node to use the same IPTV authority. Viewers on existing Flussonic nodes are not counted. See [IPTV transport support](docs/iptv-transport.md).

## Build from source

Install Rust through rustup (the repository selects its toolchain), Node.js 22.12+ and the media dependencies above.

```bash
git clone https://github.com/Delitants/FlussoniX.git
cd FlussoniX
cargo build --release --locked
npm ci --prefix web
npm run build --prefix web
```

Set the credentials and external state directory shown above, then run:

```bash
./target/release/flussonix --listen 127.0.0.1:18210 \
  --config "$flussonix_data/config.json" \
  --media-dir "$flussonix_data/media" --web-dir "$PWD/web/dist"
```

## Documentation

- [Compatibility and current limitations](docs/compatibility.md)
- [Cluster and load balancing](docs/cluster-loadbalancing.md)
- [Supported native codecs](docs/native-codecs.md)
- [GPU transcoding](docs/gpu-codec-controls.md)
- [SRT input options](docs/srt-input.md)
- [Build a preview archive](docs/packaging.md)

The first production release depends on live Flussonic migration and cluster compatibility checks, authorization/failover validation and production-scale testing. There is no confirmed release date yet.
