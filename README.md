# FlussoniX

An independent Linux streaming server with a web admin interface, transcoding and cluster load balancing. Built in Rust, with a Flussonic-compatible API in progress.

**Development preview — not yet ready to replace Flussonic in production.**
FlussoniX does not require any official Flussonic components.

## What you can do

- Manage streams, templates, server settings and cluster nodes through normal web forms.
- Receive and deliver HLS, HTTP MPEG-TS, M4F/M4S, SRT, RTSP/RTSPS and RTP/SRTP using the supported profiles.
- Use HTTPS and encrypted transports, viewer tokens and authorization callbacks, and upstream/publisher Basic authentication.
- Transcode video and audio with the CPU or supported Intel/NVIDIA GPUs. The UI reports GPU readiness.
- Preserve, convert or remove subtitles; supported conversion profiles produce HLS WebVTT.
- Build a FlussoniX source/CDN cluster with LAN stream pulls and load balancing based on uplink, CPU and RAM.

Protocol, codec and API coverage is still incomplete. Mixed Flussonic clusters are under development; automatic vendor routing remains disabled. See [compatibility](docs/compatibility.md) before planning a migration.

## Try it

Use a test server and an available port. Install **FFmpeg and FFprobe** independently; the standard CPU profile needs libx264 and AAC support. Optional GPU and protocol features require their corresponding drivers or libraries.

[Download a Linux preview](https://github.com/Delitants/FlussoniX/releases). Packaged previews may be older than the current source. Extract the package, open a terminal in its directory and set your credentials:

```bash
export FLUSSONIX_ADMIN_USER=admin
read -r -s -p "Choose an admin password: " FLUSSONIX_ADMIN_PASSWORD
echo
export FLUSSONIX_ADMIN_PASSWORD
export FLUSSONIX_PEER_KEY="$(openssl rand -hex 32)"
```

Keep the peer key the same across nodes in a FlussoniX cluster. To start the downloaded package:

```bash
./bin/flussonix --listen 127.0.0.1:18210 \
  --config runtime/config.json --media-dir runtime/media --web-dir web
```

Open [the admin interface](http://127.0.0.1:18210/admin/) and sign in as `admin` with the password you chose. This address accepts connections from the same machine; use an SSH tunnel for remote access.

In **Media → Streams**, add your input URL or use `testsrc://` for a test picture. Save the stream and open its player to check playback. Settings persist in the configuration file. Press **Ctrl+C** to stop the server and its workers.

## Build from source

Install Rust through rustup (the repository selects its toolchain), Node.js 22.12+ and the media dependencies above.

```bash
git clone https://github.com/Delitants/FlussoniX.git
cd FlussoniX
cargo build --release --locked
npm ci --prefix web
npm run build --prefix web
```

Set the credentials shown above, then run:

```bash
./target/release/flussonix --listen 127.0.0.1:18210 \
  --config runtime/config.json --media-dir runtime/media --web-dir web/dist
```

## Documentation

- [Compatibility and current limitations](docs/compatibility.md)
- [Cluster and load balancing](docs/cluster-loadbalancing.md)
- [Supported native codecs](docs/native-codecs.md)
- [GPU transcoding](docs/gpu-codec-controls.md)

The first production release depends on live Flussonic migration and cluster compatibility checks, authorization/failover validation and production-scale testing. There is no confirmed release date yet.
