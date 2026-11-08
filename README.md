# FlussoniX

FlussoniX is an independent Linux streaming server with a web admin interface, transcoding and cluster load balancing. It does not use official Flussonic components.

**Development preview:** FlussoniX is not yet ready to replace Flussonic in production. API and mixed-cluster compatibility are still being qualified. Check the [current compatibility limits](docs/compatibility.md) before planning a migration.

## What it offers

- Web forms for streams, templates, server settings and cluster nodes.
- Supported profiles for HLS, HTTP MPEG-TS, M4F/M4S, SRT, RTSP/RTSPS and RTP/SRTP, including secure transports and viewer authorization.
- CPU transcoding, supported Intel/NVIDIA GPU profiles and subtitle controls. The admin interface reports GPU readiness.
- IPTV packages and subscriber playlists.
- [FXLink](docs/fxlink.md), an optional encrypted transport between FlussoniX nodes.

Protocol directions, codecs and feature limits vary by profile; see [compatibility](docs/compatibility.md) for details.

## Try a preview

Use a test machine and an unused port. The current x86-64 release-candidate binary has been checked on Ubuntu 24.04 and requires glibc 2.39. Install FFmpeg and FFprobe separately; the standard CPU profile needs libx264 and AAC support.

[Download a Linux preview](https://github.com/Delitants/FlussoniX/releases), extract it and open a terminal in the extracted directory. Published previews may be older than the current source. Create private credentials and a data directory:

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

Save the password and peer key securely for future restarts. Start the server on a local test port:

```bash
./bin/flussonix --listen 127.0.0.1:18210 \
  --config "$flussonix_data/config.json" \
  --media-dir "$flussonix_data/media" --web-dir "$PWD/web"
```

Open [the admin interface](http://127.0.0.1:18210/admin/) and sign in. In **Media → Streams**, add `testsrc://` to check playback, then try your own input. Press **Ctrl+C** to stop the server. Use an SSH tunnel or [HTTPS](docs/https-delivery.md) for remote administration.

For verified downloads, upgrades and recovery, follow the [installation guide](docs/installation.md). To run a background service, see the [systemd guide](docs/service-installation.md).

## More information

- [Cluster and load balancing](docs/cluster-loadbalancing.md)
- [IPTV setup](docs/iptv-transport.md)
- [GPU transcoding](docs/gpu-codec-controls.md)
- [Build from source](docs/packaging.md)
