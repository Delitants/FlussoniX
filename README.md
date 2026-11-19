# FlussoniX

FlussoniX is an independent Linux streaming server with a web admin interface,
transcoding, authorization, clustering and load balancing. It does not use
official Flussonic components.

## Install

The first package release supports **Ubuntu 24.04 amd64**. Install FlussoniX,
FFmpeg, FFprobe and the required runtime libraries with:

```bash
curl -fsSL https://delitants.github.io/FlussoniX/apt/install.sh | sudo bash
```

The installer opens the setup wizard when a terminal is available. For an
unattended install, finish setup later:

```bash
sudo flussonix --init
```

The wizard creates private credentials in `/etc/flussonix`, stores server state
under `/var/lib/flussonix`, and offers to enable and start the included systemd
service at boot. Open <http://127.0.0.1:18210/admin/> after setup.

Once the repository is configured, normal updates are:

```bash
sudo apt update
sudo apt upgrade
```

See the [installation and update guide](docs/installation.md) for manual APT
setup, automation, backup, rollback, removal and portable archive use.

## Capabilities

- HLS, HTTP MPEG-TS, M4F/M4S, SRT, RTSP/RTSPS and RTP/SRTP profiles.
- CPU transcoding, supported Intel/NVIDIA GPU profiles and subtitle controls.
- Source, CDN and load-balancer roles with viewer authorization.
- IPTV packages and subscriber playlists.
- [FXLink](docs/fxlink.md), an encrypted long-haul transport between FlussoniX nodes.

Protocol direction, codec and migration limits vary by profile. Review the
[compatibility guide](docs/compatibility.md) before moving live traffic.

## Guides

- [Cluster and load balancing](docs/cluster-loadbalancing.md)
- [IPTV setup](docs/iptv-transport.md)
- [GPU transcoding](docs/gpu-codec-controls.md)
- [Advanced portable and multi-instance systemd setup](docs/service-installation.md)
- [Build from source](docs/packaging.md)
