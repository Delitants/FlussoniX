# Install, update or remove FlussoniX

The signed APT repository supports Ubuntu 24.04 on amd64. The package installs
FlussoniX, FFmpeg, FFprobe, native SRT/SRTP libraries, the web interface and a
hardened systemd service. It does not use anything from `/opt/flussonic`.

## One-command installation

```bash
curl -fsSL https://delitants.github.io/FlussoniX/apt/install.sh | sudo bash
```

The script verifies the repository key fingerprint, configures only the
FlussoniX source, installs dependencies through APT, and starts the setup wizard
when a controlling terminal is available. An unattended run installs the
package and prints the remaining command:

```bash
sudo flussonix --init
```

The wizard checks `/usr/bin/ffmpeg`, `/usr/bin/ffprobe`, SRT/SRTP libraries,
listener availability and GPU readiness before it writes configuration. It asks
whether to enable and start `flussonix.service`; accepting the default enables
autostart.

## Manual repository setup

These commands expose each step performed by the bootstrap script. Download the
public key and installer, then compare the key fingerprint with the fingerprint
pinned in the installer:

```bash
repo=https://delitants.github.io/FlussoniX/apt
work=$(mktemp -d)
curl -fsSL "$repo/install.sh" -o "$work/install.sh"
curl -fsSL "$repo/flussonix-archive-keyring.gpg" -o "$work/archive-key.gpg"
expected_fingerprint=$(sed -n 's/^EXPECTED_FINGERPRINT=//p' "$work/install.sh")
actual_fingerprint=$(gpg --show-keys --with-colons --fingerprint \
  "$work/archive-key.gpg" | awk -F: '$1 == "fpr" { print toupper($10); exit }')
test -n "$expected_fingerprint"
test "$actual_fingerprint" = "$expected_fingerprint"
```

Install the scoped keyring and deb822 source:

```bash
gpg --batch --yes --dearmor --output "$work/flussonix-archive-keyring.gpg" \
  "$work/archive-key.gpg"
sudo install -m 0644 "$work/flussonix-archive-keyring.gpg" \
  /usr/share/keyrings/flussonix-archive-keyring.gpg
cat > "$work/flussonix.sources" <<'EOF'
Types: deb
URIs: https://delitants.github.io/FlussoniX/apt
Suites: noble
Components: main
Architectures: amd64
Signed-By: /usr/share/keyrings/flussonix-archive-keyring.gpg
EOF
sudo install -m 0644 "$work/flussonix.sources" \
  /etc/apt/sources.list.d/flussonix.sources
sudo apt update
sudo apt install flussonix
sudo flussonix --init
```

Delete the temporary directory after installation. APT verifies `InRelease`
with the dedicated keyring; do not use `apt-key`, `trusted=yes`, or signature
check overrides.

## Files and service

The package owns the executable, web files and base unit under `/usr`. The setup
wizard creates only these fixed private paths:

- `/etc/flussonix/credentials.env` stores the admin password and cluster peer key.
- `/etc/systemd/system/flussonix.service.d/10-setup.conf` stores validated,
  non-secret listener and role settings.
- `/var/lib/flussonix` stores `config.json`, node identity, IPTV data, media and
  other mutable server state.

Run `sudo systemctl status flussonix` or `sudo journalctl -u flussonix` to inspect
the service. Plain `sudo flussonix --init` reports an existing setup without
changing it. Use `sudo flussonix --init --reconfigure` for an intentional change.

## Non-interactive initialization

Automation must supply all three secrets through the environment. They are not
accepted as command-line values:

```bash
export FLUSSONIX_ADMIN_USER=admin
export FLUSSONIX_ADMIN_PASSWORD='use-a-secret-store-value'
export FLUSSONIX_PEER_KEY='use-the-saved-cluster-peer-key'
sudo --preserve-env=FLUSSONIX_ADMIN_USER,FLUSSONIX_ADMIN_PASSWORD,FLUSSONIX_PEER_KEY \
  flussonix --init --non-interactive \
  --role cdn --node-name cdn-01 --listen 0.0.0.0:18210
```

Use `--no-start` to write and verify the setup without enabling or starting the
service. Supply the same peer key to nodes joining one native cluster. Keep all
three variables out of logs and clear them when the automation step finishes.
For a shared subscriber catalog and session limits across two authorities, see
the [IPTV authority high-availability guide](iptv-ha.md).

## GPU transcoding

The package installs FFmpeg and FFprobe and recommends common GPU diagnostic
tools. It does not install or replace NVIDIA, Intel or kernel GPU drivers because
the correct driver depends on the host hardware and desktop stack. The setup
wizard and **Config → Transcoding** show detected devices, FFmpeg encoders and
why a GPU profile is unavailable. Install a driver separately only after checking
the hardware and the operating-system recommendation.

## Back up and update

Back up both configuration and mutable state before an upgrade. Stop the service
or use a consistent filesystem snapshot while copying them:

```bash
sudo systemctl stop flussonix
sudo tar -C / -czf "flussonix-backup-$(date +%Y%m%d-%H%M%S).tar.gz" \
  etc/flussonix var/lib/flussonix
sudo systemctl start flussonix
```

Then update normally:

```bash
sudo apt update
sudo apt upgrade
```

An active service restarts after the package upgrade. A stopped service remains
stopped, and the enabled-at-boot state is preserved. Package upgrades do not
replace `/etc/flussonix` or `/var/lib/flussonix`.

## Qualified rollback

List versions retained by the repository, then install the exact qualified
version for the release pair:

```bash
apt-cache policy flussonix
sudo apt install flussonix=VERSION-1
```

Restart and check actual playback after rollback. A package rollback is safe only
for a release pair whose notes say its state format was qualified. If the older
version cannot read changed state, stop the service and restore the matching
backup of `/etc/flussonix` and `/var/lib/flussonix` together.

## Remove or purge

```bash
sudo apt remove flussonix
```

`apt remove` stops and disables the service, removes package-owned files, and
preserves generated configuration and all mutable state.

```bash
sudo apt purge flussonix
```

`apt purge` removes only the wizard's known generated credential and drop-in
files. It preserves `/var/lib/flussonix` and leaves administrator-created files
under `/etc/flussonix` alone. Delete retained state only after a separate backup
and an explicit review.

## Install a downloaded package

For a local `.deb`, keep its `.sha256` and `.inspection.json` beside it, verify
the checksum, and let APT resolve dependencies:

```bash
sha256sum --check flussonix_VERSION-1_amd64.deb.sha256
sudo apt install ./flussonix_VERSION-1_amd64.deb
sudo flussonix --init
```

A fully disconnected host also needs an Ubuntu package cache containing every
dependency. The FlussoniX package does not bundle system libraries or drivers.

## Release installation qualification

Release maintainers can verify the complete APT lifecycle in disposable Ubuntu
24.04 machines before publication. The repository must contain both the prior
qualified package and the release candidate:

```bash
sudo python3 scripts/qualify-apt-install.py \
  --repository /absolute/path/to/signed-apt-repository \
  --current-version 0.11.0-1 \
  --rootfs-dir /var/tmp/flussonix-apt-qualification
```

The gate checks repository signature rejection, dependency installation,
interactive-default and `--no-start` setup, reboot, remove and purge. It also
upgrades and rolls back active/enabled, active/disabled, inactive/enabled and
inactive/disabled service states. Each boot uses a private network namespace;
the host service is never enabled, disabled, started, stopped or restarted.

Machines and their root filesystems are removed after success, failure or
interruption. The owner-only result JSON remains beside the requested rootfs
path. `--keep-on-failure` retains the root filesystem for diagnostics while
still terminating every machine; remove the retained directory after review.

## Advanced portable archive

The release archive remains available for portable tests and custom layouts.
It does not install dependencies, register a service or provide automatic APT
updates. Verify its adjacent checksum before extraction:

```bash
sha256sum --check flussonix-*-linux-x86_64-*.tar.gz.sha256
tar -xzf flussonix-*-linux-x86_64-*.tar.gz
```

Run it with external owner-only credentials and state. For a reviewed portable
systemd unit or multiple isolated instances, use the
[advanced service-helper guide](service-installation.md). The APT package does
not need that helper.
