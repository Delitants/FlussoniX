# Install or upgrade a Linux preview

FlussoniX is a development preview. Use a test host and an unused port.

The archive contains the server, web interface, installation guides, an optional
service helper and a file manifest.
It contains no configuration, credentials, media, FFmpeg or Flussonic components.
Use Linux with the archive's CPU architecture; libc and CPU requirements depend
on the supplied binary's build. The current x86-64 release-candidate binary
requires glibc 2.39 and has been checked on Ubuntu 24.04. Older glibc hosts
cannot run that binary; inspect another archive's requirements separately.

## Before starting

Install FFmpeg and FFprobe independently. CPU transcoding needs libx264 and AAC
support. GPU transcoding and optional protocols need their corresponding system
drivers or libraries; the UI reports GPU readiness. Native SRT playback and
push require system libsrt 1.5 (Ubuntu 24.04: `libsrt1.5-gnutls`); direct SRTP
requires `libsrtp2-1`. Use a binary that can load those libraries on the target
host and verify the enabled listeners before serving viewers. A static musl
test build ran on Ubuntu 24.04 but could not load its glibc libsrt; a compatible
glibc build passed encrypted SRT playback on the same hosts.
Direct SRTP receivers also need an explicitly initialized owner-only key history;
see [direct SRTP setup](direct-srtp.md). The optional service helper renders a
stable history path but never initializes or resets it.
Optional DVB bitmap subtitle conversion needs the Tesseract executable and the
language models selected for the stream (for example, `tesseract-ocr-eng` on
Ubuntu).

Download the preview archive and its matching `.sha256` file into the same
directory. Set `package` to the downloaded filename without `.tar.gz`, then run:

```bash
package=flussonix-VERSION-linux-ARCH-REVISION
sha256sum -c "$package.tar.gz.sha256"
tar -xzf "$package.tar.gz"
cd "$package"
```

Keep each extracted release in its own directory. Store writable state separately:

```bash
umask 077
flussonix_data="$HOME/flussonix-data"
mkdir -p "$flussonix_data"
chmod 700 "$flussonix_data"
```

The media directory must belong to the daemon account and have mode `0700`.
The daemon creates a new media directory privately and refuses an existing
directory with broader permissions or a symlink. For an older installation,
stop it and run `chmod 700 "$flussonix_data/media"` before upgrading. HLS
delivery also rejects symlinked or foreign-owned generated files and media
directories; keep `--media-dir` under this private state path.
The daemon also needs read access to every parent directory of `--media-dir`
so it can durably sync newly created directory entries before publishing HLS.

Choose an admin password and create a cluster peer key once with
`openssl rand -hex 32`. Save both in your password manager and reuse them after
every restart. FlussoniX nodes in the same native cluster use the same peer key.
Enter the saved credentials in Bash:

```bash
export FLUSSONIX_ADMIN_USER=admin
read -r -s -p "Admin password: " FLUSSONIX_ADMIN_PASSWORD
echo
export FLUSSONIX_ADMIN_PASSWORD
read -r -s -p "Saved cluster peer key: " FLUSSONIX_PEER_KEY
echo
export FLUSSONIX_PEER_KEY
```

## Start and check playback

```bash
./bin/flussonix --listen 127.0.0.1:18210 \
  --config "$flussonix_data/config.json" \
  --media-dir "$flussonix_data/media" --web-dir "$PWD/web"
```

Choose a different port if 18210 is occupied. Open
[the admin interface](http://127.0.0.1:18210/admin/) and sign in with your saved
credentials. This listener is local to the host; use an SSH tunnel for remote
access. For a public listener, configure built-in HTTPS with `--https-listen`,
`--https-cert`, and `--https-key` for the UI and API:
remote plaintext HTTP still serves media, but denies the admin page and
management credentials. The login page is public on safe origins; management
data requires authentication. An explicit
`--allow-insecure-remote-management` override reopens remote HTTP management
for legacy API clients and prints a startup warning. The shipped UI still
refuses remote HTTP login, and API clients must use HTTPS to keep credentials
confidential.

In **Media → Streams**, add `testsrc://`, save and open its player. Then check
your own input. Press **Ctrl+C** to stop the server and its media workers.

For a separate viewer-facing LB, start a node with `--role lb` and configure
its source servers and CDN peers under **Cluster**. **Cluster → Balancers** adds
optional named HTTP playback paths such as `/lb0/channel/index.m3u8`. A
country-scoped balancer also needs a local Country or City MaxMindDB file
selected at startup with `--geoip-db /absolute/path/country.mmdb`; use an
absolute path so restarts find the same file. The UI shows whether it loaded.
Named paths accept HTTP GET playback and reject publishing POSTs. The LB
redirects eligible viewers; the selected CDN checks viewer authorization and
admission again.

For built-in IPTV, restart with `--iptv-public-origin https://your-viewer-host`
using the actual HTTP(S) origin viewers can reach. Open **Config → IPTV** to
enable it, assign streams to packages and packages to subscribers, then choose
the stream's **Built-in IPTV subscribers** viewer policy. Playlist links appear
only when the public origin is set. Subscriber tokens are stored in
`$flussonix_data/iptv.json` by default, separately from `config.json`, with
owner-only permissions. `--iptv-data /absolute/path/iptv.json` selects another
location; its parent directory must belong to the daemon user, must not be a
symlink, and must not be group or world writable. Back it up with the rest of
your state. Session limits are local on a
single standalone node. For a FlussoniX cluster, configure one shared IPTV
authority on every viewer-serving node so the limit applies across those nodes;
see [IPTV transport and authority setup](iptv-transport.md). Existing Flussonic
viewers are outside this count. The optional two-authority PostgreSQL profile
has a separate [IPTV HA setup and recovery guide](iptv-ha.md).

For an optional background service, follow [systemd installation](service-installation.md).

## Upgrade

1. Verify and extract the new archive into a new release directory.
2. Stop the running FlussoniX process and wait for it to exit.
3. Back up the external state directory, including `iptv.json` if IPTV is in
   use. Keep any referenced key, certificate and SDP files, plus your saved
   credentials and previous release directory.
4. Start the new release with the same credentials, configuration path and
   media directory and IPTV data path. Use the new release's `web` directory.
5. Sign in and check streams, templates, cluster settings, subscriber access
   and actual playback.

For a stopped instance using the state path above, a local backup can be made with:

```bash
backup="$flussonix_data.backup-$(date +%Y%m%d-%H%M%S)"
cp -a "$flussonix_data" "$backup"
```

Keep custom key/certificate/SDP files outside extracted releases. Referenced files
outside the state directory need their own backup. This is a cold upgrade:
viewers reconnect, and uninterrupted playback is not promised.

Keep `config.json.node-id` with `config.json` when backing up or moving the same
node. It is an owner-only UUID that preserves management and cluster identity
across restarts and upgrades. A new, separate node should generate its own ID.

## Roll back

Stop the new process and start the previous release with its own `web` directory
and the same external state and credentials. If the old release cannot read
changed state, keep a copy of the current state and restore the backed-up
`config.json` and, if IPTV is in use, `iptv.json` together before starting the
old release. This also reverts subscriber tokens and access edits made since
the backup.

Rollback compatibility must be checked for each release pair and configuration.
This archive workflow does not qualify a distributed rolling cluster upgrade.
