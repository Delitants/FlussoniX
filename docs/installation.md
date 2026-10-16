# Install or upgrade a Linux preview

FlussoniX is a development preview. Use a test host and an unused port.

The archive contains the server, web interface, this guide and a file manifest.
It contains no configuration, credentials, media, FFmpeg or Flussonic components.
Use Linux with the archive's CPU architecture; libc and CPU requirements depend
on the supplied binary's build.

## Before starting

Install FFmpeg and FFprobe independently. CPU transcoding needs libx264 and AAC
support. GPU transcoding and optional protocols need their corresponding system
drivers or libraries; the UI reports GPU readiness.

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
access. The login page is public; management data requires authentication.

In **Media → Streams**, add `testsrc://`, save and open its player. Then check
your own input. Press **Ctrl+C** to stop the server and its media workers.

This foreground archive workflow does not install or enable a system service.

## Upgrade

1. Verify and extract the new archive into a new release directory.
2. Stop the running FlussoniX process and wait for it to exit.
3. Back up the external state directory. Keep any referenced key, certificate
   and SDP files, plus your saved credentials and previous release directory.
4. Start the new release with the same credentials, configuration path and
   media directory. Use the new release's `web` directory.
5. Sign in and check streams, templates, cluster settings and actual playback.

For a stopped instance using the state path above, a local backup can be made with:

```bash
backup="$flussonix_data.backup-$(date +%Y%m%d-%H%M%S)"
cp -a "$flussonix_data" "$backup"
```

Keep custom key/certificate/SDP files outside extracted releases. Referenced files
outside the state directory need their own backup. This is a cold upgrade:
viewers reconnect, and uninterrupted playback is not promised.

## Roll back

Stop the new process and start the previous release with its own `web` directory
and the same external state and credentials. If the old release cannot read
changed configuration, keep a copy of the current configuration, restore the
backed-up `config.json`, and start the old release again. Restoring that backup
reverts configuration edits made since the backup.

Rollback compatibility must be checked for each release pair and configuration.
This archive workflow does not qualify a distributed rolling cluster upgrade.
