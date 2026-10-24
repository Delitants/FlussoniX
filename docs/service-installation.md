# Run a preview as a Linux service

Use this optional workflow after checking the archive on a test host. It requires
systemd, Python 3.11+ and independently installed FFmpeg/FFprobe. The packaged
`bin/flussonix-service` helper creates credentials and writes a service unit;
it never starts, enables or replaces a service.

## Prepare the directories

Verify and extract the archive as described in [installation](installation.md).
Keep its files together in a versioned directory such as
`/opt/flussonix/releases/flussonix-VERSION-linux-ARCH-REVISION`. Make that release
root-owned and readable by the service account. Use absolute paths without spaces
or substitution characters. Do not deploy under `/tmp` or `/var/tmp`: the service
uses a private temporary directory.

Create a dedicated account if it does not already exist:

```bash
sudo useradd --system --user-group --home-dir /var/lib/flussonix-preview \
  --no-create-home --shell /usr/sbin/nologin flussonix
sudo install -d -o flussonix -g flussonix -m 700 /var/lib/flussonix-preview
```

For an existing instance, stop it and back up its state first. Copy its saved
configuration and referenced files into the external state directory, assigning
them to the service account with private permissions. Keep all parent directories
traversable by that account. The helper checks access and refuses unsafe state
permissions; it does not change ownership or copy data.

From the extracted release directory, enter your **saved** admin password and
cluster peer key when prompted:

```bash
sudo python3 ./bin/flussonix-service credentials \
  --output /etc/flussonix-preview.env
```

This creates a mode-0600 file and refuses to overwrite an existing file. The
default admin username is `admin`; use `--admin-user` to choose another. Reuse the
same credentials when moving an existing instance. This helper accepts printable
ASCII credentials; the username cannot contain a colon. For a new cluster, generate
its peer key once with `openssl rand -hex 32` and save it securely.

## Review and start

Choose an unused listener port, then generate the unit. Set `flussonix_release`
to the absolute path of your extracted release:

```bash
flussonix_release=/opt/flussonix/releases/flussonix-VERSION-linux-ARCH-REVISION
sudo python3 ./bin/flussonix-service render \
  --instance preview --user flussonix \
  --release-dir "$flussonix_release" \
  --state-dir /var/lib/flussonix-preview \
  --credentials /etc/flussonix-preview.env \
  --listen 127.0.0.1:18210 --ffmpeg /usr/bin/ffmpeg \
  --output "$PWD/flussonix-preview.service"
systemd-analyze verify --man=no ./flussonix-preview.service
cat ./flussonix-preview.service
```

The helper verifies the release manifest and requires a non-root account, a
private state directory and a protected credentials file. Executable paths must
refer to real files; resolve an FFmpeg symlink with `readlink -f` if needed.
The unit references the credentials file without embedding its contents.

After reviewing the unit, install and start it explicitly. Ensure the destination
does not already belong to another instance:

```bash
sudo sh -c 'umask 022; set -C; cat "$1" > "$2"' sh \
  ./flussonix-preview.service /etc/systemd/system/flussonix-preview.service &&
sudo systemctl daemon-reload &&
sudo systemctl start flussonix-preview.service
sudo systemctl status flussonix-preview.service
sudo journalctl -u flussonix-preview.service -n 50 --no-pager
```

The installation command refuses an existing destination; activation runs only
if the new unit was written successfully. Inspect any failure before continuing.

Open [the local admin interface](http://127.0.0.1:18210/admin/), sign in and check
actual playback. Use an SSH tunnel for remote access. Once validated, optionally
enable startup at boot with `sudo systemctl enable flussonix-preview.service`.

The service writes only inside its state directory and temporary directory. Put
certificates, keys and SDP files somewhere the account can read. GPU transcoding
also requires device permissions for the service account; this helper does not
install drivers or change group membership. Check GPU readiness in the UI.

On Linux, direct media subprocesses have parent-death protection so a fatal
daemon crash does not leave them running during the shutdown grace period.
Normal cancellation and the unit's 15-second shutdown grace remain in place.
Use ordinary FFmpeg and Tesseract executables: wrappers that fork or change
credentials can lose this protection. Whole-service cgroup cleanup remains the
fallback for those cases.

## Upgrade or remove

For a cold upgrade, stop the service, back up its state and current unit, then
verify and extract the new release into a different directory. Render a new unit
using the same state and credentials and the new release path. Review it, replace
your saved service unit, run `systemctl daemon-reload`, then start and check
playback. Keep the old release and unit for rollback. If the old release cannot
read a changed configuration, restore the backed-up configuration before starting
it; doing so reverts later configuration edits.

To remove only this service registration:

```bash
sudo systemctl disable --now flussonix-preview.service
sudo rm /etc/systemd/system/flussonix-preview.service
sudo systemctl daemon-reload
```

Keep the state, credentials, release directories and backups until you decide
they are no longer needed. Removing the unit does not delete them.
