# Build a Linux preview archive

The packager requires Python 3.11+ and uses only its standard library. Build the
server and UI from the revision being packaged, using the pinned Rust toolchain
and Node.js 22.12+:

```bash
cargo build --release --locked
npm ci --prefix web
npm run build --prefix web
python3 scripts/package-preview.py \
  --binary target/release/flussonix --web-dir web/dist \
  --revision "$(git rev-parse HEAD)" --output-dir .runtime/packages
```

For cross-compiled artifacts, pass the corresponding executable with `--binary`.
The packager checks ELF64 x86_64/aarch64 headers; it does not establish libc, CPU
or target-host compatibility. Run the extracted archive on its target host before
publishing it. For native SRT playback or push, include a listener startup and
encrypted media check against the target host's system libsrt 1.5. A static
musl test binary started on Ubuntu 24.04 but could not load the host's glibc
libsrt; the tested glibc build loaded that library and delivered encrypted SRT.

The output contains a `.tar.gz` and matching `.sha256` sidecar. The archive has
`bin/flussonix`, `bin/flussonix-service`, `web`, installation guides, license
texts, `NOTICE` and `manifest.json`. Only public
build assets are accepted; runtime state and credentials are never taken from
the source checkout. Symlinks and unexpected UI files reject packaging.

The manifest records the supplied source revision and payload hashes. The caller
must ensure the supplied binary and UI were built from that revision; the
packager cannot attest build provenance. No official Flussonic runtime files are
collected or bundled.

The same input bytes, source revision, epoch and packaging toolchain produce the
same archive bytes, regardless of source permissions or modification times.
`SOURCE_DATE_EPOCH` or `--epoch` overrides the default source-commit timestamp.
Ownership, modes, timestamps, entry ordering and gzip headers are normalized.
Existing output files are never overwritten; use another output directory for
a comparison build.

Run archive regression checks with:

```bash
python3 -m unittest discover -s tests -p 'test_*preview.py' -v
```

For a release-candidate cold-upgrade check, verify both archive checksums and
extract the previous and candidate Linux packages into separate directories.
Then run `python3 scripts/smoke-cold-upgrade.py --previous OLD_DIR --current NEW_DIR`.
The smoke uses a temporary owner-only state directory and an unused loopback
port. It checks saved stream/template reads through old → new → old, then checks
that the new package can reopen its separate IPTV data after an old-version
rollback. It does not qualify live viewer continuity, changed-schema rollback,
rolling clusters, or recovery under load.
Add `--media` to start an owned `testsrc://` worker in each version and verify a
packaged HLS segment with independent FFprobe. This requires FFmpeg and FFprobe;
the smoke signals its isolated process group and waits for the daemon to exit
after each phase.

See [installation and upgrades](installation.md) for the operator workflow.
