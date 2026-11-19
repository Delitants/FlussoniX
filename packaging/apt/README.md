# FlussoniX APT repository

Build the Ubuntu 24.04 amd64 repository from one or two qualified packages:

```bash
GNUPGHOME=/private/signing-home python3 scripts/build-apt-repository.py \
  --package flussonix_0.11.0-1_amd64.deb \
  --package flussonix_0.10.0-1_amd64.deb \
  --public-key flussonix-archive-keyring.gpg \
  --signing-fingerprint FULL_SIGNING_SUBKEY_FINGERPRINT \
  --output-dir apt
```

Each package must have adjacent `.sha256` and `.inspection.json` files from the
Debian package builder. `GNUPGHOME` must be an owner-only directory containing
the exact signing key. The output contains only the normalized public key.

## Production trust anchor

- Primary certification key: `8EF4A06B60F9B89C39D1F4386570D33916751290`
  (expires 2031-11-18 UTC).
- Current signing subkey: `6F022E7F62F5F2375C24A13CD693B0BB0AEC4620`
  (expires 2027-11-19 UTC).

Start signing-subkey rotation by 2027-08-21, at least 90 days before expiry.
Publish the updated public key while the current subkey remains valid, update
the protected environment secret and `signing-key-fingerprint.txt`, then verify
one release with both the old repository trust state and a fresh installation.
The installer's pinned primary fingerprint does not change for a subkey
rotation. A future primary-key rotation requires an explicit overlap period in
which both trust anchors are distributed and independently verified.
