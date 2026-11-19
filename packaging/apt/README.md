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
