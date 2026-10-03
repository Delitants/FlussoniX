# v0.8 implementation decisions

- Ruling: Standing development/test/publication authorization and latest Continue permit inline design/implementation in an isolated codex/rtsps-v08 worktree without redundant approval menus. One fresh whole-branch review is required by executing-plans; no implementer agents. Cost if wrong: preview must remain unqualified for migration.

- Ruling: FFmpeg n6.1.1 OpenSSL TLS source explicitly does not check hostnames. Use Rustls certificate/name validation on the actual forwarded TLS connection, never a separate preflight followed by unverified reconnect. Per-worker one-shot loopback input bridges reuse FFmpeg after validation. Cost if wrong: vendor authority/digest dialects beyond the exercised subset may need adapter work; no TLS downgrade is permitted.

- Ruling: CI workflow also generates an owned temporary CA and supplies its absolute path for the new real form-persistence case; otherwise browser fixture setup cannot exercise saving. No generated keys/certs enter git or release artifacts.
