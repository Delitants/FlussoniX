# v0.8 implementation decisions

- Ruling: Standing development/test/publication authorization and latest Continue permit inline design/implementation in an isolated codex/rtsps-v08 worktree without redundant approval menus. One fresh whole-branch review is required by executing-plans; no implementer agents. Cost if wrong: preview must remain unqualified for migration.

- Ruling: FFmpeg n6.1.1 OpenSSL TLS source explicitly does not check hostnames. Use Rustls certificate/name validation on the actual forwarded TLS connection, never a separate preflight followed by unverified reconnect. Per-worker one-shot loopback input bridges reuse FFmpeg after validation. Cost if wrong: vendor authority/digest dialects beyond the exercised subset may need adapter work; no TLS downgrade is permitted.

- Ruling: CI workflow also generates an owned temporary CA and supplies its absolute path for the new real form-persistence case; otherwise browser fixture setup cannot exercise saving. No generated keys/certs enter git or release artifacts.

- Final review reproduced three Important issues: RTSP redirects escaping the TLS bridge, setup failures losing fallback state, and stalled handshakes blocking the global worker map. All three received failing regression tests before fixes. Worker-owned asynchronous setup preserves retry/fallback metadata and allows independent streams and shutdown to proceed; no application bytes leave before verification.

- Ruling: Reject upstream 3xx before forwarding any response, using strict bounded RTSP/interleaved framing. Never let FFmpeg follow an unverified redirect. Ambiguous/oversized headers and bodies are rejected, while opaque bodies and media bytes remain unchanged. Cost if wrong: redirect-dependent and authority-sensitive sources need a future verified adapter, never a trust bypass.

- Review tool limitation: the fresh reviewer supplied concrete findings and reproductions but its turn ended with a system content-filter error before a consolidated verdict. No second reviewer was dispatched. The root verified all reported issues, added focused RED/GREEN coverage and checked the final diff; a fully completed reviewer verdict is not claimed.

- Declined behaviors: full publication/push, direct RTP/SRTP, GPU/scale/migration and universal vendor authority/auth dialects remain explicit future directions, not v0.8 claims. Root inspection of independent [FFmpeg RTSP source](https://github.com/FFmpeg/FFmpeg/blob/n6.1.1/libavformat/rtsp.c) confirms control URLs use the existing control connection and a nonmatching lower transport is rejected; 3xx reconnects required the new response guard. No additional reported finding remains unfixed.
