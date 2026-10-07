# Callback-selected RTSP routing

An existing stream/template `on_play` HTTP(S) backend can return HTTP 302 with
an absolute `rtsp://` or `rtsps://` Location. A standalone or CDN node answers
the viewer's initial DESCRIBE with RTSP/1.0 302 Moved Temporarily, the original
Location, matching CSeq, an empty body and `Cache-Control: no-store`. It closes
that control connection without creating a media session, attaching a viewer
or starting an input worker. The viewer reconnects to the chosen destination,
which applies its own authorization. No peer/API credentials are forwarded.
The wire response follows [RTSP/1.0 redirection](https://www.rfc-editor.org/rfc/rfc2326.html#section-11.2).

The callback receives the existing `proto=rtsp`, decoded token, original query,
stream name, client IP and session metadata. Stream and template policy,
`X-AuthDuration` decision caching, token denial and manual revocation remain
authoritative. Cached redirect entries do not consume active media/viewer slots.
Policy changes already published by the configuration owner reject stale
decisions. A root configuration revision change during the callback returns
503 and requires a retry, even for an unrelated edit; no old redirect is emitted.

Location is limited to 8192 bytes of ASCII URI text, with a host and without
raw whitespace/control characters, backslashes, userinfo or fragments.
Relative destinations, HTTP(S) destinations and malformed ports are rejected.
Percent-encoded path/query bytes are preserved; safe encoded characters are
not reinterpreted as response headers. Direct self-redirects compare the actual
transport, host, effective port, decoded stream path and query pairs, including
trailing-slash/path-encoding variants. This does not detect arbitrary multi-node
redirect cycles; the client's redirect/reconnect policy still applies.

An encrypted control connection accepts only `rtsps://` destinations. This
check follows the actual TLS socket, including plain RTSP URI aliases inside
TLS, and runs for every response even when the decision was cached by a prior
plaintext viewer. Plaintext viewers may be directed to RTSPS. HTTP playback
continues to accept only its HTTP(S) callback destinations; protocol cache keys
remain separate.

No new configuration fields or UI inputs are required. The existing friendly
Streams/Templates authorization controls configure the backend. This provides
backend-selected routing; native adaptive RTSP LB selection/reservation is a
separate task, and the `lb` role still returns 501 for RTSP playback. It does not
add Basic/Digest viewer credentials, change publication/push, issue server-driven
REDIRECT requests for established media, or transparently move a live session.
The verified RTSPS **input** bridge continues to reject upstream 3xx responses
so redirects cannot escape its verified connection. Clients following a secure
viewer redirect must independently verify the destination's certificate.

Qualification uses owned callback servers and plain/private-CA TLS listeners
on unused loopback ports. Tests cover cached exact Location/CSeq framing, no
worker/session, TLS downgrade rejection after a plaintext cache fill, TLS URI
aliases, plaintext-to-TLS routing, IPv6 destination syntax, invalid/missing/large
Locations, direct self-loops, template inheritance, token denial, pending/cached
revocation, configuration changes and unchanged HTTP redirect behavior.
An independent FFmpeg client follows a plaintext redirect to a separately
authorized node and strictly decodes both audio and video. Secure redirect
headers are qualified with a certificate-verifying client; automated secure
multi-hop media following and mixed-vendor/client dialects remain unqualified.
All fixture processes are stopped and reaped. No official Flussonic component
is a runtime, build or test dependency.
