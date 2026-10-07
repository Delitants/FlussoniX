# Verified RTSPS input redirects

## Intent and scope

Complete the native secure LB → CDN → private-source input path with independent
decoded audio/video. Existing RTSPS inputs verify the first connection but reject
all redirects. No official Flussonic component, production change, insecure TLS
mode or new JSON UI is introduced. Existing CA controls remain sufficient.

## Architecture

Keep the worker-owned loopback bridge and opaque request copying. A validated
pre-session 301/302 response is intercepted. Verify the destination certificate
and identity using the configured trust, allocate a fresh loopback listener, and
emit a newly constructed RTSP302 containing only that owned local URL. FFmpeg
reconnects locally; the same root task owns every upstream socket and listener.
This avoids replaying OPTIONS/authentication or implementing a second RTSP client.
The original remote redirect is never passed to FFmpeg.

Only absolute, strict ASCII `rtsps` destinations without userinfo, fragments,
invalid escapes or port zero are accepted. Reuse the existing RTSP destination
validator. Query/path values come from Location; never invent or inherit a token.
The initial input's username/password disables redirect following entirely.
Plaintext destinations, unsupported 3xx, missing/duplicate Location/CSeq and
ambiguous or oversized frames fail closed. After SDP, Session or interleaved media
is observed, all redirects remain rejected. Direct credentialed inputs retain
their existing behavior.

## Bounds and ownership

At most four redirected connections after the initial connection, with exact URL
cycle detection and an overall twenty-second initial routing deadline. Initial
and per-hop TCP/TLS setup retain the ten-second maximum. Loopback listeners accept
one connection and retain the eight-second unused expiry, shortened by routing
deadline. Complete frame reads before establishment are deadline-bounded; media
after establishment has no routing deadline. Retain response bounds: headers
16KiB/64 unique headers, body64KiB, interleaved payload8192 bytes. Closing/dropping
the root bridge closes every pending/current socket and listener; no detached hop
tasks. DNS aliases and changing-query cycles are bounded by hop/deadline limits.

## Qualification

An owned private-CA round trip must redirect locally and deliver the final TLS
response. Verify bad identity/trust/expiry before application data, downgrade,
userinfo, malformed locations, body/header ambiguity, CSeq and established-session
rejection, cycle/hop bounds, stalled handoff and cancellation. Existing opaque
body/interleaving, worker fallback, unused-listener and shutdown tests must pass.
A native source/CDN/LB plus configured RTSPS relay must independently decode both
tracks over HLS, preserve independent LB/CDN authentication, and reuse one private
M4S pull. All owned fixtures and subprocesses are stopped and reaped.

Full immutable-head CI, fresh read-only whole-range review and normal-binary proof
gate authorized main/preview publication. Preserve actual preview environment,
configuration and listener protection. Mixed-vendor redirects, sustained scale,
Basic/Digest redirected inputs and live-session relocation remain unqualified.
