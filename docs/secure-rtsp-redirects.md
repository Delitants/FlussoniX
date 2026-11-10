# Verified RTSPS input redirects

Configured RTSPS inputs can follow native LB → CDN redirects while verifying
each server's TLS certificate and identity. A real native source/CDN/LB and
configured relay test independently decodes H.264/AAC for two HLS viewers, with
no LB media worker and one shared private HTTPS M4S source pull.

In the stream input editor, use an **RTSPS URL**, for example
`rtsps://lb.example:8322/channel?token=viewer-token`. Publicly trusted certificates
use the bundled public trust roots. For private certificates, set the existing **TLS CA**
field to an absolute PEM bundle containing the trust roots for every participating
TLS server. The input CA setting is `flussonix_tls_ca`. Server identity is verified
at every hop; certificate checking cannot be disabled.

An RTSPS input follows redirects to its original hostname and port by default.
For an LB that redirects to CDN listeners, add each trusted CDN origin in
**Allowed RTSPS redirects** beneath that input. Enter roots such as
`rtsps://cdn.example:322`, without a stream path, query, fragment, wildcard or
credentials. The API field is `flussonix_rtsps_redirect_origins`, an array of up
to 16 such roots. The original origin is always allowed and need not be listed.
Hostnames compare without case and the default RTSPS port is 322. A DNS alias or
different port needs its own entry. Changing the list replaces the stream's
worker; a removed destination cannot continue through the old bridge.

An input URL containing username or password may redirect only within its
original hostname and port, even if other origins are listed. Put approved
roots on the specific Stream or Template input used by the receiving node;
this does not configure another node or grant it credentials. Include each
participating server's trust root in the input CA bundle when using private
certificates.

The worker owns a loopback RTSP bridge. It intercepts an initial 301/302, verifies
the next TLS connection, then constructs a new redirect to a fresh owned loopback
listener. FFmpeg repeats its handshake through that listener. Remote redirect
responses are never forwarded to the decoder. Location supplies the destination
path and query; the bridge adds no inherited token. The CDN still
independently authorizes and consumes its bound admission ticket before media.
On Linux, each loopback connection must belong to the actual decoder process
(or the in-process RTSP probe). The bridge checks the connected TCP socket
against that process's `/proc` descriptors before forwarding any bytes, including
after redirects. A different local process cannot take the decoder's first
connection. The service must be able to read its child process's `/proc` entries;
if that check is unavailable, the input fails closed.

Only absolute strict `rtsps` destinations are accepted. Plaintext, other schemes,
userinfo, fragments, invalid URI escapes, port zero, unsupported 3xx and ambiguous
headers fail closed. An unapproved hostname or port is rejected before DNS or
TCP connection on every redirect hop. After verifying the target TLS connection, the bridge copies
only the originally configured userinfo into the owned loopback handoff. Remote
Location userinfo remains rejected. URL-cycle comparison excludes credentials.

Owned camera-style fixtures qualify Basic and Digest MD5 with `qop=auth`, a
stable realm and stale-nonce renewal after redirect. Both profiles independently
decode H.264/AAC HLS output, reuse one input worker and reject wrong passwords
before source startup. Use the existing Input URL and TLS CA fields; no extra
authentication editor is needed. Arbitrary camera dialects, Digest algorithms,
realm changes, cross-origin credential delegation and mixed-vendor redirect
interoperability remain unqualified.

The chain permits four redirects after the initial connection and detects exact
URL cycles. Initial routing has a twenty-second total deadline, including initial
setup and complete response-frame reads; individual TCP/TLS connections retain a
ten-second maximum. Each loopback listener accepts one connection and expires
after eight unused seconds, shortened by the remaining routing budget. SDP,
Session or interleaved media establishes the session: later redirects are
rejected, and the routing deadline no longer limits that stream's lifetime.
Changing-query and DNS-alias cycles remain bounded by the hop/deadline limits.

Headers remain bounded to 16 KiB and 64 unique keys, control bodies to 64 KiB and
interleaved payloads to 8192 bytes. Opaque SDP bodies and media payloads preserve
their bytes. One reusable frame buffer serves each bridge. One root task owns all
hop sockets and listeners; closing the bridge, worker failure, replacement or
shutdown releases its resources without detached hop tasks. Existing worker
fallback/retry and shared-ingest ownership remain unchanged.

Owned TLS tests cover target identity/trust/expiry rejection before application
data, invalid URI/framing, credentialed inputs, established-session rejection,
foreign local connectors at the initial and redirected listener,
exact/changing-query cycles, cancellation during redirected TLS setup, unused
handoff expiry and continued media beyond the initial routing deadline. No
official Flussonic component is a runtime, build or test dependency. Sustained
scale, WAN/loss performance and arbitrary external-client secure redirect behavior
remain unqualified.
