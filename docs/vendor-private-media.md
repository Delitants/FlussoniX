# Explicit Flussonic private-media client

`m4_ingest::flussonic::MediaInput` is an independent library client for the
stream-scoped M4F/M4S HTTP authentication contract observed through read-only
reference analysis. It shares FlussoniX's decoder, codec handling, metadata
handshake, worker bridge and native relay. Official Flussonic components are
not used at build, test or runtime.

This client is deliberately not selected by daemon source configuration or
the resolver. It does not establish viewer access, import remote policy, or
enable mixed-vendor cluster routing. Live positive/negative authentication
against an authorized reference key remains unqualified.

## Construction and name binding

Construct a client with an explicit private HTTP(S) root, literal canonical
stream name, `Transport::M4f` or `Transport::M4s`, UTF-8 cluster key, and optional
absolute PEM CA path. Management and private origins may differ; the caller
must supply the intended private root. A trusted proxy path prefix is kept.

For example, root `https://origin.example/private`, name `region/news`, and
M4F produce a control URL ending `/private/region/news/m4f`. Each name component
is URL-encoded once, while authentication binds the original UTF-8 name.
`space name` becomes `space%20name`; a literal name `literal%2Fname` becomes
`literal%252Fname`. The client never infers a name by unescaping a URL, strips
a protocol suffix from a name, or treats a name as a navigation destination.
Aliases and reference-server name decoding still require live qualification.

Names follow the existing 256-byte media-name grammar. Empty components, dot
traversal, control characters, backslashes, queries and fragments are invalid.
Roots are limited to 4096 UTF-8 bytes and reject credentials, queries,
fragments, whitespace, backslashes and zero ports. Keys are nonempty and at
most 4096 UTF-8 bytes. A client has no Debug or serialization implementation.
Errors exclude the root, privileged key and upstream HTTP diagnostic body.

## Per-request credentials and confinement

Every control connection and M4F segment GET receives:

```text
cluster-auth-time: <decimal Unix seconds>
cluster-auth-key: <lowercase SHA-1(timestamp + ":" + key + ":" + stream_name)>
```

The current timestamp is read when constructing each request. Segment paths
are not included in the signed name. Reusing a client to open another input
session regenerates the credential; no timestamp/hash is cached at client
construction or control connection setup. Both header values are marked
sensitive. This legacy SHA-1 contract changes neither native authentication
nor viewer tokens. Inventory uses a different authentication message/header.

The HTTP client disables proxies and all redirects, including same-origin
redirects and M4F segment redirects. No Basic/native fallback or credential
probe is attempted. HTTPS verifies trust, identity and validity before HTTP
headers are sent. A supplied CA replaces public roots and requires HTTPS.
Control and segments require HTTP 200; partial/asynchronous success statuses
are rejected before metadata or media publication.

## Input lifetime and media behavior

`pull_ready` runs one session and reports validated track metadata before TS
writes. The caller owns retry/backoff and cancellation. Dropping its future
cancels the current request/body; no detached media-fetch task is created.
An EOF or rejected request ends the session; reconnection is explicit.

The existing native input limits apply: five-second TCP connect timeout,
ten-second control setup and M4F segment request timeouts, fifteen-second
control-chunk idle timeout, 8192-byte signal lines, 16 MiB M4F segments and
M4S records, 32 MiB M4S buffered data, and a sixteen-name segment deduplication
window. Canonical M4F date signals determine segment paths under the configured
stream; arbitrary URLs or traversal signals are rejected before another GET.

The public method retains the existing public ingest subtitle defaults:
subtitle tracks are omitted from the native relay. The daemon's existing
subtitle controls are unchanged. Wiring explicit vendor inputs into daemon
workers with source/group viewer policy and subtitle options is future work.

## Independent qualification

`cargo test --locked --test vendor_media_auth -- --test-threads=1` exercises
owned random-port HTTP/TLS fixtures. Captured hashes are checked through
system OpenSSL. Tests cover UTF-8/space/percent names and proxy paths, HEVC/m2a/
MP3 metadata and original wire samples, per-segment and reopened-session
refresh, wrong-key rejection, errors/partial responses, control/segment
redirects, invalid signal confinement, constructor validation, cancellation,
custom trust, unknown roots, wrong identities and expired certificates.
Four literal library vectors were independently derived with Python hashlib
and OpenSSL. Test fixtures need no vendor executable, library or server.

This qualifies the explicit client's fixture behavior. It does not qualify
live vendor authentication, aliases, HLS/MPEG-TS credential carriage, source/
group policy inheritance, discovered-name provenance, loop prevention,
daemon/UI integration, or production scale.
