# Configured-source inventory and private-media binding

The independent [source authority](vendor-source-authority.md) now owns prepared
inventory and private-media clients. A complete inventory fetch produces an
opaque result tied to the exact immutable source profile that made the request.
Its candidates can construct media inputs only through that authority's current
profile check. This connects the existing library clients; daemon vendor routing,
global configuration, activation and the UI remain unchanged.

## Endpoints, transport, key and trust

The source URL still requires a server root using `m4f`, `m4fs`, `m4s` or `m4ss`.
It selects M4F/M4S and supplies the default HTTP/HTTPS management root. Four
additional local fields are supported:

| Field | Meaning |
| --- | --- |
| `api_url` | Optional explicit HTTP(S) management root, including a trusted proxy prefix. Defaults to the mapped source URL. Inventory appends `/-/streams`. |
| `private_payload_url` | Optional explicit HTTP(S) private-media root, including a trusted proxy prefix. Defaults to the effective management root. |
| `flussonix_tls_ca` | Optional absolute PEM CA file for the management HTTPS leg. |
| `flussonix_media_tls_ca` | Optional absolute PEM CA file for the private HTTPS leg. |

Endpoint overrides are explicit local authority and may intentionally choose HTTP
on a private network. The source URL's scheme supplies defaults; it does not
force an explicit override to use HTTPS. There is no automatic downgrade or
fallback. Explicit roots retain the existing clients' 4096-byte and URL validation
rules: no credentials, query, fragment, whitespace, backslash or zero port.
Endpoint/CA fields must be nonempty strings of at most 4096 UTF-8 bytes.

Both clients use the profile's explicit cluster key. Inventory and private media
keep their distinct legacy per-request credential contracts; neither tries Basic,
native peer or ambient credentials. Remote metadata never supplies endpoints,
keys, transport, trust or viewer policy. Names remain literal and are encoded
once by the existing private-media client.

Each leg captures its own TLS client during complete profile construction. A
custom CA replaces public roots and requires HTTPS for that leg. Management trust
does not implicitly become private-media trust. CA bundles retain the existing
1 MiB/128-certificate limits. Files are read separately during construction;
changing/removing a file afterwards cannot change that profile's connections.
A successful replacement rereads configured files and invalidates old candidates,
even when configuration bytes and paths are identical. Invalid trust replacement
leaves the old profile usable. Construction performs no network requests.

## Bound fetch and publication

`authority.inventory().await` captures the current profile, awaits its direct
inventory client without holding the publication lock, then rechecks profile
identity/revision. The check applies to errors as well as successes. Replacement,
removal or removal/re-addition during the request returns `Stale`; old lookup
errors cannot be mistaken for current source health. Cancellation drops the
client future without spawning a detached fetch. Existing whole-chain deadline,
pagination, byte, row and membership limits remain unchanged. Invalid later pages
never expose a partial bound result.

`BoundInventory::names()` returns literal names in sorted order. Its rows cannot
be imported or mutated. `candidate(name)` looks up that exact result and applies
the existing metadata preflight, source namespace/exclusions/static selection and
explicit local viewer-policy projection. Missing names return `Missing`.
An old result may still construct a candidate, but publication will reject it.

`authority.at_bound_current(&candidate, |view, input| ...)` checks the candidate's
profile identity/revision and holds the source publication lock through a
synchronous closure. It supplies local policy/facts and a media input constructed
from the same profile's prepared origin, transport, key and literal candidate
name. Input construction does not reread CA files or contact the endpoint.
Candidates from another authority fail even with identical settings/revisions.
A manually constructed inventory row/plain `Candidate` cannot substitute for a
`BoundCandidate`. Bound objects have no Debug/serialization implementations and
errors contain only generic variants.

Never await or re-enter the authority inside a publication closure. Viewer
permission must precede private requests, and asynchronous authorization or
activation work requires a fresh fence before committing. Exporting an input
from a closure does not revoke it on later configuration changes and is not
permission to open it after stale asynchronous work. The future daemon adapter
must govern worker/session lifetime and coordinate its global configuration
fence, leases and local stream precedence.

## What this establishes

The result carries the identity of the configured fetch client/profile. It does
not attest remote content or topology. Legacy inventory authentication signs the
request; it does not independently authenticate an HTTP response. HTTPS verifies
the configured certificate trust, hostname and validity before sending headers.
HTTP remains plaintext. Config/user provenance remains an unverified metadata
claim. A bound candidate is neither a viewer grant nor a loop-free origin proof.

Owned HTTP/TLS regressions exercise default and split origins, transport selection,
literal names, local policy, request digests checked through OpenSSL, stale fetch
success/error rejection, cross-authority fencing, full-chain failure, cancellation,
independent/frozen CA trust and redirect/auth-fallback confinement. Existing media
codec and source policy suites remain part of qualification. No vendor component
is a build, test or runtime dependency.

Live positive/negative Cluster authentication and sender encoding, aliases,
actual relay-loop/topology checks, inventory leases, daemon global configuration
coordination, worker/session revocation, complete template/group semantics,
HLS/TS private credentials and subtitle options remain integration gates. Vendor
routing stays disabled.


The independent [management identity preflight](vendor-source-identity.md) now
provides opt-in configuration identity probes around bound inventory, rejecting
direct self claims and observed ID drift under shared limits. This is not an
atomic remote snapshot, private-endpoint identity, provenance or multi-hop loop
proof. Vendor daemon routing remains disabled.

## Local inventory freshness

The optional [local inventory lease](vendor-inventory-leases.md) adds a bounded
monotonic lifetime and latest-refresh ordering to identity-observed discovery.
Failed/cancelled refreshes do not restore old grants. This is library-only local
publication fencing, not a remote lease, topology proof or worker revocation;
vendor daemon routing remains disabled.
