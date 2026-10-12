# Flussonic HTTP inventory client

`source_directory::flussonic::InventoryClient` implements the independently
derived HTTP inventory wire contract. It is a library building block for mixed
clusters. The daemon's source configuration, UI and resolver still select the
native FlussoniX protocol; vendor routes are not enabled by this increment.

Flussonic's [cluster restreaming documentation](https://flussonic.com/doc/fms/cluster/restreaming/)
describes automatic source inventory, static/on-demand selection, local
precedence and privileged cluster keys. Read-only analysis of the installed
reference identified `GET /-/streams`, `streams`/`next` pagination and the
inventory credential below. A prior read-only demo observation on
Streamer26.04.1 confirmed the inventory envelope and a following cursor page
using supplied management credentials. Live hashed Cluster authentication
has not yet been qualified against a reference key.

## Wire contract

Construct the client explicitly with an HTTP(S) management root, a UTF-8 key
and an optional absolute PEM CA path. An endpoint prefix is preserved:
`https://origin.example/gateway` becomes
`https://origin.example/gateway/-/streams`. Credentials, query strings,
fragments, control characters and backslashes are rejected in the root URL.
The key is nonempty and limited to 4096 UTF-8 bytes. This wire constructor
does not change native source-key validation.

Each page receives:

```text
Authorization: Cluster <decimal Unix seconds>:<lowercase SHA-1(timestamp + ":" + key)>
```

The header uses the current timestamp for that request and is marked sensitive
in the HTTP library. No raw peer-key header, Basic fallback or native protocol
probe is sent. SHA-1 is isolated to this legacy interoperability contract using
the already present independent `ring` dependency; native authentication and
viewer-token algorithms are unchanged.

The client owns its HTTP configuration: proxies and redirects are disabled.
HTTPS verifies the certificate chain, validity and server identity. A supplied
CA replaces public roots and requires HTTPS. All errors use generic
`LookupFailure` variants; neither the URL, response body nor credential is
included in an error. The client has no Debug or serialization implementation.

`next` is an opaque string encoded as a single `cursor` query parameter on the
same endpoint. It cannot select another origin or path. A missing or null
`next` ends pagination. Empty, non-string, excessive and repeated cursors are
invalid. Empty cursors are deliberately rejected rather than treated as a
successful end or another request for the first page.

## Atomic bounds and returned facts

One inventory operation has a 750 ms total deadline, covering headers, body
reads and every page. Its complete chain is limited to 1 MiB of response body
bytes, 10000 rows, 100 pages and 4096 UTF-8 bytes per cursor. Byte and row
budgets are shared across pages. Both advertised body lengths and actual
chunks are checked. An unfinished chain at the page cap fails. The exact
complete row/page boundary is accepted.

Every page must return HTTP 200; partial-content and asynchronous success
statuses are unavailable, even when they contain plausible JSON.
Only a fully validated chain returns `Vec<InventoryStream>`. A bad later
page, duplicate or invalid name, invalid row/envelope or exceeded budget
returns an error for the entire operation. No partial result or background
pagination task is stored. Cancellation drops the in-flight request.

Rows contain only name, static/disabled flags, optional provenance and optional
group memberships.
Omitted static defaults to true; omitted disabled defaults to false.
Present flags must be booleans. Present `named_by` must be one of
`config`, `user`, `remote` or `external`; omission remains unknown.
Disabled and on-demand rows are retained as facts for later policy decisions.
Remote inputs, viewer authentication and other fields are not imported as
configuration or access grants.

The strict membership profile accepts `groups` only as an array of unique literal
names. Omission returns `None` (unknown); an explicit empty array returns
`Some(vec![])` (known ungrouped). Null, strings, objects, nested values, invalid
names and repeated names invalidate the whole inventory, even on disabled,
on-demand or relayed rows. Names preserve case, spaces, Unicode and percent
sequences; they are not decoded or normalized. Each name uses the existing
1..256 UTF-8 byte stream-name validation. A row permits at most 64 memberships,
matching the local policy gate. The complete cursor chain permits at most 10000
memberships, counting every occurrence across rows/pages. The same group may
appear on different streams. The existing byte/row/page/deadline bounds also
apply. Exact complete membership limits are accepted; a bad later page never
returns an earlier partial result. These are the supported independent wire
profile and resource limits; live reference group encoding remains unqualified.

### Local-origin candidate preflight

`InventoryStream::local_origin_candidate()` checks these facts without fetching
media, publishing policy or applying configuration. It revalidates the literal
stream name and any explicit memberships, since library consumers can construct
or mutate public facts. Disabled rows fail. Enabled rows require explicit groups
and `named_by: config` or `named_by: user`; omitted provenance, `remote` and
`external` fail with distinct generic diagnostics. A passing row is classified
as `Static` or `OnDemand`; static status is not proof of local ownership.

This conservative preflight accepts only locally initialized provenance claims.
It does **not** verify those claims or prove the upstream input is loop-free.
Even a configured stream may pull this CDN through its input URL, and the legacy
sender's metadata has not been qualified live. `external` stays unsupported
until its authority/provenance semantics are qualified. A passing result is not
a route grant, viewer policy, activation decision or security capability, and
is not used by the daemon. It intentionally does not implement prefix/exclusion
selection, local precedence, topology checks, revision fences or inventory leases.

A future adapter must carry explicit memberships into the local policy gate
without replacing unknown membership with an empty list. Inventory `on_play`
and input fields never substitute for local source/group configuration. Policy
composition still rejects conflicting groups regardless of remote array order.

## Qualification and remaining integration

`cargo test --locked --test vendor_inventory -- --test-threads=1` exercises
owned HTTP/HTTPS fixtures, including pagination, cross-page validation,
credential hashing against OpenSSL, aggregate caps, hung responses, redirect
confinement and trusted/untrusted/wrong-name/expired certificates. Library
tests pin four literal authentication vectors derived independently with
Python hashlib and OpenSSL. No vendor code, server or package is required to
build or run these tests.

Before exposing a vendor source mode, qualify live positive/negative Cluster
authentication, effective source/group viewer-policy composition and template
inheritance. Missing policy information must not become a public grant.
The explicit [private-media library client](vendor-private-media.md) separately
qualifies M4S/M4F stream-scoped headers, literal URL/name binding, segment
requests and reopened sessions against independent fixtures; live reference
authentication and aliases remain pending. HLS/MPEG-TS credential carriage
also needs its own evidence. Preserve native prefix/except/only semantics,
local precedence, revision fencing, inventory leases, capacity limits and
relay-loop prevention during integration. Missing provenance must not be
interpreted as proof that a stream is locally owned.

The separate [local viewer-policy gate](vendor-viewer-policy.md) validates source/group callback and token layers, rejecting unsupported rules and order-dependent group conflicts. Explicit inventory memberships can be composed through the source authority below. Global configuration fencing, verified provenance/loop eligibility and live reference qualification remain daemon integration requirements.


The independent [source configuration authority](vendor-source-authority.md)
validates all supported local source/group options before projection, preserves
native namespace/exclusion/static-selection semantics and rejects stale or
cross-authority candidate publication. Source/backend replacement and removal
invalidate outstanding snapshots. This is not wired to daemon routing or
activation; global configuration fences, leases, actual topology and live vendor
interoperability remain integration requirements.


The independent [configured-source binding](vendor-source-binding.md) now retains
validated endpoints, key, transport and per-leg TLS trust under one source profile.
Opaque complete inventory results produce candidates for current-profile media
construction; stale fetch success/errors and cross-authority publication fail.
This remains library-only: actual topology, live interoperability, global daemon
fences, leases and worker/session lifetime still gate vendor routing.


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


## Explicit modern management observations

The opt-in [modern inventory client](vendor-modern-inventory.md) supports bounded
configured custom IDs and UUID aliases, modern selected stream metadata and one
explicit whole-refresh budget up to 30 seconds. It checks config identity around
the complete cursor chain and in every page. Missing memberships and relay claims
remain facts, not route or viewer grants. The strict legacy client and existing
source-authority/lease APIs keep their contracts; vendor daemon routing stays
disabled pending authority, relay/topology, policy and global integration.
