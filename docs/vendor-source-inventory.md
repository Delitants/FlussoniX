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

Rows contain only name, static/disabled flags and optional provenance.
Omitted static defaults to true; omitted disabled defaults to false.
Present flags must be booleans. Present `named_by` must be one of
`config`, `user`, `remote` or `external`; omission remains unknown.
Disabled and on-demand rows are retained as facts for later policy decisions.
Remote inputs, viewer authentication and other fields are not imported as
configuration or access grants.

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
Qualify M4S/M4F stream-scoped private-media headers, URL/name binding,
segment requests and reconnects separately. HLS/MPEG-TS credential carriage
also needs its own evidence. Preserve native prefix/except/only semantics,
local precedence, revision fencing, inventory leases, capacity limits and
relay-loop prevention during integration. Missing provenance must not be
interpreted as proof that a stream is locally owned.
