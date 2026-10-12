# Explicit modern vendor discovery

`source_directory::flussonic::modern::ModernInventoryClient` is an independent,
opt-in management client. It addresses configured custom server IDs, modern
inventory metadata and background refresh latency without changing the strict
legacy client or its source-authority/lease APIs. No official Flussonic component
is a build, runtime or fixture dependency.

The client returns complete discovery observations. It does not construct a
source-authority candidate or private-media input, admit a viewer, grant a lease,
activate a worker, or enable vendor daemon routing.

## Fixed management profile

Construction takes an explicit HTTP(S) root, privileged Cluster key, optional CA
file and refresh budget. The root may contain a trusted proxy prefix. Existing
origin/key/CA validation, verified TLS, prepared trust, no-proxy/no-redirect
behavior and legacy Cluster request digest are shared with the strict client.
Credentials do not enter URLs, diagnostics or returned facts. A CA replaces
public roots, requires HTTPS and is captured when the client is prepared.

A refresh performs:

1. GET the configured root's `/streamer/api/v3/config`; read `stats.id`.
2. GET `/streamer/api/v3/streams` with fixed
   `select=name,named_by,groups,static,disabled` and `limit=100`, following the
   complete cursor chain at that same endpoint. Each page must supply a matching
   `server_id`.
3. GET the same config endpoint again and require the same observed identity.

A cursor is one opaque query value, never a navigation target. There is no
fallback to `/-/streams`, a different host, another credential, or another API
profile after a denial, timeout, redirect or malformed response. Unknown response
fields, including upstream inputs and remote viewer policy, are discarded.

## Observed identity

`ObservedServerId::parse` accepts nonempty UTF-8 up to 256 bytes. Whitespace-only
values, embedded control characters and nil UUID aliases are rejected. Trailing
CR/LF from configured identity files is supported.

Recognized UUID spellings, with optional trailing CR/LF, compare as UUIDs. This
covers hyphenated, simple, braced and URN forms and letter case, preventing a
native direct-self check from being bypassed through those spellings. Other
custom IDs compare by exact original UTF-8 bytes: no trimming, case folding or
normalization is applied. Original text is available for deliberate inspection;
there is no automatic Debug, Display, header/URL conversion or serialization.

The caller supplies its authoritative current local identity. The first self
claim is rejected before inventory requests. A changed page or final config
identity rejects the entire refresh. These HTTP observations are not signed
identities, atomic remote snapshots, proof of source ownership, relation to a
separate media endpoint, or a transitive topology/loop attestation. Configured
custom aliases and global local-ID coordination remain caller responsibilities.

## Time and memory bounds

The caller explicitly chooses one whole-refresh budget in `(0, 30 seconds]`.
Every request, including connection and response body, has a timeout of
`min(5 seconds, refresh budget)`. All config probes and inventory pages share the
whole deadline and a final deadline check after synchronous bounded parsing.
This is a budget suitable for a caller's background refresh, not a scheduler;
the library starts no background task. Dropping the future cancels its pending
request without detached pagination.

All response bodies, including both config probes, share 1 MiB. Other complete
refresh limits remain 100 pages, 10,000 unique stream names, 4,096 bytes per cursor,
64 groups per stream and 10,000 memberships. Literal stream/group names keep the
existing validation and 256-byte bound. Cyclic/malformed cursors, duplicate names,
invalid rows, identity changes and exhausted limits reject the complete refresh.
No partial result is published. The parser consumes JSON values using the
existing JSON transport; no new duplicate-JSON-member defense is claimed.

A longer discovery budget does not extend any local lease. Existing strict
`InventoryClient`, `SourceAuthority::identity_inventory` and
`leased_identity_inventory` retain their 750 ms discovery budget and established
contracts. A future adapter must explicitly bind modern observations to the
current source/policy authority and coordinate refresh/lease timing.

## Membership, policy and relays

The shared parser preserves missing provenance/groups as unknown and explicit
empty groups as empty. It validates every row, including disabled, on-demand,
relay and external rows. Existing `local_origin_candidate` checks keep rejecting
remote/external claims, missing provenance and missing groups. The modern client
never manufactures missing memberships or turns absent policy into public access.

The authorized live reference's test stream is `named_by=remote`, consistent with
its CDN relay role. Supporting that topology needs explicit relay eligibility,
source ownership, hop/loop handling and viewer-policy rules. Those rules are not
provided by this observation client. Modern observations cannot be imported into
the existing opaque bound/leased candidate types.

## Qualification scope

Owned HTTP/TLS regressions cover selected fields, encoded cursor confinement,
custom/UUID identity handling, self/drift rejection, complete-chain validation,
shared time/byte/page/row/group budgets, cancellation, denial/redirect handling,
frozen TLS trust and unchanged legacy behavior. Live qualification uses bounded
read-only requests and records authentication, complete observations and remaining
policy/topology limitations separately. No production configuration is rewritten.
