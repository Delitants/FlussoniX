# Vendor management identity preflight

`SourceAuthority::identity_inventory(local_id)` adds an opt-in direct self-source
sanity check to [configured-source binding](vendor-source-binding.md). It remains
an independent library API. The native daemon, configuration schema, resolver,
activation and UI do not select it. No official vendor component is a product
build, test or runtime dependency.

## Reference basis

Read-only static inspection of the installed 26.04.1 reference found:

| Inspected function | Behavior relevant to this increment |
| --- | --- |
| `apiv3_peer_protocol:fetch_remote_server/2` | Requests `/streamer/api/v3/config` and reads `stats.id` into the remote server record. |
| `remote_server:is_server_remote/1` | Compares that server identity with `cluster_app:our_server_id/0`. |
| `cluster_app:our_server_id/0` | Uses the configured cluster server ID, with the local server ID as default. |
| `apiv3_peer_protocol:streams_list/3` | Requests `/-/streams`; the inspected stream parsing does not consume a transitive hop path. |

The installed public schema describes the server runtime ID as a UUID. A retained
read-only demo inventory observation contains `next`, `streams`, `timing` and
`estimated_count`, without a server ID. We therefore do not require or invent an
identity field in inventory pages. The public [cluster restreaming manual](https://flussonic.com/doc/fms/cluster/restreaming/)
describes source discovery and local precedence; it does not establish that
`named_by: config/user` proves an input dependency is loop-free.

These are observations of the inspected reference, not live Cluster-auth or
end-to-end topology qualification. Reference modules were treated as inert data;
no vendor code was loaded/executed, files changed or production host contacted.

## Request and publication contract

The caller supplies its authoritative current non-nil local UUID. It must match
the identity used for the surrounding cluster; an arbitrary or stale caller ID
cannot establish self-source detection. Local identity/global configuration
coordination remains a future daemon responsibility.

The operation captures the current source profile and performs, in order:

1. Authenticated GET of the configured management root plus `/streamer/api/v3/config`.
2. Complete existing paginated `/-/streams` inventory.
3. A second GET of that same configuration endpoint.

Both probes require HTTP 200 and a non-nil, hyphenated 36-character UUID at
`stats.id`. Letter case is immaterial. Missing/malformed IDs, a self ID or an
observed change between probes fail. A nil local ID fails before network I/O.
A failed first probe never fetches inventory; an invalid inventory never exposes
partial facts or proceeds to the second probe. There is no alternate identity
field, endpoint, credential, transport or fallback probe.

All requests share the existing 750 ms operation deadline and **one 1 MiB body
budget**, including both configuration bodies and every inventory page. Existing
row/page/cursor/membership limits apply. Configuration response fields other than
`stats.id` are discarded, never imported as source, viewer policy or topology.
The same prepared management client preserves explicit proxy prefixes, frozen
verified TLS trust, no proxies/redirects and per-request hashed Cluster credentials.
Private media retains its independently configured origin/trust and distinct
stream-scoped authentication.

Only complete successful discovery constructs an opaque `IdentityInventory`.
`server_id()` exposes the observed UUID, and `names()`/`candidate()` delegate to
the existing literal-name, local-origin metadata preflight, filters and local
policy projection. Candidates cannot be imported or substituted with a plain
`BoundCandidate`. `at_identity_current` constructs private media under the same
current-profile publication lock. Replacement/removal/cross-authority and stale
success/error behavior are preserved. Cancellation drops the directly awaited
probe or inventory future; no detached work is created. Diagnostics remain generic.

The original `inventory()` and `at_bound_current` APIs retain their existing
behavior. Callers explicitly choose identity-observed discovery; there is no
silent network expansion or new daemon source mode.

## What remains unproved

This is a negative sanity check, **not an origin attestation or loop-free route
grant**. Two separate probes are not an atomic remote snapshot: a load balancer
can serve different backends between them, or a server can change and change
back. An HTTP response is unsigned; request authentication does not authenticate
its content. HTTPS verifies the configured endpoint certificate, not its input
configuration or honesty. The explicit private-media endpoint may identify a
different server. Identity may change after discovery, and no lease is created.

A configured stream may itself pull another CDN, including this node, despite a
non-self management ID and `named_by: config`. Multi-hop dependencies, aliases,
NAT/proxies, input failover and changing runtime configuration still need actual
topology eligibility and live reference qualification. Viewer authorization,
fresh fences after async work, worker/session revocation and global ConfigStore
coordination remain required. Vendor daemon routing stays disabled.

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


[Private endpoint checks](vendor-endpoint-checks.md) now qualify affirmative
media credential acknowledgment. Playback IDs and the acknowledgment marker do
not bind the private endpoint to the observed management server ID.
