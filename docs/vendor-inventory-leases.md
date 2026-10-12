# Local vendor inventory leases

`SourceAuthority::leased_identity_inventory(local_id, lifetime)` extends the
[identity preflight](vendor-source-identity.md) with opt-in local freshness and
refresh ordering. This independent library API is not selected by the daemon.
It adds no official vendor dependency, configuration field, network endpoint or
background task. Existing unleased APIs keep their behavior.

The purpose is to stop retained discovery results from authorizing new local
publication indefinitely. This is a FlussoniX local validity rule, not a lease
negotiated with a vendor server or a claim of remote topology/freshness.

## Discovery and refresh ordering

The caller supplies its current authoritative non-nil local UUID and a lifetime
greater than zero and at most 30 seconds. This cap matches the existing native
source inventory lifetime. Invalid lifetimes and nil IDs fail before I/O and do
not supersede a current lease. A future that has never been polled changes nothing.

A valid attempt reserves a new opaque turn under the source publication lock,
capturing that exact source revision/profile and a monotonic expiration time.
The lifetime starts at reservation **before** network I/O. The new turn
immediately supersedes all previous leased results from that authority, even
when rows/settings are identical. Concurrent attempts are ordered by this
reservation; the latest reservation wins, regardless of completion order.

Discovery then directly awaits the existing fixed-origin config / complete
inventory / config sequence. Its aggregate 750 ms deadline, 1 MiB body budget,
page/row/membership limits, TLS, authentication and identity checks are unchanged.
No publication lock is held across network I/O and no detached fetch is spawned.
A failed or cancelled attempt does not restore the old turn. The caller must
perform another successful leased refresh before it can publish again.

Both successful and failed discovery pass, in order, through:

1. The source revision and exact profile identity fence.
2. The newest reserved turn check.
3. The strict monotonic expiration check (`now < expires_at`).
4. The captured inventory result.

Local fence errors take precedence over an old upstream error. Complete facts
are constructed while holding that same lock, and expiration is checked again
before returning them. Replacement/removal invalidates old profiles and clears
the current turn; failed configuration validation leaves existing state intact.

## Opaque results and guarded publication

Only successful leased discovery constructs `LeasedIdentityInventory`.
`names()` and `candidate()` retain the existing literal-name, metadata preflight,
source-filter, static-selection and local viewer-policy checks. An old result
may remain inspectable and form an old candidate; neither operation grants
publication. Private fields and absence of serialization prevent importing or
manufacturing a leased candidate through the public API.

Both result and candidate expose the observed management UUID and their
process-local `Instant` deadline. The UUID remains a remote claim and the
`Instant` is not a portable timestamp. A copied deadline does not renew a lease.

`at_leased_current(candidate, closure)` holds the source publication lock while
checking the current profile, refresh turn and expiry, constructing media through
the same prepared private origin, and entering the synchronous closure. Expiry
is checked before construction and again before closure entry. Source changes
and refresh reservations cannot interleave with publication. Never await or
re-enter this authority in the closure.

`LeaseFailure` contains generic `InvalidLifetime`, `Superseded`, `Expired` and
wrapped existing `BindingFailure` diagnostics. No endpoint, credential, stream
name or upstream response body is included.

## Limits and adapter responsibilities

This guard controls **entry into a local publication closure**. Expiry after
entry does not interrupt the closure, revoke an exported `MediaInput`, cancel
old-profile I/O, stop a worker or disconnect an admitted viewer. Callers must
coordinate those lifetimes separately and re-enter a current fence after
asynchronous authorization/activation work. Expiration is checked on use; it
creates no timer and does not automatically free caller-retained results.

The unleased `inventory`, `identity_inventory`, `at_bound_current` and
`at_identity_current` APIs neither renew nor supersede a lease. They remain
explicit unleased modes, not alternate leased admission paths. A future daemon
adapter must consistently choose the leased path when it requires this guard.

The remote server may change its inputs, rows or identity at any time, including
between the two identity probes. A local time limit does not attest those facts,
make the remote snapshot atomic, prove the private-media server identity, or
prevent multi-hop relay loops. Live vendor authentication/encoding, topology
eligibility, caller/local identity coordination and daemon global configuration,
resolver/activation/viewer-session integration remain qualification gates.
Vendor daemon routing stays disabled.

Owned loopback regressions cover publication before/after expiry, expiration
during discovery, concurrent held success/error ordering, immediate supersession,
failed/cancelled refreshes, invalid parameters, cross-authority/source-edit
fencing, publication serialization against source replacement, actual configured
M4S media and unchanged unleased APIs. No live vendor
endpoint or production configuration is used by these tests.


## Explicit modern management observations

The opt-in [modern inventory client](vendor-modern-inventory.md) supports bounded
configured custom IDs and UUID aliases, modern selected stream metadata and one
explicit whole-refresh budget up to 30 seconds. It checks config identity around
the complete cursor chain and in every page. Missing memberships and relay claims
remain facts, not route or viewer grants. The strict legacy client and existing
source-authority/lease APIs keep their contracts; vendor daemon routing stays
disabled pending authority, relay/topology, policy and global integration.
