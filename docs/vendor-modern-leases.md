# Modern configured-source freshness leases

`SourceAuthority::leased_modern_inventory(&local_id, lifetime)` adds an opt-in
local freshness bound to [modern configured-source discovery](vendor-modern-source-binding.md).
The authority must have been constructed with `new_modern`. This library API
adds no daemon routing, source configuration field, UI option, dependency or
background task. Existing unleased modern and strict legacy APIs are unchanged.

## Independent timing and ordered refreshes

The caller supplies an already validated `ObservedServerId` representing its
current authoritative local identity and a lifetime in `(0,30 seconds]`.
The authority's modern wire budget remains fixed at construction and is separate
from the lease lifetime. For example, a 10 second wire budget and a 3 second
lease can complete network discovery after the lease has expired; that result
is rejected. Successful completion never restarts or extends the lifetime.
Pending network work remains bounded by the existing wire budget; expiry is
checked at result/publication boundaries and does not spawn a cancellation timer.

A valid polled refresh reserves an opaque turn under the existing source
publication lock and captures the current profile plus `Instant::now() + lifetime`.
This immediately supersedes every previous leased result from the authority.
Concurrent attempts are ordered by reservation, not completion. Invalid lifetime,
removed source and wrong discovery mode reject before reservation or network I/O.
An unpolled future has no effect. A valid direct-self attempt reserves a turn
before its remote identity rejection, just like any other failed refresh.

The internally prepared modern client then performs its complete config / cursor
pages / config sequence. The fixed endpoints, per-page identity check, independent
TLS trust, configured Cluster authentication, body/page/row/membership limits and
whole-refresh/per-request deadlines remain unchanged. No source lock spans an
await, no detached request is started, and no partial observations escape.

After either success or failure, the same source lock checks, in order:

1. The captured revision and exact profile identity.
2. The newest reserved refresh turn.
3. Strict monotonic expiry (`now < expires_at`).
4. The wire result.

The checks make stale/superseded/expired local work take precedence over delayed
upstream errors. Complete bound facts are constructed under this lock and expiry
is checked again before returning. A failed or cancelled attempt never restores
an older lease. A new successful leased refresh is required for publication.
Valid source replacement or removal clears the turn and invalidates old profiles;
invalid source preparation changes nothing. Re-adding equal settings does not
revive a previous result or change the immutable modern mode and wire budget.

## Opaque facts and publication

Only successful leased modern discovery constructs `ModernLeasedInventory`.
It exposes sorted literal `names()`, the untrusted observed `server_id()` and the
process-local `expires_at()` deadline. Inspection can continue after expiry.
`candidate(name)` returns `ModernLeasedCandidate` only through the existing
local-origin, membership-aware policy, prefix/exclusion/selection and complete local
viewer-policy checks. Relayed and disabled rows remain ineligible. Unknown
membership is accepted only under the modern configured-origin
policy-equivalence rule; group-dependent or
unresolved policy still fails. Remote keys, inputs and policy never override local configuration.

Both types have private fields, no public constructor/importer and no Debug or
serialization. Plain observations, unleased bound candidates, legacy identity
candidates and copied deadlines cannot manufacture a modern lease grant.
Retained facts may create a late candidate, which still must pass publication.

`at_modern_leased_current(candidate, closure)` holds the source publication lock
across exact-profile, newest-turn and expiry checks, configured media-input
construction and synchronous closure entry. Expiry is checked before construction
and again before entry. Source changes and refresh reservations cannot interleave
with this closure. Generic `LeaseFailure` diagnostics reveal no credentials,
endpoints, stream names, raw identities or upstream bodies.

Never await or re-enter the same authority inside the closure. Expiry after entry
does not interrupt it or revoke an exported input, media worker or admitted
viewer. Asynchronous authorization/activation must re-enter a current fence;
worker/session lifetime and global configuration ordering remain caller duties.
Unleased `modern_inventory` and `at_modern_current` neither supersede nor extend
a lease; adapters requiring freshness must consistently choose the leased path.

## Qualification and remaining integration gates

Owned loopback regressions exercise actual authenticated modern discovery and
M4S MP3 media, strict expiry and time spent refreshing, longer-than-legacy wire
refresh, overlapping held successes/errors, immediate supersession, failure and
cancellation, invalid/wrong-mode/unpolled calls, source edits/removal/re-addition,
cross-authority candidates, unleased isolation, projection eligibility and
publication serialization. Existing modern HTTP/TLS binding and legacy lease
regressions run alongside them. Live read-only qualification is recorded
separately and opens no private media when candidate eligibility is absent.

This is a local publication validity rule. It is not a remote lease, atomic or
signed snapshot, source ownership/relay-loop proof, private-endpoint identity,
viewer authorization, global configuration fence or worker/session revocation.
Authoritative local-ID/alias coordination, relay and membership policy, topology,
resolver/activation/load balancing integration and viewer lifetimes remain gates
before enabling vendor daemon routing. No official Flussonic component is used.
