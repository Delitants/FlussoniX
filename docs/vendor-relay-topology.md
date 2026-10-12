# Explicit configured relay paths

## Design and scope

Extend the opt-in daemon-owned modern coordinator with explicit local relay paths.
Each path names 2..8 configured source keys, their expected observed server IDs,
and literal stream names. The first name identifies the path and the last hop is
the declared origin. At most 64 paths are installed atomically. Repeated source
keys, semantic server IDs (including UUID aliases), local identities, duplicate
entry names and invalid fields are rejected. Names at adjacent hops may differ
only through the explicit local path; there is no inferred rewriting or URL
following. A valid edit, including an equal edit or removal, replaces the route
generation. Invalid edits preserve it.

Every supplied inventory must come from the matching installed source generation
of this coordinator and have the pinned observed identity. Intermediate rows must
report remote provenance; the terminal row must report config/user provenance.
All rows require enabled state, explicit memberships, source namespace/exclusion
selection and independently supplied local viewer policy. Missing or unresolved
facts never become public permission. Normal candidates still reject remote rows.

An opaque relay candidate retains every hop's source handle, latest-refresh ticket,
start-based deadline, local policy and the exact installed path generation.
Publication holds config -> coordinator -> sources (sorted source key) through
one synchronous closure. Every profile, source generation, identity epoch,
configuration revision, route generation, refresh turn and expiry is rechecked;
only the entry source's private media input is constructed. Its policy is exposed
without replacing intermediate policy checks or granting viewer admission.
No lock crosses network I/O. The caller must never await or reenter these owners
in the closure. Exported media inputs/workers/sessions require later fences.

This validates a locally asserted path against leased management observations.
It does not attest actual remote inputs, ownership epochs, content equivalence,
an atomic multi-server snapshot, or the identity of a separate private endpoint.
Remote IDs and provenance remain untrusted claims. The local operator asserts
the edges; neither inventory fields nor matching names invent an edge. Automatic
vendor routing stays disabled until those integration contracts are qualified.
Native routing, API schema, UI, authentication, media and subtitle behavior stay
unchanged. No vendor runtime component is used.

## API

`RelayHop::new(source_key, expected_server_id, literal_name)` creates a bounded
local declaration. `RelayRoute::new(hops)` validates path structure.
`ModernSourceCoordinator::replace_relay_routes(routes)` atomically installs local
declarations; they are not persisted or exposed in the operator UI yet.
`relay_candidate(entry_name, ordered_inventory_refs)` binds complete observations.
`publish_relay(candidate, closure)` fences the entire path. The candidate exposes
only the declared observed origin ID, hop count and minimum lease deadline.
Construction can retain expired or superseded inventory facts; they can never
pass publication. No inspection or candidate construction renews a hop's lease.

Configured paths may survive ordinary global edits, but every old candidate and
inventory is fenced by those edits. Fresh handles/observations are required.
Alias edits invalidate source handles and old candidates as before. Route edits
invalidate relay candidates without revoking ordinary direct-origin candidates.
The embedding operator must resupply local paths and viewer policies after restart.

## Verification and remaining work

Behavior-first regressions exercise positive two/eight-hop paths, explicit name
mapping, actual entry-only private MP3 input, policy/filter/disabled/unknown gates,
UUID/custom identity semantics, repeated/local identities and bounds. They also
exercise global edits, aliases, source replacement, route edits/removal, newest
refreshes, failed/cancelled refreshes and every hop's expiry. A bounded publication
test observes concurrent route/config/refresh updates blocking until closure
return; it is not OS scheduler attestation. Preserve legacy/direct APIs.

Full CI and an independent branch review precede the guarded preview upgrade.
Owned fixtures require no reference service. Actual vendor relay ancestry,
private-endpoint relation, full policy/template semantics, daemon activation and
async admission/worker/session lifetime remain integration gates.

Reference context: [Flussonic cluster restreaming](https://flussonic.com/doc/fms/cluster/restreaming/).
