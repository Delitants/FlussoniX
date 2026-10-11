# Local configuration authority for vendor source candidates

`source_directory::flussonic_config` connects the independent inventory and
local viewer-policy libraries through a validated, immutable source profile.
It is a library building block. The daemon's configuration schema, resolver,
activation, UI and native source behavior remain unchanged. No vendor
executable or library is needed to build, test or run it.

## Complete supported profile

`SourceAuthority::new(source, auth_backends)` validates every supplied field
before extracting any policy layer. It accepts a complete object in this
explicit supported subset, not arbitrary Flussonic configuration or a native
FlussoniX source object after normalization. The only source fields are:

| Field | Contract |
| --- | --- |
| `url` | Required `m4f`, `m4fs`, `m4s` or `m4ss` server root; existing native URL identity grammar, without embedded credentials, stream path, query or fragment. |
| `cluster_key` | Required explicit nonempty UTF-8 legacy key, at most 4096 bytes; no global/environment fallback. Native source key validation is unchanged. |
| `prefix` | Optional valid literal namespace, without wildcards. Names are selected without renaming. |
| `only`, `except` | Optional arrays of at most 1024 exact names or `prefix/*` patterns, each at most 256 UTF-8 bytes. Existing native visibility/selection semantics apply. |
| `on_play`, `flussonix_token_sha256` | Explicit local policy fields supported by the [policy gate](vendor-viewer-policy.md). |
| `group_config` | Optional map of local group names to complete supported group objects. |

A group object permits only `name`, `on_play` and `flussonix_token_sha256`.
An optional `name` must equal its map key exactly; it is validated before being
removed from the policy projection. Empty groups inherit. All groups and named
backends validate, even if no current stream matches them. Source policy
validates even if a group would override it. Authentication backends retain the
policy gate's strict array of local `name`/HTTP(S) `url` entries.

Unknown fields and unsupported rules fail rather than being discarded. This
includes templates, `extra`, country/domain/protocol restrictions, `cache`,
`dvr`, `segment_duration`, `drain`, statistics, native normalized fields,
management/private endpoint overrides and custom CA fields. These features need
explicit implementation before this profile can accept them. No endpoint is
contacted and no transport is selected by this configuration library.

The combined serialized JSON value
`{"source": source, "auth_backends": auth_backends}` is limited to 1 MiB.
Each complete group object is limited to 16 KiB before projection. The policy
gate separately enforces its 16 KiB source/group policy and backend entry limits,
128 configured groups/backends and 64 unique memberships. Exact limits are
accepted. Inputs are parsed `serde_json::Value` objects; this is not a raw JSON
importer and does not introduce duplicate object-member detection.

## Candidate binding and source filters

`snapshot()` obtains the current immutable profile and its authority-local
revision. A snapshot can outlive edits and asynchronous inventory requests.
`snapshot.candidate(row)` revalidates the public inventory facts through the
[local-origin preflight](vendor-source-inventory.md#local-origin-candidate-preflight),
then applies the source namespace/exclusions and composes local viewer policy
using explicit memberships. Missing memberships never become an empty list.
Missing local policy or conflicting groups fail. Remote policy/input fields
never supply local authority.

Candidate names remain literal and unchanged. Prefix matching does not accept
the namespace's parent or a sibling prefix. Exclusions are relative to a
configured namespace. `only` affects static selection, not on-demand visibility:
an empty list keeps valid candidates visible but selects none eagerly.
On-demand inventory rows are never marked for eager activation. These are
candidate facts only; no worker is started.

## Publication fence

A candidate keeps its originating immutable profile identity and revision.
`authority.at_current(&candidate, |view| ...)` invokes a synchronous closure only
while that exact profile is still current, holding the authority lock throughout
the closure. The view contains the literal source/name, borrowed native policy,
static/on-demand kind and static-selection flag. Candidate policy is otherwise
private. The closure is the place for an eventual atomic publication operation;
perform network work before it, and never await or re-enter the same authority
from inside it.

`replace(source, auth_backends)` validates the complete replacement before
committing it under the lock. Invalid edits leave current authority unchanged.
Every valid replacement advances the local revision and creates a new profile
identity, even if its bytes are identical. Source and backend changes invalidate
old candidates and delayed old snapshots equally. `remove()` invalidates current
authority; re-adding identical settings cannot revive previous candidates.
Candidates cannot cross authority instances with equal configuration/revisions.
Replacement is unconditional: callers remain responsible for ordering edits and
fencing any asynchronous configuration load before calling it.

Stale publication never calls the closure. A publication panic propagates to the
caller and poisons this authority; later operations return `Unavailable` rather
than publishing or panicking again. Recovery requires constructing a new
validated authority. Errors contain only generic variants. Authority, snapshots,
candidates and views have no Debug/serialization implementations; native `Policy`
and its existing diagnostic/session behavior are unchanged.

## Integration limits

This fence governs this independent authority object. It does not acquire the
daemon's global configuration revision, revoke already published viewer sessions,
stop workers, establish inventory leases or verify a loop-free origin. Config/user
provenance is still an unverified claim. A future daemon adapter must coordinate
replacement/removal with the native configuration fence, retain local stream
precedence, capacity, leases, content/failover identity and source ordering, and
recheck all authority after asynchronous work. The intended lock order is global
configuration before this authority. Copied policy values cannot substitute for
a fresh publication check.

Live positive/negative Cluster authentication, sender group/provenance encoding,
aliases, actual relay-loop prevention, template/group parity, endpoint/trust
binding and HLS/TS credential carriage remain qualification gates. Viewer
permission must precede private media requests, and workers must retain subtitle
preserve/convert/drop behavior. Vendor routing remains disabled.
