# Local viewer policy for future Flussonic sources

`source_directory::flussonic_policy::PolicySet` is an independent library gate
for explicit, locally configured source/group viewer policies. It is not wired
to discovery, source configuration, the resolver, worker activation or the UI.
Native FlussoniX source selection and viewer-policy semantics are unchanged.

Flussonic documents source-local `on_play` in its
[cluster restreaming settings](https://flussonic.com/doc/fms/cluster/restreaming/#extra-settings).
Read-only inspection of the installed reference separately identified
source settings followed by group-map overlays, with whole-field replacement.
Inventory receipt and privileged media authentication do not establish viewer
permission. No reference module is needed at build, test or runtime.

## Explicit inputs and composition

Construct a `PolicySet` from a source policy object, a map of local group names
to policy objects, and an array of local named authentication backends. These
are policy inputs, not complete source, stream or inventory objects. Passing
extra fields is an error. A future configuration adapter must validate its
complete input before extracting these layers; discarding unknown security
fields to make a projection pass would defeat the gate.

Each source/group layer accepts only:

- `on_play`: an HTTP(S) callback URL, `auth://` reference to a local named
  backend, or the native structured callback with `url`, `session_keys` and
  positive `max_sessions`.
- `on_play: "true"`: an explicit public callback decision. This special value
  is accepted only as the whole string, not a boolean or structured URL.
- `flussonix_token_sha256`: the existing independent local token guard. It
  remains in effect when only the callback field is overridden.

For a stream, `for_groups` requires an explicit array of literal group names.
An empty array means known absence of memberships; null/missing metadata must
not be turned into an empty array by a future adapter. Unconfigured groups do
not contribute settings. A configured group field replaces the corresponding
source field. `on_play` is atomic: a replacement callback does not retain the
old callback's session keys or limits. Token guards are a separate field.

Matching groups must agree on every field they both override, after native
policy normalization and local backend resolution. Equivalent URL/string and
structured-default callbacks can agree. Different callbacks, session keys,
limits, or token hashes cannot; conflicts fail regardless of membership order.
Independent group fields compose. This deliberately rejects order-dependent
reference configurations rather than guessing which group should win. It is a
conservative supported subset, not a claim of complete vendor precedence.

An empty source/group policy is inheritance, not an explicit public grant.
If no matching layer supplies a callback or token guard, resolution fails.
Explicit public access requires `"true"`; a token-only policy is protected
local authority. Returned policies use the existing native `Policy` and
`PlaybackAuth`, including callback denial, token checks and session limits.

## Rejection and bounds

All configured layers and backends validate before construction succeeds,
including unmatched groups and source values later overridden by a group.
Null values, unresolved/duplicate backend names, templates, unsupported
country/domain restrictions, `soft_limitation`, `extra`, protocol restrictions
and arbitrary fields are rejected. No partial result is returned. Template
inheritance is intentionally unsupported here; it needs its own qualified
local adapter rather than a default that could remove protection.

Named backends accept exactly `name` and an HTTP(S) `url`. Backend callbacks
cannot recursively refer to another backend or include unsupported backend
rules. There are at most 128 local group configurations, 128 local backends
and 64 unique memberships per stream. Names use existing name validation.
Each policy object and backend entry is limited to 16 KiB serialized JSON.
Errors contain only generic enum variants, with no callback URL, credential,
policy body or group name. `PolicySet` has no Debug/serialization interface.

## Qualification and integration limits

`cargo test --locked --test vendor_viewer_policy -- --test-threads=1` covers
inheritance, atomic replacement, equivalent/conflicting groups, strict
rejection, bounds and real native authorization against an owned callback.

The gate is immutable after construction; it does not acquire configuration
revision authority. A future resolver must bind it to the current local
configuration revision, revalidate after asynchronous work, and preserve local
stream precedence, leases, content identity and failover policy equality.
The inventory library returns explicit group membership facts separately from
omission; live sender encoding and daemon integration remain unqualified.
Discovered-name provenance and relay-loop eligibility remain separate requirements. Viewer authorization must precede
private media requests, and daemon pulls must retain subtitle options.
Live positive/negative vendor authentication, aliases/name decoding and HLS/TS
private credential carriage remain unqualified. Vendor routing stays disabled.


The [inventory library](vendor-source-inventory.md#local-origin-candidate-preflight)
preserves validated explicit memberships separately from omission and offers a
conservative local-origin candidate check. The source authority described below
connects these libraries under its own fence. A future daemon adapter must also
require current global authority and qualify provenance/loops before publishing
the resulting local policy. Remote
policy fields remain ignored, and vendor routing remains disabled.


The [source configuration authority](vendor-source-authority.md) now validates a
complete supported local source/group profile before policy projection and binds
inventory candidates to immutable configuration identity/revision. It remains
independent of the daemon's global configuration and does not establish verified
provenance, leases, topology or live interoperability.
