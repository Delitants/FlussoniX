# Local viewer policy for future Flussonic sources

`source_directory::flussonic_policy::PolicySet` is an independent library gate
for explicit, locally configured source/group viewer policies. The source
authority and opt-in coordinator use it; automatic discovery selection,
the resolver, worker activation and the UI do not.
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
Null values, unresolved/duplicate backend names, unsupported
country/domain restrictions, `soft_limitation`, `extra`, protocol restrictions
and arbitrary fields are rejected. No partial result is returned. The existing
`PolicySet::new` rejects template references. Templates require the explicit
adapter described below.

Named backends accept exactly `name` and an HTTP(S) `url`. Backend callbacks
cannot recursively refer to another backend or include unsupported backend
rules. There are at most 128 local group configurations, 128 local backends
and 64 unique memberships per stream. Names use existing name validation.
Each policy object and backend entry is limited to 16 KiB serialized JSON.
Errors contain only generic enum variants, with no callback URL, credential,
policy body or group name. `PolicySet` has no Debug/serialization interface.

## Explicit local templates

`PolicySet::with_templates(source, groups, templates, backends)` accepts an
additional map of local template names to policy-only objects. Each template
accepts the same two policy fields as a source layer. Source and group layers
may additionally name one template with a literal `template` string. Template
defaults apply first, then direct fields replace those defaults. The resulting
group fields override source fields under the existing conflict rules.
Callbacks remain atomic; replacing a callback never clears a separate token
guard. An empty template supplies no authority. Public access still requires
the explicit `"true"` callback value; a token-only policy remains protected.

All template definitions validate, including unused ones. Missing references,
invalid names, null values and malformed direct settings fail even when a later
layer could override them. Nested templates are unsupported, so neither cycles
nor recursive backend/template resolution can enter the policy. There are at
most 128 templates, each limited to 16 KiB, with existing name validation.
Templates cannot contain stream inputs, transcoder settings, protocol/domain
restrictions or any other field that this policy gate cannot enforce. These
semantics are a conservative local subset, not full vendor template parity.

`SourcePreparation::with_policy_templates(source, groups, templates)` validates
these inputs against the already captured local backend definitions. Only after
complete validation does it materialize the source/group layers and pass them
through the existing strict source adapter. No template reference survives into
the source profile. Input mutation cannot change an existing preparation.
Capture/prepare/install and publication retain the coordinator's configuration,
identity, generation and lease fences. A new policy needs a new ordered
installation; any global configuration edit invalidates old preparations and
candidates. Direct source-authority constructors and `with_policy` retain their
previous strict rejection of template references.

These policy-only maps are explicit embedding inputs. The adapter does not
implicitly extract policy from remote inventory or the daemon's general-purpose
stream templates, and it does not persist them or expose new UI settings.
Discarding unsupported fields from a complete stream template to fit this API
would lose rules and is not a supported integration.

## Qualification and integration limits

`cargo test --locked --test vendor_viewer_policy -- --test-threads=1` covers
inheritance, atomic replacement, equivalent/conflicting groups, strict
rejection, bounds and real native authorization against an owned callback.
`vendor_policy_templates` additionally exercises template inheritance, complete
validation, collection bounds and real callback/token/session enforcement.
`vendor_coordination` checks frozen template inputs, ordered replacement and
global template-edit invalidation against owned HTTP inventory servers.

The gate is immutable after construction; it does not acquire configuration
revision authority. A future resolver must bind it to the current local
configuration revision, revalidate after asynchronous work, and preserve local
stream precedence, leases, content identity and failover policy equality.
The inventory library returns explicit group membership facts separately from
omission; live sender encoding and daemon integration remain unqualified.
Discovered-name provenance and relay-loop eligibility remain separate requirements. Viewer authorization must precede
private media requests, and daemon pulls must retain subtitle options.
Live end-to-end vendor viewer authorization, aliases/name decoding and HLS/TS
private credential carriage remain unqualified. Private-media acknowledgment is
qualified separately in [endpoint checks](vendor-endpoint-checks.md) and is not
viewer authorization. Vendor routing stays disabled.


The [inventory library](vendor-source-inventory.md#local-origin-candidate-preflight)
preserves validated explicit memberships separately from omission and offers a
conservative local-origin candidate check. The source authority described below
connects these libraries under its own fence. A future daemon adapter must also
require current global authority and qualify provenance/loops before publishing
the resulting local policy. Remote
policy fields remain ignored, and vendor routing remains disabled.

## Next daemon integration

Persisted operator settings must capture complete supported local policy,
template and backend inputs under one configuration revision. Validate complete
objects before projection; never treat unknown membership or missing policy as
public. Preserve local stream precedence and source selection/exclusion rules.
The current explicit adapter is available without changing the native schema.

The [explicit direct-source admission adapter](vendor-viewer-admission.md) now
uses the existing authorizer before private-media handoff and rechecks candidate
fences after callbacks. Automatic daemon admission must use that same authorizer
before opening private media.
Because callbacks await, recheck configuration, source generation, lease,
content identity and equivalent failover policy after admission and immediately
before activation. Do not hold publication locks across an await. Configuration
replacement must retire affected workers and sessions; copying a policy out of
a synchronous publication closure does not authorize later reuse. Worker lifetime
integration, relay admission, friendly persisted controls and automatic vendor
source activation remain pending.


The [source configuration authority](vendor-source-authority.md) now validates a
complete supported local source/group profile before policy projection and binds
inventory candidates to immutable configuration identity/revision. It remains
independent of the daemon's global configuration and does not establish verified
provenance, leases, topology or live interoperability.
