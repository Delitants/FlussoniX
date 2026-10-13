# Modern discovery configuration coordination

This increment adds an opt-in library coordinator owned by the daemon. Native
routing remains unchanged: no vendor source is selected automatically.

## Contract

The daemon coordinator owns the same Arc<ConfigStore> as App, captures the actual
startup node name as its primary local identity, and accepts at most 16 explicit
aliases. Custom IDs compare exactly; UUID aliases compare semantically. Invalid
startup names leave this optional coordinator unavailable without changing native
startup acceptance. The advertised name is captured at startup too. Alias edits
validate completely before replacement, invalidate all handles even when equal,
and cannot change the primary name (restart required).

Capture source and backend settings with the global config revision. Prepare
clients outside locks. Install only under the same config revision and identity
epoch; installed handles are opaque, coordinator-specific, and replace prior
handles for that source even at the same config revision. App retains its public
configuration handle for Rust API compatibility; substituting that handle is not
an ordered configuration edit. The App coordinator accessor verifies the exact
store pointer and permanently retires the coordinator after observing a mismatch,
including if the original handle is subsequently restored. Existing borrowed
coordinator references prevent safe mutable access to App while in use. Use the
ConfigStore edit methods for ordered runtime changes, or create a new App for a
new store. At most 64 installed
sources. Config revisions, including unrelated edits and remove/re-add, fence
old preparations, handles, results, candidates, and both successful and failed
in-flight refreshes. Failed preparation/install cannot reactivate an old handle.

Only a matching derived flussonix_transport is removed from normalized settings.
All other settings must be understood by the strict source projection; native
routing fields such as source groups/drain/public destinations are unsupported.
No implicit peer key or policy is imported. Existing config schema is unchanged.
The schema has no vendor viewer-policy fields. A captured preparation therefore
has unresolved policy by default. The explicit with_policy step accepts only
local source/group policy objects and validates them against captured backends;
those settings are frozen into the same ordered prepared source generation.
Changing policy requires a new capture/prepare/install, with the same replacement
fences. This policy preparation step alone grants no viewer permission and is
not an operator UI.

The optional `with_policy_templates(source, groups, templates)` step supports
explicit local policy-only template defaults before direct source/group fields.
It validates every template and reference against captured backends, then
materializes the inherited layers through the same strict adapter. Nested or
unsupported templates fail; neither remote inventory nor general-purpose native
stream templates are implicitly imported. Frozen inputs and the same global
revision/generation fences apply. See [viewer policy](vendor-viewer-policy.md#explicit-local-templates)
for precedence, bounds and the pending daemon integration.

Explicit direct-source viewer admission is now available through `admit`,
`admitted_current` and `publish_admitted`, using the embedding owner's existing
native authorizer. It checks local stream precedence before policy publication
and rechecks all candidate fences after callbacks and before media handoff.
See [viewer admission](vendor-viewer-admission.md) for lifetime requirements and
the distinction between native grant revocation and source/configuration changes.
Automatic resolver activation, relay admission and worker ownership are pending.

Refresh uses the existing latest-turn, start-based (0,30s] lease; all local aliases
are checked at the first config probe before page requests. Publication holds
locks in config -> coordinator -> source order through a synchronous closure.
Never await or reenter any of these owners in that closure. No lock spans network
I/O. Exports, workers and sessions are not revoked after closure return; async
consumers must re-fence. This grants no relay, private endpoint identity, viewer
admission, ownership, or daemon routing permission.

## Library usage

The caller supplies trusted local policy, never policy copied from discovery:

```rust,ignore
let coordinator = app.modern_source_coordinator()?;
coordinator.replace_aliases(&["explicit-local-identity-alias"])?;
let captured = coordinator.capture("m4ss://origin.example")?;
let prepared = captured
    .with_policy(&local_source_policy, &local_group_policies)?
    .prepare(Duration::from_secs(10))?;
let handle = coordinator.install(prepared)?;
let inventory = coordinator.inventory(&handle, Duration::from_secs(20)).await?;
let candidate = inventory.candidate("channel")?;
coordinator.publish(&candidate, |view, input| {
    // Synchronous publication only; view.policy is not a viewer grant.
})?;
```

Prepare may read configured CA files; move it to a blocking executor when needed.
A new capture reserves the newest preparation turn even if later preparation
fails or is abandoned; the already installed current handle remains valid until
a successful installation or a global configuration/identity edit. No rollback
restores an older preparation. Policy objects are not persisted by this library
API, so an embedding owner must supply them again after restart. The current
operator UI does not expose this opt-in adapter.

## Implementation and verification plan

1. Record clean baseline and private preview rollback; work in an isolated branch.
2. Add behavior-first owned HTTP regressions and API scaffolding; observe RED.
3. Implement opaque capture/prepare/install/refresh/publication coordination,
   early alias rejection, and shared daemon configuration ownership.
4. Verify edits/remove-readd, same-revision install ordering, cross-owner misuse,
   failed preparation, alias bounds/UUID equivalence/custom exactness, held success
   and error, lease expiry/supersession/cancellation and synchronous lock ordering.
5. Publication serialization tests independently observe bounded config and alias
   mutation blocking; they do not attest OS scheduling internals.
6. Run focused and related Rust checks, formatter, all-target Clippy and build;
   obtain one independent read-only review. Publish qualified commit, run full CI,
   update the preview with original arguments/environment/settings, and reap owned
   tests/probes. Preserve records and leave vendor routing disabled.

## Review focus and deferred qualifications

Review lock order, opaque owner binding, all invalidation paths, errors after
in-flight edits, alias validation and early self rejection, normalized source
projection, bounds, no background task/resource leak, and native API compatibility.
Live cdn5 inventory may establish current management compatibility but cannot
establish the daemon's global identity or relay eligibility. Existing live rows
were all ineligible; private media, relay/loop prevention, full policy/template
parity, vendor resolver activation, runtime alias UI and mixed-vendor load routing
remain separate increments. No remote media is needed for these local ordering
contracts. CPU/GPU and subtitle behavior are unchanged.
