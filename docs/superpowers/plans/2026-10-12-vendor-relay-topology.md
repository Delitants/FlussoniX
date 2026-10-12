# Explicit relay topology implementation

> Execute inline using executing-plans. Existing user authorization covers this
> increment, isolated worktree, tests, publication and preview upgrade.

**Goal:** bind explicit configured relay paths to every hop's leased inventory and
one globally ordered synchronous publication fence.

**Design:** [relay contract](../../vendor-relay-topology.md). Paths assert local
edges; observed IDs/provenance do not attest hidden remote input topology. Keep
automatic vendor routing disabled.

**Baseline:** 98a5179. Worktree `.worktrees/vendor-relay-coordination`; private
task records `.runtime/vendor-relay-coordination-records`. Preserve all prior
sealed archives. Test runner restores the accepted preview binary after builds.

## Task 1: explicit paths and leased publication

Implement `modern_coordinator::RelayHop`, `RelayRoute`, `RelayFailure`, opaque
`RelayCandidate`, atomic `replace_relay_routes`, `relay_candidate` and
`publish_relay`. Keep route generation separate from ordinary source handles.
Validate 2..8 hops and <=64 paths, exact literal names/source keys, semantic IDs,
local aliases, source/ID uniqueness and duplicate entry names. Reject unexpected
source identity/role, disabled/unknown/unresolved/excluded rows and cross-owner
use. Intermediate remote classification is crate-private, available only through
the all-hop coordinator; existing direct/local-origin gates remain unchanged.

Add a crate-private modern lease guard to hold all source locks simultaneously,
sorted by unique source key. Check all latest turns/deadlines before constructing
entry media and immediately before synchronous closure entry. Hold config and
coordinator locks throughout, with no network await. Never export a hop candidate
or refresh intermediate leases implicitly.

Write owned HTTP fixture regressions before the implementation; retain a genuine
behavioral RED against a compiling API scaffold. Verify current path, bounds,
literal mapping, entry-only media and all failure/invalidation cases. Run the
whole new suite plus related config/auth/source/binding/lease/coordinator suites,
formatter, all-target Clippy, library auth vectors and build. Mutation-check a
non-entry lease fence, restoring exact source before final checks.

## Task 2: qualification and release

Record immutable candidate and proofs; obtain one fresh independent review of
the whole baseline..HEAD range. Fix actionable findings with RED/GREEN evidence.
Publish the candidate branch and require exact-head full CI. Guard the preview
upgrade by unchanged launch identity/settings, install atomically with rollback,
verify HTTP/admin/config/credentials and native inventory. Push qualified main,
reap owned probes and clean only this merged worktree. Seal private evidence;
final verification is read-only.

## Review Focus

- A stale/expired/superseded non-entry hop must prevent closure entry and media I/O.
- No ordered list or remote metadata may bypass explicit route generation or
  substitute an inventory/source owner, semantic server identity or terminal role.
- Holding all source locks must preserve config -> coordinator -> source order;
  duplicate sources must never reach a recursive/repeated lock acquisition.
- Missing membership/policy and conflicting groups must not become public access.
- Explicit path assertions must not be described as attested actual remote
  ancestry, private endpoint identity, ownership epoch or content equivalence.
