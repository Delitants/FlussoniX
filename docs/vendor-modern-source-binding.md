# Modern configured-source binding

`SourceAuthority::new_modern(source, backends, budget)` opts one independent local
source authority into the [modern management profile](vendor-modern-inventory.md).
It is a library API; daemon routing, configuration schema, resolver, activation
and UI do not select it. No official Flussonic component is a dependency.

## Prepared local profile

The constructor applies the existing complete source/group/backend validation
and prepares management and private-media clients internally from that profile.
It accepts no externally prepared client or imported discovery result. Existing
native source URL mapping supplies M4F/M4S transport and default origins; explicit
`api_url` and `private_payload_url` overrides preserve trusted proxy prefixes.
The explicit Cluster key and independent per-leg captured TLS CA rules remain
unchanged. No environment credential fallback or cross-leg trust fallback is
introduced.

The modern budget must be in `(0,30 seconds]`; the wire client's whole-refresh,
per-request and aggregate resource limits still apply. Mode and budget belong to
the authority and remain fixed across `replace`, `remove` and re-addition.
Replacement prepares new clients from newly validated settings and recaptures
trust outside the publication lock. Invalid edits leave current grants/settings
intact; every valid replacement creates a fresh profile identity, even when all
configuration values are equal.

`SourceAuthority::new` remains strictly legacy with the existing 750 ms budget.
Modern authorities reject the legacy inventory, UUID identity and leased UUID
APIs before network I/O; legacy authorities reject modern discovery. There is no
endpoint downgrade or automatic profile selection. Choosing another mode or
budget requires a new authority. No daemon configuration field is added.

## Opaque complete results and current publication

`modern_inventory(&local_id)` captures the current profile before I/O, then
performs its internally prepared modern config/pages/config lookup. The caller
supplies its authoritative current `ObservedServerId`; global local-ID and custom
alias coordination remain caller responsibilities.

Only complete wire success can produce `ModernBoundInventory`. After either
success or failure, the source revision and exact profile identity are checked
under the publication lock. Replacement/removal/cross-authority work fails as
stale, including delayed failures; no earlier partial facts or external manual
observations can be imported. Cancellation drops the directly awaited request,
without starting detached discovery.

A result exposes sorted literal `names()` and an observed `server_id()`. These
remain inspectable facts after edits. `candidate(name)` produces an opaque
`ModernBoundCandidate` only after the existing local-origin, explicit membership,
prefix/exclusion/selection and local viewer-policy projection checks. Unknown
groups remain unknown; remote/external provenance remains ineligible. Untrusted
inventory inputs, keys and viewer policy never override local configuration.

`at_modern_current(candidate, closure)` checks the exact source/profile under the
existing publication lock before constructing the configured private-media input
and entering a synchronous closure. Source edits cannot interleave with that
closure. Plain observations, manual candidates, ordinary bound candidates and
legacy identity/lease candidates cannot substitute for the modern type. Neither
new type has Debug, serialization or public constructors/importers.

Never await or re-enter the same authority in the closure. Exporting an input or
copying policy does not revoke it on later edits or authorize later private I/O;
async viewer admission and activation must return through a current fence. Worker
and session revocation remain future daemon responsibilities.

## Qualification and remaining gates

Owned HTTP/TLS regressions exercise actual modern requests and native M4F/M4S
media construction, split proxy-prefixed origins, literal name/key binding,
local policy and filtering, stale success/error, replacement/removal, independent
authorities, retained budgets/modes, cancellation, publication serialization and
frozen independent trust. Existing legacy binding, identity, lease and wire tests
run alongside them. Live read-only qualification records complete bound discovery
and negative authentication/self checks separately from eligibility or routing.

This provides configured-client binding, not a local freshness lease, signed
identity, atomic remote snapshot, relation to a separate private endpoint,
verified source ownership or relay-loop proof. Modern local freshness ordering,
authoritative local-ID coordination, relay/membership policy, global ConfigStore
ordering, viewer admission and worker/session lifetime remain integration gates.
The supplied live CDN stream is relayed and lacks groups; it must not become a
local-origin route merely because complete bound discovery succeeds.
