# Omitted Flussonic group membership

Some modern Flussonic management responses omit stream group membership even
when the stream is locally configured. FlussoniX keeps that observation unknown;
it does not invent an empty membership list.

An explicitly configured modern source can nevertheless serve the stream when
its local viewer policy is independent of membership. The source must supply an
explicit policy, and the effective policy for every configured group must equal
the source policy. Groups without local overrides contribute nothing. This
comparison includes callback URL, session keys, session limits and token guard.
It is cached on the immutable source profile and invalidated by profile changes.

For example, a source-wide token restriction can protect streams whose group
field is omitted when no group changes that restriction. If a named group adds
a different callback or token restriction, the stream remains unavailable until
its membership is known. Missing local policy never grants public playback.
An explicit local Allow policy remains an explicit operator decision.

Known memberships retain the existing group composition, conflict detection and
limits. Malformed membership fields still reject the inventory. Disabled,
relayed, external and unknown-provenance rows do not gain configured-origin
eligibility. Prefix and exclusion filters still apply. Cluster credentials
remain private upstream credentials and never authorize a viewer.

This rule applies to modern configured-origin binding only. The raw inventory
preflight and legacy binding continue to reject unknown membership. Explicit
all-hop relay binding still requires known membership; there is no new relay or
identity assertion and no additional source option.

## Lifetime and policy changes

The resulting candidates retain source revision, identity, refresh ordering and
lease expiry checks. A group edit that makes policy depend on membership fences
old candidates and existing playback. New requests cannot reuse the old input
or session to avoid the new rule. An invalid edit changes no current policy.

## Evidence and limits

Read-only reference inspection found no stream `groups` field in the installed
26.04.1 public/private schemas. Its peer parser retains an omitted field as
undefined; source policy composition folds an absent membership list without
group overrides. These facts do not prove the actual memberships of a live
stream. FlussoniX uses the stronger local policy-equivalence condition above.
No vendor module is loaded, executed, bundled or required by this implementation.

Owned regressions cover callback/token preservation, group-dependent refusal,
origin checks, exclusions, missing authority, stale publication, lease expiry,
shared playback and policy-edit retirement. Live observations and decoded-media
qualification are recorded separately; this change does not establish complete
mixed-vendor or group configuration parity.
