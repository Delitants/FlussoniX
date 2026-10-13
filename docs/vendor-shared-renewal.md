# Explicit shared-input lease renewal

`SharedInputs::renew(&source_handle, lifetime).await` refreshes complete modern
source discovery while existing admitted viewers continue using the published
lease. It returns fresh `inventory`, `renewed` and `retired` counts. Schedule and
own the future in the embedding runtime before the current deadline; no timer
or detached refresh task is started by this API.

A successful refresh atomically publishes the next lease and updates equivalent
shared workers. Their generation, upstream connection, FFmpeg process, native
hub, subtitle conversion state and independent viewer grants remain unchanged.
A newly admitted viewer can join using the returned inventory. Immutable
standalone candidates and admissions keep their original tickets and become
obsolete when the next ticket is published; this API only renews managed shared
workers.

Equivalence requires the same installed source/profile and local configuration,
observed server ID, literal stream name, canonical local viewer policy, candidate
kind and selected-static status. Every refreshed row passes the existing direct
origin, membership, filter and local policy projection again. Missing,
ineligible or changed routes retire immediately. Equal canonical policies can
survive changes to group membership. Observed management IDs remain claims,
not private-endpoint identity attestations.

The old and proposed leases must both remain valid at publication. Lifetime is
in `(0, 30 seconds]`, measured when reservation starts, before network I/O.
Failure, cancellation or deadline expiry never extends the published deadline.
A newer renewal attempt supersedes an older pending attempt even if the newer
one fails. An ordinary `coordinator.inventory` refresh retains its original
immediate-invalidation behavior and supersedes any pending renewal. Config,
identity, source-installation changes and registry shutdown fence late results.
Retired generations are never restored by a pending response.

Current checks acquire configuration, coordinator, source, route and member
locks in that order. Publication holds the source lock across all route updates;
readers cannot observe a new lease with old routes. Network I/O holds none of
these locks. Original source and per-viewer write/read fences still apply after
renewal; renewal does not confer viewer access.

Owned HTTP tests cover preserved and retired routes, revocation, expiry, failed
and cancelled refreshes, competing attempts, global edits, ordinary refresh,
shutdown and media fencing after renewal. Owned FFmpeg tests verify HLS remains
available beyond the old deadline with the same PID and one upstream connection.
Automatic daemon vendor routing, HLS session retention, scheduled renewal and
source failover remain separate integration work. No new live vendor, GPU,
capacity or production continuity qualification is claimed.
