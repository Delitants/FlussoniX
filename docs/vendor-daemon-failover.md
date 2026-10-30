# Vendor source failover

FlussoniX can recover HTTP playback across explicitly equivalent Flussonic
origins. Configure the sources in **Cluster → Source servers**:

1. Choose **Flussonic** and enter each origin's address and Cluster key.
2. Set a local viewer policy and any processing template.
3. Give two to eight origins the same **Failover group** and **Content identity**.

The identity declares that the same literal stream names represent the same
content across these origins. Names or reported server IDs alone do not prove
equivalence. A single member may be saved during setup; grouped playback needs
two. Members must have matching prefixes, exclusions, resolved processing and
one equal viewer policy for every possible upstream group membership. Changing
these settings for an established group requires updating its members together.

A source without a failover group also recovers automatically after temporary
media loss. It retries only that origin after revalidating its current catalog;
no other source is inferred to carry equivalent content. Each stream keeps its
own viewer policy, including upstream group overrides. Recovery uses the same
original permission, recent viewer demand and bounded backoff described below.

An established M4F input can keep its worker and packager when its authenticated
control stream ends cleanly after at least one complete segment. Before opening
a new control session, it fetches a fresh, complete catalog for that exact
source. Scheduled and EOF renewals serialize per source; EOF renewal has a
five-second maximum, cannot outlive the old lease, and uses one of four EOF
slots per registry. Removal, changed source identity or policy, mismatched
group pins, configuration edits, expired demand, and last-viewer revocation
close the old worker. A revoked viewer stays closed even if another viewer
keeps the shared worker active.

The reopened M4F control gets a new playback-session ID. Replayed segment names
are skipped before media fetch; the MPEG-TS muxer keeps its timestamps and
continuity counters. A changed audio/video track map, unknown backward source
timestamp, first novel announcement beyond the last published duration plus
one second, or a contiguous announcement whose first served audio/video frame
is far ahead closes the worker for supervised recovery. Sparse subtitle
presence may change. A fifth EOF within the rolling 30-second budget falls
back to supervision; old EOFs expire only after new media progress. This can
preserve playback only when the origin replays
the missing announcements and still serves their media. It cannot recover a
segment that has disappeared from the origin's memory window. M4S/M4SS and
other transport formats retain their existing reconnect behavior.

The daemon shares one private input and packager across viewers. A healthy
backup stays selected when a preferred origin returns. Temporary management or
private-media failures can select another member. The selected origin's fresh
catalog is checked for removal, disable or invalid policy before rotating after
private-media loss. If no backup qualifies, a freshly validated original can
be retried after the bounded alternative search. Explicit private rejection and invalid media block rotation;
a management API success alone cannot clear that block. Valid authenticated
private metadata from the same origin can clear it. Unrelated settings saves
preserve these barriers and the selected-origin history. Changing the source
relationship, its processing or policy starts a new selection plan.

An announced M4F segment returning404 with exactly one affirmative Cluster
acknowledgment is treated as temporary media unavailability. The same applies
to segment400 only when it also carries exactly one `X-Reason: no_dvr` header:
an absent memory segment can fall through to an origin without DVR. Before any
metadata is delivered, the reader may skip one distinct unavailable announcement
and wait for the next on the same authenticated control session. Duplicate
announcements cannot refetch that object. The original worker startup watchdog,
viewer permission and source authority still bound the wait.

A second unavailable object, or any unavailable object after metadata, stops
the pull without accepting its response body. Existing supervised recovery can
revalidate the current catalog and reopen a single configured origin or choose
an eligible member of an explicit failover group.
Control400/404 and authorization401/403 still block rotation, as do segment200/400/404
responses with invalid or missing acknowledgments. Other segment400 responses,
including missing, duplicate or altered `no_dvr` reasons, remain rejected.
Native inputs cannot use these private exceptions. Existing5xx failures remain
transient even without acknowledgment. Source/viewer revocation takes
precedence; these unavailable-object errors introduce no credential fallback
or retry of the missing object. They do not prove why a particular reference
segment was missing or promise seamless recovery.

On-demand recovery uses original native viewer permission and recent playback
demand. [Selected static streams](vendor-static-activation.md) recover through
separate daemon ownership without viewer sessions. Background work does not
renew viewer activity or bypass callbacks, revocation, limits or authorization
expiry. Media and retries stop when neither owner remains. Every origin retains
its own private credentials and TLS trust.

HTTP/HTTPS HLS, MPEG-TS, M4S and M4F outputs use the replacement worker. Continuous
responses close at replacement and clients reconnect. HLS gets a new generation
and discontinuity; seamless playback is not promised. Cached segments still
require current viewer and source authority. Local packaging failures retry the
same origin instead of being interpreted as an origin's transport failure.

The existing active-pulls table reports selected source, group, availability and
switch count. FlussoniX load balancers use group catalog authority for placement
on FlussoniX CDNs and never start media workers. Vendor CDN destinations remain separate work. CDN/standalone SRT callers reconnect to the shared recovered input.

Discovery is coalesced per source, with four concurrent queries and the same
five-second wire profile as foreground/static lookup. A sixteen-second group
selection bound covers the selected origin and two waves of alternatives for
up to eight members in an otherwise uncontended group. Network results still
publish in declared rotation order; selected denial/removal is checked first.
Recovery owns at most eight concurrent jobs, each bounded by twenty seconds,
with retries backing off from one to thirty seconds. M4F-only vendor sources
with active static or viewer demand use a two-second maximum plus a stable
per-stream spread of less than 500 ms, because a longer pause can outlive the
origin's short live-segment window. M4S and mixed-protocol groups keep the
original schedule. The 256-stream cap and eight-job limit still bound probe
load. Vendor SRT admission has a
twenty-second bound; native admission and later transport timers are unchanged.
Contention may exhaust these deadlines; no stale lease or authority is extended.
There are at most 256 recovery streams and 20,000 retained native session anchors;
the selector also bounds stream history without evicting denial authority.
Configuration, source, identity, route and lease changes fence pending work.

Owned multi-origin tests cover management and private-only outages, independent
credentials, sticky selection, rejection/removal barriers, concurrent viewers,
revocation, edits during discovery, renewal, demand expiry and independent HLS
decode. These tests do not establish production vendor interoperability,
throughput or new GPU capacity.
