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
on FlussoniX CDNs and never start media workers. Vendor CDN destinations and SRT delivery remain separate work.

Discovery is coalesced per source, with four concurrent queries, a four-second
group lookup budget and a 750 ms wire budget per origin. Recovery owns at most
eight concurrent jobs, with retries backing off from one to thirty seconds.
There are at most 256 recovery streams and 20,000 retained native session anchors;
the selector also bounds stream history without evicting denial authority.
Configuration, source, identity, route and lease changes fence pending work.

Owned multi-origin tests cover management and private-only outages, independent
credentials, sticky selection, rejection/removal barriers, concurrent viewers,
revocation, edits during discovery, renewal, demand expiry and independent HLS
decode. These tests do not establish production vendor interoperability,
throughput or new GPU capacity.
