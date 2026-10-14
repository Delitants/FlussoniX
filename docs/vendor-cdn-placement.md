# Load balancing vendor-backed CDNs

A native FlussoniX load balancer can send HTTP, RTSP and RTSPS viewers to a FlussoniX CDN
that pulls from a configured Flussonic source. The existing uplink/CPU/RAM
selection, CDN capacity reservation and public delivery endpoints are reused.

Configure the Flussonic source and its viewer policy on each CDN through
**Cluster → Source servers**. Configure the Flussonic source on the LB too,
including its intended viewer policy, then add the FlussoniX CDNs as peers with
management and public delivery addresses. The LB discovers stream names through
the independent vendor catalog; it needs no local stream rows and never pulls
private media or transcodes. The CDN resolves its own vendor source, viewer
policy and optional processing template.

Existing deployments with local LB stream metadata still work. Local rows,
including disabled rows, take precedence over discovered vendor names. Vendor
namespace exclusions, overlapping-prefix rejection and current source/group
policies apply on the LB just as on the CDN. A missing or unavailable vendor
stream never falls back to a weaker native source.

The LB checks viewer authorization before placement. A successful, current CDN
redirect refreshes normal native authorization-session accounting, preserving
user/session limits without starting a worker on the LB. Failed or stale
placements do not refresh playback accounting. The CDN checks its current
viewer policy too; a capacity ticket never grants access. It consumes the
stream-bound, HTTP-only, five-second ticket once and returns a clean relative
playback URL. Only the subsequent clean request starts or joins the shared
private pull. Other query values, including the viewer token, are retained. Internal tickets
are removed from authorization callback query strings; unrelated raw query bytes
and repeated parameters are preserved there.
Relative redirects keep the current HTTP/HTTPS authority and transport.

For RTSP, configure each peer's public RTSP/RTSPS address and enable the
corresponding CDN listener. The LB uses the same load measurements and never
pulls media. The CDN reserves its exact current vendor source or equivalent
origin selection for five seconds, without creating viewer permission. DESCRIBE
independently authorizes the viewer, validates the token and transport binding,
then consumes the ticket once before starting or joining media. A source,
configuration, lease or replica change invalidates that reservation. RTSPS
requests select only RTSPS peers. HTTP and native RTSP tickets cannot substitute
for a vendor RTSP reservation.

Wrong-stream, expired, replayed and wrong-protocol tickets fail. A denied viewer or a source
change during authorization cannot consume a valid ticket or start media.
Concurrent redemption has one winner. CDN ticket redemption alone leaves no
playback occupancy. Origin Cluster credentials remain in the independent source
client; native CDN RPCs use only the configured native peer key. Invalid tickets
fail before vendor discovery, and LB nodes reject incoming placement tickets.

LB catalog observations keep their existing bounded source leases and cache.
Placement rechecks that exact source/configuration/policy authority after waiting
for CDN telemetry and admission. Expiry, source edits or owner shutdown prevent a
redirect and start no LB media. An abandoned CDN capacity ticket expires after its
normal five-second lifetime. The admin session reauthorization action can use a
current cached LB catalog policy for an active LB session. It performs no source
discovery or media pull; missing, expired, changed or inactive routes return 404.
Reauthorization applies to that node's sessions, not remote CDN viewers.
HTTPS viewer requests select only HTTPS delivery
addresses, and callback redirects cannot downgrade them.

Owned tests exercise discovered and locally configured LB routes, independently
decoded HLS segments, M4S delivery, source/policy edits, lease expiry, callback and
group guards, shutdown, concurrent RTSP redemption and verified RTSPS redirects.
Flussonic HTTP/HTTPS and RTSP/RTSPS destinations are described below. Seamless recovery and SRT LB placement remain pending. CDN/standalone SRT playback uses the existing guarded listener. [Equivalent-origin failover](vendor-daemon-failover.md),
[static activation](vendor-static-activation.md) and CDN/standalone
[RTSP/RTSPS playback](vendor-daemon-playback.md) are supported separately. Configure equivalent intended viewer
policies on LB and CDN; this increment does not infer equivalence between them.

## Flussonic CDN destinations

In **Cluster → CDN peers**, choose **Flussonic**, enter its management URL,
public delivery URL and explicit Cluster key, then add stream mappings. Each
mapping links an LB stream name to the corresponding name on that CDN. The LB
stream still needs its intended viewer policy, either locally or through its
configured source. Configure the CDN's intended viewer policy independently.

Only mapped, enabled streams advertised by that CDN's direct stream API are
eligible, including configured streams waiting for a viewer and idle streams
discovered from its sources. A running stream or an entry in a list response is
not required; the LB looks up the mapped name directly. The viewer's subsequent
request lets the CDN start its own pull after authorization.

A missing direct route (404), a failed lookup or an invalid response excludes
that destination. Configured sources, prefixes and exclusions alone do not prove
that a stream is available. The LB does not create missing CDN streams or infer
equivalent content from matching names. Mapping is the operator's declaration
of the destination. Cached observations expire after one second; a failed
refresh never reuses the expired route.

The LB checks coherent server identity and current CPU, RAM and outgoing
bandwidth through the Flussonic management API. Keep clocks synchronized within
five seconds. HTTPS management verifies certificates; an optional local CA file
supports private certificates. HTTPS viewers require HTTPS public delivery.
The redirect preserves the media suffix and raw viewer query, including repeated
parameters. Cluster credentials and native admission tickets never enter it.
The destination independently authorizes the viewer.

For RTSP viewers, optionally enter **Public RTSP URL** and **Public RTSPS URL**
listener roots, such as `rtsp://cdn.example:554` and `rtsps://cdn.example:555`.
Configure and test the corresponding listeners on the CDN; management metadata
alone does not attest playback availability. The mapped CDN name and accepted
viewer query bytes are preserved in the RTSP 302 redirect. Viewers must support
redirects and verify the destination certificate. RTSPS viewers require an
RTSPS destination; plain RTSP may use RTSPS when no plain endpoint is configured.
The existing RTSP parser rejects duplicate query keys and incoming LB tickets.
HTTP and RTSP share identity continuity, fresh load checks and pending egress
holds for each observed server. No native admission RPC or ticket is sent to a
Flussonic destination, and the LB starts no media worker.

Capacity holds last five seconds and apply **only to this LB**. They prevent its
concurrent redirects from reusing the same reported headroom; they are advisory,
not reservations on Flussonic and not coordination between multiple LBs. Holds
survive unrelated configuration saves and are shared by destinations reporting
the same server identity. Identity continuity covers all mappings under a
management profile; a changed identity excludes that profile. Final placement
rechecks the latest accepted load observation. Native
FlussoniX destinations retain CDN-owned atomic admission. Vendor SRT
destinations, missing dynamic routes, and production throughput qualification
remain separate work. The HTTP adapter is based on the
[Flussonic management API](https://flussonic.com/doc/api/reference/) and its
[HTTP load-balancing model](https://flussonic.com/doc/fms/cluster/loadbalancer/).
