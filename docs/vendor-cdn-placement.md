# Load balancing vendor-backed CDNs

A native FlussoniX load balancer can now send HTTP viewers to a FlussoniX CDN
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

Wrong-stream, expired, replayed and RTSP tickets fail. A denied viewer or a source
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
group guards, shutdown and HTTPS redirect boundaries. This does not enable
Flussonic CDN destinations, automatic vendor failover, seamless recovery, static
vendor activation or vendor RTSP/SRT delivery. Configure equivalent intended viewer
policies on LB and CDN; this increment does not infer equivalence between them.
