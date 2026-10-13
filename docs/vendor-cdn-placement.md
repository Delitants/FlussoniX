# Load balancing vendor-backed CDNs

A native FlussoniX load balancer can now send HTTP viewers to a FlussoniX CDN
that pulls from a configured Flussonic source. The existing uplink/CPU/RAM
selection, CDN capacity reservation and public delivery endpoints are reused.

Configure the Flussonic source and its viewer policy on each CDN through
**Cluster → Source servers**. On the native LB, configure stream metadata with
the same name and the intended LB viewer policy, and add the FlussoniX CDNs as
peers with their management and public delivery addresses. LB metadata can have
no inputs: the LB does not ingest or transcode the stream. The CDN resolves its
own vendor source, viewer policy and optional processing template.

The LB checks viewer authorization before placement. The CDN checks its current
viewer policy too; a capacity ticket never grants access. It consumes the
stream-bound, HTTP-only, five-second ticket once and returns a clean relative
playback URL. Only the subsequent clean request starts or joins the shared
private pull. Other query values, including the viewer token, are retained. Internal tickets
are removed from authorization callback query strings; unrelated raw query bytes
and repeated parameters are preserved there.
Relative redirects keep the current HTTP/HTTPS authority and transport.

Wrong-stream, expired, replayed and RTSP tickets fail. A denied viewer or a source
change during authorization cannot consume a valid ticket or start media.
Concurrent redemption has one winner. Redemption alone leaves no playback
occupancy, and origin Cluster credentials never enter viewer redirects or LB
requests. Invalid tickets fail before vendor discovery.

This increment qualifies owned HTTP LB/CDN/origin fixtures and existing native
routing regressions. It does not enable Flussonic CDN destinations, direct vendor
inventory on an LB, automatic vendor failover, seamless recovery, static vendor
activation or vendor RTSP/SRT delivery. Configure equivalent intended viewer
policies on LB and CDN; this increment does not infer equivalence between them.
