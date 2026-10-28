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
Flussonic HTTP/HTTPS, RTSP/RTSPS and HTTPS-based SRT destination lookup are
described below. Existing connections do not migrate between CDNs.
CDN/standalone SRT playback uses the guarded listener. [Equivalent-origin failover](vendor-daemon-failover.md),
[static activation](vendor-static-activation.md) and CDN/standalone
[RTSP/RTSPS playback](vendor-daemon-playback.md) are supported separately. Configure equivalent intended viewer
policies on LB and CDN; this increment does not infer equivalence between them.

### Bounded live topology check

From a source checkout, `scripts/qualify-live-vendor.py --topology` accepts the
same `--binary`, `--source`, `--stream` and owner-only `--cluster-key-file`
arguments as its single-node mode. It starts an isolated loopback LB and CDN,
uses on-demand source activation and a random local viewer token, follows the
LB placement and CDN ticket redirects, decodes one HLS segment, checks denial
without the viewer token at both nodes, and confirms the LB has no media stream.
It stops both process groups and deletes their files. Against the authorized
cdn4 test stream, both secure M4SS and M4FS source pulls delivered decoded
1920×1080 H.264/AAC through this path. An invalid Cluster key prevented LB
placement.

Adding `--output m4s` follows the same guarded redirects and validates native
M4S track metadata and a framed media record. Both secure source transports
advertised H.264/AAC and delivered native media in the live loopback topology.
The HLS mode performs strict FFmpeg decoding; the M4S mode checks wire framing
and metadata, not independent elementary-stream decoding.
Adding `--output m4f` checks a live control announcement and retrieves its
timestamped M4F segment, including track metadata and a nonempty media box.
M4SS and M4FS source pulls both passed. The M4FS relay preserves the origin's
multi-digit announcement sequence; this check does not fully decode M4F sample
tables or qualify every reference stream.

This verifies one live source and two local FlussoniX nodes. It does not
establish placement across production CDN hosts, private LAN routing, uplink
saturation decisions, failover under loss, HEVC, or target-scale capacity.

A bounded cross-host check also ran the current FlussoniX CDN binary on an
isolated port at cdn5, with a local FlussoniX LB and the live M4SS source at
cdn4. The LB placed the viewer on cdn5; cdn5 served a 1.43 MB HLS segment that
FFmpeg decoded, and both nodes returned 403 without the viewer token. The
viewer and LB management path used an SSH tunnel because the Linux test host
cannot connect directly to the isolated CDN port; cdn5 reached its cdn4 source
directly. The disposable daemon, tunnel and files were removed afterward.

A second bounded check ran the current FlussoniX binary on isolated port
`28210` on both authorized CDN hosts: cdn4 as LB and cdn5 as CDN. The LB
queried and admitted the CDN over their direct private-LAN route, then placed
the viewer there with a single-use ticket. cdn5 pulled the live M4SS source
from cdn4 and served a 1.56 MB HLS segment that FFmpeg decoded. The observed
HTTP sequence was 302 placement, 302 ticket redemption, 200 playlist and 200
segment. Both nodes returned 403 without the viewer token, and the LB reported
zero local media streams. The private HTTP peer route required the explicit
per-peer `flussonix_allow_insecure_peer_http` setting; without it the default
peer-key transport guard rejected placement. Only the disposable config used
that exception. The viewer reached each isolated listener through an SSH
tunnel because the Linux test host cannot connect directly to those ports.
Both daemons, tunnels and temporary files were removed and port `28210` was
verified free on each host afterward.

A third bounded check used a temporary one-day CA and a certificate matching
cdn5's private IP. The remote cdn4 LB queried and admitted cdn5 through
`https://172.16.0.8:28213/` with `flussonix_tls_ca` and no plain-HTTP peer
exception. The viewer reached cdn5 through a verified HTTPS SSH tunnel. Its
untrusted-certificate control failed verification; with the test CA, the
placement/ticket/playlist/segment sequence returned 302/302/200/200 and a
1.56 MB segment that FFmpeg decoded. Both nodes denied tokenless playback with
403, and the LB again reported zero local media streams. The temporary
certificate, private key, daemons, tunnels and files were removed afterward;
the original services remained active and the isolated ports were free.
Direct public viewer delivery, sustained load and failover remain unqualified
by these bounded checks.

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
HTTP, RTSP and SRT share identity continuity, fresh load checks and pending egress
holds for each observed server. No native admission RPC or ticket is sent to a
Flussonic destination, and the LB starts no media worker.

Capacity holds last five seconds and apply **only to this LB**. They prevent its
concurrent redirects from reusing the same reported headroom; they are advisory,
not reservations on Flussonic and not coordination between multiple LBs. Holds
survive unrelated configuration saves and are shared by destinations reporting
the same server identity. Identity continuity covers all mappings under a
management profile; a changed identity excludes that profile. Final placement
rechecks the latest accepted load observation. Native
FlussoniX destinations retain CDN-owned atomic admission, including SRT through
the lookup described below. Missing dynamic routes and production throughput
qualification remain separate work. The HTTP adapter is based on the
[Flussonic management API](https://flussonic.com/doc/api/reference/) and its
[HTTP load-balancing model](https://flussonic.com/doc/fms/cluster/loadbalancer/).

### SRT lookup for compatible clients

SRT has no redirect response. A client with lookup support can call
`GET /flussonix/api/v1/srt-lookup` over **HTTPS on the LB**, with one URL-form-encoded
`streamid` parameter, then connect directly to the returned CDN. Existing SRT
URLs need client integration to use this extension.

For each Flussonic peer, enter **Public SRT URL** with an explicit port and choose
**SRT encryption** to match its configured listener. Configure the passphrase on
the CDN and client; neither Cluster keys nor passphrases are returned. This
setting is an operator declaration, not a remote encryption check. The client
must enforce encrypted SRT and verify the LB's HTTPS certificate.

The default lookup only selects destinations declared encrypted. An explicit
`encryption=any` also permits declared unencrypted listeners. A successful
response has `Cache-Control: no-store` and these fields:

```json
{
  "address": "srt://cdn.example:9998",
  "streamid": "#!::r=news,u=viewer-token,m=request",
  "encryption_required": true,
  "expires_in": 5
}
```

Pass the returned stream ID as a separate SRT client option. Its values are
opaque: do not decode percent sequences or interpret plus signs again. The LB
maps only the resource name and supplies `m=request` if missing. IDs must fit
512 UTF-8 bytes after mapping; mapped names cannot contain commas. Duplicate or
unknown lookup parameters and stream ID fields are rejected.

The lookup uses SRT viewer policy, with the original stream ID in the callback
query. Admin, Basic and Cluster credentials do not grant viewer access. Callback
redirects are rejected. The CDN independently checks the token and encryption.
HTTP, RTSP and SRT share fresh vendor load observations and five-second per-LB
advisory holds; no media crosses the LB. Configuration changes and expired
source authority prevent a pending lookup from returning a destination.

This extension supports native FlussoniX CDNs and explicitly mapped Flussonic
peers. It does not provide a single-address SRT relay or production throughput
qualification. Flussonic capacity holds remain local to each LB. Owned
independent SRT tests cover encrypted audio/video decoding, destination token
denial and mismatched passphrases; they do not establish live Flussonic SRT
interoperability.
For Flussonic destinations, `expires_in` describes the advisory placement
estimate, not a redeemable admission ticket. Request a fresh lookup before
connecting after that interval.


### Native FlussoniX SRT destinations

Choose **FlussoniX** as the peer type, enter its management address and **Public
SRT URL**, and match the **SRT encryption** setting to its enabled global playback
listener. Configure a trusted management CA when needed. The client still calls
the HTTPS lookup and then connects directly; the LB carries no media.

The LB checks the CDN's enabled listener and encryption metadata. The CDN then
reserves capacity for five seconds through its authenticated native admission
API. Concurrent LBs share this CDN-owned reservation ledger with HTTP and RTSP;
SRT also respects the listener's connection limit. Actual connections and other
traffic still compete for resources: a reservation does not guarantee delivery.
If admission fails, lookup tries another eligible CDN within its time budget.

A native response appends `flussonix_ticket=<opaque UUID>` to the returned stream
ID. Pass the **entire returned stream ID unchanged** to the SRT client. The ticket
is single-use and binds the stream, viewer token, encryption, listener generation
and source configuration. Its lifetime starts when the CDN issues it, so connect
promptly; an expired ticket requires a new lookup. The full ID,
including this field, must fit 512 UTF-8 bytes; lookup checks that before asking
for a reservation. Supplying a ticket to the lookup endpoint is rejected.

The CDN independently authorizes playback before redeeming the ticket and
starting media. The internal ticket is removed from its authorization callback
and session query, while opaque viewer fields remain unchanged. A wrong token,
stream, transport or listener cannot redeem it. Configuration changes, revoked
viewer grants and expired source or replica authority prevent playback. Source
reservations never pull upstream media by themselves.

Existing ticketless SRT clients keep their normal viewer policy. Native tickets
are never sent to Flussonic destinations. Listener support and owned decoder tests
do not establish live Flussonic interoperability or production capacity.

When a native CDN switches to an equivalent origin, an existing SRT connection
can close. Request a **fresh HTTPS lookup** and connect using the new stream ID;
an old ticket cannot be reused. The CDN can recover its upstream automatically
while authorized playback demand remains recent. A complete origin outage
rejects lookup and stops stale source media; restored availability permits a new
lookup and connection.

Owned daemon tests cover this reconnect flow with HEVC and MP2/MP3 audio over
both M4F and M4S private inputs, including origin failure and total outage with
restoration. These tests do not qualify uninterrupted SRT connections or gapless
timestamps. See [native recovery qualification](cluster-loadbalancing.md#encrypted-srt-reconnection-after-native-origin-recovery).
