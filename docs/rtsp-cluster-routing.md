# Native adaptive RTSP cluster routing

The native `lb` role routes initial RTSP DESCRIBE requests to a CDN or standalone
node using measured uplink, CPU, RAM, session capacity and fresh local-media
readiness. The viewer receives RTSP/1.0 302 and reconnects to the selected node.
That CDN reuses its local worker or pulls one stream from a configured private
source. An owned three-node test decodes both audio and video through this path
using FFmpeg and verifies one shared private M4S pull for two viewers.

In Cluster → Peers, set **Public RTSP URL** and/or **Public RTSPS URL** to the
node's listener root, for example `rtsp://cdn.example:8554` and
`rtsps://cdn.example:8322`. These native extension fields are
`flussonix_rtsp_url` and `flussonix_rtsps_url`; HTTP delivery, management API and
private source endpoints remain separate. Listener roots cannot contain a path
prefix, credentials, query or fragment. The selected node must actually enable
the corresponding listener. Missing eligible capacity returns 503. Configured
`on_play` callback redirects take precedence over adaptive placement.

The LB authorizes the viewer before placement. It starts no media worker and
retains no live playback grant. The CDN independently authorizes before consuming
the reservation and acquiring media. Builtin tokens and callback policy are not
replaced by peer credentials. The original non-ticket query bytes and order are
preserved; internal tickets are removed from callback `qs`. No peer/API secret
is included in viewer Location. Control-only decisions can remain cached for
reconnects but do not linger in playback capacity after the request ends.

An opaque UUID ticket lasts five seconds and binds the stream, actual RTSP/TLS
transport and SHA256 of the viewer token. Stream/transport/token mismatches do
not consume it. Missing, expired or replayed tickets fail before media startup.
HTTP and RTSP tickets cannot admit each other's protocols. The CDN consumes a
valid ticket on DESCRIBE; SETUP and PLAY continue on the same connection without
another consumption or cleanup redirect. Direct playback without a ticket is
still supported. Restarting a node invalidates all its old tickets.

Actual TLS transport controls security, including plain URI aliases inside TLS.
An RTSPS viewer is routed only to RTSPS; a plain RTSP viewer can upgrade when
only a secure endpoint is configured. TLS framing and downgrade prevention are
tested using private-CA certificate-verifying clients. Automatic secure multi-hop
FFmpeg decoding and mixed-vendor client interoperability remain unqualified.
Clients must verify the destination certificate and bound reconnect/redirect
loops. Exact self-endpoints are excluded; arbitrary DNS aliases and multi-node
cycles are not detected. Existing verified RTSPS input deliberately rejects
upstream control redirects.

The peer-only `GET /flussonix/api/v1/rtsp-routing` endpoint reports compact load,
role, listener capabilities and fresh native-RTP ready names without stream
configuration or full statistics. Snapshots are cached for one second with
per-peer single flight; configuration edits invalidate the cache, and failed
refreshes never reuse expired data. Native pools are limited to 64 peers,
snapshot fetch concurrency to eight, snapshot bodies to 2 MiB and admission
responses to 16 KiB. Native snapshot probes time out after 500 milliseconds;
the collection phase ends after 4.5 seconds and keeps successful observations
while cancelling unfinished probes. Admission requests retain the existing
three-second timeout. Verified TLS/custom CAs and no redirect following apply
to both requests. Placement has an overall eight-second deadline. Failed admission tries each eligible candidate
at most once. Pending configuration changes and revocation block stale replies.

Selection excludes draining nodes, observations older than ten seconds, full
session capacity, projected uplink at or above 90%, CPU at or above 90%, and RAM
at or above 95%. It uses the existing weighted score and locality preference.
Capacity counters retain existing authorization-session identities and reconnect
grace; they are not a separate raw-socket quota. Requests sharing configured
session keys can share an identity. Each request reserves an estimated 2 Mbit/s
against its node's uplink capacity;
the CDN rechecks measured capacity and summed pending bandwidth under the ledger
lock. The shared ledger is capped at 20000 outstanding tickets. Peer admission
accepts protocol `http` (the default), `rtsp` or `rtsps`; RTSP variants also require
`token_hash`. The reservation itself starts no viewer callback, grant or worker.

This is an independently implemented native profile, not a claim of Flussonic
RTSP cluster dialect parity. Two-second recordings qualify functionality, not
sustained scale or latency. Actual bitrate/ABR, codec and CPU/RAM cost reservation,
distributed global viewer limits, transparent movement of live sessions, DVR and
full API parity remain pending. Existing H.264/HEVC and AAC/MP2/MP3 packetizer
profiles apply; this cluster increment specifically decodes H.264/AAC via private
M4S. No official Flussonic component is a runtime, build or test dependency.

Ticket-bearing HTTP requests also use control authorization: rejected cross-protocol tickets and valid cleanup redirects leave no phantom HTTP playback slot. The clean media request retains normal authorization and playback accounting. A failing cross-protocol occupancy assertion qualifies this correction.

Cached policy decisions retain callback user limits and unique-user policy. Admission rechecks current global and user capacity and constructs the live grant atomically under the same lock. In-flight policy requests retain their cache entry but do not own capacity; cancellation releases any constructed grant. Control requests do not extend playback reconnect grace. Regressions cover warmed cached decisions, callback limits, unique-user revocation, cancellation and a healthy last peer behind 63 stalled snapshot peers.
