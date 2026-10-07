# Native adaptive RTSP cluster routing

Continue the user's approved streaming/cluster development. Preserve the topology:
viewer → LB → CDN → configured private source, with local-ready reuse. Extend the
existing native measured uplink/CPU/RAM selection and admission ledger. This is
an independent native profile, not a claim of Flussonic RTSP LB parity.

## Addresses and viewer flow

Peers have optional `flussonix_rtsp_url` (rtsp) and `flussonix_rtsps_url` (rtsps)
listener-root URLs. Validate strict ASCII URI bytes/percent escapes, host, port
(nonzero when explicit), no credentials/userinfo, query, fragment or path prefix.
Only peers accept these fields. Friendly Cluster peer inputs expose each URL;
validation, save, edit and clearing require no JSON. Existing endpoint fields and
HTTP routing remain compatible.

Initial DESCRIBE on LB resolves stream policy and authorizes before networking
for placement. Explicit on_play redirects retain precedence. Otherwise choose a
native CDN/standalone with the required enabled RTSP/RTSPS listener and a safe
configured public URL. TLS socket determines secure routing, including plain URI
aliases. Plain clients use RTSP if configured, otherwise may upgrade to RTSPS.
Never use HTTP public URLs for RTSP or downgrade RTSPS. Reject exact self-endpoint
routes even when the routing ticket changes the query. DNS aliases/indirect cycles
remain a client/reconnect qualification issue.

Return RTSP302/CSeq/original viewer query plus opaque single-use ticket; no body,
worker or retained viewer grant on LB. Canonical stream segments are escaped and
all original non-ticket query bytes/order preserved. Strip internal ticket fields
before authorization callback qs. No API/peer credentials enter viewer Location.

CDN authorizes independently, then validates ticket stream, actual transport and
SHA256 of the current viewer token before removing it. Wrong stream/transport/token
must not destroy another request's ticket. Expired/missing/replayed tickets return
503 before worker acquisition. HTTP tickets cannot admit RTSP and vice versa.
A successful initial DESCRIBE consumes once and continues on the same connection;
SETUP/PLAY do not consume again. No extra cleanup redirect. Direct no-ticket playback
still works. Control authorization retains its cached decision but has no playback linger after
a redirect or rejected ticket; valid media admission promotes the grant to normal
playback occupancy. Grant ownership accounts for the pending authorized request while
media starts. Normal media/policy/revocation fences remain authoritative.

## Routing control and reservations

Add peer-only GET `/flussonix/api/v1/rtsp-routing`: compact measured load, roles,
listener capabilities and fresh native-RTP ready stream names, without full stream
statistics, configurations or secrets. Factor shared node load generation; preserve
all existing node fields. Cache this snapshot for 1 second with per-peer single
flight; invalidation on configuration revision, no stale-on-error. Bound pool to
64 peers, concurrent network snapshot fetches to 8, response body to 2 MiB and
requests to the existing 3-second management timeout. Placement has an overall
8-second deadline, including queued snapshot work and retries. Pools above64 fail explicitly.

Capacity counters retain existing auth-session identity/reconnect-grace semantics;
a raw-socket quota is not added in this increment.

Use existing select() hard ceilings: age<=10000ms, uplink<0.9 after projected
request, CPU<0.9, RAM<0.95, session capacity and no drain. Require finite nonnegative
metrics in range, checked counters and positive usable uplink. Native RTSP compares
per-node projected 2-Mbit/s request/reservation estimates rather than a fixed1%
across heterogeneous uplinks. Keep existing weighted score/locality preference.
This estimate is not a strict real-bitrate guarantee; dynamic ABR/codec/CPU/RAM
cost reservation and sustained capacity remain pending.

Peer POST admit accepts optional protocol=http(default)/rtsp/rtsps. RTSP requires a
64-hex token hash, eligible role/listener and resolvable stream; no viewer callback,
grant or media worker is started by admission. Store protocol/token binding and
five-second expiration in the existing in-memory ledger, plus requested bandwidth.
The shared ledger has a hard cap of 20000 outstanding reservations.
Expose reserved_mbps and check summed outstanding bandwidth under the ledger lock.
Old HTTP requests retain existing behavior and single-use cleanup redirect.
Failed admission tries another candidate at most once each. Cache is only a ranking
hint: the selected CDN always rechecks actual capacity. A process restart loses
all old tickets. No distributed global auth limits are added.

Config changes or grant revocation during placement prevent stale redirect emission.
Unused/abandoned reservations expire within five seconds. Peer TLS uses existing
verification/custom CA and no-follow clients; never follow a control redirect.
Missing/unknown/drained/saturated/stale/incompatible/source-unavailable nodes fail
closed. Source route resolution and authorization happen again on viewer arrival.

## Qualification and release

Independent loopback source/CDN/LB listeners on OS-allocated unused ports; protected
preview backup before shared builds. Tests: endpoint persistence/invalid input,
friendly browser fields, load/capability/self-route exclusion, protocol/token/stream
binding and replay/expiry, no auth bypass/no early workers, concurrent reservation
capacity, bounded cached polling, config/revocation races, alternate admission
retry, exact query/credential separation, real private M4S pull/shared worker reuse
and strict independently decoded audio/video following native plaintext redirect.
Verify TLS routing with a private-CA validating client; secure automatic multi-hop
FFmpeg and mixed-vendor profiles remain unqualified. Preserve standalone callbacks,
HTTP cluster/TLS, RTSP/RTSPS and input verification regressions. Stop/reap all owned
FFmpeg tests. Full exact-head CI plus independent review before GitHub main and
preview activation; preserve config/credentials/effective environment/listeners.
No official Flussonic runtime/build/test dependency or production/CDN changes.
