# Native cluster resource-pressure routing

The native HTTP and RTSP/RTSPS load balancers use the same pressure ranking and
telemetry validation. This implements a bounded part of the
[cluster design](cluster-loadbalancing.md); it does not implement the four
Flussonic balancer modes or establish mixed-vendor parity.

For each eligible CDN or standalone delivery node:

```text
viewer estimate = max(2 Mbps, highest fresh shared output Mbps * 1.25)
projected uplink = measured utilization + (reserved Mbps + viewer estimate) / uplink capacity Mbps
pressure = max(projected uplink / 0.90, CPU utilization / 0.90, RAM utilization / 0.95)
```

The existing strict ceilings remain: projected uplink and CPU below 90%, RAM
below 95%, observation age at most ten seconds, available session capacity, and
no node/peer drain. Pending reservation counts also consume session capacity.
Missing, negative or invalid metrics, nonpositive uplink capacity, overflowing
counts/age, and source-only or LB-only roles exclude a candidate. Pending
bandwidth comes from `reserved_mbps`, rather than assuming every reservation
has the same cost. HTTP includes probe collection time in observation age;
RTSP includes the age of its cached snapshot.

Candidates within 0.05 normalized pressure of the best candidate form the
shortlist. Prefer a ready stream within that set, then lower pressure. Exact
ties at that preferred readiness and pressure rotate across sorted configured
hostnames. A ready node
outside the margin receives no locality advantage. These are fixed native
defaults; no new legacy API mode or UI policy setting is introduced.

Rotation belongs to each LB process and is shared by HTTP and RTSP/RTSPS.
Different tied candidate sets have independent cursors, so interleaving
protocols with different available listeners cannot concentrate their requests
through a single global counter. At most 64 recently used sets are retained;
fixed-size SHA-256 keys identify sorted, length-prefixed hostnames. The least
recently used set is evicted when a new set needs space. A new, evicted or
restarted set begins with its alphabetically first hostname. Singleton sets
need no cursor. A request takes one turn from its first tied set and retains it
through retries, even if refusal or expired telemetry changes the remaining
set. Selection holds no rotation lock across a network request.

The viewer estimate is divided by each node's own capacity. A fresh rate from
any valid observed native CDN applies to every candidate for that stream,
including a cold CDN which has not pulled it yet. Missing or stale measurements
retain the 2 Mbps migration fallback. Ranking and the outgoing admission hint
use the same estimate. The CDN reserves the greater of that hint, its own fresh
local estimate and 2 Mbps; an older LB cannot reduce a known local stream cost.
Before each admission attempt, the LB rechecks observation ages and recomputes
the estimate and all unattempted candidates' projected loads. A slow failed
attempt can expire a measurement; cold candidates excluded by its previous
cost are then reconsidered using the fallback. Already attempted nodes are
not retried in that placement.
CPU and RAM remain observed utilization, without an incremental codec or cold
pull cost model. The final CDN admission reservation still rechecks measured
capacity and active sessions under its own ledger lock, after any wait for a
worker observation. Advisory telemetry neither authorizes a
viewer nor starts a media worker. Routing redirects new requests; established
viewers stay on their chosen node.

## Shared output measurements

The producer samples bytes from the shared encoded MPEG-TS output. Completed
intervals last at least one second; the estimate uses the higher rate of the
last two completed intervals which are at most three seconds old. Equal rates
use the newer interval. Reads and zero-byte events never refresh the sample.
Long gaps, counter overflow, clock regression and worker replacement reset it.
Stopped workers and workers still warming up have no measured rate.

Stream statistics expose `flussonix_output_mbps` and
`flussonix_output_rate_age_ms` as native extensions. Peer-authenticated
`/flussonix/api/v1/node` and `/flussonix/api/v1/rtsp-routing` include a bounded
`stream_bitrates` map: stream name to `{ "mbps": number, "age_ms": integer }`.
At most 256 live worker measurements are included, matching the worker limit.
The LB adds probe collection/cache age before accepting a measurement; an
observation older than three seconds is unknown. No statistics, routing or
admission call starts a worker or refreshes its idle timer.

Native `/flussonix/api/v1/admit` accepts an absent bitrate hint (2 Mbps) or a
finite positive numeric `bitrate_mbps` up to 1,000,000 Mbps. Explicit null,
strings, arrays, zero, negative and larger values return 400 without inserting
a reservation. Valid values above 100 Mbps keep their actual cost. A derived
cost beyond the numeric admission bounds fails closed with 503.

## Qualification and limits

`tests/cluster_pressure.rs` exercises CPU/RAM bottlenecks, bounded readiness
preference, deterministic stateless selection, invalid metrics/estimates and
the existing hard gates. Rotation unit cases cover probe-order independence,
exact versus near ties, readiness, hard gates, full-width turns, independent
candidate sets, retry state and bounded eviction. Owned HTTP/RTSP/RTSPS fixtures
exercise sequential and concurrent distribution with real CDN reservations,
refusal fallback and interleaved protocols with different candidate sets.
Owned HTTP/RTSP cluster fixtures in `tests/rtsp_cluster.rs` exercise authenticated
placement and real CDN admission using controlled advisory telemetry, including
actual pending bandwidth on heterogeneous uplinks. Invalid HTTP telemetry
returns503 without admission or worker start. Fixture servers and applications
are stopped on both success and assertion failure.

`tests/cluster_bitrate.rs` qualifies measurements produced before the first
management request, actual worker replacement, native hint validation, large
reservations and a CDN's live output overriding a smaller hint. Shared sampler
unit cases cover peak expiry, decimal Mbps, long gaps, zero events and overflow
or clock regressions. HTTP/RTSP/RTSPS placement fixtures qualify applying a
warm peer's measured bitrate to a cold delivery candidate and its real ledger.
Delayed failed-admission fixtures cover bitrate expiry and cold-node
reconsideration for both HTTP and RTSP. A blocked-worker regression changes
measured uplink while admission waits and verifies refusal with no reservation.

The existing cluster integration suites qualify authorized source/CDN delivery,
private source pulls, secure endpoints, coalescing and admission refusal/retry.
This increment does not measure production placement quality, sustained load,
multi-LB fairness, LAN bottlenecks or GPU/CPU cost reservation. Rotation covers
exact ties on one LB with a retained candidate set; it is not weighted fairness,
per-stream affinity or a distributed policy. Independent LBs and restarts can
begin at the same hostname; changing sets or frequent eviction can still bias
traffic. Distributed fairness remains future work. The rate is an advisory shared TS
estimate with headroom, not exact per-protocol network overhead or an ABR
rendition/viewer cost model. Cold streams with no fresh observation still use
2 Mbps until they produce a measurement. Bursts, changing codecs/configuration,
and load changes between observation and admission are not fully predicted.
