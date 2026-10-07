# Native cluster resource-pressure routing

The native HTTP and RTSP/RTSPS load balancers use the same pressure ranking and
telemetry validation. This implements a bounded part of the
[cluster design](cluster-loadbalancing.md); it does not implement the four
Flussonic balancer modes or establish mixed-vendor parity.

For each eligible CDN or standalone delivery node:

```text
projected uplink = measured utilization + (reserved Mbps + 2 Mbps) / uplink capacity Mbps
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
shortlist. Prefer a ready stream within that set, then lower pressure, then
lexicographically smaller configured hostname for a stable tie. A ready node
outside the margin receives no locality advantage. These are fixed native
defaults; no new legacy API mode or UI policy setting is introduced.

The existing 2 Mbps viewer estimate is divided by each node's own capacity.
CPU and RAM remain observed utilization, without an incremental codec or cold
pull cost model. The final CDN admission reservation still rechecks measured
capacity under its own ledger lock. Advisory telemetry neither authorizes a
viewer nor starts a media worker. Routing redirects new requests; established
viewers stay on their chosen node.

## Qualification and limits

`tests/cluster_pressure.rs` exercises CPU/RAM bottlenecks, bounded readiness
preference, stable ties, invalid metrics/estimates and the existing hard gates.
Owned HTTP/RTSP cluster fixtures in `tests/rtsp_cluster.rs` exercise authenticated
placement and real CDN admission using controlled advisory telemetry, including
actual pending bandwidth on heterogeneous uplinks. Invalid HTTP telemetry
returns503 without admission or worker start. Fixture servers and applications
are stopped on both success and assertion failure.

The existing cluster integration suites qualify authorized source/CDN delivery,
private source pulls, secure endpoints, coalescing and admission refusal/retry.
This increment does not measure production placement quality, sustained load,
multi-LB fairness, LAN bottlenecks or GPU/CPU cost reservation. Equal-pressure
hostname ties are deterministic and can concentrate equivalent new requests;
distributed fairness and measured bitrate estimation remain future work.
