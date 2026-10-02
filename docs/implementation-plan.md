# Implementation plan and release gates

The current deliverable is the design, reference evidence and operation inventory. No runtime endpoints, protocol adapters or web application have been implemented. The sequence below is the development backlog; listed tests are planned, not passed.

## Gate 0 — Establish executable contracts

Create an independent fixture harness and a disposable reference lab. Pin the installed schema hashes, package version and codec fixtures. Record route-level and behavior-level assertions, including disagreement between online schemas and 26.04.1.

Prioritize normal M4F/M4S handshakes and media framing early, because they are the largest compatibility uncertainty. Write a minimal transport/container specification before committing the media core to a particular representation.

**Exit:** reviewable M4F/M4S contract draft, sample normal-flow fixtures, version profile and an acceptance catalogue with no invented packet grammar. Static analysis from this design is input to this gate, not its completion.

## Gate 1 — Working configuration and API foundation

Create the Rust workspace and daemon, compatibility router, configuration parser/store, explicit/effective configuration split, and stream/template repositories.

Implement Streams/Templates CRUD, configuration validation, API credentials, query projection/filtering/cursors and stream-supervisor state transitions. Start with real persisted behavior and meaningful status; do not return success for an unimplemented media pipeline.

**Exit:** restart persistence, partial update/null/reset semantics, templates, read-only versus edit credentials and exact first-scope response contracts pass against fixtures.

## Gate 2 — Streaming vertical slice

Implement HLS/HLSS and TSHTTP/TSHTTPS ingest, bounded encoded timeline, HLS/MPEG-TS playback, and SRT adapter. Implement RTSP/RTSPS client and server roles for inbound pull/publication and outbound playback/push, plus direct RTP/SRTP receive and transmit. Include on-demand start/idle shutdown, source failover, TLS, normal publish/push paths and requested URL aliases.

Build shared RTP/RTCP packetization, SDP handling, UDP and TCP-interleaved transports, bounded jitter queues and sender-report clock mapping. Add libsrtp with provisioned-key and protected-SDES profiles; track DTLS-SRTP as a separately verified negotiation profile. RTSPS listener/client TLS and SRTP media protection have independent configuration and tests.

Connect viewer/publisher auth, configured backend rules and IPTV authorization. Add session visibility and revocation. Implement the first Streams and Templates UI pages against working APIs.

**Exit:** controlled source → authorized viewer, reconnect, token denial/revocation, on-demand operation, multitrack and timestamp changes work across requested directions. No unbounded queues or per-viewer ingest processes.

## Gate 3 — M4F/M4S and mixed-node clustering

Implement receiving and serving/publishing roles, then source discovery, peers, source filters, credential behavior and balancing. Finish all four interoperability rows in the compatibility matrix.

Implement the user's LB → CDN → source-over-LAN flow, with separate public/private endpoints and reuse of fresh local streams. Add source discovery/candidate resolution, native uplink/CPU/RAM selection, per-edge admission reservations, stream-start coalescing, affinity, draining and source/CDN/LB failure handling. Keep the four reference balancing modes separately testable. See the [detailed cluster plan](cluster-loadbalancing.md).

Native cluster coordination adds placement leases, explicit authority, fencing, health, routing and resource reservations. Build the Config and Cluster UI surfaces. Native consensus compatibility does not imply compatibility with legacy peer control messages.

**Exit:** a reference Flussonic node accepts FlussoniX as upstream and downstream for the tested transports. Chained relay, source outage, node loss, rejoin and configuration changes have quantified behavior. Segment/timestamp preservation is verified where required.

## Gate 4 — CPU/GPU transcoding

Deliver the isolated codec-worker protocol, CPU pipelines, NVIDIA GPU backend, device inventory, admission control and failure recovery. Integrate video ladders, GOP alignment, audio conversion and volume controls with HLS/TSHTTP/M4F/M4S/SRT/RTSP/RTSPS/RTP/SRTP in both inbound and outbound pipelines.

These capabilities are first-release requirements; Gate 4 is an implementation order, not a decision to defer transcoding beyond the first release. Additional GPU vendor support is hardware-gated.

**Exit:** CPU and a named GPU/driver combination meet declared encode profiles in real time; streams recover from worker/device failures without affecting unrelated streams.

## Gate 5 — Release qualification

Complete the first-scope UI, API and transport matrix. Run comparative benchmarks and long-duration streaming in the lab, then package a reproducible Linux service/container with upgrade/rollback and configuration export.

Validate interoperability on every supported reference version. A future migration plan is written only after the user identifies actual migration servers.

**Exit:** first-release acceptance report and explicit remaining feature gaps. Full Flussonic parity remains a tracked objective rather than a blanket release claim.

## Acceptance catalogue

| ID | Requirement | Test evidence |
|---|---|---|
| API-01 | Partial PUT/null/reset | Before/after explicit and effective state versus reference |
| API-02 | Collection behavior | Cursor traversal, sorting, projection, filters, default envelope |
| API-03 | Path handling | Multi-segment names and encoded source URL routing |
| CFG-01 | Text and JSON validation | Valid/invalid config and diagnostic locations; validation changes no state |
| CFG-02 | Durable configuration | Restart and failed-write recovery; template changes reconcile correctly |
| STR-01 | On-demand/static/disabled | Active transitions, last viewer departure and retry policy |
| STR-02 | Input failure | Source outage, fallback order, discontinuity and recovery time |
| HLS-01 | HTTP/HTTPS ingest and playback | TS/fMP4, track selection, changing init data and playlist windows |
| TS-01 | MPEG-TS over HTTP/HTTPS | Pull, long-lived playback, POST publication/push and reconnect |
| SRT-01 | Required SRT roles | Stream IDs, encryption, latency configuration and authorization |
| RTSP-01 | RTSP inbound/outbound | Pull, incoming publication, playback serving and push publication; UDP/TCP, SDP, auth and reconnect |
| RTSPS-01 | RTSPS inbound/outbound | All RTSP roles over TLS; peer validation and explicit UDP media protection |
| RTP-01 | RTP inbound/outbound | Direct receive/transmit, unicast/multicast, RTCP, payload mappings, jitter and pacing |
| SRTP-01 | SRTP inbound/outbound | Receive/transmit plus SRTCP, key/profile interoperability, rekey/restart, replay and wrong-key rejection |
| M4-01 | Mixed-node transport | Both directions, pull and push, plain and TLS |
| M4-02 | Identity and clocks | Original segment hashes, ordering, track metadata, DTS/PTS/UTC |
| M4-03 | Lifecycle | Late join, normal disconnect, codec change and restart |
| AUTH-01 | API credentials | Basic/Bearer and view/edit capabilities |
| AUTH-02 | External decisions | Allow/deny/redirect, renewal, expiry, outage and parallel rules |
| AUTH-03 | IPTV | Existing token/package/subscriber semantics and playlist access |
| AUTH-04 | Global limits | Concurrent admissions, reconnect, revocation and owner failure |
| CL-01 | Legacy sources/peers | Discovery, filters, local precedence and source outage |
| CL-02 | Native ownership | Partition, lease expiry, fencing, recovery and explicit legacy authority |
| LB-01 | Required topology | Viewer redirect to CDN; local-ready reuse or private source pull |
| LB-02 | Resource selection | Heterogeneous uplinks, CPU/RAM limits, LAN and shared-link budgets |
| LB-03 | Admission races | Multiple LBs, burst reservations, expiry and no double accounting |
| LB-04 | Shared source ingest | Concurrent first viewers start one logical pull per CDN/profile |
| LB-05 | Failures and drain | Stale metrics, no capacity, source/CDN/LB outage and drain |
| LB-06 | Auth and protocol routing | Token/path/query preservation, session accounting, transport-specific routing |
| LB-07 | Legacy LB modes | Clients/bitrate/usage/streams, affinity, filters and units versus reference |
| LB-08 | Endpoint separation | Public redirects and private media paths including unavailable private route |
| LB-09 | Source directory | Updates, precedence, equivalent alternatives and loop prevention |
| TC-01 | CPU | Audio/video output profiles, alignment, latency and A/V synchronization |
| TC-02 | GPU | Named device/driver, real-time ladder, admission and recovery |
| UI-01 | Required screens | Streams/Templates/Config/Cluster with accurate persisted/runtime state |
| PERF-01 | Fan-out | Throughput, CPU/Gbit/s, RAM, connection count, queue delays and TLS overhead |
| PERF-02 | Sustained load | Provisional 24-hour soak, no growth after warm-up and quantified reconnect gaps |

Performance gates will bind to hardware and fixture profiles. Suggested engineering targets, pending measurements: metadata API p95 below 100 ms under the declared media load; control-plane outage does not block established media delivery; memory stays within configured queue/cache budgets. Media latency and failover limits must be stated per protocol and GOP length.

## Test boundaries

The supplied functionality demo was read only through configuration and stream metadata GETs. It is not used for production migration, new stream creation, authentication changes, load generation, packet mutation or failover experiments.

The future test harness uses synthetic/owned media and explicitly configured lab nodes. Read-only demo observations inform UI and behavior questions; they do not substitute for independent fixtures or prove complete compatibility.

## Later scope

DVR archive parity, RTMP, WebRTC, DASH/LL-HLS extensions, VOD, DRM, ad insertion, broadcast hardware, Watcher/VSaaS and other public/private API families are tracked separately. Their operations are inventoried so they are not silently forgotten. The user can move any of these into the first release.
