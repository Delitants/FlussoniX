# FlussoniX

A new media server designed for Flussonic API and streaming interoperability.

**Status: initial design and reference analysis. No streaming server or compatibility claim exists yet.**

First release: **Media → Streams, Media → Templates, Config, Cluster**, viewer/publisher authentication, **HLS, HLSS, TSHTTP, TSHTTPS, M4F, M4S, SRT, RTSP, RTSPS, RTP, SRTP**, and **CPU/GPU transcoding**.

RTSP/RTSPS and RTP/SRTP are required in **both inbound and outbound directions**, including pull/receive and serve/transmit/push roles as applicable.

Recommended implementation: Rust server core, supervised FFmpeg transcoding workers, TypeScript/React administration UI. The compatibility baseline is Flussonic **26.04.1**, with explicit profiles for other versions when required.

- [Architecture and language decision](docs/architecture.md)
- [API, protocol and authentication contracts](docs/compatibility.md)
- [LB → CDN → source architecture and adaptive selection](docs/cluster-loadbalancing.md)
- [RTSP/RTSPS and RTP/SRTP inbound/outbound contract](docs/rtsp-rtp-support.md)
- [Implementation milestones and acceptance gates](docs/implementation-plan.md)
- [Administration UI design](docs/ui-design.md)
- [Reference findings and limitations](docs/reference-findings.md)
- [Generated API operation inventory](docs/evidence/api-operations.csv)
- [Schema provenance and version comparison](docs/evidence/schema-inventory.json)
- [Sanitized demo observations](docs/evidence/demo-observation.json)

The supplied network host is a **functionality demo only**. It is not a migration source or a performance baseline. Future migration servers and capacity requirements have not been supplied.

The implementation will be independently written. Reference schemas, observed behavior and static protocol analysis define interoperability tests; vendor binaries and UI bundles are not application dependencies.

The small utility in `tools/inspect_demo.py` performs only bounded, authenticated GETs of configuration and stream metadata. It writes aggregate observations without source URLs, stream names, tokens or credentials. It was used to examine the demo; it is not a migration tool.
