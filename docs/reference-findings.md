# Reference findings

Observed 2026-09-14 HST / 2026-09-15 UTC. Reference inspection only.

## Local installation

- Installed package: **Flussonic 26.04.1**.
- Changelog begins with Media Server 26.04.
- Public management schema: 139 operations / 74 paths / 508 component schemas.
- Private management schema: 172 operations / 96 paths / 560 component schemas.
- Bundled streaming schema: 58 operations / 57 paths / 73 component schemas.
- Downloaded public management schema: 139 operations / 74 paths / 461 component schemas; info version 26.03-499.
- Downloaded public streaming schema: 54 operations / 53 paths / 58 component schemas.
- Downloaded callback schema: two operations; info version 21.12.1.

The installed public management schema has the same method/path set as the downloaded public schema. This does not establish identical field semantics. The generated evidence records hashes and the streaming route differences.

The package contains compiled BEAM modules, native libraries, schemas and bundled web assets. Thirty of the top-level contrib .erl files are actually ZIP-wrapped compiled escripts. In particular, m4s_debug.erl is not readable Erlang source.

## Static toolkit analysis

Used the supplied Erlang BEAM Toolkit in extract mode on local m4f_reader, m4s_reader and m4stream modules. The toolkit documents that extraction does not load the inspected modules. The bundled runtime path was rejected because of symlinks; the installed /usr/lib/erlang/bin runtime worked and the outputs report OTP 27.

Extraction completed into:

- /tmp/flussonix-m4f_reader-static
- /tmp/flussonix-m4s_reader-static
- /tmp/flussonix-m4stream-static

Module SHA-256 values:

| Module | SHA-256 |
|---|---|
| m4f_reader | 982de7effbc2bc86bea02bbd93a07ff30953153952e26780ba8d33b730c4fbf7 |
| m4s_reader | fcd8e371175bb0d1b65d8f25130a211f06d7aa6e498fa31eb7777e1d38ba61ca |
| m4stream | 11cd5a8600349b97574c6efb4a88fa96e08953b4808c44ae6c02e1f59dd88ecc |

The analysis supports separating M4F signaling/segment fetch from M4S persistent framing and identifies shared container dependencies. Complete framing, authorization, container decoding, discovery and edge-case semantics remain to be established. No source rebuilding, binary patching, live runtime tracing or deployment occurred.

## Functionality demo

The user explicitly clarified that the supplied host is **a functionality demo only**. It is not a migration source. These observations are examples of functionality, not migration acceptance criteria.

At the complete inventory snapshot:

| Observation | Value |
|---|---|
| Version | 26.04.1 |
| Stream definitions enumerated | 176, all unique; both pages retrieved |
| Primary input schemes | 90 TSHTTP, 55 M4F, 18 RTSP, 3 HLSS, 10 with no scheme returned |
| All configured inputs | 177; one stream has a second input |
| Static / disabled flags | 4 / 10 |
| Streams with playback authorization | 175, using iptv scheme |
| Streams with transcoder settings | 6 |
| Advertised encoder device | CPU Encoder |
| Reported current active streams / clients | 5 / 4 at that instant |
| Reported input / output | 154,925 / 8,658 kbit/s at that instant |
| Cluster sources/peers/templates | Not present in the returned full configuration |
| DVR settings | None returned for the selected stream fields |

The scheme-less inputs require interpretation; they must not automatically be labelled invalid or empty. The transcoder samples include AAC audio conversion at several bitrates. No GPU was advertised by this demo snapshot; GPU support remains an explicit product requirement.

Initial inspection used two metadata GETs, the complete inventory used three, and a supplemental profile inspection used one: **six authenticated metadata GETs total**. No media URLs were opened, so inspection did not intentionally activate on-demand playback.

Raw configuration responses and credentials were not saved. The retained observation files omit stream names, source/backend URLs, account records and tokens. A snapshot does not establish peak load, available hardware elsewhere or a complete behavior contract.

## RTSP/RTP scope extension

RTSP, RTSPS, RTP and SRTP were subsequently added by the user to the first release, in both inbound and outbound directions. This is a product requirement independent of the demo's RTSP inputs.

The installed public schema defines RTSP/RTSPS input patterns, `rtp: udp`, `wait_rtcp`, an RTP input pattern and a TLS-composed RTSPS listener. No SRTP text or SRTP-named schema component was found in that file. Standalone SRTP interoperability/configuration and RTSP publication directions remain lab-verification items; the absence of a public schema entry does not establish absence of runtime support.

## Cluster and LB follow-up

The user specified one LB selecting CDN nodes by uplink saturation, CPU and RAM; each selected CDN reuses local media or pulls from a source over the LAN. This topology is now an explicit first-release requirement independent of the functionality demo.

Read Flussonic's cluster-restreaming and load-balancer manuals, the installed source/peer/balancer schemas, and statically extracted `balancer`, `cluster_source` and `cluster_peer` using the supplied toolkit. Extraction writes only temporary analysis workspaces; no demo/network-host changes were made.

| Module | SHA-256 |
|---|---|
| balancer | d46354d1bf8f7b0b908d23f6ef6f0688133439bf662207e1c6842c128bb77eea |
| cluster_source | 47de301c2738fa4131e84ea5377cb525dea027357885342ccfd041ef4d05f9fb |
| cluster_peer | ffc19d15e87bc5e9f6011e59d206f423f7e9182ce4ed177b527aecb7a1208e60 |

Static evidence shows balancer mode dispatch, ranking, content/country candidate filtering, maximum-bitrate checks and client-affinity state; source candidate/prefix resolution is separate. The local bitrate-limit function applies a factor of 1024 to its observed-rate argument, so the compatibility suite must verify units end to end rather than infer them from an `*_kbit` field name. These observations are not a runtime test or a proof of the complete transitive selection policy.

See [cluster and load balancing](cluster-loadbalancing.md) for the proposed native policy, source roles, private/public endpoints and release gates.

## Evidence limits

No M4F/M4S interchange test, media capture, codec benchmark, UI rendering test, mutation or migration was performed. Static analysis and schemas identify implementation questions; lab tests must answer them before any compatibility claim.
