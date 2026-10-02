# Administration UI design

Build a familiar operational interface with FlussoniX branding and independently written components. Preserve the requested information architecture and workflows. Initial reference evidence is the installed UI entry point and localization keys. A six-screen Superdesign UI draft is now available in [UI review](ui-review.md); Streams and Cluster render checks were completed. A pixel-level comparison against the vendor UI has not been completed.

## Navigation

```text
FlussoniX          Media ▾              Config         Cluster
                  Streams
                  Templates

Node status        streams · viewers · input/output bitrate · CPU · memory
```

Media opens Streams by default. Keep navigation and stream identity visible while editing. Use a compact light interface, dark readable navigation, restrained blue actions, thin separators and dense operational tables. Suggested values are a 48 px header, 36–40 px table rows, 14 px body text and a 13 px table font; these are design choices, not extracted exact vendor measurements.

## Streams

```text
Media / Streams                         Search…      + Add stream
All · Running · Waiting · Disabled · Problems       Filters

Status  Name / title   Input   Bitrate   Viewers   Transcode   Actions
●       news-hd        M4F     6 Mb/s    120       CPU         …
●       sports        SRT     8 Mb/s    340       GPU         …
○       backup        HLSS    —         0         Off         …
```

Rows show useful state, not every field. A row opens stream details with Overview, Input, Output, Transcoder, Auth and Diagnostics. DVR can be added when the capability exists.

Input editing preserves ordered failover sources and per-input options. Include RTSP/RTSPS pull and incoming publication plus direct RTP/SRTP receive. Output settings include RTSP/RTSPS playback and push, and direct RTP/SRTP transmit. Show UDP/TCP transport selection, SDP/track settings, bind interfaces/ports, TLS trust and SRTP key references when applicable; distinguish control encryption from media encryption. Output settings expose protocol availability and push destinations. Transcoder settings show audio/video tracks, renditions, CPU/GPU choice, required capabilities and estimated resource availability. Auth settings distinguish inherited and explicit rules.

Always distinguish saved configuration, effective template settings and current stream state. A saved configuration can still have an unavailable source. Display that failure in the row and detail view rather than a generic success indicator.

Creation needs name, optional title, source or publish mode and optional template. Advanced protocol options expand when relevant. Form validation comes from the same contract as the API; destructive operations require ordinary product confirmation.

## Templates

List templates with name, source/publish pattern and number of dependent streams. Show inherited versus overridden settings in stream details.

Editing a template previews affected streams and whether reconciliation will reconnect inputs or restart encoders. Save a single revision. Do not imply that a template edit is isolated to the currently viewed stream.

## Config

Provide structured sections for listeners/TLS, API access, authentication backends, runtime limits and advanced settings, plus a Flussonic-style text editor.

Validate without applying, show precise diagnostics, then display the configuration diff and affected services. Applying commits a revision. Show saved/applied status and runtime failures separately. Keep existing secret values masked while allowing explicit replacement.

## Cluster

Views: Overview, Sources, Peers/CDN nodes, Balancing. The overview shows the LB → CDN → source topology, node roles, media freshness, ingress/egress, viewers, CPU/GPU pressure and reachability.

Show separate public delivery, private media and management endpoints. Per-stream views identify its sources, ready CDN replicas, selected upstream and on-demand pull state. Balancer views show configured uplink capacity, observed/projected saturation, CPU, memory headroom, pending admissions and telemetry age. Expose node draining and explain selection/rejection decisions. Legacy modes and the native adaptive policy are visibly distinct. See the [cluster design](cluster-loadbalancing.md).

Cluster capability views include RTSP/RTSPS and RTP/SRTP receive/transmit roles, supported keying profiles and media-port reachability.

Source editing includes protocol, origin URL, filters, prefix and inherited stream settings. Peer details expose capabilities and reference-version compatibility state. A mixed node displays the directions/features that have actually been tested.

Actions for draining, placement and native cluster changes must reflect implemented capabilities. A healthy API heartbeat alone must not make a stalled-media node appear healthy.

## UI behavior and verification

Use TypeScript/React with reusable tables/forms and a typed API client. The UI exercises the compatibility API; FlussoniX-only controls use a separate extension endpoint. Virtualize long lists, cancel obsolete requests, pause polling in background tabs and use bounded incremental updates.

Keyboard navigation, visible focus, labelled controls and status text accompany colors. Test empty/error/loading states, large stream lists, validation diagnostics, source outage, encoder resource errors and saved-versus-runtime differences. Render comparison with a reference UI is future work, using screenshots or a permitted lab instance.
