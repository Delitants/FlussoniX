# Vendor stream playback

The daemon can now play streams from explicitly configured Flussonic source
profiles through HTTP or HTTPS. Supported delivery paths are `index.m3u8`,
`fmp4/index.m3u8`, `mpegts`, `m4s`, `m4f` and their guarded HLS/native segments.
The independent M4S/M4F private input and existing FFmpeg packaging are reused;
no official Flussonic runtime components are used.

Configure a source as described in [saved profiles](vendor-source-selection.md).
Omitted source kind remains native FlussoniX. CLI startup owns the vendor runtime
automatically. Embedders using `App` must own and join
`App::run_vendor_playback(cancel)` and subsequently call `media.stop_all()`. An
App runtime is single-use: duplicate starts are rejected, and stop or abort
permanently cancels its source requests, HLS retention and shared generations.

Local configured streams, including disabled streams, take precedence. A vendor
source's prefix reserves that literal namespace, without renaming upstream
streams. Exclusions return 404. One prefixless vendor source reserves every
nonlocal name; overlaps outside one explicit equivalent group return 503 before discovery. There
is no native fallback on vendor absence, denial or error. SRT
uses the same reserved namespace and independent viewer admission. A native FlussoniX LB can redirect to a vendor-backed FlussoniX CDN using
[HTTP placement tickets](vendor-cdn-placement.md). Configured vendor sources on
the LB discover and authorize names through the same bounded catalog, without
local stream rows or any LB media pull. Explicit groups support
[automatic vendor failover](vendor-daemon-failover.md) and
[static startup](vendor-static-activation.md).

CDN and standalone nodes also serve vendor streams over the existing opt-in RTSP
TCP, unicast UDP and RTSPS TLS listeners. RTSP viewers authorize independently
and share the private input, processing template and RTP packager with HTTP.
Revocation and source/replica changes close the affected connection; reconnect
to obtain current authority. [RTSP/RTSPS load balancing](vendor-cdn-placement.md)
uses source-fenced CDN reservations and independent viewer admission. Foreign
admission tickets fail without media pulls.

The existing opt-in SRT playback listener also serves vendor streams on CDN and
standalone nodes. Callers select `#!::r=STREAM,m=request,u=VIEWER_TOKEN` and use
the listener passphrase when encryption is configured. Every caller authorizes
independently and shares the HTTP/RTSP processing worker and private input.
Revocation, source edits and replica changes close affected callers; reconnect
for current authority. LB and draining nodes refuse SRT playback before source
discovery. SRT has no redirect mechanism, so LB placement remains separate.
Native UTF-8 subtitles stay in M4S/M4F or convert to HLS WebVTT; they have no
implemented MPEG-TS mapping and are absent from SRT audio/video output.

Every HTTP request, RTSP DESCRIBE and SRT caller passes current source/local policy and the App's actual native
viewer authorizer. Shared input, HLS retention and cached files never authorize
requests. Native peer headers cannot bypass vendor viewer policy. Callback
redirects are preserved for HTTP/RTSP and HTTPS rejects downgrade redirects.
SRT rejects callback redirects without starting private media. Query parameters
are carried into HLS child URLs. Private Cluster credentials remain bound to
the independent source clients and never enter FFmpeg inputs or output URLs.
The admin session reauthorization action supports current active vendor routes
through the same native callback renewal path. It performs no source discovery;
missing, inactive, excluded or stale routes return 404. Callback denial closes
existing guarded responses without reconnecting the private input.

Cold discovery is serialized per source, with at most four concurrent source
queries, a three-second wire budget, four-second request selection budget and
500ms minimum spacing between attempts. Complete inventories are cached for their bounded source
lease; absence may remain cached until its original deadline. Managed routes use
the latest renewed lease. Active sibling routes refresh through shared atomic
renewal; installing an idle source is serialized with attachment. Configuration,
source and identity changes still invalidate pending/active generations.

HLS sessions retain admitted demand for up to 30 seconds without another request.
Default source leases last 30 seconds and renewal is scheduled with a 10 second
margin. Active continuous responses also keep renewal demand. Native supervision
retains only current shared workers and can retire them, without attempting a
privileged native restart. These are bounded schedules, not availability or
real-time guarantees.

Sources use copy output and existing default subtitle filtering unless a local
[processing template](vendor-processing-templates.md) is selected. Existing
transcoding/subtitle controls then apply through the guarded shared packager.
The [source policy editor](vendor-source-editor.md) configures local viewer policy
without JSON. Existing configured-stream controls are
unchanged. No new GPU, production-performance or live vendor qualification is
claimed.

Owned tests cover M4S and M4F input, original MP3/MPEG Layer II M4F segments, HLS
segment authorization, shared continuous outputs, HTTPS and redirect downgrade
protection, lease renewal past the original deadline, native supervision, local
precedence, namespace isolation, held discovery/callback cancellation and cleanup.

Owned RTSP tests cover M4S/M4F inputs, independently decoded HEVC/MP3/MP2, CPU AAC,
TCP, UDP, verified TLS, shared HTTP media, per-viewer revocation, static ownership,
source edits during startup and automatic replica recovery.

Owned SRT tests cover clear/encrypted listeners, independently decoded
AVC/HEVC/MP3/MP2 from M4F, CPU AAC, wrong transport keys, denied viewers,
independent revocation, static/HTTP sharing, held callback/metadata edits,
namespace precedence, replica recovery, pending writes and accepted-byte accounting.
