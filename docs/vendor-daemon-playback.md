# Vendor stream playback

The daemon can now play streams from explicitly configured Flussonic source
profiles through HTTP or HTTPS. Supported delivery paths are `index.m3u8`,
`fmp4/index.m3u8`, `mpegts`, `m4s`, `m4f` and their guarded HLS/native segments.
The independent M4S/M4F private input and existing FFmpeg packaging are reused;
no official Flussonic runtime components are used.

For a bounded live source check, run `scripts/qualify-live-vendor.py` from a
source checkout with `--binary`, `--source`, `--stream` and
`--cluster-key-file` pointing to an owner-only key file. It starts an isolated
loopback CDN with on-demand activation, requests one HLS segment through the
selected private source transport using a random local viewer token, probes and
decodes the media, then removes
its process group and files. It does not edit the source server. Local checks
against the authorized cdn4 test stream passed for both M4SS and M4FS input,
delivering decoded 1920×1080 H.264/AAC HLS. A deliberately invalid Cluster
key produced no media, and local requests without the random viewer token
returned 403. This is single-stream
interoperability evidence, not a mixed-cluster failover or capacity result.

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

Modern source inventories can omit group membership. The
[policy-equivalence rule](vendor-group-membership.md) permits configured-origin
playback only when all memberships produce the same explicit local viewer
policy. A group override that changes the policy keeps unknown membership
ineligible and fences existing playback after an edit.

All daemon source handles use one five-second complete discovery profile,
regardless of whether foreground playback, static polling or a replica lookup
creates the cached handle first. Requests remain serialized per source, with
four concurrent source queries and 500ms minimum probe spacing. Direct lookup
and static catalog polling have six-second selection limits. Complete inventories
are cached for their original bounded lease; active siblings use atomic renewal.
Configuration, source and identity changes still fence pending/active work.

Replica selection has a sixteen-second limit: five seconds for the selected
origin followed by up to seven alternatives in two waves under the four-query
limit, plus bounded selection headroom. The selected origin is checked before
alternatives, and publication follows declared rotation order. Background
recovery/static jobs have twenty seconds, retaining their eight-job concurrency
limit and cancellation on owner/configuration changes. Vendor SRT admission
also allows twenty seconds; native SRT admission remains ten. RTSP description,
placement and post-admission media timers are separate phases and are unchanged.

These are outer bounds, not completion guarantees under contention. A busy
source/group gate or query pool can still exhaust them. Discovery does not
extend a thirty-second lease, admit partial rows, bypass a denial or authorize
media. This coordination adds no input retry or public source option.

Private input failures emit a `flussonix::private_input` warning with the protocol,
processing phase, existing retry class, last HTTP status, private acknowledgment
marker and metadata progress. Status zero means no response headers were received
for the current request. A later decoding or output failure can retain an earlier
successful HTTP status; the phase identifies where processing stopped. These
fields contain no endpoint, stream name, credential, session ID, header value,
response body or raw error. They supplement the stream's `input_closed` summary
without changing admission, retry or timeout behavior. Revoked authority does not
emit this failure warning.

An optional debug event records a skipped unavailable M4F startup announcement
using only fixed protocol, HTTP status, acknowledgment and metadata fields. One
such announcement can be skipped before the first metadata, under the original
startup watchdog; the missing object is never refetched. A second missing
object still ends the pull as transient. Successful continuation emits no
terminal failure warning. Authorization, source changes and shutdown still
cancel the wait.

An HLS manifest request retains its normal eight-second read grace and also
honors the worker's remaining configured input startup allowance (15 seconds by
default). That allowance starts when the worker is created; another request
does not reset it. Worker failure, authorization and source/lease changes still
end the wait. Segment requests do not wait for missing files. Discovery keeps
its separate budgets above. This avoids an early 503 while a valid cold worker
is still starting; it does not add input retries or extend its watchdog.

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

## Large-catalog daemon regression

An owned delayed modern endpoint serves 951 configured-origin rows, with the
requested stream last. Eight concurrent authenticated M4S responses share one
complete discovery, one private input and one packager. They receive media for
32 seconds, with a two-second data-idle bound, across the original 30-second
lease and one shared catalog renewal.
The same worker supplies an independently decoded HLS audio segment; incorrect
viewer tokens cannot open native media or cached HLS segments. Removing the
stream from the catalog closes all eight responses after a new complete refresh,
within two seconds of observing removal and before the prior lease could expire.
It prevents a private-media retry. All fixture sockets, media processes and samples are
reclaimed, including assertion-failure paths.

The regression fails with the former 100-row page request because the actual
daemon discovery deadline expires before the final row. It passes with the
1,000-row request while preserving complete-catalog and authorization gates.
This is a bounded owned-origin integration check, not live Flussonic
interoperability, production capacity or seamless playback qualification.
