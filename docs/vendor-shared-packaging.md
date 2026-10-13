# Shared private input packaging

`Engine::package_shared` consumes a `SharedInputAttachment` and local output
settings, returning `PackagedPlayback`. The existing FFmpeg worker supplies live
TS, native M4S/M4F and TS/fMP4 HLS. This connects the opt-in shared source worker
to packaging; it does not enable automatic daemon discovery or playback routes.

Use one source registry and Engine pairing per embedding runtime. Hand the first
attachment's unique startup handle to this API before starting it elsewhere.
Later attachments with the same input generation and output signature reuse the
worker. Incompatible live workers/settings are rejected. Retired source
identity is checked directly, so a fresh generation can replace an obsolete
packager without inheriting its retry backoff. Settings are immutable within an
input generation; retire/reselect explicitly before changing them.

Settings accept only `transcoder`, `flussonix_input_timeout`,
`flussonix_subtitle_tracks`, `flussonix_hls_subtitles` and
`flussonix_hls_captions`. Existing profile/capability validation applies. Input
URLs, keys and source overrides are rejected. When present, the input timeout
must be an integer from 1 through 300 seconds; invalid values fail before startup. FFmpeg receives local MPEG-TS on
stdin; only the independently implemented source client contacts the origin.
Configured push/direct-output integration is deferred until their output fences
are connected to source authority. Existing native daemon outputs are unchanged.

The Engine's worker owner runs, cancels and joins the unique shared input and
pipe tasks. They hold aggregate source/demand control, never an individual viewer
grant. The first viewer being revoked does not stop another authorized viewer,
including during the startup-owner handoff. Cloned playback handles and
subscriptions retain one attachment until final drop. Last authorized demand,
source/configuration/refresh changes, lease expiry and Engine shutdown end the
input generation. Call `Engine::stop_all` during embedding shutdown; existing
Engine ownership semantics still apply.

The packager has a separate mandatory TS channel bounded to 128 chunks of at
most 64 KiB each (8 MiB). Channel reservation backpressures the source before
committing another chunk. Metadata wakes the consumer before draining the pipe,
so a complete M4F GOP cannot overflow a live-viewer ring during process startup.
The existing bounded viewer ring remains independent; slow viewers may report
lag without stalling others. A stalled required consumer is terminated by the
worker watchdog. Both private input and downstream pipe writes recheck source
and aggregate demand at every writer poll, including the final child-stdin copy after pending I/O. Specific unsupported
subtitle diagnostics are recorded before closing the required pipe.

`PackagedPlayback` exposes guarded TS/M4S/M4F subscriptions. `Engine::read_shared`
checks the viewer, source and exact installed output worker before/after HLS or
segment reads. A cached segment never substitutes for authorization. Embedding
handlers must use these APIs and admit the actual viewer request through their
shared native authorizer; ordinary Engine/Worker APIs remain privileged APIs.
Instant checks cannot recall bytes already committed, and nominal 100 ms idle
checks are not a real-time guarantee.

Full copy shares the original native hub, retaining original M4S records and
M4F segments, codec metadata and selected native text. Transcoding uses the
existing decoded-output bridge. Native text preservation with unsupported
transcoding fails explicitly, including text appearing later in a sparse M4F
segment. Text filtering and HLS conversion can operate independently. Native
conversion uses the input generation's exact state and matching service settings;
observed source text-track counts reach worker statistics even when filtered.

Owned tests exercise real private HTTP and FFmpeg, one pull/packager across
viewers, revocation, first-viewer handoff, stale startup, metadata waits, source
replacement and HLS fencing, spawn failure, late headers, stalled consumers and
cleanup. Independent audio decoding verifies HLS copy and CPU conversion; a
multitrack M4F GOP preserves the original segment. English/German native text is
preserved or filtered, and HLS conversion produces WebVTT. Existing native
codec/TLS/transcode suites retain their separate coverage; no new GPU hardware
or live vendor qualification is claimed.

An explicit [shared-input renewal API](vendor-shared-renewal.md) can preserve
equivalent workers beyond a discovery deadline. Automatic route activation, HLS
session retention, scheduled renewal and source failover remain integration
steps. Without successful explicit renewal, workers stop at the lease deadline.
