# Explicit direct-source viewer admission

The opt-in modern coordinator can now authorize a viewer against an explicit
direct-source candidate before a private-media handoff. This is an independent
Rust library path; automatic vendor routing and operator UI settings remain
disabled. It does not import remote viewer policy or require vendor components.

## Admission contract

`ModernSourceCoordinator::admit(candidate, auth, request)` consumes a bound,
leased candidate and uses the existing native `PlaybackAuth`. An embedding App
must pass its shared `app.playback_auth`; do not create an authorizer per request.
The embedding owner also retains the native authorizer's renewal duties.
Source selection and policy-equivalent failover remain the caller's duties.

Before publishing any viewer policy, admission checks the exact coordinator,
global configuration revision, local identity epoch, installed source generation,
latest refresh and lease deadline. The viewer's literal name must equal the
candidate's name. A locally configured stream, including a disabled one, takes
precedence: admission rejects it without replacing its existing viewer authority.
Unknown policy or group membership remains rejected by candidate construction.

The candidate's validated local policy is published under these fences, then
native control authorization runs without holding configuration, coordinator or
source locks. Token checks, callback denials and redirects, session identity,
revocation and capacity limits retain native semantics. No private-media HTTP
request starts during admission. After the await, every fence is checked again
for all outcomes, including denial and redirect. A late response cannot become
a decision for a replacement source. Failed or cancelled admission drops its
own native control grant without marking playback demand or stopping workers.

`AdmissionOutcome` contains `Denied`, `Redirect`, or an opaque `AdmittedCandidate`.
Only the allowed result owns a real native control grant and the exact candidate.
It is not cloneable, serializable or printable through Debug. Generic failures
contain no stream name, callback URL, credential or policy body.

## Media handoff and lifetime

`publish_admitted(admitted, closure)` checks the same source/configuration/lease
fences and native grant cancellation immediately before entering the synchronous
closure. It promotes the control grant to native playback demand and supplies
the current candidate view, private-media client and a borrowed native grant.
Never await or reenter these owners inside the closure.

`admitted_current(admitted)` rechecks the fences without promoting playback or
renewing the lease. `AdmittedCandidate::cancelled()` and `is_cancelled()` report
native grant revocation only. They do not report configuration or lease changes.
Async consumers must retain the admission, watch grant cancellation, and recheck
current authority before private I/O and delivery. These are instant checks;
exported clients and existing bytes are not automatically recalled after a
closure returns. A runtime owner still needs to cancel/reconcile the worker on
configuration, source, lease or policy changes while preserving other viewers'
shared-worker ownership. This increment starts and stops no shared worker.

The existing metadata-only `publish` and declared-path `publish_relay` APIs retain
their previous contracts and grant no viewer permission. Relay admission,
worker/session lifecycle integration, persisted friendly controls and automatic
vendor resolver activation remain pending. Media/subtitle options are unchanged;
a future daemon pull must preserve configured subtitle options explicitly.

The [guarded private-pull primitive](vendor-private-pull.md) now retains admission
through actual HTTP reads, metadata/stream writes and native fan-out, with idle
authority checks and per-owner cancellation. It forwards native subtitle and HLS
conversion options. The [shared-input registry](vendor-shared-input.md) now owns
one private pull per generation with independent viewer attachments. The
[FFmpeg/HLS adapter](vendor-shared-packaging.md) packages those generations.
Automatic daemon routing, lease renewal and failover remain pending.

## Validation

`vendor_viewer_admission` uses owned HTTP inventory, callback and private-media
endpoints. It checks callback-before-pull ordering with actual MP3 media, denied
and redirected viewers making no private request, builtin token rejection before
callback, native session limits, revocation, local-stream precedence, cancellation,
and stale success/denial/redirect responses after configuration/alias/source/refresh
edits or lease expiry. It also checks every fence again before media handoff.
Existing cluster and native authorization suites retain their separate coverage.
