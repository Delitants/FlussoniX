# Guarded private input lifetime

`ModernSourceCoordinator::pull_admitted` retains an existing viewer admission
through one actual M4S or M4F input session. The caller borrows its admitted
candidate, output writer, optional native hub, metadata sender and typed
`PrivatePullOptions`. Automatic vendor routing remains disabled.

The pull rechecks configuration, identity, source generation, latest refresh,
lease and native viewer revocation before startup. The ingest path checks current
authority before control/segment request initiation, after HTTP headers/body
reads, before metadata and caption publication, at every output-write poll,
and before native hub publication. A writer that resumes after waiting must
check again. M4F segments assembled after an edit are discarded before publication
or another segment request. The credential acknowledgment checks still apply to
every private response.

Native grant revocation and an optional owner cancellation token wake the pull
directly. A nominal 100 ms timer also checks source authority during idle network
reads and pending writes; this is not a real-time scheduling guarantee. No lock
spans network I/O or an output await. Checks apply when work is initiated or
published; they do not serialize all network activity atomically with edits or
recall requests/bytes already submitted. Earlier valid hub records remain owned
by their existing consumers.

Dropping or aborting the pull drops its pending HTTP bodies. No detached monitor,
automatic retry, lease renewal, global worker stop or shared-hub close is created.
Cancellation affects this pull only. An owning worker should pass its own
generation's cancellation token and use pointer/generation checks when retiring
it. A cancelled viewer must not stop another viewer's shared worker. The [shared-input registry](vendor-shared-input.md) adds actual shared worker
ownership and independent viewer attachments on this guarded transport.
The [FFmpeg/HLS adapter](vendor-shared-packaging.md) packages the shared input.
Equivalent-source renewal/failover and daemon resolver activation remain separate
integration work.

`PrivatePullOptions` forwards native subtitle preservation/filtering, the owning
worker's detected-track counter and the existing HLS conversion state. Conversion
observes original source tracks/frames before native text filtering, so HLS
conversion can be used while native text output is filtered. Existing input
defaults are unchanged. Trust and privileged credentials remain frozen in the
source profile; these options cannot replace the origin or its TLS settings.

Failures distinguish owner cancellation, current-authority failure and generic
input failure without including URLs, credentials or media. Every later startup
rechecks admission. A still-current admission may be reused after an input
failure; an obsolete or revoked admission cannot be revived. The caller continues
to own native authorizer renewal and downstream delivery fences.

The owned `vendor_private_pull` suite covers both protocols, stale startup,
late headers, source/alias/configuration/refresh/lease changes, idle cancellation,
pending output writes, partial M4F bodies, independent pull ownership and HTTP
body cleanup. Native text tests verify preservation/filtering and HLS-conversion
input for English and German UTF-8 text. They do not claim new CEA/DVB/teletext
decoder qualification; shared-worker behavior has its own suite.
