# Shared private source workers

This opt-in registry shares one actual private M4S/M4F input and bounded TS/native
fan-out among independently admitted viewers. It is a source-worker component,
not an automatic daemon route or the FFmpeg/HLS/transcoding adapter. Keep one
registry per embedding runtime; separate registries do not deduplicate pulls.

An attachment supplies an already admitted viewer and immutable input options.
It must match the existing worker's source authority, exact refresh ticket,
configuration revision, literal name, normalized policy and subtitle/conversion
options. Caller-owned source selection must enforce policy-equivalent selection
before admission; attaching a different policy is not a failover operation.
Current incompatible generations reject joining rather than silently replacing
a valid worker. Obsolete generations are cancelled and replaced explicitly.

The first attachment returns the only `SharedInput` startup handle. Its `run`
future owns the input session. The embedding runtime must run and join that
future. Dropping an unstarted handle, aborting the run, upstream termination or
registry shutdown closes only that generation. No task is spawned or detached
by the component. Shutdown cancels registered generations; later explicit
attachment can start a new one. Retiring an old generation cancels its input
future, but the caller must join it before requiring upstream socket teardown.
Registry retirement compares exact generation pointers so an
old handle cannot stop a replacement with the same stream name. Retirement is
an explicit operation for the trusted registry owner; viewer disconnect/revocation
must release its attachment instead of calling retirement.

Viewer handles and subscriptions retain their own native grant. Cloning a viewer
or creating its subscription retains the same attachment; final drop removes it.
Revocation of one grant does not revoke another or close a worker that still has
authorized demand. The aggregate guard checks source authority once and tests
remaining native grants without creating media clients. No viewer grant is kept
solely as an upstream anchor after its attachment is gone. The final attachment
leaving wakes cancellation; idle native revocation/source checks use a nominal
100 ms timer. This is not a real-time timing guarantee.

TS, M4S and M4F signal subscriptions check viewer and worker authority before and
after reads; M4F segment and track reads also check it. Existing instant-check,
queued-byte and scheduling limits remain. A subscription holds its attachment
until it is dropped. The TS ring retains at most 128 chunks/8 MiB, with chunks
bounded to 64 KiB; native hubs retain their existing limits. Lagging readers fail
explicitly. At most 256 source names and 20,000 attachments per worker are held.

Subtitle preservation/filtering and the existing HLS conversion state remain
generation-owned. Registry/attachment validation never invents remote policy or
renews an inventory lease. Credential acknowledgments and private transport
validation still apply to every input response.

The lock order is registry, configuration, coordinator, source, mutable route,
attachment map.
No lock crosses an await. Viewer destruction never acquires registry/configuration
locks. Output hubs are closed by their owning input generation only.

The [packaging adapter](vendor-shared-packaging.md) now connects this input to the
existing FFmpeg/HLS worker with a separate bounded mandatory pipe. An explicit
[renewal API](vendor-shared-renewal.md) preserves equivalent current managed
routes. The [session owner](vendor-hls-sessions.md) supplies bounded retention
and scheduled renewal. Automatic discovery selection, policy-equivalent failover,
persisted operator settings and load routing remain integration work. Native daemon workers keep
their current behavior.

The owned `vendor_shared_input` suite exercises actual private HTTP, independent
native authorizations, shared MP3 TS/M4S fan-out, M4F control/segment reuse,
revocation and final-grant release, configuration/identity/source/refresh/expiry
fences, stale generation retirement, pending headers, EOF, abort/drop/shutdown,
bounded slow-reader failure and subtitle conversion-state identity. Existing
private-pull suites retain text preservation/filtering/conversion and codec/TLS
coverage; this increment does not add a new subtitle decoder or GPU claim.
