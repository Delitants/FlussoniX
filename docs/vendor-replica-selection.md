# Equivalent vendor-origin selection

The Rust library now provides an opt-in selection engine for equivalent vendor
origins. The daemon now uses it for explicitly configured groups; see
[vendor source failover](vendor-daemon-failover.md) for setup and behavior.
Ungrouped direct playback retains its existing behavior.

An embedding owner registers `ReplicaSelector` with two to eight distinct
configured Flussonic sources. Each member must have the same explicit local
content-namespace ID. The literal stream name identifies content within that
namespace. Registration is an operator assertion of equivalence; matching names
or observed server IDs alone do not establish it.

Members must have equal namespaces, exclusions and resolved processing settings.
This first contract also requires one equal normalized viewer policy across
every possible configured group membership on every member. Group-dependent
policies are conservatively unsupported by the selector; they continue to work
for ordinary direct vendor playback. Source clients retain their own Cluster
credentials and independent management/private TLS trust.

Callers own discovery. A lookup visits each member at most once, starts with the
currently selected origin and retains a healthy fallback when a preferred origin
recovers. Only transient management failures permit rotation. Authoritative
absence, disable, malformed metadata, policy mismatch or HTTP rejection from the
selected origin blocks fallback through later outages. That same origin must
return valid metadata and matching policy, or the owner must replace the selector
or configuration. Ineligible backups can be skipped. Modern management redirects
and non-server HTTP errors are authoritative; legacy inventory classification
is unchanged. Absence and disabled metadata are checked under the inventory
lease fence before changing route state; expired or superseded facts cannot block
a recovered route.

Choices and admissions remain opaque. Config, owner identity, source generation,
inventory lease, lookup ordering and selected-route generation are checked before
publication and after asynchronous native viewer authorization. A denied viewer
or callback redirect receives no private input. `publish_admitted` checks the
route before marking playback activity. The optional borrowed `pull_admitted`
checks authority during writes and idle input, and stops on route loss, lease
expiry, viewer revocation, shutdown or caller cancellation. Exported clients need
equivalent guards; selection status is diagnostic data, not authorization.

The selector retains at most 20,000 stream entries. New names fail closed at
capacity; authoritative blocks are not evicted to make room. It owns no retry
tasks, media workers, background I/O or lease renewal. Shutdown is permanent.
The daemon supplies typed private-input failure classification and shared-worker
recovery through guarded managed routes. Automatic static activation uses separate grantless ownership; see
[vendor static activation](vendor-static-activation.md). Vendor RTSP/RTSPS delivery is described in [vendor playback](vendor-daemon-playback.md).
Vendor RTSP LB placement and SRT delivery remain separate work. The standalone selector API itself still owns no daemon maintenance.
The owned fixtures and independent audio decode do not establish production
vendor interoperability or throughput.
