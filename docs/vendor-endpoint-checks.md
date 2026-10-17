# Vendor endpoint checks

An explicit private-media client now requires both HTTP200 and one exact
`cluster-auth: yes` response header for its M4F/M4S control connection and every
M4F segment. This prevents an ordinary public200 response from being mistaken
for acknowledgment of the supplied Cluster credential. A denied response
remains denied even if it carries the marker. The check repeats on reconnect;
previously published media is not retroactively revoked.

## Reference findings

Read-only static inspection of the installed reference found:

- `web_router:check_cluster_auth/2` sets `cluster-auth: yes` after checking the
  stream-scoped digest. The value contains no server identity or signed response.
- M4S and M4F playback handlers use `x-playback-id` and `x-sid` for the playback
  session. Those fields are not the management `stats.id`.
- The inspected readers consume protocol/version and playback information,
  without comparing a private response server identity to management identity.

Bounded verified-HTTPS checks against an authorized reference observed a stable
management identity, successful M4S and M4F controls with affirmative markers,
and incorrect-key rejection. No serving-server identity header was observed.
M4F success was intermittent, including with version4 requested; the checks do
not establish version negotiation as the cause or qualify payload delivery.
Connections were closed, no remote settings changed, and no reference code was
executed or shipped by FlussoniX. Media bodies and credentials were not retained.

Later bounded captures using the independent pre-fix Rust client delivered
1920x1080 H.264/AAC over both M4F and M4S with strict local AV decoding; incorrect
Cluster keys produced no metadata or media. A second M4F session was rejected,
so those captures alone do not qualify reconnect reliability. Separate read-only
checks observed a closed explicit M4F playback ID rejected with
`connection_closed`, fresh IDs accepted at the control boundary, and one fresh-ID
segment returning404. A no-ID control also succeeded after elapsed time. These
observations do not establish a single cause for all intermittent failures.

Explicit vendor M4F pulls now send a fresh opaque `x-playback-session-id` for
each pull, shared by its control and segment requests. Reopening or concurrently
pulling the same input uses distinct IDs, matching the inspected reader's session
lifetime. The ID is not a credential or server identity assertion. Status and
Cluster acknowledgment checks still apply to every response; no retry or
credential fallback is introduced. Public/native inputs and vendor M4S retain
their existing header behavior.

## What operators can rely on

The check establishes that the configured endpoint acknowledged the credential
on that response. HTTPS also verifies the configured endpoint certificate.
Neither establishes the actual upstream ancestry, origin ownership, content
identity, or which backend a path-routing proxy selected. Matching DNS names,
playback IDs or separately fetched config IDs cannot strengthen that claim.

Management and private-media roots therefore remain explicit trusted operator
configuration. Split endpoints and declared relay paths need deployment-specific
validation; the application must not infer an identity binding from these headers.
A proxy must preserve the acknowledgment header for the explicit private client.
Missing acknowledgment fails closed rather than retrying with public/viewer/native
credentials. Automatic vendor routing remains disabled.

## Regression coverage

Owned HTTP fixtures exercise missing, invalid, non-ASCII, combined and duplicate
markers at both response boundaries, misleading playback/server headers, a later
segment losing acknowledgment, reconnects, valid media publication, and unchanged
public/native-peer inputs. Existing fixtures retain codec, TLS, status, redirect,
cancellation, source binding and relay-publication coverage. No official vendor
component is required to build, test or run this behavior.

Owned session regressions reject missing, reused or mismatched playback IDs and
incorrect credentials while checking exact segment bytes, metadata, sequential
reopening and concurrent pulls. Native and vendor M4S checks assert that the new
header is absent.
