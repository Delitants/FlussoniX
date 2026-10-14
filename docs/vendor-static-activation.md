# Keep vendor streams active

In **Cluster → Source servers**, choose a Flussonic source and set **Stream
activation**:

- **All static streams** (default): keep eligible, enabled static streams active.
- **Selected streams**: enter names or subtrees such as `sports/*`. Names are
  relative to the configured prefix; streams are never renamed.
- **On demand**: start inputs only for authorized viewers.

Exclusions and locally configured streams take precedence, including disabled
local streams. Configure a local viewer policy and any processing template.
Static startup creates no viewer sessions, calls no viewer authentication
backend, and consumes no viewer limits. Each viewer still needs authorization.
Viewers and static owners share one compatible input and packager.

CDN and standalone nodes refresh authenticated catalogs every ten seconds.
Selection expires within thirty seconds without a successful refresh. Removal,
disable, demotion, expired authority or configuration changes retire stale
ownership. Equivalent-origin groups use the existing guarded failover and
private rejection barriers. Each selected origin must match its activation
settings. Load balancers perform no eager vendor polling or media startup.

The Cluster page reports selected and active streams, discovery failures and
capacity. At most 256 shared vendor inputs and eight concurrent startup/recovery
jobs are supported per node; discovery uses four concurrent jobs. Select fewer
streams or use On demand when capacity is reached. HLS, MPEG-TS, M4S and M4F
outputs use the normal processing and authorization paths.

Owned tests cover automatic startup, CPU audio conversion, renewal, failover,
viewer sharing and shutdown. Production vendor interoperability and capacity
qualification remain pending. Vendor RTSP/SRT delivery is separate work.
