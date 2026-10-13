# Configure a Flussonic source

Open **Cluster → Source servers → Add source**, then choose **Flussonic** as the
source type. Enter an M4F/M4FS/M4S/M4SS server address and its Cluster key. The key
is a private upstream credential; it does not grant viewer access.

Management and private media endpoints can use separate HTTP or HTTPS addresses.
For private certificate authorities, enter absolute PEM file paths on the
receiving FlussoniX node. Vendor media trust does not inherit the management CA;
leaving its field empty uses public roots. Configure each private CA separately.

Choose a viewer policy: no callback with a token guard, explicit allow without a
callback, a named backend or a callback URL. New profiles start unresolved and cannot serve viewers until
applicable policy exists. An optional SHA256 token guard runs before callbacks.
Group overrides use upstream group names and inherit unset source policy fields.
Conflicting matching groups deny playback. Callback session limits and ordered
session identity keys, including repeated keys, are editable without JSON.

Use distinct stream prefixes for multiple vendor sources. A profile without a
prefix reserves all nonlocal names. Overlapping vendor namespaces block playback;
exclusions never fall back to another source. Local configured streams take
precedence.

**Save** applies a Cluster edit. In **Config**, **Stage changes → Validate →
Save & apply** uses the existing staged workflow. Native settings incompatible
with a vendor profile must be explicitly removed. Switching back to native also
requires explicitly clearing vendor viewer policies; the editor never silently
drops them.

Choose a [Processing template](vendor-processing-templates.md) to apply existing
Media template transcoding/subtitle settings. Template inputs and viewer policies
are not inherited. Clear this selection explicitly before switching source kind.

Vendor profiles currently serve on-demand HTTP/HTTPS HLS, MPEG-TS, M4S and M4F.
Static activation, failover, load balancing and RTSP/SRT delivery remain pending.
The native source editor retains its existing controls.
