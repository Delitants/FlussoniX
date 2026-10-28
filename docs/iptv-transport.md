# IPTV subscriber tokens by playback transport

`on_play: "iptv://"` uses the built-in subscriber and package store. Each viewer must present its current subscriber token. Tokens are checked again at final admission. Subscriber edits revoke affected sessions immediately on a standalone node and by the lease deadline on nodes using a shared authority.

Subscriber tokens are bearer credentials in playlist and media URLs. Configure an HTTPS IPTV public origin and serve the playlist over HTTPS when viewers cross an untrusted network. An explicitly configured HTTP origin exposes those tokens to observers on that link; use it only on a trusted network.

| Playback route | Token carrier | Current behavior |
| --- | --- | --- |
| HLS/HLSS manifest, media segment, or key request | `?token=...` on **each** HTTP(S) request | Checked before media access; a key URL that has no configured handler remains unavailable. |
| MPEG-TS over HTTP/HTTPS | `?token=...` | Checked before opening the continuous response. |
| M4S/M4F HTTP(S) playback | `?token=...` | Checked before media access. The M4S/M4F ingest URLs are source credentials, not subscriber playback tokens. |
| RTSP/RTSPS playback | `?token=...` on the initial DESCRIBE URL | The token is bound to the session; an edit closes the session. |
| SRT listener playback | `u=...` in `#!::r=<stream>,m=request,u=<token>` stream ID | Checked before media delivery. |
| Native load-balancer redirect | The original viewer token is retained in the destination URL or stream ID | With a shared authority, the load balancer validates the viewer, releases its temporary slot before redirecting, and the destination checks the token and acquires its own lease. |
| Configured RTP/SRTP or push output | No per-viewer token carrier | Unsupported for an IPTV-protected stream; worker startup fails closed when such an output is enabled. |

**SRT subscriber-token exposure:** the SRT Stream ID is sent in the connection handshake and is not protected by SRT media encryption. The current direct `u=<token>` profile therefore exposes a reusable IPTV token to an observer on the network path, even when an SRT passphrase is configured. Use this profile only on a trusted network; use an HTTPS playback route for subscribers crossing an untrusted network. Treat an exposed token as compromised and rotate it.

On a single standalone node, subscriber records and `max_sessions` stay local. The shared-authority profile uses one FlussoniX node with `--iptv-lease-authority` and an HTTPS listener. Configure `--iptv-authority https://<authority>/` and the same peer key on every FlussoniX node that serves IPTV viewers, including a source or load balancer that accepts viewer requests. A load balancer that also hosts the authority needs its own `--iptv-authority` URL. Use `--iptv-authority-ca` when the authority has a private CA. Plain HTTP is accepted only with explicit literal-loopback test options.

Manage packages, subscribers and playlists on the authority. CDN subscriber files do not grant access. The authority applies one `max_sessions` count to FlussoniX viewers across its nodes; existing Flussonic-node viewers are outside that count. If the authority is unavailable, new admissions fail and existing viewers stop by their short lease deadline. After an authority restart, admissions pause for 11 seconds so old leases expire before new ones are issued. This single-authority profile is still under release qualification. The optional [two-authority PostgreSQL profile](iptv-ha.md) is also under qualification and requires external synchronous replication and primary fencing. Mixed-version rolling upgrades of separately deployed older binaries are not qualified.

Session counts use playback identities, not a device fingerprint. Continuous playback connections consume separate slots and release them when they close. HLS requests for the same stream, protocol, IP address and subscriber token on one node share a recent segment group that holds its slot for 30 seconds after the last segment, even when requests come from different players behind the same NAT. The same grouping applies to finite M4F segment requests. A second FlussoniX node acquires its own authority lease. If an exact per-player HLS limit is required, this compatibility profile is insufficient until client-specific session URLs are available.

Each client node checks authority readiness every five seconds and shows the result in its node status and IPTV page. Readiness requires an authenticated response, enabled IPTV, the end of restart quarantine and room in the authority lease ledger. A lost authority or rejected readiness check clears the client's ready state; viewer admission still performs its own live authorization check.

Every lease-authority node requires its own `--iptv-authority` URL at startup, including an LB that authorizes IPTV viewers before redirecting them.
