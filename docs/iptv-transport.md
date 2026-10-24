# IPTV subscriber tokens by playback transport

`on_play: "iptv://"` uses the built-in subscriber and package store. Each viewer must present its current subscriber token. Tokens are checked again at final admission; successful subscriber edits cancel existing IPTV sessions.

| Playback route | Token carrier | Current behavior |
| --- | --- | --- |
| HLS/HLSS manifest, media segment, or key request | `?token=...` on **each** HTTP(S) request | Checked before media access; a key URL that has no configured handler remains unavailable. |
| MPEG-TS over HTTP/HTTPS | `?token=...` | Checked before opening the continuous response. |
| M4S/M4F HTTP(S) playback | `?token=...` | Checked before media access. The M4S/M4F ingest URLs are source credentials, not subscriber playback tokens. |
| RTSP/RTSPS playback | `?token=...` on the initial DESCRIBE URL | The token is bound to the session; an edit closes the session. |
| SRT listener playback | `u=...` in `#!::r=<stream>,m=request,u=<token>` stream ID | Checked before media delivery. |
| Native load-balancer redirect | The original viewer token is retained in the destination URL or stream ID | Each destination node checks its own subscriber store; independent stores are not a coordinated cluster authority. |
| Configured RTP/SRTP or push output | No per-viewer token carrier | Unsupported for an IPTV-protected stream; worker startup fails closed when such an output is enabled. |

Subscriber records and `max_sessions` are local to a node. Multi-node subscriber distribution and a global session limit remain separate release qualifications.
