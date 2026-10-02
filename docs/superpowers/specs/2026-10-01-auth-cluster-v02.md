# Playback sessions and measured uplink

Continue the approved streaming/cluster/authentication design. The next preview closes authorization lifecycle gaps and makes adaptive routing account for other services sharing the uplink. Existing test installations remain isolated from production Flussonic.

## Session contract

Keep management and peer credentials separate. A peer media request bypasses viewer callbacks but never produces a viewer grant for an ordinary client. Keep string `on_play` compatibility and accept the management schema's object form with `url`, `session_keys` and `max_sessions`. Reject unsupported policy fields. Resolve named source backends before transferring policy to an edge, preserving object options. Support literal keys name/proto/ip/token, require name/proto, preserve ordering and duplicate keys. The current manual permits omitting IP for roaming; the installed schema description differs. This preview follows the current documented key choices, without claiming the same vendor identifier hash or arbitrary query keys.

Implement a bounded per-node session registry with single-flight checks per identity. Session UUIDs remain stable across renewal; tokens/raw queries never appear in management session responses. Callbacks send the published GET parameters including proto, new_session/update_session, request_number starting at zero, UUID, client counts, duration, bytes and original query. Normalize media endpoints to hls/mpegts/m4f/m4s. Default renewal is 180 seconds, X-AuthDuration overrides it (bounded to 1..3600 seconds). Recognize X-UserId, X-Max-Sessions and X-Unique; enforce limits across streams per user on this node. HTTP 302 returns a validated HTTP(S) redirect before starting media. Global distributed session limits remain open.

A backend timeout/network error/5xx retains an existing explicit allow or deny, with a bounded retry interval; a new session without approval fails closed. Explicit denial is cached and cancels every live grant for that identity. HTTP media bodies select both worker and session cancellation. Track continuous connections, activity and bytes; expire inactive allowed sessions after 30 seconds. Config updates discard stale approval and recheck changed effective policy; late callbacks cannot overwrite a newer policy or a manual revocation.

Add GET/DELETE /sessions/{id} and POST /sessions/reauth?name=... with edit/view permissions and published status/envelope behavior. Reauth checks active sessions promptly; deleting a session disconnects its bodies and caches denial. Source deletion/disable and authentication changes reconcile related sessions. No production session API mutations are permitted.

## Uplink contract

Add --uplink-interface auto|process|NAME. Auto chooses a Linux default-route interface; NAME validates an existing interface and reads its tx_bytes counter. Process is an explicitly labelled compatibility fallback using the daemon's HTTP egress. Keep HTTP egress separate from interface egress. Interface traffic includes all processes on that interface; shared LAN/public NICs require an operator-selected interface/capacity and are not described as WAN-only accounting.

Sample CPU, RAM and bytes independently of request volume. Missing/reset/stale counters and initial warmup are unknown, never zero-load evidence; adaptive routing and admission exclude unknown measurements. Expose source/interface/age/validity in node telemetry and the current UI. Production routes, counters, interfaces and Flussonic configuration are read-only.

## Verification and release

Owned media, allocated local ports and private ignored credentials only. Test callback single flight, required parameters, limits across streams, redirect, timeout retention, scheduled deny/revoke, metadata identity continuity, config-change races, TS/M4 body termination and session APIs. Test counter deltas/reset/missing/stale/warmup and interface-aware exclusion. Run existing streaming/wire/browser regressions, then one fresh whole-branch review and fix important findings. Qualify the next binary on the existing isolated remote test units and publish code/release with exact limitations; preserve the full remaining transport/mixed-vendor/GPU scope.

Primary contract sources: installed 26.04.1 public management schema; [authorization API](https://flussonic.com/doc/api/authorization.json); [authorization lifecycle](https://flussonic.com/doc/fms/auth/description/). This document defines the implemented subset, not full reference parity.
