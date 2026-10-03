# Standalone HTTPS preview

The v0.10 profile serves existing media, publication, management and admin routes directly over TLS using independently implemented Rust code. No Flussonic components or TLS proxy are needed.

```bash
flussonix --listen 127.0.0.1:18210 \
  --https-listen 0.0.0.0:18443 \
  --https-cert /etc/flussonix/server-chain.pem \
  --https-key /etc/flussonix/server.key \
  --config /etc/flussonix/config.json --media-dir /var/lib/flussonix/media
```

Set the normal admin/peer environment credentials first. Certificate/key paths must be readable by the service account. Both TLS flags are required with the HTTPS listener. Add `--https-only` to skip the HTTP bind entirely; it requires HTTPS and does not affect separate optional RTSP/RTSPS listeners. Without HTTPS options, existing HTTP behavior is preserved. Bind only unused test ports when another media server runs on the host.

TLS1.2/1.3 and HTTP/1.1 are supported. Certificates/keys are parsed and matched, and all requested sockets bind before workers start. The client still validates certificate chain, dates and DNS/IP identity; a private CA must be installed/configured in the client. Pending TLS handshakes are capped at128 with a five-second deadline. Idle clients cannot serialize other accepted handshakes; shutdown drops pending sockets and drains established HTTP connections for at most five seconds.

Use ordinary HTTPS URLs for `/{stream}/index.m3u8`, `/{stream}/fmp4/index.m3u8`, `/{stream}/mpegts`, `/{stream}/m4s`, `/{stream}/m4f` and M4F segment paths. These retain the current H.264/AAC native wire profile. Every playlist, init/segment and continuous body keeps its existing authorization. Receive MPEG-TS with POST to `https://HOST:PORT/STREAM/mpegts?password=PASSWORD&token=PUBLISHER_TOKEN`; publisher password/callback/renewal/worker ownership stay separate from viewer and peer credentials.

Open `/admin/` on HTTPS to obtain HTTPS playback/publication links. Config displays actual HTTP/HTTPS bind addresses and whether HTTP is disabled. Those values are read-only startup status; staged stream configuration does not change listeners or certificates. TLS termination never substitutes forwarded headers for the real socket client IP. Trusted-proxy configuration is not implemented in this profile.

HTTPS viewer requests at a native LB select only peers with an explicit HTTPS public delivery URL, before reserving admission. HTTP management and private LAN endpoints remain independent. A callback redirect to plaintext HTTP is denied for an HTTPS viewer. With no eligible HTTPS CDN, return503 instead of downgrading. Ticket-redemption redirects are relative and retain TLS while removing the admission ticket.

This does not establish full private-CA input/cluster trust configuration, secure push support, all vendor wire roles, HEVC/m2a/MP3, certificate reload/expiry reporting, SRTP or encrypted SRT. Those remain in the [full contract](secure-output-codecs.md). Independent TLS clients/decoders, native payload checks and the isolated lab are recorded in [qualification](qualification.md).

Continuous MPEG-TS playback starts at the current emitted packet and can join mid-GOP. An independent decoder may log initial missing-parameter-set warnings until the next keyframe; both HTTP and HTTPS share this existing behavior. HLS segments and native M4 bootstrap are tested separately. Immediate clean TS late-join bootstrap is not claimed by this preview.
