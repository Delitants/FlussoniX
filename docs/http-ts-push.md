# HTTP and HTTPS MPEG-TS push

Streams and Templates accept `http://`, `https://`, `tshttp://` and `tshttps://`
destinations in the existing `pushes` array. These send a continuous HTTP/1.1
POST with `Content-Type: video/mp2t` and chunked transfer encoding. The aliases
select the same plain or TLS transport. This implements the MPEG-TS HTTP
publishing direction described in the [reference push documentation](https://flussonic.com/doc/fms/play/push/),
with an independently written adapter and explicitly bounded profile.

In **Input → Push destinations**, choose **HTTP (MPEG-TS)** or
**HTTPS (MPEG-TS, verified TLS)**. Enter the receiver's publishing URL using
the masked field. Set enabled, connection timeout and retry interval using
normal controls. HTTPS additionally offers an optional absolute PEM CA file
on the sending host; leaving it empty uses bundled public roots. Custom roots
replace public roots. Certificate chain, expiry and the original DNS/IP identity
must verify before any HTTP credentials or media are sent. There is no insecure
mode, plaintext fallback, proxy or redirect following.

At most four destinations total can mix HTTP, HTTPS, SRT, RTSP and RTSPS.
Template inheritance, an explicit empty stream override and disabled entries
retain existing semantics. Enabled destinations keep an on-demand stream active;
publication streams wait for their publisher. Destination edits replace the
shared worker and require viewers to reconnect. Discovered CDN mirrors do not
inherit source push destinations.

```json
{
  "pushes": [
    {"url": "tshttp://receiver.example/live/channel/mpegts"},
    {"url": "tshttps://publisher:encoded-password@secure.example/live/channel/mpegts", "flussonix_tls_ca": "/etc/flussonix/receiver-ca.pem"}
  ]
}
```

Optional URL credentials use Basic authentication for this configured receiver
only. Percent-encode punctuation and spaces in userinfo. Credentials are decoded
once as UTF-8, then removed from the request URL. The username is 1–256 bytes
without a colon; the password is at most 1024 bytes and may be empty. Neither
may contain control characters. This does not enable Basic viewer authentication.
The original encoded path and query are sent to the receiver. Runtime diagnostics
show only scheme, host and port, never userinfo, paths, queries, certificate
contents or raw network errors. Plain HTTP provides no transport encryption.

URLs are at most 4096 bytes, have valid percent encoding and a host/nonzero port,
and contain no whitespace, controls or fragment. `connect_timeout` is an integer
from 1–30 seconds, default 3; `retry_timeout` is 1–300 seconds, default 5.
`disabled` is boolean and `comment` at most 1024 bytes. Unknown or foreign
transport options fail validation without changing saved configuration.

The adapter writes the shared worker's processed MPEG-TS bytes directly, without
another remux or encoder. Its shared broadcast has 64 chunks of at most 12032
bytes; a destination retains only its current chunk. Lag terminates the attempt.
Each retry subscribes afresh after its cooldown, so failed queued media is not
replayed. Connect/DNS/TLS has the connection timeout, within a startup window of
`connect_timeout + 5` seconds covering headers and first body delivery. After
delivery starts, each body write must progress within ten seconds. Response or
control traffic does not extend that limit. Each attempt exclusively owns both
halves of its TCP/TLS socket; cancellation drops those owners directly, including
when the receiver stops reading. There is no background upload driver.

Response heads are capped at 16 KiB and 100 fields, with at most four head blocks
including interim responses. Protocol upgrades and malformed or excessive replies
terminate the request. 401/403 report `push_auth_denied`; 3xx report
`push_redirect_refused`; other non-2xx report `push_rejected`. A receiver may
acknowledge with 2xx before the upload ends, or defer its response until EOF.
Successful early acknowledgement leaves the upload active until the receiver
closes, cancellation, lag or a body-progress stall. Response body bytes are
discarded with a 16 KiB cap. Closed or failed connections retry independently.

`stats.flussonix_pushes` retains mixed-array indices. HTTP entries report
sanitized `endpoint`, `status`, `attempts`, `body_bytes`, optional `http_status`,
fixed `last_error` and `pid: 0`. `sending` and byte counters mean locally completed
body writes, not remote acknowledgement or successful decoding. Body bytes
also contribute to process HTTP egress metrics and `http_push_bytes_out`; they
are not counted as RTSP RTP. HTTP framing and TLS/network overhead are excluded.

Owned independent receivers qualify actual strict decoding of all six CPU pairs:
H.264 or HEVC with AAC, MPEG Layer II or MP3, over HTTP and verified HTTPS. Tests
also cover percent-encoded Basic credentials and queries, shared-worker ownership,
status retries alongside a healthy destination, refusal to follow redirects,
untrusted/wrong-identity/expired certificates before any publishing request,
stalled output bounds and socket closure before a blocked receiver resumes.

Authored H.264/HEVC copy fixtures qualify CEA-608 and CEA-708 with DVB bitmap and
teletext carriage on both transports. Independent payload oracles verify every
authored CEA command and original subtitle descriptors/PES bodies. Choosing to
drop separate subtitle tracks removes DVB/teletext while preserving embedded
CEA in video. HLS CEA conversion runs alongside retained HTTP output and is
verified through the resulting English WebVTT cues. HLS subtitle controls and
original transport filtering remain separate; this is the existing worker policy,
not a second subtitle converter inside the HTTP adapter.

HLS segment push, M4F/M4S push, HTTP/2, proxy authentication, Digest publishing,
mutual TLS, automatic certificate rotation, every third-party recorder dialect,
additional media combinations, WAN faults, long-duration synchronization and
production throughput/capacity are outside this qualification. Worker CPU/GPU
selection applies before the adapter; these HTTP tests qualify CPU and copy
profiles. No official Flussonic runtime component is linked or required.
