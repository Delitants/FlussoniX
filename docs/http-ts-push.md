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

## Intel GPU qualification

Opt-in tests qualify the existing shared hardware worker with native HTTP and
verified HTTPS publishing on Intel GeminiLake UHD Graphics 600 (`8086:3185`),
Linux `6.17.0-41-generic` / `i915`, `/dev/dri/renderD128`, FFmpeg `7.1.1-1ubuntu4.2`, independent Intel
media driver `25.3.0+dfsg1-1` and GMM `22.8.1+ds1-1`. The driver and its dependency
were isolated in private test files; no host package or preview environment was
changed, and neither is distributed with FlussoniX.

The original measured source is synthetic lavfi video and sine audio at
640×360 and 25 fps, with hardware upload and H.264 encoding,
`h264_vaapi`, CQP 24, low-power disabled, with independent AAC 96 kb/s, MPEG
Layer II 192 kb/s or MP3 128 kb/s audio at 48 kHz. Each worker sends both outputs
simultaneously. Tests verify the running encoder arguments and loaded independent
Intel driver, exact Basic header/path/query, private-CA HTTPS, template inheritance,
one reused worker, native destination PID zero and separate HTTP egress counters.
Independent full decoding requires at least 50 video and 80 audio frames per
output, changing decoded content, correct codecs and zero decoder errors.

A separate GPU→CPU→GPU replacement test decodes both outputs from all three
generations and verifies old encoder reaping, socket closure and stopped counters.
Receivers use unused loopback ports and bounded captures; encoders, decoders and
receivers are stopped after the tests, including assertion failures. These are
qualification tests for existing functionality, rather than evidence of a new
production change or a software-fallback implementation.

With an independently installed working driver environment, run:

```
cargo test --locked --test http_push gpu:: -- --ignored --test-threads=1 --nocapture
```

A real unsupported-HEVC replacement also rejects without replacing the running
H.264 GPU worker, interrupting its HTTP/HTTPS uploads or falling back to CPU.

Hardware tests are explicitly ignored in ordinary CI, whose runners lack this
device. Passing ordinary CI does not qualify GPU delivery. The same host rejects
the tested `hevc_vaapi` encoder profile; HEVC GPU publishing remains unqualified.
CBR, low-power encoding, hardware decode, Main10, NVIDIA, GPU subtitle conversion,
other hardware/driver versions and production throughput need separate evidence.
The deployed daemon still needs its own usable independent libVA environment;
these tests do not install a driver or change its capability/readiness checks.

### Compressed upstream decoding

`gpu::upstream::` extends the same Intel CQP24 profile with independently
pre-encoded 8-bit 640×360/25 fps H.264/MP3 and HEVC/Layer II MPEG-TS sources.
The FlussoniX worker receives actual compressed bytes over a private-CA verified
`tshttps://` input with percent-encoded Basic credentials and an unchanged query.
Software decoders feed NV12 hardware upload and `h264_vaapi`; this qualifies
HEVC **input decoding**, not HEVC hardware encoding. Each source is transcoded
to H.264 with AAC96, Layer II192 or MP3 128 kb/s at48 kHz, sent simultaneously
to independent HTTP and verified HTTPS publishing receivers.

The sources themselves and all twelve delivered outputs are fully decoded by
independent FFprobe/FFmpeg. Assertions require the expected codecs, changing
content, at least50 video/80 audio frames per output and no strict decoder errors.
The running encoder must receive only a loopback proxy input without upstream
credentials, use the actual Intel encoder and map the private Intel driver and
GMM dependency. Reports capture their resolved paths and SHA256 hashes plus the
three driver environment variables from the running process. Template inheritance,
shared-worker reuse, exact input/output Basic headers and paths, sanitized
statistics, input socket closure, encoder reaping and stopped counters are checked.

A separate denial test supplies an untrusted source CA and wrong input credentials.
Both must stop the encoder without sending any media, then close input/output
sockets. A configured receiver may see an empty initial POST; failed TLS must
precede the upstream HTTP request. Hardware cases
remain explicitly ignored in ordinary CI. Under a working independent driver:

```
cargo test --locked --test http_push gpu::upstream:: -- --ignored --test-threads=1 --nocapture
```

These finite, paced loopback sources originate from synthetic imagery and tones;
they do not qualify real broadcast defects, live reconnection/failover, every
compressed codec, HLS/RTSP/SRT input to this GPU publishing profile,
hardware decoding, 10-bit input, resolution changes, WAN or sustained capacity.
No source server encoder runs during delivery. Owned producer and decoder commands
have deadlines and kill-on-drop; stream and receiver cleanup runs on test panics.

HLS segment push, M4F/M4S push, HTTP/2, proxy authentication, Digest publishing,
mutual TLS, automatic certificate rotation, every third-party recorder dialect,
additional media combinations, WAN faults, long-duration synchronization and
production throughput/capacity are outside this qualification. Worker CPU/GPU
selection applies before the adapter; these HTTP tests qualify CPU, copy and the
named Intel H.264 profile above. No official Flussonic runtime component is linked
or required.

The private-driver HTTP GPU qualification helpers additionally require
`LIBVA_DRIVERS_PATH` to name a directory containing the independent qualified
`iHD_drv_video.so` and `libigdgmm.so.12` files, with that same directory included
in `LD_LIBRARY_PATH` and `LIBVA_DRIVER_NAME=iHD`. They verify both mapped files
resolve inside that directory and record their hashes. This deliberately isolated
test layout is not a production requirement; a normally installed system driver
and GMM can serve FlussoniX without these variables. See [admin dependency and
encoder readiness](vaapi.md#dependency-and-encoder-readiness-in-the-admin-ui).


### Native M4 source decoding

`gpu::native::` qualifies the same Intel H.264 CQP24 software-decode/hardware-encode
profile with M4S frame records and M4F single chunks. Plain `m4s://` / `m4f://`
and verified private-CA `m4ss://` / `m4fs://` inputs carry H.264/MP3 or HEVC/Layer II
at 8-bit 640×360/25fps. Each of the eight input combinations is transcoded to
H.264 with AAC96, Layer II192 or MP3 128kbps at48kHz and sent concurrently to
HTTP and verified HTTPS receivers:24 cases and48 strictly decoded outputs.

The fixture begins with independent FFmpeg-encoded TS. FlussoniX's TS decoder and
native record/sample-table packers construct the native source; this is not an
independent vendor dialect writer or proof of full Flussonic interoperability.
Independent FFmpeg fully decodes both the original and the native TS remux with
identical frame counts. The finite M4F source serves only two-track windows,
excluding its audio-only trailing window to maintain stable live metadata.
Published outputs require at least50 video/80 audio frames, changing content and
zero strict decoder errors. The worker receives compressed media over `pipe:0`,
uses software decoding and NV12 upload, and must map the actual independent Intel
driver and GMM. This qualifies HEVC input, not HEVC hardware encoding.

Native control and every M4F segment request must retain the exact percent-encoded
Basic identity and token query, with no cluster peer header. Publishing uses its
separate Basic identity; neither identity appears in encoder arguments or worker
statistics. Template inheritance, one shared encoder, closed input/output sockets,
reaped encoder and frozen egress counters are asserted. Wrong source Basic,
untrusted CA and denied M4F segments must start no media encoder and send no media.
A destination may see one empty initial POST while native metadata is pending;
that connection must close. Failed TLS precedes upstream HTTP.

These opt-in hardware tests use the independently installed system driver/GMM
without special libVA variables. Expected mapped files default to the system
paths; `FLUSSONIX_HTTP_GPU_IHD_FILE` and `FLUSSONIX_HTTP_GPU_GMM_FILE` may select
other independently installed files. `FLUSSONIX_HTTP_GPU_EVIDENCE_DIR` retains
source/remux/output TS and reports, including mapped dependency hashes and the
running encoder's three driver-related environment variables.

```
cargo test --locked --test http_push gpu::native:: -- --ignored --test-threads=1 --nocapture
```

A nonignored source/remux and M4 packer characterization also runs in ordinary CI;
it does not qualify hardware. Finite paced loopback sources, owned TLS and bounded
captures do not qualify vendor packed-GOP dialects, M4F multiple chunks, Main10,
hardware decoding, HEVC GPU encoding, other GPUs/drivers, subtitles in this
matrix, live recovery, mixed-vendor clusters, WAN faults or sustained capacity.
No official Flussonic component is used; all owned test media processes and
listeners are stopped, including on assertion failures.
