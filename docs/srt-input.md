# SRT inputs

Add an SRT MPEG-TS source in **Media → Streams** or **Media → Templates**. Caller
mode connects to an existing SRT listener:

```text
srt://source.example:8002?mode=caller&passphrase=choose-a-long-secret&pbkeylen=24&tsbpdmode=1
```

Listener mode accepts one incoming SRT sender:

```text
srt://0.0.0.0:9000?mode=listener&passphrase=choose-a-long-secret&pbkeylen=32&tsbpdmode=0
```

The input uses the system **libsrt 1.5** library. FlussoniX receives encrypted
transport packets itself and gives FFmpeg only a local MPEG-TS pipe. The source
URL, passphrase and stream ID do not appear in the decoder's command line.
Secrets remain in the saved, access-controlled configuration and in process
memory. A mismatched passphrase is rejected; plaintext is not accepted as a
fallback for an encrypted input.

`passphrase` accepts 10–79 printable ASCII characters. `pbkeylen` selects
AES-128 (`16`), AES-192 (`24`) or AES-256 (`32`); omit both for plaintext.
`tsbpdmode` accepts `1` or `0` for timestamp-based packet delivery; `tsbpd`
is an alias. `streamid` (or `srt_streamid`) is an optional caller stream ID of
up to 512 UTF-8 bytes. Listener stream IDs have no effect and are rejected.
Percent-encode reserved URL characters in query values. For existing
Flussonic-style input URLs, `#passphrase=` is also accepted, with every later
`#` treated as part of the secret.

`latency`, `timeout` and `listen_timeout` are in **microseconds**;
`connect_timeout` is in **milliseconds**. The default latency is 120 ms, the
connection timeout is 3 seconds, and media inactivity timeout is 10 seconds.
The default listener accept timeout is 15 seconds. The stream's separate
**input timeout** also bounds how long its worker waits for usable media.

The native receive buffer scales with configured latency, starting at 8 MiB
plus about 50 Mb/s worth of media per latency second and capped at 64 MiB by
default. For a higher bitrate or a longer latency, set `rcvbuf` in **bytes**
(64 KiB–128 MiB), large enough for at least latency × expected bitrate plus
retransmission headroom. This is a per-input memory allowance. Unsupported
options and malformed URLs fail when saved. Rendezvous mode and non-MPEG-TS
payloads are not supported by this profile.

Local interoperability tests cover encrypted AES-128/192/256 inputs in caller
and listener modes, TSBPD on/off, an SRT stream ID, a passphrase containing `#`,
and decoder command-line confinement. Sustained cross-host bitrate/loss and
all upstream SRT dialects remain release qualification work.
