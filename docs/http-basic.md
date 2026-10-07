# HTTP Basic input and publisher authentication

HTTP Basic is supported for configured upstream inputs and incoming HTTP(S)
MPEG-TS publications. Viewer playback authorization remains its separate
token/callback policy. Admin credentials and cluster peer keys do not authorize
protected publications or become upstream credentials.

## Upstream inputs

Enter credentials in the existing Input URL field, for example
`hlss://username:password@origin.example/live/index.m3u8`. Percent-encode reserved
characters in the username/password. The profile supports HLS/HLSS, TSHTTP/TSHTTPS,
raw HTTP(S) HLS/continuous TS and M4S/M4SS/M4F/M4FS inputs. Keep the existing
TLS CA field for a private CA; secure inputs verify trust and identity before
sending application data.

The native fetcher percent-decodes a UTF-8 pair, removes userinfo from resource
URLs and sends a sensitive Basic header on requests to that configured origin.
Credentials are preemptive: no challenge is needed before sending them.
Credentialed HLS and TS use an origin-scoped fetcher even over plaintext HTTP;
FFmpeg receives only a local URL. Native M4 control/segment requests use the
same credentials. Basic credentials cannot be combined with a native peer key.
Redirects and HLS URI references must remain on the same normalized HTTP(S)
origin, with no substituted userinfo. At most three redirects are followed.
HLS variant/media playlists, segments, keys and subtitle resources inherit the
same origin-scoped header. Summaries mask configured URL credentials; editing
the URL retains its saved userinfo.

## Receiving publications

Use the existing Publisher password in Streams/Templates. A publisher sends
`Authorization: Basic BASE64(publisher:PASSWORD)` with
`POST /STREAM/mpegts`, over HTTP or HTTPS. Any valid nonempty Basic username
is accepted syntactically; the effective stream/template password is the
authorization identity, matching the existing password-only publisher policy.
This is not a new per-user account system. The `on_publish` callback, ownership,
renewal and configuration fences still apply.

Missing credentials on a password-protected stream and malformed/wrong Basic
headers return401 with
`WWW-Authenticate: Basic realm="FlussoniX publisher", charset="UTF-8"`.
Legacy query-password publications still work; a wrong query password retains403.
Combining an Authorization header with a query password, or sending multiple
Authorization fields, returns400. These checks happen before reading the body,
calling the publisher callback or starting a worker.

Usernames are at most256 UTF-8 bytes, nonempty and contain neither colon nor
control characters; passwords are at most1024 UTF-8 bytes and contain no control
characters. Password colons are retained. Credentials use exact UTF-8 bytes;
other legacy encodings and Unicode normalization are not added. Header parsing
is bounded to8192 bytes and recognizes the Basic scheme case-insensitively.
The password is compared by the existing publisher policy.

For example, an independent client can publish an owned TS file with
`curl --user publisher:PASSWORD --data-binary @input.ts https://HOST/STREAM/mpegts`.
FFmpeg supports preemptive `-auth_type basic` with an output URL containing the
publisher credentials; this client option is documented by
[FFmpeg HTTP protocols](https://ffmpeg.org/ffmpeg-protocols.html#http).
The challenge/header format follows the bounded UTF-8 Basic profile described
above, based on [RFC7617](https://www.rfc-editor.org/rfc/rfc7617.html).

## Qualification and limits

Owned upstream fixtures independently decode H.264/AAC from all eight explicit
plain/secure input schemes, including percent-encoded punctuation and UTF-8
credentials. Separate fixtures exercise wrong passwords, failed TLS before any
HTTP request and origin-changing redirects; the configured source must first
authenticate for the redirect test to count. Existing unauthenticated, native
peer, private-CA and HLS resource-rewriting regressions remain applicable.

Publication fixtures exercise challenge/denial before body polling, ambiguous
credentials, template inheritance and independently decoded media sent by a
separate HTTP client over actual HTTP and verified HTTPS sockets. Applications,
fixture listeners and owned workers are stopped on success and assertion failure.

This increment qualifies receiving publication and input credential handling.
Outgoing HTTP POST push, viewer Basic/Digest, proxy authentication, incoming
HTTP Digest, per-user publisher identities, arbitrary vendor-client parity and
sustained production capacity remain outside this bounded profile. No official
Flussonic binary, library or asset is a build, test or runtime dependency.
