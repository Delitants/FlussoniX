# Incoming publisher Digest qop-auth

Extend the existing configured RTSP/RTSPS ANNOUNCE authentication profile with
MD5 `qop=auth`. Advertise `qop="auth"`; continue accepting the legacy omitted-qop
response for older publishers. Basic, query passwords, stream/template password
inheritance, callbacks, admission ownership, and media processing retain their
existing contracts. No vendor component or new account/UI field is required.

Require qop, cnonce, and nc together. Only `auth` is supported. The client nonce
is nonempty printable ASCII, at most256 bytes; nc is exactly eight hexadecimal
digits and nonzero. Compute the response using the received nc and cnonce without
normalization. Unknown/duplicate parameters and ambiguous malformed strings are
rejected. Algorithm remains MD5. Header/framing limits, three-challenge cap and
absolute30-second admission deadline remain unchanged.

The server nonce remains random and bound to the original URI and connection.
Successful authentication ends initial admission: that control connection cannot
admit a second ANNOUNCE, and another connection cannot reuse its nonce. No
unbounded nonce-count table is needed; failed attempts never become an authorized
session. This is initial publication authentication, not a per-method viewer or
publisher Digest service. Retaining legacy responses means this is a compatibility
extension, not enforcement of qop or a stronger hash.

Qualification: real wire admission/rejection tests; retained legacy/Basic/query,
callback, revocation, deadline and ownership regressions; independently generated
FFmpeg H.264/AAC publication and strict decoded shared TS over TCP, unicast UDP
and a verifying owned TLS relay. The fixture must observe qop-auth on ANNOUNCE,
not infer it solely from successful playback. Full vendor-free CI and fresh
whole-branch review precede publication and preview activation.

References: [RFC7616 sections3.4 and5.4](https://www.rfc-editor.org/rfc/rfc7616.html)
and [FFmpeg independent Digest implementation](https://github.com/FFmpeg/FFmpeg/blob/n7.1/libavformat/httpauth.c).
SHA algorithms, session algorithms, auth-int, per-user accounts, viewer Digest,
vendor migration and sustained capacity remain unqualified.
