# Incoming RTSP publisher authentication

This bounded increment closes the configured-publication credential-header gap.
RTSP/RTSPS ANNOUNCE accepts Basic or legacy MD5 Digest using the existing effective
stream/template `password`. A nonempty username is a publisher-supplied label,
not an account or management credential. Existing query-password admission remains
compatible. Supplying both a password query and Authorization is rejected.

An absent header/password on a protected stream receives a connection-bound
Digest challenge. The legacy profile advertises MD5 without qop; Basic is accepted
preemptively. The nonce is random, bound to the exact original ANNOUNCE URI and
connection, usable once for admission, and constrained by one absolute 30-second
negotiation deadline and at most three challenges. Wrong headers fail with 401;
wrong query passwords retain 403. Unsupported/malformed credentials fail closed.
Each retry rereads effective configuration; authenticated admission still runs
on_publish, checks policy currency, reserves existing publisher capacity, and
waits until RECORD before starting one shared worker. SETUP/RECORD remain bound
to that connection's admitted stream and Session. Header credentials never enter
callback metadata, logs, saved config or generated publication URLs.

Independent FFmpeg Digest publication must strictly decode H.264/AAC over TCP,
UDP and verified TLS. Owned Basic wire clients must negotiate actual native
sessions. Tests cover wrong passwords, management credentials, nonce replay on
another connection, method/URI/realm substitutions, unsupported algorithms/qop,
ambiguous query/header credentials, inherited password and callback denial.
Friendly form help explains header credentials without adding JSON controls.

No official Flussonic components or production endpoints participate. This does
not qualify receiving viewer Basic/Digest, SHA-256/session algorithms, auth-int,
per-user publisher accounts, arbitrary vendor dialects or migration capacity.

Sources: independently implemented RTSP authentication handling guided by
[RTSP 1.0](https://www.rfc-editor.org/rfc/rfc2326.html) and the legacy compatibility
discussion in [Digest authentication](https://www.rfc-editor.org/rfc/rfc7616.html).
