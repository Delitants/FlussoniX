# Incoming publisher Digest qop-auth plan

Spec: ../specs/2026-10-07-rtsp-digest-qop.md. Execution: inline, authorized ongoing
development. Base:72d9ff0. Isolated branch:codex/rtsp-digest-qop.

## Task1: Wire admission and bounded validation

Add wire tests deriving responses independently of production helpers. Observe
RED: valid qop-auth receives401. Implement MD5 auth response and complete tuple
validation in src/rtsp/publication/auth.rs, retaining legacy behavior. Test
parameter substitutions, malformed tuple/nc/cnonce, connection/method/URI binding,
single successful admission and no worker before RECORD. Expected GREEN: new
wire tests plus existing authentication cases pass. Interface: same Auth verify
result and existing challenge loop; parser admits at most nine parameters.

## Task2: Independent media and release

Observe the authenticated ANNOUNCE at an owned transparent test relay, asserting
that FFmpeg sends qop-auth with nc/cnonce. Reuse strict H.264/AAC decode for TCP,
UDP and verified TLS. Update receiving profile, compatibility and qualification
documentation; remove the obsolete inbound-auth pending sentence in that same
profile. Run fmt, warnings-denied Clippy, full publication regression and normal
build. Run entire Rust and browser suites via exact-head vendor-absent CI. One
fresh read-only whole-range reviewer precedes authorized GitHub publication.
Activate qualified preview preserving config, credentials and full environment;
reap owned fixtures and archive qualification records before removing worktree.

## Review focus

- Partial tuple or malformed/zero nonce count must not become legacy acceptance.
- Digest uses exact method/URI/nc/cnonce and connection-bound nonce.
- Compatibility fallback must not be described as enforced qop or stronger MD5.
- Observational media fixture must prove incoming qop without leaking credentials.
- Existing admission callbacks, retry/deadline limits and exclusive ownership
  remain binding; no worker may start merely for a challenge.
