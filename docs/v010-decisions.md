# v0.10 decisions

- Standing development, test-host and GitHub publication authorization permits inline work in an isolated branch and one fresh final review without repeat approval menus. Cost if wrong: a preview needs additional qualification before migration.
- Native HTTPS output/publication is the next independently testable profile. Retain the H.264/AAC codec subset and separate private plaintext lab paths; HEVC/m2a/MP3, full private-CA HTTP input/cluster trust, secure push, SRTP and encrypted SRT remain mandatory subsequent profiles. Cost if wrong: those migration cases are not qualified by this release.
- Reuse the existing rustls certificate loader and Axum socket metadata, with128 pending handshake futures and five-second expiry, rather than a separate proxy or unbounded handshake tasks. HTTP/1.1 is the declared profile. Cost if wrong: other protocol/capacity profiles require further work.
- HTTPS requests require HTTPS public CDN endpoints and callback redirects. HTTP management/private LAN addresses remain separate. Cost if wrong: plaintext-only public CDNs return503 until configured with secure delivery.

Qualification corrections: use the implemented native `fmp4/index.m3u8` path instead of the unimplemented vendor alias; assert the real `session_id` UUID and renewal counters rather than absent `id` fields. No product timeout, retry or publisher behavior changed for those corrections.

Final review found no runtime Critical/Important issues. The stale README future-HTTPS item was regraded to an Important advertised-capability contradiction and corrected as documentation only; no mirrored prose test or product change. Cost if wrong: deployment guidance needs correction. No deferred minors remain.

The reviewer independently reran eight HTTPS tests and probed180 idle clients:128 pending sockets, verified HTTP200 after4.741seconds under saturation, exit0 in0.017seconds and complete socket cleanup. Executor owns the exact static artifact, lab, production preservation, full204/15 suites, CI and release-hash gates the reviewer declined. Cost if wrong: preview qualification is incomplete. Broader codec/trust/push/scale directions remain declared future gates; cost if wrong: those migration cases are unqualified.

Python3.13 strict artifact verification caught an owned lab CA lacking keyUsage. Regenerate its CA/leaf with keyCertSign/cRLSign; retain strict chain/identity checks. No product verification relaxation.
