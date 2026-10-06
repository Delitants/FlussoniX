# Encrypted elementary RTP input and output

Intent: continue the standalone streaming software toward secure inbound/outbound H264/HEVC with AAC/MP2/MP3, preserving the current friendly Streams/Templates configuration and shared processing worker. User requests the next two tasks without stopping and has authorized local tests and GitHub publication.

## Scope and design

Task1 adds native elementary SRTP/SRTCP reception. Task2 adds native elementary SRTP/SRTCP destinations and SDP/UI qualification. Extend the existing independent libsrtp2 AES_CM_128_HMAC_SHA1_80 integration; do not delegate public crypto sockets to FFmpeg or implement cryptography. Static per-stream/destination key-file references remain the only application key input. Alternatives: inline SDES would expose keys in admin/downloads, and DTLS negotiation is a separate subsystem; choose out-of-band static keys.

## Global constraints

- Independent system FFmpeg and libsrtp2 only; no official Flussonic runtime components, installation changes, or production/CDN service modifications.
- Work in an owned isolated worktree. Preserve existing root preview/configuration/credentials until exact-head qualification, then update only owned preview49285 under existing publication authorization.
- AES_CM_128_HMAC_SHA1_80; owner-only absolute regular key files containing30 base64-decoded bytes. Load once per transport generation into bounded per-track contexts; erase temporary buffers. At most8 tracks/4 destinations.
- Secure static SDP requires RTP/SAVP; plaintext requires RTP/AVP. Reject profile mismatch, inline crypto keys, negotiation/remote resource attributes and unsupported cryptography. Downloaded SDP contains no key or key path. This is out-of-band keying, not SDES negotiation.
- Authenticate/decrypt before structural admission, peer/SSRC pinning or decoder relay. Reject plaintext, wrong keys, tamper/replay. Source filters still apply before crypto. Authenticated malformed candidates cannot seize peer ownership or lose rollover/replay history.
- Public SR/RR uses SRTCP; the decoder receives only regenerated plaintext SDP and authenticated plaintext media/control on trusted owned loopback pairs. Protect validated private feedback before public transmission. Load key and initialize all crypto before opening public sockets.
- Each secure destination/track generation assigns a fresh unique random SSRC and its own sequence starting at0; preserve payloads and marker/timestamp spacing. Do not reuse shared packetizer SSRC/sequence as a restarted encryption epoch. Existing sessions cross sequence rollover; uncoordinated receiver late joins after rollover remain unsupported.
- Preserve common destination media/wall clock, related CNAME, actual generation-fenced SDP, bounded pacing/cancellation/loss/queue semantics and existing MP2T/plaintext behavior. Account ciphertext/trailer/IP/UDP egress bytes.
- UI offers elementary on secure URLs, labeled SDP/key fields and preserved template overrides. Changing secure to plaintext removes key reference while retaining elementary profile; switching to MP2T removes input SDP. No raw keys or editable JSON fields.

## Qualification

Task1: config/SDP mismatch/no downgrade; two same-PT tracks have independent authenticated/replay states; malformed-before-pin, wrong-key/tamper/replay/plaintext, maximum datagram/trailer bound; encrypted SR and private RR feedback; cancellation/socket release; real independent FFmpeg SRTP senders with H264/HEVC and AAC/MP2/MP3 into shared native/TS playback, audio-only and CPU processing.
Task2: all media packets and SR encrypted, independent decipher/strict decode, common clock/CNAME, separate fresh SSRCs for lanes/destinations/restart, unchanged shared worker, auth-first malformed RTCP feedback; secure SDP no secret, actual management auth/generation guard; UI key references/profile preservation/inheritance/clear/download/mobile; independent H264/HEVC x AAC/MP2/MP3, multiple/audio-only, CPU conversion and available internal H264 VAAPI output. Tests may add keys only to private owner-only independent receiver SDP; never management SDP or logs. Whole exact-head CI validates regressions/vendor absence.

## Limits

No DTLS/SDES negotiation, rollover-state migration, hostile-localhost isolation, WAN/throughput/capacity parity or GPU HEVC delivery claim. Separate subtitle tracks use MPEG-TS. Scoped independent driver files qualify available H264 VAAPI only.
