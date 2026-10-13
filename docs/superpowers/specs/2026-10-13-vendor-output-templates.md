# Vendor processing templates

Vendor sources may select a local Media template through
`flussonix_output_template`. Only `transcoder`, `flussonix_input_timeout`,
`flussonix_subtitle_tracks`, `flussonix_hls_subtitles` and
`flussonix_hls_captions` are projected. This explicitly selects processing, not
template input, viewer policy, activation, publication or push settings. No
reference retains the existing empty processing profile.

The reference is source-only and requires explicit Flussonic source kind. It
must name an existing template and resolve to a complete valid processing
profile. Config validation, replacement, source/template edits, template deletion
and reopen use the same gate. Failure leaves revision, disk and saved config
unchanged, with generic diagnostics. Management/private client projection removes
the validated local reference; it never receives processing or template inputs.
GPU profiles use existing syntax and startup readiness validation; this increment
does not qualify hardware or change encoder capabilities.

HTTP playback captures processing from the actual ConfigStore at the candidate's
exact revision before admission/attachment. Template and source edits invalidate
the existing source/config generation, active bodies and HLS retention. Fresh
requests cannot use old processing, weaker policy or stale segments. Private
source access still follows actual native viewer admission.

Configured attachment constructs subtitle options under the registry lock. Each
new shared-input generation owns a fresh native text conversion state; compatible
viewers reuse the current state's exact Arc. Preserve/drop and configured native
services must match. Conversion state is never cached by stream name outside the
worker lifetime. Existing explicit embedding APIs retain their current behavior.
Configured HLS packaging shares the existing session reservation and retention
path. Output signature checks remain mandatory, including CPU/GPU encoding and
subtitle processing. Unsupported native-text preservation with transcoding fails
explicitly, including late-discovered text; there is no silent subtitle loss.

The friendly source editor offers a Processing template selector and readable
processing summary. Configure CPU/GPU codecs and USA/European/native subtitle
selectors in the existing Media template editor. Switching source kind never
silently clears this reference: operators explicitly remove it before native
conversion. Group objects remain viewer-policy-only.

Owned tests cover persisted references and atomic rejection; independent decoding
of CPU output; native text preserve/drop and English/German HLS WebVTT conversion;
one private pull and packager across concurrent viewers; source/template edits,
cached-segment authorization and process cleanup. Existing caption/codec and GPU
qualification remains separate. Vendor LB, failover, static activation and RTSP/SRT
delivery are deferred. No official Flussonic components or production mutations.
