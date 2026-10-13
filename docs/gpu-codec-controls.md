# NVIDIA codec controls and readiness

Extend the independent transcoder profile with `hevc_nvenc`, alongside existing `h264_nvenc`. Retain audio defaults, template inheritance and CPU/copy behavior. NVIDIA video encoding uses 8-bit yuv420p, GOP 50, no B frames and the requested video bitrate. No new GPU decoder or device selection profile is implied. GPU caption conversion remains explicitly unqualified and rejected for both NVIDIA encoders.

Each daemon lazily checks H.264 and HEVC readiness once using its configured independent FFmpeg executable, default GPU selection, a single 320x180 synthetic frame and null output. Each check is bounded to five seconds and launched without input, output or diagnostic retention. Concurrent capability requests share one result; unavailable hardware is cached until daemon restart. Child processes are killed and reaped on timeout and killed on cancellation. No official Flussonic process, binary or library is used. Readiness demonstrates encoder initialization only; it is not real-time, delivered-media or named GPU/driver qualification.

The authenticated native capabilities API adds `transcoding.gpu_profiles`, with encoder, codec and status (`available`, `unavailable`, `probe_failed` or `timed_out`). Private paths, driver diagnostics and environment data are never included. A missing executable yields a sanitized probe failure. The admin shows human-readable readiness, adds NVIDIA HEVC to Streams/Templates, and retains independent audio choices.

A GPU worker must pass the cached matching readiness check before replacing a running worker or opening a source. The check runs before the worker mutex and configuration-route recheck, so a slow GPU does not hold the shared CPU worker lock and stale requests cannot replace current streams. Configuration remains portable and structurally valid on a host without GPU; saving a profile does not claim that host can run it. Starting unavailable profiles returns a clear encoder-specific error, with no software fallback.

Qualification here covers configuration/inheritance, the real local FFmpeg readiness result, sanitized failures, timeout/cancellation reaping, concurrent request coalescing, nonblocking CPU startup, and browser persistence/display. An owned NVIDIA H.264 media profile is now qualified below. Device inventory/admission, multiple devices, driver reset/recovery, NVIDIA HEVC delivery and production throughput remain pending.

Admin capability loading is separate from node/configuration loading and is cancelled on sign-out. Slow readiness therefore does not block listing or editing streams, and repeated management polling does not start new probe requests.

Reference: [NVIDIA FFmpeg hardware encoding documentation](https://docs.nvidia.com/video-technologies/video-codec-sdk/13.1/ffmpeg-with-nvidia-gpu/index.html). Actual option support and initialization are checked with the installed independent FFmpeg, not inferred from documentation.


## Owned NVIDIA H.264 media qualification

A GeForce GTX 750 Ti with NVIDIA 580.159.03, Linux 6.17.0-41 and independent
FFmpeg 7.1.1 passed actual FlussoniX worker encoding with AAC, MPEG Layer II and
MP3 audio. Generated M4S records and M4F segments were remuxed into MPEG-TS and
independently decoded; TS-HLS and fMP4-HLS also decoded successfully. Consumers
shared one encoder worker. GPU-to-CPU-to-GPU replacement reaped old workers,
and an unavailable HEVC request preserved the current working stream.

The app reports H.264 NVENC available and HEVC NVENC unsupported on this card.
The GTX 750 Ti has no HEVC encoder. A GTX 950 supports 8-bit 4:2:0 HEVC, but not
10-bit HEVC; that card still needs its own delivered-media qualification.
See [NVIDIA's codec support matrix](https://developer.nvidia.com/video-encode-decode-support-matrix).
These bounded owned tests do not establish production capacity or performance.
