# First working copy implementation

Approved by the user on 2026-10-01 HST: develop on this Linux host, choose unused test ports, publish the first working copy to Delitants/FlussoniX.

Spec: architecture.md, compatibility.md, cluster-loadbalancing.md, rtsp-rtp-support.md and the reviewed UI design. The full first-release protocol matrix remains binding. This milestone is the first executable vertical slice and does not claim full parity.

1. Configuration/API: Rust daemon, persisted streams/templates/sources/peers/auth backends; partial updates, reset, template inheritance, validation without mutation, Basic/Bearer view/edit permissions. Tests cover rollback, restart persistence, multi-segment names and cursor traversal.
2. Streaming: one supervised FFmpeg worker per active stream; bounded shared MPEG-TS fan-out and bounded TS/fMP4 HLS windows. Test owned synthetic media, CPU transcode, first-viewer coalescing, failed input, playback auth and resource cleanup. Transport schemes supported by installed FFmpeg are capability-checked; unsupported requested behavior is rejected.
3. Cluster: native source discovery and LAN HLS pull, configured/public/private endpoint separation, authenticated peer telemetry, adaptive selection using uplink/CPU/RAM and readiness, stale/drain exclusion and resource reservations. Test real source/CDN/LB processes on independently allocated ports, including concurrent first viewers.
4. Admin UI: implement the reviewed Streams/Templates/Config/Cluster layout against real APIs, expose accurate runtime state and capability status. No fictional operational metrics.
5. Qualification: run Rust checks and real decoded-media integration tests; produce a per-feature capability report, start a test instance on an unused port, perform a fresh final code review and fix important findings.
6. Publish: exclude local state, credentials, toolchains, vendor binaries/source extracts and generated media; create the authorized GitHub repository and push the reviewable first working copy. Publication is already authorized by the user.

M4F/M4S wire adapters remain the highest-priority compatibility gate; the independently implemented AVC/AAC subset and current qualification are recorded in qualification.md. Generic fMP4 output is never labelled M4F/M4S interoperability. No production/demo configuration will be changed.

Execution: inline. New repository starts on develop in the requested workspace; there is no existing branch/worktree to protect. Rust and FFmpeg are independent build/runtime dependencies, not vendor package dependencies.

Review focus: unauthorized media/cache access; config persistence failure; stale metrics/admission races; unsupervised child processes; path traversal and credential disclosure; unsupported protocol claims.
