# v0.9 implementation decisions

- Standing development, testing and GitHub publication authorization permit inline execution in the isolated `codex/publish-v09` worktree. One fresh whole-branch reviewer is required by the execution skill; no implementer agents. Cost if wrong: this remains a preview unqualified for migration.

- HTTP MPEG-TS publication is the next receiving direction. It uses configured `publish://` streams and the existing independent pipeline without adding dependencies. RTSP publication and direct RTP/SRTP remain later directions. Cost if wrong: those protocols require later releases.

- Actual native decoding exposed incompatible MPEG-TS codec tags in tee FLV output, AAC framing in fMP4 and diagnostic text contaminating the old binary stderr adapter. Publication uses a separate passthrough FLV copy mux, an fMP4 AAC bitstream filter and an owned one-shot loopback binary connection. CPU encoding remains single-pass. Cost if wrong: unusual codecs require further qualification.

- A deliberately slow synthetic native-pull test needs a 20-second qualification wait because input arrives below realtime; the product startup watchdog remains 15 seconds. Input probing is bounded to 1 MiB/one second. Cost if wrong: late-advertised tracks require a configurable probe profile.

Browser fixtures were refreshed before the complete run; all 14 cases pass without a timeout workaround. The publication selector, masked password/callback fields, template inheritance and summary masking received RED/GREEN coverage. Fresh whole-branch review reproduced peer discovery leaking explicit/inherited publisher passwords and identified a misleading template publication URL. The credential issue remained Important. The UI issue was regraded from Minor to Important because it broke a common template receiving workflow. Both received failing regressions before one fix pass: discovery now allowlists playback policy, identity and display metadata; templates give setup guidance and inheriting stream forms expose their real endpoint. Neither finding is deferred.

The reviewer declined final release evidence because Task 3 belongs to the executor; exact standalone/CDN/CI/checksum verification remains a release gate. Cost if wrong: the preview is unqualified. The documented eight-second internal binary connection acceptance bound remains separate from the configurable body/media watchdog. Cost if wrong: exceptionally slow senders need another profile. Additional codecs, GPU hardware, other publication directions and complete vendor authorization/API parity remain future gates. Cost if wrong: further implementation or qualification is required.


- Lab ruling: publication upload and source API requests use separate owned SSH forwarding connections for qualification. Sharing one WAN connection caused bounded lookups to time out under upload; quiet roundtrip measured 559 ms against the unchanged 750 ms deadline. Unique fixture names avoid previous test-policy caches. Cost if wrong: deployments with WAN lookup paths require separate performance qualification. Initial wrong-content-type harness requests were corrected; no product timeout workaround was added.


- CI ruling: fix the pre-existing reserve/drop/rebind collision in the shared UDP test helper, using disjoint monotonic ranges below the default ephemeral range, with a RED/GREEN nonreuse regression. Product code/listeners remain unchanged. Cost if wrong: the fixture allocation strategy needs further work; no new product capability is claimed.
