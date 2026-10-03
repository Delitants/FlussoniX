# v0.7 implementation decisions

The following decisions were recorded during development and final review.

- Ruling: Standing approval to develop/test/publish and latest Continue authorize the next UDP direction-matrix milestone; developer persistence overrides redundant skill approval menus. Owned isolated worktree protects current main/services. No new dependencies or UI design changes planned.

- Ruling: Use inline implementation with one fresh whole-branch reviewer required by executing-plans; no task implementer subagents. Protocol milestone is RTSP controlled unicast UDP playback, not direct-RTP/SRTP/publishing or complete migration.

- Final: Ruling: Keep bound pool sockets unconnected and target exact peer endpoints with send_to/recv_from filtering; ephemeral same-family route probes determine wildcard advertised source IP. Avoids Linux source route pinning without releasing reserved ports or new deps/unsafe. Cost if wrong: multi-interface UDP fails qualification.

- Final: Ruling: Bound each queue-drain attempt to 65 raw reads and reject reuse/retarget unless WouldBlock proves empty; retries can finish draining. Preserve previous endpoint after failed retarget. Cost if wrong: temporary 503 SETUP under queued input flood; no stale report may renew replacement.

- Final: Ruling: Reject schedules more than 30 seconds ahead and check subscriber eviction on the existing control tick even while pending. This closes discontinuous/lagged UDP rather than retaining silent viewers. Existing valid-long-wait test reduced from 100 to 10 seconds to stay inside the declared horizon. Cost if wrong: legitimate sources with >30-second forward timestamp gaps must reconnect.

- Final: Ruling: Restore accidental third-party Node engine metadata from the broad version substitution. This simply removes an unrelated change; no dependency/version or feature changes. Cost if wrong: none to supported Node22 installs.

- Final: Ruling: Remote deployment, exact qualification, CI/publication/checksums remain required and will be completed after fixes; remaining protocol directions and production scale remain explicitly later milestones. Cost if wrong: preview must not be used for migration or claimed full parity.

No deferred minor review findings remain.
