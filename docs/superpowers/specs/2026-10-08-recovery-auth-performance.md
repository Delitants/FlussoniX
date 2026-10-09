# Candidate-scoped recovery authorization

Recovery currently enumerates all authorization entries once to discover demand, then rebuilds that whole map at each startup guard. Remove those repeated scans without relaxing authorization or changing public APIs, configuration, UI, or playback behavior.

Organize the existing primary session cache into per-stream buckets keyed by the existing session identity. Global iteration, admission capacity and eviction retain their existing meaning; empty buckets must be removed. This is the primary cache layout, not a second demand index or ledger. Guards access only the candidate stream's current bucket and revalidate current published policy and mutable authorization state. The per-pass RecoveryDemand contains the original activity timestamp and stream name, never frozen permission or retained session references.

Keep authority -> entries -> state lock order, exact worker retirement, local stream precedence, the 30-second activity bound, actual allow deadline, callback renewal deadline, revocation, cancellation, denial, policy changes and operator Stop. New legitimate playback sessions must be visible to guards immediately, including after recovery worker publication. Revoking an earlier viewer cannot cause recovery cleanup to terminate a shared worker acquired by a newly authorized viewer. Controls and retired/replaced cache entries cannot create demand without real authorized playback.

The 20,000-entry cache bound remains global across buckets. Guard cost is proportional to the current candidate stream's sessions, with no whole-map construction, unrelated session state locks or unrelated token hashing. The once-per-pass whole-cache enumeration remains. This is an algorithmic improvement, not a production throughput claim.

Qualification must reproduce unrelated-session contention before optimization and the shared-worker teardown race before its review fix. Verify authorization boundaries after capture, eviction and current multi-session selection; run source/queued-stop and real-daemon blackout regressions, then full exact-head CI. Preserve preview environment, settings and web assets when publishing the verified artifact. Clean up all owned media fixtures.

Review correction: the initial reference-only candidate design could ignore new foreground demand when retiring a worker. Current-stream cache buckets replace that design and preserve the base implementation's shared-worker behavior.
