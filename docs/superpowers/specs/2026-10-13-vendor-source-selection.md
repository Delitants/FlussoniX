# Explicit vendor source selection

Saved source rows need an explicit vendor boundary before daemon playback can safely select the opt-in modern source pipeline. A URL alone continues to identify a native FlussoniX source, preserving existing configurations.

This increment adds `flussonix_source_kind` (`native` or `flussonic`, absent means native) to source configuration. Flussonic rows require a URL identity, an explicit Cluster key and the existing strict modern profile. Local `on_play`, `flussonix_token_sha256` and policy-only `group_config` may be persisted only on these vendor rows. No remote policy is imported and missing viewer policy remains unresolved. Native-only settings and mismatched URL/transport fail atomically. At most 64 vendor rows may be configured, including on-demand sources.

All configuration entry points use the same projection as coordinator preparation, without network requests. Native discovery, static activation and peer-authenticated private pulls must never be selected for a vendor row; low-level native directory calls also reject vendor/unknown types before I/O. Local configured streams keep existing precedence. Unmarked/native source behavior remains unchanged.

This is a configuration and isolation prerequisite. Automatic daemon vendor playback, static vendor demand, vendor failover/LB and UI editing of vendor policies are separate increments. No official Flussonic component is used and no production endpoint is contacted. The current preview remains on its qualified build.
