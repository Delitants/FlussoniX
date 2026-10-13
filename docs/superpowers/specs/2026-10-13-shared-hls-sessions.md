# Shared HLS session ownership

Retain independently admitted HLS playback between playlist/segment requests and
renew discovery leases while those sessions have current demand. Use one owner
per shared input registry. This connects existing packaging and explicit renewal;
it does not activate automatic vendor selection or daemon routes.

Every request supplies a real opaque HLS admission from the registry's coordinator
and authorizer. An internal native session identity groups retained handles; no
caller-supplied session string or cached admission replaces request authorization.
Package through existing guarded APIs before retaining the handle. Settings or
subtitle-option mismatch must preserve existing valid sessions. Cache hits never
bypass admission, source authority, or media generation checks.

Keep at most a configured number of distinct native sessions (1..=20000), counting
in-flight reservations. Idle retention is in (0,30s], touched only by successfully
packaged admitted requests. External playback handles retain their own lifetime;
removing a cache entry cannot revoke another viewer or an active response.

A unique caller-owned run future sweeps idle/closed sessions and schedules staged
renewal before the earliest current source deadline. Bound concurrent renewals to
8, with a retry delay; failed/cancelled refresh does not extend old authority.
Follow all authorized registry attachments, including active responses whose
HLS cache entry expired. Group by installed source generation and avoid
per-session source/configuration locks during maintenance. Cancel work for sources
without authorized registry demand. The loop performs no input retry, source failover or
viewer callback renewal; the embedding owner still runs PlaybackAuth::renew_due.

Run cancellation/drop/abort clears retained handles and cancels pending renewal
I/O. Late packaging cannot repopulate a stopped epoch. No task is spawned inside
the component. The embedding owner joins the loop and calls Engine::stop_all.
A subsequent explicit run may start a fresh epoch; overlapping run owners fail.

Expose the currently managed candidate as metadata for fresh request admission;
return only a currently authorized-demand route, re-fenced against the registry's
coordinator. It grants no media permission and performs no network discovery.

Owned HTTP and FFmpeg tests must prove session grouping, one pull/packager across
sequential HLS requests, idle expiry, independent viewer revocation, continuous
scheduled renewal past an old lease deadline, failed/late refresh fencing,
capacity reservation, cancellation/abort, and no late epoch installation.
