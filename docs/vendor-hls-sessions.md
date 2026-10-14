# Retained HLS sessions and scheduled source renewal

`SharedHlsSessions` connects admitted shared-input packaging to bounded HLS
retention and scheduled source renewal. Use one instance per registry/Engine
pair. This is an opt-in embedding component; it does not enable daemon vendor
selection, public routes, or new operator configuration fields.

For every playlist or segment request, obtain a fresh native admission from the
registry's coordinator and shared PlaybackAuth, then call
`sessions.package(&engine, admission, input_options, output_settings).await`.
Only actual HLS admissions are accepted. Internal native session identities
retain one playback handle per session. An ID, cached segment or retained handle
never substitutes for admission. Denial/redirect handling stays with the caller.
Output/subtitle mismatch preserves the existing valid session.

Read files through `Engine::read_shared`; continuous responses retain their own
`PackagedPlayback` or subscription. Dropping an HTTP request leaves the retained
session until idle expiry. Repeated successfully packaged requests update its
idle deadline. The default is 30 seconds, configurable in `(0,30s]`; the cache
and concurrent startup reservations share a capacity of at most 20,000 native
sessions. Routine requests touch one session, with periodic full expiry sweeps.

Start and own `sessions.clone().run(cancel)` in the embedding runtime. It spawns
no tasks. The loop follows authorized registry demand, including active responses
whose HLS cache entry expired, and groups renewal by installed source generation.
Current source checks are bounded by the registry's 256-worker limit, rather
than the session count. At most eight renewal futures run at once; unattempted
sources precede retries. No authorized demand cancels pending refresh work.
Staged refresh preserves the existing lease on failure; expiry still fails closed.

Defaults use a 30-second proposed lease, 10-second renewal margin, one-second
retry delay and 250 ms maintenance cadence. Configure enough margin for the
profile's wire budget and source count. Initial discovery also needs enough
remaining lifetime to reach maintenance. These are bounded scheduling settings,
not real-time availability guarantees. `SharedInputs::current_candidate(name)`
returns current managed metadata for subsequent fresh admission without another
ordinary inventory refresh; it grants no media permission.

`shutdown`, cancellation, dropping or aborting the owned run future clear cache
handles and cancel pending renewal I/O. They preserve independently held response
handles, which retain their own authorization/source fences. Packaging waiting
at startup observes cancellation and cannot populate a stopped epoch. Join the
run future before starting another one. Late reservations from an earlier epoch
cannot affect the replacement owner's cache or capacity.

The embedding owner still maintains `PlaybackAuth::renew_due`, selects cold
sources explicitly, handles native deny/redirect outcomes, and calls
`Engine::stop_all` on shutdown. The daemon now integrates persisted source
policies and [equivalent-source recovery](vendor-daemon-failover.md); always-on
vendor activation remains separate work.
No production capacity, new GPU or live vendor qualification is implied.
