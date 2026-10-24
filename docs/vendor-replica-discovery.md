# Replica discovery and catalog refresh race

The original diagnosis below is retained as historical evidence. The [catalog-consumption correction](#catalog-consumption-correction) records the later implementation and validation.

A foreground replica lookup can return HTTP 503 even when its last declared replica has a healthy current catalog. The lookup retains an earlier inventory while waiting for unavailable predecessors. A background refresh supersedes that inventory before the lookup applies it. The existing freshness check correctly rejects the retained inventory; selection does not consume the replacement result and returns unavailable.

## Observed failure

The original full-project validation remains failed: `vendor_daemon_failover::eighth_replica_is_reachable_with_slow_unavailable_predecessors_in_lookup_budget` returned HTTP 503 after 10.002934 seconds, with no private media requests. A later isolated pass did not clear that failure.

One further run of the retained original executable passed. Three pre-registered traced attempts of the unchanged eight-replica fixture then produced PASS, FAIL, PASS. The failure returned the same HTTP 503 after 10.004168 seconds and identified the exact rejected fact as `Coordination(Lease(Superseded))` for the healthy eighth replica. It was reproduced in isolation, without a preceding suite or a private media connection.

The failed trace showed:

1. The healthy source's background catalog job started near the foreground lookup, waited for a query permit, and completed its catalog about five seconds later.
2. Foreground selection retained that completed catalog while other replica queries remained outstanding.
3. At about 10.003673 seconds, another background refresh acquired the healthy source slot and began a new discovery turn.
4. At about 10.004168 seconds, selection applied its retained inventory. The freshness check returned `Lease(Superseded)`, and the request returned unavailable before private media admission.

The poll interval is measured from job start, including semaphore wait, rather than catalog completion. The group waits for all alternative query results before observing them in declared order. Those two existing behaviors allow the next catalog refresh to overlap consumption of a result obtained during the previous refresh. Loopback port ordering and scheduling change which side wins the overlap.

The earlier full-suite failure was not instrumented, so its exact internal interleaving cannot be reconstructed. The same fixture, status, approximate duration and pre-media boundary were reproduced with the supersession cause visible. This establishes a concrete failure mechanism; it does not prove every discovery failure has this cause.

## Deterministic controls

A temporary three-origin probe uses real configuration, coordinator, session ownership, authenticated loopback management endpoints, source cache and replica selection. It starts no private media. An explicit request barrier holds the unavailable middle replica until the healthy final replica has completed discovery. The probe then either leaves that catalog alone, completes a healthy refresh through the same `replica_inventory` path used by background polling, or makes the refresh unavailable, before releasing the predecessor.

| Case | Required behavior | Traced outcome |
| --- | --- | --- |
| No refresh while predecessor is held | Select the healthy final replica | PASS |
| Healthy catalog refresh before predecessor release | Select using the current healthy catalog | FAIL: `Lease(Superseded)` becomes unavailable |
| Unavailable refresh before predecessor release | Reject the old positive catalog | PASS |

Before either refresh, the retained inventory is current. After either refresh starts, it is explicitly verified as superseded. The healthy replacement completes successfully, but selection still returns unavailable. These barriers reproduce the defect in about half a second rather than relying on the ten-second poll/timeout overlap.

The first probe attempt failed during fixture setup because its synthetic peer key was below the existing minimum length. That attempt is preserved and excluded from behavioral evidence. Only the fixture credential length was corrected; the cases, barriers, expected selection and existing cooldown were retained.

The same corrected three-case probe was rebuilt and repeated after all production trace statements were removed: two controls passed and the healthy-refresh regression failed again. The baseline production code therefore reproduces the defect without diagnostic tracing. The probe-only module inclusion was then removed and every baseline tracked file was verified byte-for-byte unchanged. The failing probe is archived for the correction task; it is not registered in the shipped test suite by this diagnosis-only report.

## Correction scope

Consume each source's current completed discovery result at the point it is observed in declared replica order, under the existing per-source serialization. If a refresh is in flight, any wait must remain inside the current selection deadline. Do not grant authority from superseded positive or negative facts, bypass the selected-source denial barrier, change declared replica preference, import old source generations, or issue unbounded retries.

A bounded correction should retain the failing healthy-refresh case as a regression and cover an in-flight healthy refresh, an unavailable or authoritatively rejected replacement, configuration replacement and cancellation. Keep the existing five-second discovery, sixteen-second group selection, four-query limit, lease lifetime, cooldown and authorization checks. Increasing deadlines or changing the test's success condition would not correct this result-consumption race.

## Status

Diagnosis only. Temporary tracing and probes are archived separately; all baseline tracked source bytes have been restored and verified. No transport, buffering, FEC, deadline, lease, authentication or preview behavior is changed. No full-project rerun or new media qualification is claimed. Publication remains held while the real discovery regression is unfixed. The original FXLink burst-loss failure also remains unresolved and requires separately registered follow-up work.

## Catalog-consumption correction

Replica selection now reads each source's latest completed discovery result while holding the same per-source lock used by catalog refresh. A new refresh cannot supersede the result during synchronous observation. An in-flight refresh remains bounded by the existing caller deadline. Both successful catalogs and completed discovery errors are retained, and starting another query invalidates the prior cached result so cancellation cannot restore it. Missing/incomplete results fail closed.

Selected-source checks, ordered alternative observations and the original-source retry use this serialized consumption path. Observation performs no network probe or retry. Direct/static positive-cache readers retain their existing authority checks. The five-second discovery allowance, sixteen-second group selection budget, four-query limit, 500 ms cooldown, thirty-second lease limit and authentication/authorization behavior are unchanged.

Nine real authenticated loopback regression scenarios use request barriers and the complete Runtime owner. The completed and in-flight healthy-refresh cases failed against the original code while seven controls passed. The same harness passes all nine after the correction. Controls reject failed/disabled catalogs and replaced configuration, preserve declared replica order and selected-source denial, and cancel selection while a refresh retains its source lock. The unchanged original eight-replica integration regression also passes in a separate targeted run.

The complete project command passed on the frozen corrected source: 2,107 tests passed, zero failed and 29 retained their existing ignore settings, across all 154 integration targets and 157 test groups. All 460 library tests, including the nine new regressions, passed in that final run. Formatting and strict release all-target Clippy also passed. The initial targeted GREEN included an unused-result warning, preserved in its log; the discard was then made explicit and the final strict Clippy and project builds are clean. This is a new corrected-source result, not a rescore of either historical failed run.

This corrects the demonstrated discovery race only. Original diagnostic failure evidence and its historical attribution limit remain intact. The separate FXLink burst-loss failure is unresolved; no new burst qualification, transport retuning, merge, release, default-on change or accepted-preview/CDN deployment is claimed.

## Independent review limits

The fresh read-only review found no Critical or Important production defect. Three nonblocking test extensions are deferred: force a transient failed refresh before a later healthy alternative; force healthy and authoritatively rejected refreshes during original-source retry; and assert zero alternative management requests, including `/config`, at the selected-denial barrier.

The source review traced the relevant generation, lease, policy, ordering and cancellation fences. It did not establish exhaustive runtime proof for every selected/retry/sibling refresh interleaving, exact deadline/expiry boundaries, sustained-contention deadlock freedom, or complete direct/static/media behavior. Existing project regressions are exercised separately by the full suite. No merge, release, deployment or broader transport-performance readiness is implied.
