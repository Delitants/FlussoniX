# API operation inventory status

`api-operations.csv` began as a snapshot of the public and private v3 schemas
installed under `/opt/flussonic/lib/web-1/priv/`. Its original
`not_implemented` and `schema_observed_only` values described an early design
state, not the current FlussoniX source. The schema files are reference input;
they are not shipped or loaded by FlussoniX.

On 2026-11-01, source inspection found handlers for 44 exact method/path pairs.
The management dispatcher in `src/server.rs` handles Config, Sessions,
Streams, Templates, Cluster peers/sources and Auth backends; its IPTV
subdispatcher in `src/server/iptv_management.rs` handles the root, package and
subscriber routes. The main router and media fallback expose the listed IPTV
playlist, HLS index and HTTP MPEG-TS paths. Those rows say
`handler_present_contract_unqualified` with `source_inspected_only`: the named
method reaches a handler, but this status does **not** assert matching schema,
response shape, error codes, side effects, or complete Flussonic behavior.

Eighteen first-release rows say `no_exact_handler_seen` with
`source_inspected_only`. They include the installed paths for HTTP proxies,
monitoring probes, balancers, admin view-token issuance, input selection and
several HLS playlist aliases. The current router/dispatcher has no matching
branch or file alias for those exact paths. This is a source finding, not a
runtime status-code or full contract check. A native `/health` route and native
`fmp4/index.m3u8` path do not satisfy the differently named reference paths.

The two first-release authorization callback rows say
`caller_present_contract_unqualified`: `src/playback_auth.rs` sends a configured
HTTP GET with viewer fields, and `src/server/publication.rs` sends a configured
HTTP POST for publisher authorization. Their `/play` and `/publish` labels are
reference callback paths, not paths served by FlussoniX. Source inspection
does not prove complete callback field or decision parity.

The other 168 operations say `not_assessed`. This is deliberately different
from `not_implemented`: an unreviewed route may exist.
Existing endpoint tests, the schema and this inventory are separate evidence;
none alone establishes complete API compatibility. Update a row to a stronger
status only after recording direct behavior evidence for its exact method/path.
