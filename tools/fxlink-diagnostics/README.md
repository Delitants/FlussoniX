# Private FXLink packet diagnostics

This tool creates a separate instrumented source tree. Normal FlussoniX builds do not include the recorder. Use it for diagnosis; capture overhead makes its results unsuitable for performance claims.

`instrument.py OUTPUT --quinn-source PATH --shaper-source PATH` copies the current source and adds hooks. It rejects an existing output directory. Build that copy with an isolated Cargo target and retain its resolved lock file. `transport_test.rs` is installed as the private `packet_diagnostics` integration test.

Set `FLUSSONIX_PACKET_TRACE` to a new owner-only output filename when starting a private daemon. Set `FLUSSONIX_SHAPER_TRACE` for the private shaper. The recorder keeps the latest 65,536 events, counts overwritten or busy events, and exports on graceful shutdown. The Rust recorder has 6 MiB of fixed event data at that cap; the Python shaper stores dictionaries and strings, which use more memory for the same bounded event count. It never writes payloads, URLs, keys or tokens. A forced kill or failed export leaves no valid complete trace; a trace with lost events cannot support an absence claim. Existing files are never overwritten.

Run `python3 -B packet_trace.py TRACE...` to inspect the captured joins. Source queue copies are joined to packet numbers and encrypted UDP segments, including GSO offsets. Identical repair payloads retain separate source copy ordinals under the FXLink source contract: no evicting sends, fixed peer allowance after TLS setup, and datagrams that fit the 1200-byte path floor. The analyzer refuses source-copy attribution if it observes an evicting send or a payload over 1100 bytes on that connection. This is an FXLink diagnostic, not a general-purpose 0-RTT or arbitrary-MTU QUIC analyzer. UDP acceptance means the socket adapter accepted the send. Ingress proves receipt at that socket boundary. Payload hashes establish record-level observations; they cannot by themselves identify which identical copy reached dispatch. Explicit shaper drops and incomplete or conflicting observations are reported separately.

The controls use ephemeral loopback sockets and synthetic records. They exercise real Quinn backpressure, pending writes, receive-buffer eviction and an FXLink v7 round trip. The shaper controls exercise its actual scheduling methods with a minimal test base and real UDP delivery. These are component checks, not a media qualification, fairness test or remedy for the retained playback failure.

## Failure checkpoints

In an owned private daemon, set both `FLUSSONIX_PACKET_TRACE` (normal exit
capture) and `FLUSSONIX_PACKET_CHECKPOINT` (first-failure capture) to distinct,
fresh filenames in an owner-only directory. A private monitor exports the
checkpoint after a short 200 ms post-roll. It performs file I/O outside the
transport path. Only the recorder freezes; existing stream retirement and
cancellation behavior stays in place.

The receiver writes `<checkpoint>.trigger` when the first expiry is observed.
An owned Python runner can coordinate its source and proxy with:

```python
from packet_trace import checkpoint_cohort

capture_task = asyncio.create_task(checkpoint_cohort(
    receiver_checkpoint_path, source_checkpoint_path,
    shaper.packet_trace, proxy_checkpoint_path,
))
```

Run this alongside the registered case. On failure, await the coordinator
before stopping its owned nodes; preserve its error or timeout. On a healthy
case, cancel and join the task before normal cleanup. The coordinator creates
`<source checkpoint>.request`, waits for all three captures and verifies their
shared first-failure identity and peer roles. Existing requests and exports
are never replaced. These files carry no authorization credential.

The `.coverage.json` sidecar is published atomically after the trace is complete
and binds its exact SHA-256, clock origin, stop time, failure context and interval
counters. Reason 1 identifies a record returned by the reassembler's expiry;
reason 2 carries only the current head candidate at a conservative outer fence;
reason 3 identifies a decoded record that exceeded its publication deadline.
An external request retains the failure identity with a separate local
observation time. These clocks do not establish a synchronized media deadline.

Each 100 ms interval counts attempts, stored events, rejected attempts and
evictions charged to the overwritten event's interval. Rust uses 160,000 bytes
of counter data for a fixed 500-second scope, in addition to its existing bounded
event ring. Python uses larger list/integer objects for the same fixed interval
count. Event capacity is unchanged. Exceeding the accounting scope invalidates
window qualification; no counter ring silently reuses an earlier interval.

```python
from packet_trace import read_checkpoint, window_coverage

trace = read_checkpoint(checkpoint_path)
at = trace['coverage']['trigger']['at_ns']
quality = window_coverage(trace, max(0, at - 2_000_000_000), at + 100_000_000)
```

`quality['complete']` describes local capture coverage only. It requires full
containing intervals, no relevant rejection/eviction and matching retained event
counts. Earlier losses do not invalidate a later intact window. A partial
terminal interval, malformed/truncated sidecar, mismatched trace or missing peer
cannot qualify. Shutdown waits for admitted producers before export. Early
shutdown may shorten post-roll and leave the requested window incomplete.

Legacy `FXTRACE1` readers and whole-trace completeness remain unchanged. Window
coverage never overrides the global flag or authorizes packet-absence, identical
copy attribution, exclusive repair effectiveness or cross-clock timing claims.
No additional media qualification is performed by these component controls.

The Python recorder belongs to the shaper's event loop. Freeze it on that loop
before offloading an export; it does not support concurrent producer threads.
Rust uses explicit producer admission and quiescence for concurrent callers.

The proxy recorder must be enabled before traffic. Disabled checkpoint export
is rejected; attempts made while disabled are counted as gaps, including after
reenabling. The coordinator deadline covers the whole async operation. A timed-out
file operation may finish in its worker thread; preserve its outputs as evidence
and do not treat that late completion as a qualified coordinator result.
