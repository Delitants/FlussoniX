# FlussoniX delivery sampling and pacing patch

This directory contains quinn-proto 0.11.19, copied from the installed crates.io package. Original MIT and Apache-2.0 licenses are retained. The packaged .cargo_vcs_info.json records upstream revision 8192ed399a26a6e8f0aba43d10ec8badaa5b905e, path quinn-proto; this revision was not separately verified against the remote repository.

The additive BbrConfig::delivery_rate_sampling option defaults to false. FlussoniX enables it for FXLink, including the controller-rate pacing integration described below. Default workspace members and dependency features remain those of the application; vendor regression checks explicitly disable default features and enable rustls-ring. No protocol version, packet encoding, authentication, FEC, queue limit, credit limit or record deadline changes here.

When enabled, tracked ack-eliciting packets carry a fixed-size delivery snapshot. One ACK batch supplies a rate using delivered bytes divided by the larger send/ACK interval. Zero, shorter-than-minimum-RTT and unrepresentable samples are discarded. Application-limited state belongs to the sent flight. Only explicit ACKs from the current path generation can credit this model; Retry inference cannot. Path changes reset timing anchors while preserving monotonic packet/recovery sequence fences.

Valid non-application-limited lower and equal samples reach the existing ten-round maximum filter so old peaks can expire. Application-limited lower samples cannot depress that filter. Startup window growth compares the actual congestion window with its target; BDP arithmetic uses wider intermediates. This is a bounded correction to the existing BBR controller, not a BBRv3 implementation. Startup pacing can retain a previous high rate even after the bandwidth filter ages it out.

Controller::pacing_rate is an additive default method returning an optional nonzero byte-per-second rate. Default controllers, including BBR without the opt-in, return None and keep the existing window/RTT-derived pacing algorithm. Opted-in BBR supplies its calculated rate once available; until then pacing uses the existing fallback. Qlog remains in bits/s, with saturating conversion at its representable limit.

The controlled pacer uses integer token accounting, including fractional-byte credit, and settles elapsed time at the previous rate before changing it. Its two-millisecond target quantum retains the existing ten-to-256-MTU bucket bounds. Capacity/rate/MTU updates clamp remaining credit without filling a new bucket. New, reset and cloned paths initialize pacing from the selected controller. Explicit rates do not inherit the fallback's zero-RTT or large-window bypass. Long waits recheck within one second without granting bytes. This adds per-path accounting fields, with no new per-packet allocation, queue or task.

Congestion-window admission stays independent. Existing ACK-only, loss/MTU-probe and path-validation exceptions remain, so this is not a hard limit on all QUIC traffic. Startup exit/gain policy is unchanged; pacing integration alone does not establish sustained media recovery. The fixed-round Startup timer and equivalent-window adapter used in the earlier diagnostic remain unshipped.

SentPacket retains its existing 128-byte size guard. STREAM-frame metadata moves from an inline small vector to a boxed slice: empty DATAGRAM metadata does not allocate, while packets containing STREAM metadata incur a slice allocation. This tradeoff has not been performance-qualified against upstream STREAM workloads. No separate delivery-history map, task or queue is added.

The existing upstream tests are retained, with focused delivery, filter, Startup, overflow, Retry, migration, rate-transition, fractional-credit and simulated QUIC emission regressions. rustfmt.toml preserves the upstream 2024 formatting style without changing its Rust edition.

Sampling background: [IETF BBR working draft](https://www.ietf.org/archive/id/draft-ietf-ccwg-bbr-06.html), an evolving draft rather than a conformance claim.

`Controller::application_demand()` is an additive default method returning `None`.
Only delivery-sampling BBR supplies a connection-local handle, shared across path
clones but freshly created for each connection. It retains at most sixteen weak
subscription registrations. A validity-bearing application snapshot distinguishes
retirement from active/idle and known unread originals; paused admission and
already encoded data count as demand. CPU/FEC waits and optional parity-only work
do not. Retirement invalidates held ready-data guards, and dropping a registration
cannot affect a reused slot. Application callbacks run outside the registry lock
and must not call into QUIC or own media payloads.

Known upstream demand prevents a new application-limited marker; historical
packet snapshots and sampler bubbles are unchanged. Opted-in empty polls also
check the next normal MTU's congestion-window admission and readonly explicit
pacer credit. The latter settles time at the old rate and clamps to the selected
rate/MTU capacity without moving tokens or timers. Legacy pacing remains
unmeasured and default controllers retain their previous classification.

The application supplies weak native-reader/bootstrap metadata, cancellation
fences and stack guards around encoded original admission. This adds fixed
per-connection/subscription metadata, with no new media queue, payload copy,
per-packet allocation or task. Retained regressions cover reader eviction/drop/EOF,
weak ownership, cancellation with a held guard, registration bounds/reuse,
connection/path isolation, historical limited samples, genuine idle and actual
simulated QUIC cwnd/pacer boundaries. This corrects signaling; independent media
qualification is documented separately and Startup policy is unchanged.

## Delivery-sampling startup initialization

The explicit-rate variant now initializes minimum RTT and its age from the first
tracked ACK carrying a nonzero RTT, including an application-limited handshake.
An absent prior sample is not treated as an already expired ten-second sample.
Snapshotless ACKs still cannot credit delivery or initialize this state; later
RTT aging and ProbeRtt behavior retain their existing rules.

On first measured pacing initialization, that variant applies its existing 2.885
startup gain to the nominal initial-window/minimum-RTT rate and retains a higher
measured target. Subsequent Startup rates remain nondecreasing. Default/legacy
BBR keeps the previous unit-gain initialization and window-derived pacer. The
12,000-byte FXLink initial window, congestion admission, controller/pacer byte
credit, wire formats and media deadlines are unchanged.

This is a bounded correction to the local opt-in delivery-sampling variant,
not a BBRv3 migration or a claim of complete draft conformance. Conceptual context:
[IETF BBR draft section 5.6.2](https://www.ietf.org/archive/id/draft-ietf-ccwg-bbr-06.html#section-5.6.2).
The retained older Google/quiche-derived unit-gain branch remains the legacy
control. Regression coverage uses tracked handshake and media ACKs, an injected
clock and the real token-bucket pacer, including genuine later RTT expiry,
startup-rate growth and legacy behavior. These component results do not qualify
500 ms streaming or prove exclusive causality for the earlier live capture.
