# DVB recognition decisions

| Decision | Reason and cost |
|---|---|
| Native Rust bitmap state plus optional Tesseract | Independent deployment with no official vendor runtime; models and OCR accuracy need deployment tuning. |
| Explicit composition page and recognition models | Deterministic language/service ownership; automatic discovery remains future work. |
| u32 Rust identities; DVB IDs65536+page, URLdvbN | Full16bit pages without collisions; external Rust source consumers must adjust. Old wire selectors remain stable. |
| Coding0 interlaced SDR luminance profile | Bounded testable broadcast bitmap subset; advanced/HDR/character objects and styling remain unqualified. |
| Initially750ms, revised to1500ms before implementation | German probe519ms under concurrent tests needed headroom; OCR failures can add up to1.5seconds subtitle-publication delay. |
| At most two OCR processes per daemon, one OpenMP thread each | Bound recognition CPU; accelerated high-concurrency bursts may suppress intervals. |
| Eight valid pending images and64intervals per service | Source timing survives asynchronous completion with bounded memory; canceled workers may briefly retain two additional images during cleanup. |
| Two generation-owned workers, initially20ms polling, now event-driven | Broadcast work/reset/expiry changes and wait for actual pending deadlines; idle OCR workers have no periodic timer. The separate100ms HLS playlist watcher and production capacity still need tuning. |
| Mean word confidence60; low scores emit no text | Avoid presenting weak recognition as conversion success; faint/unusual fonts may lose intervals. |
| Original-track policy separate from HLS conversion | Keeps compatible outputs intact; original-format players remain necessary for pass-through. |
| Serialize accelerated OCR delivery fixtures | Qualify nominal text/timing without artificial process overload; this is not a concurrency/soak result. |

The stream API cannot choose executable paths. A trusted deployment may set `FLUSSONIX_TESSERACT`; missing or unsupported models are explicit per-service failures. Input/output, child count, deadline, text and native cache bounds are fixed for this preview. Capacity, alternate bitmap profiles, GPU/real HEVC caption browser delivery and regional subtitle relay require separate qualification.
