# CEA-708 implementation decisions

| Decision | Reason | Tradeoff if wrong |
|---|---|---|
| Continue development and GitHub publication under existing authorization. | The user asked to proceed; prior work established isolated development and exact-head CI before publication. | Revert the feature branch. |
| Implement CEA-708 before European text/bitmap conversion. | Existing H.264/HEVC video extraction already supplies digital caption bytes. | Teletext/OCR arrives later. |
| Reuse the approved form layout. | Caption format/service fields extend the existing controls. | Later UI polish. |
| Retain existing Rust Service fields, with private digital identities and strict channel/service wire rows. | Preserves existing consumers and CC URLs without leaking internal IDs. | Later internal representation refactor. |
| Qualify plain text, BMP P16 and bounded16×42 windows. | Selectable text first; no claim of exact screen styling or alternative encodings. | Some styled/international captions need later support. |
| Bound unfinished packets to five source seconds and delayed queues to4096bytes/512tokens per service. | Prevents stale or unbounded decoder state without stopping AV. | Exceptionally slow signaling resets captions. |
| Route digital assets through the existing in-memory caption delivery path. | Real-media tests exposed a CC-only dispatch gate. | Future AV filenames beginning with s+digit need explicit routing. |
| Use pinned Shaka source only in a private interoperability probe. | Provides an independent text/timing comparison without a product dependency. | Its fixture comparison does not qualify every command or delay. |
