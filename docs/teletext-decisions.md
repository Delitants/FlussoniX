# Teletext decisions and tradeoffs

This stage covers the independently implemented Level 1 Latin profile. Broader protocol, codec and migration requirements remain open.

- Ruling: Continue authorized inline design/development/publication without repeated skill handoff approvals — user said continue and standing authorization persists — cost if wrong: revert isolated feature branch.
- Ruling: Native bounded Level1 Latin teletext before broader enhancement/OCR — matches next independent decoder stage and avoids new runtime dependencies — cost if wrong: enhanced/non-Latin services need follow-on work.
- Ruling: Widen internal public Rust Service/Cue IDs to u16 for stable1024+page identity — preserve wire schema and URLs while avoiding opaque per-instance slots — cost if wrong: external Rust source consumers require numeric type adjustment.
- Ruling: Treat X26/X28/M29/alternate sets as unsupported and clear/degrade affected text — avoid silent wrong characters — cost if wrong: even simple pages carrying optional enhancement packets may be excluded.
- Ruling: Reuse existing approved form layout and one private copy session — scope is functionality, not redesign — cost if wrong: later UI polish or extraction tuning.
- Final: Ruling: Enhanced/non-Latin decoding remains deferred with clear/degrade behavior — qualified Latin extraction is useful while avoiding wrong characters — cost if wrong: affected broadcasters need follow-on decoder work.
- Final: Ruling: Exact typography/position/color/flashing/height remains flattened to plain text — selectable readable text meets this stage while original carriage stays available — cost if wrong: presentation-sensitive services need layout work.
- Final: Ruling: Arbitrary program selection and multisection PSI remain unqualified — documented first-program/single-section subset stands; same-profile PAT ownership is fixed — cost if wrong: multiplex operators need additional selection/PSI support.
- Final: Ruling: DVB OCR, GPU conversion, real HEVC browser playback and native separate-subtitle relay remain pending — fixtures do not establish these capabilities — cost if wrong: those users cannot migrate yet.
- Final: Ruling: Sustained deployment scale and broad broadcaster compatibility remain unclaimed — owned bounded fixtures prove this profile only — cost if wrong: later scale/field qualification may reveal bottlenecks or unsupported streams.
- Final: Ruling: Public Rust u16 source adjustment remains accepted — stable wire selectors/URLs and collision-free native page identities take priority in this preview — cost if wrong: external Rust consumers must update source.
- Final: Ruling: Correct the qualification guard to await both independent HLS variants reaching silence — CI reproduced the premature fMP4 check; no weaker assertion or increased timeout — cost if wrong: a different underlying fault still fails qualification and delays publication.

Fresh review found three subtitle integrity issues: mixed SEI deadline ordering, PAT/PMT program ownership and repeated page transaction timing. Six focused regressions reproduced all three causes and are included with their fixes. No Minor findings were deferred.
