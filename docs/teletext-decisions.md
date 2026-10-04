# Teletext decisions and tradeoffs

This stage covers the independently implemented Level 1 Latin profile. Broader protocol, codec and migration requirements remain open.

- Ruling: Continue authorized inline design/development/publication without repeated skill handoff approvals — user said continue and standing authorization persists — cost if wrong: revert isolated feature branch.
- Ruling: Native bounded Level1 Latin teletext before broader enhancement/OCR — matches next independent decoder stage and avoids new runtime dependencies — cost if wrong: enhanced/non-Latin services need follow-on work.
- Ruling: Widen internal public Rust Service/Cue IDs to u16 for stable1024+page identity — preserve wire schema and URLs while avoiding opaque per-instance slots — cost if wrong: external Rust source consumers require numeric type adjustment.
- Ruling: Treat X26/X28/M29/alternate sets as unsupported and clear/degrade affected text — avoid silent wrong characters — cost if wrong: even simple pages carrying optional enhancement packets may be excluded.
- Ruling: Reuse existing approved form layout and one private copy session — scope is functionality, not redesign — cost if wrong: later UI polish or extraction tuning.
