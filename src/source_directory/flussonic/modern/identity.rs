//! Observed configured identities. No header/URL conversion or automatic logging.
use super::LookupFailure;
use uuid::Uuid;

/// Bounded untrusted identity. Custom IDs preserve exact UTF-8, including trailing
/// CR/LF from configured identity files. Recognized UUID spellings, optionally
/// followed by CR/LF, compare semantically to prevent direct-self alias bypass.
/// This observation is not cryptographic identity or a topology attestation.
/// Intentionally no Debug/Display/serialization implementation.
#[derive(Clone)]
pub struct ObservedServerId {
    text: Box<str>,
    uuid: Option<Uuid>,
}
impl ObservedServerId {
    /// Nonempty UTF-8 up to 256 bytes. Whitespace-only IDs, embedded controls and
    /// nil UUID aliases are invalid. Only trailing CR/LF controls are supported.
    /// Non-UUID custom IDs are not trimmed, case-folded or otherwise normalized.
    pub fn parse(text: &str) -> Result<Self, LookupFailure> {
        let core = text.trim_end_matches(['\r', '\n']);
        if text.len() > 256 || core.trim().is_empty() || core.chars().any(char::is_control) {
            return Err(LookupFailure::Invalid);
        }
        let uuid = Uuid::parse_str(core).ok();
        if uuid.is_some_and(|id| id.is_nil()) {
            return Err(LookupFailure::Invalid);
        }
        Ok(Self {
            text: text.into(),
            uuid,
        })
    }
    /// Original untrusted response/configured text, for deliberate inspection.
    pub fn as_str(&self) -> &str {
        &self.text
    }
}
impl PartialEq for ObservedServerId {
    fn eq(&self, other: &Self) -> bool {
        match (self.uuid, other.uuid) {
            (Some(a), Some(b)) => a == b,
            (None, None) => self.text == other.text,
            _ => false,
        }
    }
}
impl Eq for ObservedServerId {}
