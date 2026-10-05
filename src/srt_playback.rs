//! Shared listener playback; the listener is disabled unless explicitly enabled.
mod native;
mod selection;
pub use native::{Listener, Socket};
pub use selection::Selection;
#[derive(Clone)]
pub struct Settings {
    latency_millis: u32,
    client_limit: usize,
    passphrase: String,
}
impl Settings {
    pub fn new(
        latency_millis: u32,
        client_limit: usize,
        passphrase: String,
    ) -> std::io::Result<Self> {
        if !(1..=10000).contains(&latency_millis) || !(1..=4096).contains(&client_limit) {
            return Err(std::io::Error::other("invalid SRT playback limits"));
        }
        if !passphrase.is_empty()
            && (!(10..=79).contains(&passphrase.len())
                || !passphrase.bytes().all(|b| (0x20..=0x7e).contains(&b)))
        {
            return Err(std::io::Error::other("invalid SRT playback passphrase"));
        }
        Ok(Self {
            latency_millis,
            client_limit,
            passphrase,
        })
    }
    pub fn latency_millis(&self) -> u32 {
        self.latency_millis
    }
    pub fn client_limit(&self) -> usize {
        self.client_limit
    }
    pub fn encrypted(&self) -> bool {
        !self.passphrase.is_empty()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn settings_validate_exact_bounds_without_secret_diagnostics() {
        assert!(Settings::new(120, 128, String::new()).is_ok());
        assert!(Settings::new(1, 1, "0123456789".into()).is_ok());
        assert!(Settings::new(10000, 4096, "a".repeat(79)).is_ok());
        for (latency, limit, secret) in [
            (0, 128, ""),
            (10001, 128, ""),
            (120, 0, ""),
            (120, 4097, ""),
            (120, 128, "short"),
            (120, 128, "owned\nsecret"),
            (120, 128, "nonASCIIésecret"),
        ] {
            let error = Settings::new(latency, limit, secret.into())
                .err()
                .unwrap()
                .to_string();
            assert!(!error.contains(secret) || secret.is_empty());
        }
        assert!(Settings::new(120, 128, "a".repeat(80)).is_err());
    }
}
