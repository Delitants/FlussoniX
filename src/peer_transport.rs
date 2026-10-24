//! One policy for every native HTTP request that carries a reusable peer key.
use url::{Host, Url};

pub(crate) const INSECURE_KEY_TRANSPORT: &str = "peer key requires HTTPS or literal loopback";

pub(crate) fn require(url: &Url, allow_insecure_http: bool) -> Result<(), &'static str> {
    match (url.scheme(), url.host()) {
        ("https", Some(_)) => Ok(()),
        ("http", Some(Host::Ipv4(address))) if address.is_loopback() => Ok(()),
        ("http", Some(Host::Ipv6(address))) if address.is_loopback() => Ok(()),
        ("http", Some(_)) if allow_insecure_http => Ok(()),
        _ => Err(INSECURE_KEY_TRANSPORT),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plaintext_exception_requires_a_literal_loopback_or_explicit_relationship_opt_in() {
        for address in [
            "http://localhost:18210",
            "http://10.0.0.7:18210",
            "http://remote.example",
        ] {
            let url = Url::parse(address).unwrap();
            assert_eq!(require(&url, false), Err(INSECURE_KEY_TRANSPORT));
            assert_eq!(require(&url, true), Ok(()));
        }
        for address in [
            "http://127.8.9.10:18210",
            "http://[::1]:18210",
            "https://remote.example",
        ] {
            assert_eq!(require(&Url::parse(address).unwrap(), false), Ok(()));
        }
    }
}
