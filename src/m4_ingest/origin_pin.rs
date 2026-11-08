//! One bounded destination set per M4 input generation.
use std::{future::Future, net::SocketAddr, time::Duration};
use url::Url;

pub(super) async fn resolve_with<F, Fut>(origin: &Url, lookup: F) -> Result<Vec<SocketAddr>, String>
where
    F: FnOnce(String, u16) -> Fut,
    Fut: Future<Output = Result<Vec<SocketAddr>, String>>,
{
    let port = origin
        .port_or_known_default()
        .ok_or("invalid M4 origin port")?;
    let answers = match origin.host().ok_or("invalid M4 origin host")? {
        url::Host::Ipv4(ip) => vec![SocketAddr::new(ip.into(), port)],
        url::Host::Ipv6(ip) => vec![SocketAddr::new(ip.into(), port)],
        url::Host::Domain(host) => {
            tokio::time::timeout(Duration::from_secs(5), lookup(host.to_owned(), port))
                .await
                .map_err(|_| "M4 origin DNS timed out")??
        }
    };
    let mut addresses = Vec::with_capacity(answers.len().min(8));
    for address in answers {
        let address = SocketAddr::new(address.ip(), port);
        if !addresses.contains(&address) {
            addresses.push(address);
            if addresses.len() == 8 {
                break;
            }
        }
    }
    if addresses.is_empty() {
        return Err("M4 origin DNS returned no addresses".into());
    }
    Ok(addresses)
}

pub(super) async fn lookup(host: String, port: u16) -> Result<Vec<SocketAddr>, String> {
    tokio::net::lookup_host((host.as_str(), port))
        .await
        .map(|answers| answers.collect())
        .map_err(|_| "M4 origin DNS failed".into())
}

pub(super) fn pin(
    builder: reqwest::ClientBuilder,
    origin: &Url,
    addresses: &[SocketAddr],
) -> reqwest::ClientBuilder {
    match origin.host() {
        Some(url::Host::Domain(host)) => builder.resolve_to_addrs(host, addresses),
        _ => builder,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        net::SocketAddr,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    #[tokio::test]
    async fn normalizes_lookup_to_configured_port_and_eight_distinct_ips() {
        let origin = url::Url::parse("https://media.example:8443/owned/m4f").unwrap();
        let answers: Vec<SocketAddr> = [
            "127.0.0.2:1",
            "127.0.0.2:2",
            "[::1]:3",
            "127.0.0.3:4",
            "127.0.0.4:5",
            "127.0.0.5:6",
            "127.0.0.6:7",
            "127.0.0.7:8",
            "127.0.0.8:9",
            "127.0.0.9:10",
        ]
        .map(str::parse)
        .into_iter()
        .map(Result::unwrap)
        .collect();
        let addresses = resolve_with(&origin, |host, port| async move {
            assert_eq!(host, "media.example");
            assert_eq!(port, 8443);
            Ok(answers)
        })
        .await
        .unwrap();
        assert_eq!(addresses.len(), 8);
        assert_eq!(addresses[0], "127.0.0.2:8443".parse().unwrap());
        assert_eq!(addresses[1], "[::1]:8443".parse().unwrap());
        assert_eq!(addresses[7], "127.0.0.8:8443".parse().unwrap());
    }

    #[tokio::test]
    async fn literal_origin_never_asks_dns_and_empty_dns_fails_closed() {
        let literal = url::Url::parse("http://[::1]:8080/owned/m4s").unwrap();
        let addresses = resolve_with(&literal, |_, _| async {
            panic!("literal address was sent to DNS")
        })
        .await
        .unwrap();
        assert_eq!(addresses, vec!["[::1]:8080".parse().unwrap()]);
        let domain = url::Url::parse("https://media.example/owned/m4s").unwrap();
        let result = resolve_with(&domain, |_, _| async { Ok(Vec::new()) }).await;
        assert!(
            result.is_err(),
            "empty DNS answers admitted an unpinned pull"
        );
    }

    #[tokio::test]
    async fn stalled_dns_has_absolute_five_second_deadline() {
        let domain = url::Url::parse("https://media.example/owned/m4s").unwrap();
        let outcome = tokio::time::timeout(
            Duration::from_secs(6),
            resolve_with(&domain, |_, _| async {
                std::future::pending::<Result<Vec<SocketAddr>, String>>().await
            }),
        )
        .await
        .expect("M4 DNS exceeded its deadline");
        assert!(outcome.is_err());
    }

    #[tokio::test]
    async fn lookup_failure_never_returns_an_unpinned_address_set() {
        let domain = url::Url::parse("https://media.example/owned/m4s").unwrap();
        let result = resolve_with(&domain, |_, _| async {
            Err("synthetic DNS failure".to_owned())
        })
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn cancelling_resolution_drops_the_pending_lookup() {
        struct DropFlag(Arc<AtomicBool>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let observed = dropped.clone();
        let (started, ready) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let domain = url::Url::parse("https://media.example/owned/m4s").unwrap();
            resolve_with(&domain, |_, _| async move {
                let _guard = DropFlag(observed);
                let _ = started.send(());
                std::future::pending::<Result<Vec<SocketAddr>, String>>().await
            })
            .await
        });
        tokio::time::timeout(Duration::from_secs(2), ready)
            .await
            .expect("lookup never started")
            .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(dropped.load(Ordering::SeqCst));
    }
}
