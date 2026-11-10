//! The root input task owns the entire verified redirect chain.
use std::{
    collections::HashSet,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};
use tokio_rustls::client::TlsStream;

pub(super) fn loopback(source: &url::Url, local: SocketAddr) -> Result<String, String> {
    let mut url = source.clone();
    url.set_scheme("rtsp")
        .map_err(|_| "cannot translate RTSPS URL")?;
    url.set_host(Some("127.0.0.1"))
        .map_err(|_| "cannot translate RTSPS host")?;
    url.set_port(Some(local.port()))
        .map_err(|_| "cannot translate RTSPS port")?;
    Ok(url.into())
}
fn identity(url: &url::Url) -> String {
    let mut key = url.clone();
    let _ = key.set_password(None);
    let _ = key.set_username("");
    if let Some(host) = url.host_str() {
        let _ = key.set_host(Some(&host.to_ascii_lowercase()));
    }
    let _ = key.set_port(Some(url.port().unwrap_or(322)));
    key.into()
}
pub(super) async fn relay(
    source: url::Url,
    mut listener: TcpListener,
    mut upstream: TlsStream<TcpStream>,
    ca: Option<PathBuf>,
    policy: super::RedirectOrigins,
    deadline: tokio::time::Instant,
    decoder_pid: Arc<AtomicU32>,
) -> Result<(), String> {
    let credentials = !source.username().is_empty() || source.password().is_some();
    let mut visited = HashSet::from([identity(&source)]);
    let mut redirects = 0;
    loop {
        let expires = (tokio::time::Instant::now() + Duration::from_secs(8)).min(deadline);
        let downstream = loop {
            let (candidate, _) = tokio::time::timeout_at(expires, listener.accept())
                .await
                .map_err(|_| "RTSPS decoder connection timed out")?
                .map_err(|_| "RTSPS decoder connection failed")?;
            let owned = loop {
                let pid = decoder_pid.load(Ordering::Acquire);
                if pid != 0 {
                    break tokio::time::timeout_at(expires, super::peer::belongs(pid, &candidate))
                        .await
                        .map_err(|_| "RTSPS decoder peer check timed out")?
                        .map_err(String::from)?;
                }
                if tokio::time::Instant::now() >= expires {
                    return Err("RTSPS decoder PID timed out".into());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            };
            if owned {
                break candidate;
            }
            // A foreign local process cannot consume this bridge or refresh
            // the absolute startup deadline.
        };
        drop(listener);
        let _ = downstream.set_nodelay(true);
        let (mut request, mut response) = downstream.into_split();
        let (read, mut write) = tokio::io::split(upstream);
        let mut read = tokio::io::BufReader::new(read);
        let (target, cseq) = tokio::select! {
            _ = tokio::io::copy(&mut request, &mut write) => return Ok(()),
            redirect = super::forward_responses(&mut read, &mut response, deadline) => redirect.map_err(|_| "RTSPS response rejected or closed")?,
        };
        if !policy.permits(&target) || redirects == 4 || !visited.insert(identity(&target)) {
            return Err("RTSPS redirect scope, cycle or hop limit rejected".into());
        }
        redirects += 1;
        // No target application data is written before its own TLS verification.
        let next =
            tokio::time::timeout_at(deadline, super::connect(target.as_str(), ca.as_deref()))
                .await
                .map_err(|_| "RTSPS routing timed out")??;
        let next_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|_| "cannot bind RTSPS redirect bridge")?;
        let mut handoff = target.clone();
        if credentials {
            handoff
                .set_username(source.username())
                .map_err(|_| "cannot retain RTSPS credentials")?;
            handoff
                .set_password(source.password())
                .map_err(|_| "cannot retain RTSPS credentials")?;
        }
        let local = loopback(
            &handoff,
            next_listener
                .local_addr()
                .map_err(|_| "RTSPS redirect address unavailable")?,
        )?;
        let reply = format!(
            "RTSP/1.0 302 Moved Temporarily\r\nCSeq: {cseq}\r\nLocation: {local}\r\nCache-Control: no-store\r\n\r\n"
        );
        tokio::time::timeout_at(deadline, response.write_all(reply.as_bytes()))
            .await
            .map_err(|_| "RTSPS routing timed out")?
            .map_err(|_| "RTSPS decoder disconnected")?;
        // Close both halves of the old hop before accepting its replacement.
        drop(request);
        drop(response);
        drop(read);
        drop(write);
        upstream = next;
        listener = next_listener;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn credential_scope_and_cycle_identity_normalize_case_and_default_port() {
        let source = url::Url::parse("rtsps://user:secret@CAMERA.example/entry?token=one").unwrap();
        let same = url::Url::parse("rtsps://camera.example:322/entry?token=one").unwrap();
        assert!(
            super::super::RedirectOrigins::parse(source.as_str(), None)
                .unwrap()
                .permits(&same)
        );
        assert_eq!(identity(&source), identity(&same));
        for different in [
            "rtsps://camera.example:323/entry",
            "rtsps://alias.example/entry",
            "rtsps://camera.example./entry",
        ] {
            assert!(
                !super::super::RedirectOrigins::parse(source.as_str(), None)
                    .unwrap()
                    .permits(&url::Url::parse(different).unwrap())
            );
        }
        let query = url::Url::parse("rtsps://camera.example/entry?token=two").unwrap();
        assert_ne!(identity(&source), identity(&query));
    }
}
