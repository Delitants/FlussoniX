//! The root input task owns the entire verified redirect chain.
use std::{collections::HashSet, net::SocketAddr, path::PathBuf, time::Duration};
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
    let _ = key.set_port(Some(url.port().unwrap_or(322)));
    key.into()
}
pub(super) async fn relay(
    source: url::Url,
    mut listener: TcpListener,
    mut upstream: TlsStream<TcpStream>,
    ca: Option<PathBuf>,
    deadline: tokio::time::Instant,
) -> Result<(), String> {
    let credentials = !source.username().is_empty() || source.password().is_some();
    let mut visited = HashSet::from([identity(&source)]);
    let mut redirects = 0;
    loop {
        let expires = (tokio::time::Instant::now() + Duration::from_secs(8)).min(deadline);
        let (downstream, _) = tokio::time::timeout_at(expires, listener.accept())
            .await
            .map_err(|_| "RTSPS decoder connection timed out")?
            .map_err(|_| "RTSPS decoder connection failed")?;
        drop(listener);
        let _ = downstream.set_nodelay(true);
        let (mut request, mut response) = downstream.into_split();
        let (read, mut write) = tokio::io::split(upstream);
        let mut read = tokio::io::BufReader::new(read);
        let (target, cseq) = tokio::select! {
            _ = tokio::io::copy(&mut request, &mut write) => return Ok(()),
            redirect = super::forward_responses(&mut read, &mut response, deadline) => redirect.map_err(|_| "RTSPS response rejected or closed")?,
        };
        if credentials || redirects == 4 || !visited.insert(identity(&target)) {
            return Err("RTSPS credentialed redirect, cycle or hop limit rejected".into());
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
        let local = loopback(
            &target,
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
