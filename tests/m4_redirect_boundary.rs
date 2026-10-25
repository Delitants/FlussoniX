use flussonix::m4_ingest;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
    time::{Duration, timeout},
};

#[tokio::test]
async fn plain_http_m4_source_cannot_redirect_control_get_to_another_origin() {
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target_port = target.local_addr().unwrap().port();
    let reached = Arc::new(AtomicBool::new(false));
    let target_reached = reached.clone();
    let target_task = tokio::spawn(async move {
        if let Ok(Ok((mut socket, _))) = timeout(Duration::from_secs(5), target.accept()).await {
            target_reached.store(true, Ordering::SeqCst);
            let mut request = [0; 1024];
            let _ = socket.read(&mut request).await;
            let _ = socket
                .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
                .await;
        }
    });
    let source = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source_port = source.local_addr().unwrap().port();
    let source_task = tokio::spawn(async move {
        let (mut socket, _) = source.accept().await.unwrap();
        let mut request = [0; 1024];
        let _ = socket.read(&mut request).await.unwrap();
        let response = format!(
            "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:{target_port}/internal\r\nContent-Length: 0\r\n\r\n"
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });
    let (metadata, _received) = oneshot::channel();
    let outcome = timeout(
        Duration::from_secs(6),
        m4_ingest::pull_ready(
            &format!("m4s://127.0.0.1:{source_port}/stream"),
            None,
            &mut tokio::io::sink(),
            None,
            metadata,
        ),
    )
    .await
    .expect("M4 control request did not finish");
    assert!(outcome.is_err(), "redirect cannot provide an M4 stream");
    source_task.await.unwrap();
    assert!(
        !reached.load(Ordering::SeqCst),
        "untrusted source redirected the daemon to a second origin"
    );
    target_task.abort();
}

#[tokio::test]
async fn plain_http_m4_source_can_redirect_within_its_origin() {
    let source = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = source.local_addr().unwrap().port();
    let source_task = tokio::spawn(async move {
        for response in [
            format!(
                "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:{port}/next\r\nContent-Length: 0\r\n\r\n"
            ),
            "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n".into(),
        ] {
            let (mut socket, _) = source.accept().await.unwrap();
            let mut request = [0; 1024];
            let _ = socket.read(&mut request).await.unwrap();
            socket.write_all(response.as_bytes()).await.unwrap();
        }
    });
    let (metadata, _received) = oneshot::channel();
    let outcome = timeout(
        Duration::from_secs(6),
        m4_ingest::pull_ready(
            &format!("m4s://127.0.0.1:{port}/stream"),
            None,
            &mut tokio::io::sink(),
            None,
            metadata,
        ),
    )
    .await
    .expect("M4 same-origin redirect did not finish");
    assert!(outcome.is_err(), "404 is not an M4 stream");
    timeout(Duration::from_secs(1), source_task)
        .await
        .expect("same-origin redirect was not followed")
        .unwrap();
}
