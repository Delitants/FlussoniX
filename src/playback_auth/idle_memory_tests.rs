use super::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::test]
async fn background_renewal_inhibits_idle_cleanup_without_a_media_worker() {
    let controller = crate::idle_memory::test_support::controller();
    let mut auth = PlaybackAuth::new(10);
    auth.idle_memory = controller.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let callback = format!("http://{}/auth", listener.local_addr().unwrap());
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
    let server = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        for index in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut buffer = [0; 1024];
            while !bytes.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = socket.read(&mut buffer).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buffer[..n]);
            }
            if index == 0 {
                socket.write_all(b"HTTP/1.1 200 OK\r\nX-AuthDuration: 1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
            } else {
                let _ = entered_tx.send(());
                let _ = finish_rx.await;
                let _ = socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await;
                break;
            }
        }
    }));
    let policy = Policy::from_config(&json!({"on_play":callback}), &json!({})).unwrap();
    let snapshot = auth.publish("owned", Some(policy)).unwrap();
    let result = auth
        .authorize(
            snapshot,
            ViewerRequest {
                name: "owned".into(),
                proto: "hls".into(),
                ip: "127.0.0.1".into(),
                ..Default::default()
            },
        )
        .await;
    let AuthOutcome::Allowed(grant) = result else {
        panic!("owned initial callback denied");
    };
    {
        let all = auth.entries.lock().unwrap();
        let entry = all.values().next().unwrap();
        entry
            .state
            .lock()
            .unwrap()
            .callback_lanes
            .values_mut()
            .next()
            .unwrap()
            .next_check = Instant::now();
    }
    assert_eq!(controller.snapshot()["active_operations"], 0);
    let mut renewal = Box::pin(auth.renew_due());
    tokio::time::timeout(Duration::from_secs(3), async {
        tokio::select! { _=&mut renewal=>panic!("renewal completed before held callback"),result=entered_rx=>result.unwrap() }
    }).await.unwrap();
    let active = controller.snapshot()["active_operations"].clone();
    drop(renewal);
    let _ = finish_tx.send(());
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    drop(grant);
    assert_eq!(controller.snapshot()["active_operations"], 0);
    assert_eq!(
        active, 1,
        "a pending renewal must inhibit heap cleanup even without a worker"
    );
}
