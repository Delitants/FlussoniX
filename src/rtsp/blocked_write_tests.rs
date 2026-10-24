use super::*;
use crate::vendor_test_fixture::Fixture;
use serde_json::json;
use tokio::io::AsyncReadExt;

async fn admit(f: &Fixture, token: &str, url: &url::Url) -> Playback {
    let viewer = ViewerRequest {
        name: "owned".into(),
        proto: "rtsp".into(),
        ip: "127.0.0.1".into(),
        token: token.into(),
        qs: format!("token={token}"),
        user_agent: String::new(),
        referer: String::new(),
        host: "127.0.0.1".into(),
    };
    match f.app.rtsp_admit(viewer, url, false).await.unwrap() {
        crate::server::rtsp_access::Admission::Playback(p) => p,
        _ => panic!("unexpected redirect"),
    }
}
#[tokio::test]
async fn pending_vendor_rtsp_write_is_cancelled_without_retiring_other_static_viewer() {
    let f = Fixture::configured("cdn", json!({"on_play":"true"}), None, false).await;
    let url = url::Url::parse("rtsp://127.0.0.1:45555/owned").unwrap();
    let first = admit(&f, "first", &url).await;
    let id = f.app.playback_auth.snapshots()[0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let other = admit(&f, "other", &url).await;
    let session = Session {
        playback: first,
        url,
        id: "owned-session".into(),
        senders: HashMap::new(),
        report_epoch: None,
        playing: true,
        initial: VecDeque::new(),
        pending: None,
        pacer: None,
        receiver: None,
        rtp_info: String::new(),
        feedback_cursor: 0,
    };
    let (mut writer, mut reader) = tokio::io::duplex(1);
    writer.write_all(&[0]).await.unwrap(); // Fill the only byte of socket capacity.
    let cancel = CancellationToken::new();
    let mut write = Box::pin(bounded_write(&mut writer, &[1], &cancel, Some(&session)));
    std::future::poll_fn(|cx| {
        assert!(
            std::future::Future::poll(write.as_mut(), cx).is_pending(),
            "write never blocked"
        );
        std::task::Poll::Ready(())
    })
    .await;
    assert!(f.app.playback_auth.revoke(&id));
    let result = tokio::time::timeout(Duration::from_millis(100), write.as_mut()).await;
    drop(write);
    assert!(
        other.current(),
        "revocation retired another valid static-stream viewer"
    );
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 1);
    drop(writer);
    let mut received = vec![];
    reader.read_to_end(&mut received).await.unwrap();
    drop(session);
    drop(other);
    f.stop().await;
    assert!(
        result.is_ok(),
        "blocked write waited for the 2-second socket timeout"
    );
    assert_eq!(
        result.unwrap().unwrap_err().kind(),
        std::io::ErrorKind::Interrupted
    );
    assert_eq!(received, [0], "revoked media reached the writer");
}
