use super::*;
use crate::vendor_test_fixture::Fixture;
use std::{sync::atomic::AtomicUsize, task::Poll};
use tokio_util::sync::CancellationToken;
fn request(token: &str) -> ViewerRequest {
    ViewerRequest {
        name: "owned".into(),
        proto: "srt".into(),
        ip: "127.0.0.1".into(),
        token: token.into(),
        ..Default::default()
    }
}
#[tokio::test]
async fn pending_write_cannot_retry_after_source_edit_or_individual_revocation() {
    for edit in [false, true] {
        let f = Fixture::configured("cdn", json!({}), None, false).await;
        let first = f.app.ts_admit(request("first")).await.unwrap();
        let other = f.app.ts_admit(request("other")).await.unwrap();
        let mut revision = f.app.config.revision();
        let writes = AtomicUsize::new(0);
        let cancel = CancellationToken::new();
        let mut send = Box::pin(crate::srt_playback::send_with(
            b"private media",
            &f.app,
            &first,
            &cancel,
            &mut revision,
            |_| {
                writes.fetch_add(1, Ordering::Relaxed);
                Ok(false)
            },
        ));
        assert!(matches!(futures_util::poll!(send.as_mut()), Poll::Pending));
        assert_eq!(writes.load(Ordering::Relaxed), 1);
        if edit {
            let mut source = f.app.config.snapshot()["sources"][0].clone();
            source["except"] = json!(["owned"]);
            f.app
                .config
                .put("sources", &f.origins[0].key, source)
                .unwrap();
        } else {
            use sha2::{Digest, Sha256};
            let hash = format!("{:x}", Sha256::digest(b"first"));
            let id = f
                .app
                .playback_auth
                .snapshots()
                .into_iter()
                .find(|s| s["user_id"] == hash)
                .unwrap()["id"]
                .as_str()
                .unwrap()
                .to_owned();
            assert!(f.app.playback_auth.revoke(&id));
        }
        let denied = tokio::time::timeout(Duration::from_millis(100), send.as_mut())
            .await
            .unwrap()
            .is_err();
        drop(send);
        let other_current = other.current();
        let count = writes.load(Ordering::Relaxed);
        let bytes = f.app.srt_egress.load(Ordering::Relaxed);
        drop(first);
        drop(other);
        f.stop().await;
        assert!(denied);
        assert_eq!(count, 1);
        assert_eq!(bytes, 0);
        assert_eq!(other_current, !edit);
    }
}
#[tokio::test]
async fn accounts_only_successfully_accepted_transport_bytes_once() {
    let f = Fixture::configured("cdn", json!({"only":[]}), None, false).await;
    let mut playback = f.app.ts_admit(request("first")).await.unwrap();
    playback.attach();
    assert_eq!(playback.worker.viewers.load(Ordering::Relaxed), 1);
    let before = f.app.playback_auth.snapshots()[0]["bytes"].clone();
    assert!(playback.write_current(13, || Ok(false)).unwrap().is_ok());
    assert!(
        playback
            .write_current(13, || Err(std::io::Error::other("owned failed send")))
            .unwrap()
            .is_err()
    );
    assert_eq!(f.app.playback_auth.snapshots()[0]["bytes"], before);
    let mut revision = f.app.config.revision();
    let cancel = CancellationToken::new();
    crate::srt_playback::send_with(
        b"private media",
        &f.app,
        &playback,
        &cancel,
        &mut revision,
        |_| Ok(true),
    )
    .await
    .unwrap();
    let sent = f.app.playback_auth.snapshots()[0]["bytes"].clone();
    let bytes = f.app.srt_egress.load(Ordering::Relaxed);
    drop(playback);
    f.stop().await;
    assert_eq!(sent, json!(before.as_u64().unwrap() + 13));
    assert_eq!(bytes, 13);
}
