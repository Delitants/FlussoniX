#[allow(dead_code)]
#[path = "support/caption_fixture.rs"]
mod fixture;
#[allow(dead_code)]
#[path = "support/subtitle_fixture.rs"]
mod original;
use flussonix::media::Engine;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::io::AsyncWriteExt;
async fn run(cpu: bool) {
    let d = tempfile::tempdir().unwrap();
    let e = Arc::new(Engine::new(
        d.path(),
        &std::env::var("FLUSSONIX_CAPTION_TEST_FFMPEG").unwrap_or("ffmpeg".into()),
    ));
    let mut cfg = json!({"inputs":[{"url":"publish://"}],"flussonix_hls_captions":[{"service":1,"language":"en","name":"English"},{"service":2,"language":"es","name":"Spanish"}]});
    if cpu {
        cfg["transcoder"] = json!({"encoder":"libx264","vb":300})
    }
    cfg["flussonix_subtitle_tracks"] = json!("preserve");
    let mut p = e
        .publish_guarded("group/owned", &cfg, std::future::ready(true))
        .await
        .unwrap();
    let mut subscriber = p.worker.subscribe();
    let captured = tokio_util::sync::CancellationToken::new();
    let cancel = captured.clone();
    let capture = tokio::spawn(async move {
        let mut bytes = vec![];
        loop {
            tokio::select! {biased;_=cancel.cancelled()=>break,result=subscriber.recv()=>match result{Ok(chunk)=>{bytes.extend(chunk);assert!(bytes.len()<2*1024*1024);},Err(e)=>panic!("capture failed: {e}")}}
        }
        bytes
    });
    let input = original::inject(&fixture::digital_transport());
    p.stdin.as_mut().unwrap().write_all(&input).await.unwrap();
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            if e.read("group/owned", "fmp4/av.m3u8").await.is_ok()
                && e.read("group/owned", "av.m3u8")
                    .await
                    .is_ok_and(|b| String::from_utf8_lossy(&b).matches("#EXTINF:").count() >= 5)
            {
                break;
            }
            assert!(
                p.worker.alive.load(std::sync::atomic::Ordering::Relaxed),
                "{}",
                p.worker.stats()
            );
            tokio::time::sleep(Duration::from_millis(30)).await
        }
    })
    .await
    .unwrap_or_else(|err| panic!("{err}: {}", p.worker.stats()));
    for prefix in ["", "fmp4/"] {
        let master = String::from_utf8(
            e.read("group/owned", &format!("{prefix}index.m3u8"))
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert!(master.contains("#EXT-X-MEDIA:TYPE=SUBTITLES"), "{master}");
        assert!(master.contains("LANGUAGE=\"en\""));
        assert!(master.contains("URI=\"s1.m3u8\""), "{master}");
        assert!(master.contains("URI=\"s2.m3u8\""), "{master}");
        let av = String::from_utf8(
            e.read("group/owned", &format!("{prefix}av.m3u8"))
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        let list = String::from_utf8(
            e.read("group/owned", &format!("{prefix}s1.m3u8"))
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert!(av.contains("#EXTINF:"));
        let mut words = String::new();
        let mut empty = false;
        for segment in list.lines().filter(|s| s.ends_with(".vtt")) {
            let vtt = String::from_utf8(
                e.read("group/owned", &format!("{prefix}{segment}"))
                    .await
                    .unwrap()
                    .to_vec(),
            )
            .unwrap();
            assert!(vtt.starts_with("WEBVTT\nX-TIMESTAMP-MAP=LOCAL:00:00:00.000,MPEGTS:"));
            if !vtt.contains("-->") {
                empty = true
            }
            words += &vtt;
        }
        assert!(words.contains("USA708"), "{words}");
        assert!(words.lines().any(|l| l == "LIVE708"), "{words}");
        assert!(words.contains("00:00:02.160 --> 00:00:03.000"), "{words}");
        assert!(empty, "silence must publish an empty segment");
    }
    for prefix in ["", "fmp4/"] {
        let list = String::from_utf8(
            e.read("group/owned", &format!("{prefix}s2.m3u8"))
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        let mut words = String::new();
        for file in list.lines().filter(|f| f.ends_with(".vtt")) {
            words += &String::from_utf8(
                e.read("group/owned", &format!("{prefix}{file}"))
                    .await
                    .unwrap()
                    .to_vec(),
            )
            .unwrap();
        }
        assert!(words.contains("ESPAÑOL"), "{words}");
        assert!(!words.contains("USA708"));
        assert!(words.contains("s2-"));
    }
    captured.cancel();
    let ts = capture.await.unwrap();
    let descriptors = original::descriptors(&ts);
    for (desc, body) in [
        (original::DVB_DESC, original::DVB_BODY.to_vec()),
        (original::TTX_DESC, original::teletext_body()),
    ] {
        let (pid, _) = descriptors
            .iter()
            .find(|(_, v)| v.as_slice() == desc)
            .expect("original subtitle descriptor");
        let payloads = original::pes_bodies(&ts, *pid);
        assert!(!payloads.is_empty());
        assert!(payloads.iter().all(|p| p == &body));
    }
    assert_eq!(p.worker.stats()["hls_captions"]["status"], "running");
    assert!(p.worker.stats()["hls_captions"]["cues"].as_u64().unwrap() > 0);
    e.stop_all().await;
}
#[tokio::test]
async fn copy_delivers_two_digital_services_in_ts_and_fmp4() {
    run(false).await
}
#[tokio::test]
async fn cpu_extracts_digital_services_before_encoding() {
    run(true).await
}
#[tokio::test]
async fn digital_resources_share_auth_and_revoke_old_tokens() {
    use axum::{body::Body, http::Request};
    use http_body_util::BodyExt;
    use sha2::{Digest, Sha256};
    use tower::ServiceExt;
    let d = tempfile::tempdir().unwrap();
    let app = flussonix::server::App::new(
        d.path().join("config.json"),
        d.path().join("media"),
        flussonix::server::Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-peer-secret".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let cfg = json!({"static":false,"inputs":[{"url":"publish://"}],"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-viewer")),"flussonix_hls_captions":[{"service":1,"language":"en","name":"English"},{"service":2,"language":"es","name":"Spanish"}]});
    app.config
        .put("streams", "group/owned", cfg.clone())
        .unwrap();
    let mut p = app
        .media
        .publish_guarded("group/owned", &cfg, std::future::ready(true))
        .await
        .unwrap();
    p.stdin
        .as_mut()
        .unwrap()
        .write_all(&fixture::digital_transport())
        .await
        .unwrap();
    let get = |path: String| {
        let app = app.clone();
        async move {
            flussonix::server::router(app)
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap()
        }
    };
    let master = get("/group/owned/index.m3u8?token=owned-viewer".into()).await;
    assert_eq!(master.status(), 200);
    let body = String::from_utf8(
        master
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(body.contains("s1.m3u8?token=owned-viewer"), "{body}");
    let list = get("/group/owned/s1.m3u8?token=owned-viewer".into()).await;
    assert_eq!(list.status(), 200);
    let body = String::from_utf8(
        list.into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    let file = body.lines().find(|s| s.contains(".vtt")).unwrap();
    let path = format!("/group/owned/{file}");
    let vtt = get(path.clone()).await;
    assert_eq!(vtt.status(), 200);
    assert_eq!(vtt.headers()["content-type"], "text/vtt; charset=utf-8");
    assert_eq!(get("/group/owned/s1.m3u8".into()).await.status(), 403);
    app.config
        .put(
            "streams",
            "group/owned",
            json!({"flussonix_token_sha256":"f".repeat(64)}),
        )
        .unwrap();
    assert_eq!(get(path).await.status(), 403);
    app.media.stop_all().await;
}
