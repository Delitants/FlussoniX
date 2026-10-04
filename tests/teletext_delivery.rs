#[path = "support/teletext_fixture.rs"]
mod fixture;
use fixture::original;
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
    let mut cfg = json!({"inputs":[{"url":"publish://"}],"flussonix_hls_captions":[{"teletext_page":888,"language":"de","name":"German"},{"teletext_page":889,"language":"fr","name":"French"}]});
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
    let input = fixture::transport();
    p.stdin.as_mut().unwrap().write_all(&input).await.unwrap();
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            // Wait for both independent variants to reach the quiet tail.
            let mut ready = true;
            for prefix in ["", "fmp4/"] {
                ready &= e
                    .read("group/owned", &format!("{prefix}av.m3u8"))
                    .await
                    .is_ok_and(|b| String::from_utf8_lossy(&b).matches("#EXTINF:").count() >= 5);
            }
            if ready {
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
        assert!(master.contains("LANGUAGE=\"de\""));
        assert!(master.contains("URI=\"ttx888.m3u8\""), "{master}");
        assert!(master.contains("URI=\"ttx889.m3u8\""), "{master}");
        let av = String::from_utf8(
            e.read("group/owned", &format!("{prefix}av.m3u8"))
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        let list = String::from_utf8(
            e.read("group/owned", &format!("{prefix}ttx888.m3u8"))
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
        assert!(words.contains("GRÜSSE"), "{words}");
        assert!(words.lines().any(|l| l == "LIVE &lt;&amp;&gt;"), "{words}");
        assert!(words.contains("00:00:02.160 --> 00:00:03.000"), "{words}");
        assert!(
            empty,
            "{prefix} silence must publish an empty segment: {list}"
        );
    }
    for prefix in ["", "fmp4/"] {
        let list = String::from_utf8(
            e.read("group/owned", &format!("{prefix}ttx889.m3u8"))
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        let mut words = String::new();
        for file in list.lines().filter(|f| f.ends_with(".vtt")) {
            words += std::str::from_utf8(
                &e.read("group/owned", &format!("{prefix}{file}"))
                    .await
                    .unwrap(),
            )
            .unwrap();
        }
        assert!(words.contains("français"), "{words}");
        assert!(!words.contains("GRÜSSE"));
        assert!(words.contains("ttx889-"));
    }
    captured.cancel();
    let ts = capture.await.unwrap();
    let descriptors = original::descriptors(&ts);
    {
        let (desc, body) = (original::DVB_DESC, original::DVB_BODY.to_vec());
        let (pid, _) = descriptors
            .iter()
            .find(|(_, v)| v.as_slice() == desc)
            .expect("original subtitle descriptor");
        let payloads = original::pes_bodies(&ts, *pid);
        assert!(!payloads.is_empty());
        assert!(payloads.iter().all(|p| p == &body));
    }
    for desc in [fixture::TTX888_DESC, fixture::TTX889_DESC] {
        let (pid, _) = descriptors
            .iter()
            .find(|(_, d)| d.as_slice() == desc)
            .expect("original teletext descriptor");
        let payloads = original::pes_bodies(&ts, *pid);
        assert_eq!(payloads.len(), 5);
        let page = if desc == fixture::TTX888_DESC {
            888
        } else {
            889
        };
        let national = if page == 888 { 1 } else { 4 };
        let words = if page == 888 {
            &b"GR]SSE"[..]
        } else {
            &b"fran~ais"[..]
        };
        assert!(
            payloads
                .iter()
                .any(|p| p == &fixture::page_body(page, national, words))
        );
    }
    assert_eq!(p.worker.stats()["hls_captions"]["status"], "running");
    assert!(p.worker.stats()["hls_captions"]["cues"].as_u64().unwrap() > 0);
    e.stop_all().await;
}
#[tokio::test]
async fn copy_delivers_two_teletext_pages_in_ts_and_fmp4() {
    run(false).await
}
#[tokio::test]
async fn cpu_extracts_teletext_before_encoding() {
    run(true).await
}

async fn empty_or_missing(selected: u16) {
    for cpu in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let e = Engine::new(dir.path(), "ffmpeg");
        let mut cfg = json!({"inputs":[{"url":"publish://"}],"flussonix_hls_captions":[{"teletext_page":selected,"language":"de","name":"German"}]});
        if cpu {
            cfg["transcoder"] = json!({"encoder":"libx264","vb":300});
        }
        let mut p = e
            .publish_guarded("owned", &cfg, std::future::ready(true))
            .await
            .unwrap();
        p.stdin
            .as_mut()
            .unwrap()
            .write_all(&fixture::transport_with_events(false))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(12), async {
            loop {
                let mut ready = true;
                for prefix in ["", "fmp4/"] {
                    ready &= e
                        .read("owned", &format!("{prefix}av.m3u8"))
                        .await
                        .is_ok_and(|b| {
                            String::from_utf8_lossy(&b).matches("#EXTINF:").count() >= 5
                        });
                }
                if ready {
                    break;
                }
                assert!(
                    p.worker.alive.load(std::sync::atomic::Ordering::Relaxed),
                    "{}",
                    p.worker.stats()
                );
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        })
        .await
        .expect("silent or missing teletext must not stall AV");
        for prefix in ["", "fmp4/"] {
            let list = e
                .read("owned", &format!("{prefix}ttx{selected}.m3u8"))
                .await
                .unwrap();
            for file in std::str::from_utf8(&list)
                .unwrap()
                .lines()
                .filter(|f| f.ends_with(".vtt"))
            {
                let bytes = e.read("owned", &format!("{prefix}{file}")).await.unwrap();
                assert!(!std::str::from_utf8(&bytes).unwrap().contains("-->"));
            }
        }
        let stats = p.worker.stats();
        assert_eq!(
            stats["hls_captions"]["status"],
            if selected == 888 {
                "running"
            } else {
                "degraded"
            }
        );
        if selected == 777 {
            assert_eq!(
                stats["hls_captions"]["last_error"],
                "teletext_page_unavailable"
            );
        }
        e.stop_all().await;
    }
}
#[tokio::test]
async fn announced_but_absent_teletext_keeps_copy_and_cpu_av_progressing() {
    empty_or_missing(888).await;
}
#[tokio::test]
async fn unannounced_selected_page_reports_degraded_without_stalling_av() {
    empty_or_missing(777).await;
}
#[tokio::test]
async fn teletext_resources_share_auth_and_revoke_old_tokens() {
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
    let cfg = json!({"static":false,"inputs":[{"url":"publish://"}],"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-viewer")),"flussonix_hls_captions":[{"teletext_page":888,"language":"de","name":"German"},{"teletext_page":889,"language":"fr","name":"French"}]});
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
        .write_all(&fixture::transport())
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
    let mut resources = vec![];
    for prefix in ["", "fmp4/"] {
        let master = get(format!(
            "/group/owned/{prefix}index.m3u8?token=owned-viewer"
        ))
        .await;
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
        for page in [888, 889] {
            assert!(
                body.contains(&format!("ttx{page}.m3u8?token=owned-viewer")),
                "{body}"
            );
            let path = format!("/group/owned/{prefix}ttx{page}.m3u8");
            assert_eq!(get(path.clone()).await.status(), 403);
            let list = get(format!("{path}?token=owned-viewer")).await;
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
            let vtt_path = format!("/group/owned/{prefix}{file}");
            let vtt = get(vtt_path.clone()).await;
            assert_eq!(vtt.status(), 200);
            assert_eq!(vtt.headers()["content-type"], "text/vtt; charset=utf-8");
            assert_eq!(
                get(vtt_path.split('?').next().unwrap().into())
                    .await
                    .status(),
                403
            );
            resources.extend([format!("{path}?token=owned-viewer"), vtt_path]);
        }
    }
    app.config
        .put(
            "streams",
            "group/owned",
            json!({"flussonix_token_sha256":"f".repeat(64)}),
        )
        .unwrap();
    for path in resources {
        assert_eq!(get(path).await.status(), 403);
    }
    app.media.stop_all().await;
}
