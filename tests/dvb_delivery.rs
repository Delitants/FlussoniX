#[path = "support/dvb_fixture.rs"]
mod fixture;
use fixture::carrier::original;
use flussonix::media::Engine;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::io::AsyncWriteExt;
static SERIAL: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
async fn run(cpu: bool) {
    let _serial = SERIAL.acquire().await.unwrap();
    let d = tempfile::tempdir().unwrap();
    let e = Arc::new(Engine::new(
        d.path(),
        &std::env::var("FLUSSONIX_CAPTION_TEST_FFMPEG").unwrap_or("ffmpeg".into()),
    ));
    let mut cfg = json!({"inputs":[{"url":"publish://"}],"flussonix_hls_captions":[{"dvb_page":1,"ocr_language":"eng","language":"en","name":"English"},{"dvb_page":2,"ocr_language":"deu","language":"de","name":"German"}]});
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
        assert!(master.contains("LANGUAGE=\"en\""));
        assert!(master.contains("URI=\"dvb1.m3u8\""), "{master}");
        assert!(master.contains("URI=\"dvb2.m3u8\""), "{master}");
        let av = String::from_utf8(
            e.read("group/owned", &format!("{prefix}av.m3u8"))
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        let list = String::from_utf8(
            e.read("group/owned", &format!("{prefix}dvb1.m3u8"))
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
        assert!(words.contains("EUROPE DVB"), "{words}");
        assert!(words.lines().any(|l| l == "LIVE &lt;&amp;&gt;"), "{words}");
        assert!(words.contains("00:00:02.160 --> 00:00:03.000"), "{words}");
        assert!(
            empty,
            "{prefix} silence must publish an empty segment: {list}"
        );
    }
    for prefix in ["", "fmp4/"] {
        let list = String::from_utf8(
            e.read("group/owned", &format!("{prefix}dvb2.m3u8"))
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
        assert!(words.contains("GRÜSSE"), "{words}");
        assert!(!words.contains("EUROPE DVB"));
        assert!(words.contains("dvb2-"));
    }
    captured.cancel();
    let ts = capture.await.unwrap();
    let descriptors = original::descriptors(&ts);
    for (desc, page, text) in [
        (fixture::ENG_DESC, 1, "EUROPE DVB"),
        (fixture::DEU_DESC, 2, "GRÜSSE"),
    ] {
        let (pid, _) = descriptors
            .iter()
            .find(|(_, v)| v.as_slice() == desc)
            .expect("original DVB descriptor");
        let payloads = original::pes_bodies(&ts, *pid);
        assert_eq!(payloads.len(), 5);
        assert!(payloads.contains(&fixture::bitmap(page, text)));
    }
    assert_eq!(p.worker.stats()["hls_captions"]["status"], "running");
    assert!(p.worker.stats()["hls_captions"]["cues"].as_u64().unwrap() > 0);
    e.stop_all().await;
}
#[tokio::test]
async fn copy_delivers_two_dvb_languages_in_ts_and_fmp4() {
    run(false).await
}
#[tokio::test]
async fn cpu_extracts_dvb_before_encoding() {
    run(true).await
}

#[tokio::test]
async fn dvb_resources_share_auth_and_revoke_old_tokens() {
    let _serial = SERIAL.acquire().await.unwrap();
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
    let cfg = json!({"static":false,"inputs":[{"url":"publish://"}],"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-viewer")),"flussonix_hls_captions":[{"dvb_page":1,"ocr_language":"eng","language":"en","name":"English"},{"dvb_page":2,"ocr_language":"deu","language":"de","name":"German"}]});
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
        for page in [1, 2] {
            assert!(
                body.contains(&format!("dvb{page}.m3u8?token=owned-viewer")),
                "{body}"
            );
            let path = format!("/group/owned/{prefix}dvb{page}.m3u8");
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
#[tokio::test]
async fn missing_slow_and_oversized_ocr_keep_both_variants_progressing() {
    use std::os::unix::fs::PermissionsExt;
    let _serial = SERIAL.acquire().await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    for (mode, script, reason) in [
        ("missing", None, "dvb_ocr_unavailable"),
        ("slow", Some("exec /bin/sleep 60"), "dvb_ocr_timeout"),
        (
            "large",
            Some("exec /usr/bin/head -c 100000 /dev/zero"),
            "dvb_ocr_output_limit",
        ),
    ] {
        let path = dir.path().join(mode);
        if let Some(body) = script {
            std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        for cpu in [false, true] {
            let e = Engine::new_with_ocr(
                dir.path().join(format!("{mode}-{cpu}")),
                "ffmpeg",
                path.to_str().unwrap(),
            );
            let mut cfg = json!({"inputs":[{"url":"publish://"}],"flussonix_hls_captions":[{"dvb_page":1,"ocr_language":"eng","language":"en","name":"English"}]});
            if cpu {
                cfg["transcoder"] = json!({"encoder":"libx264","vb":300})
            }
            let mut p = e
                .publish_guarded("owned", &cfg, std::future::ready(true))
                .await
                .unwrap();
            p.stdin
                .as_mut()
                .unwrap()
                .write_all(&fixture::transport())
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
                    tokio::time::sleep(Duration::from_millis(30)).await
                }
            })
            .await
            .unwrap_or_else(|err| panic!("{err}: {}", p.worker.stats()));
            for prefix in ["", "fmp4/"] {
                let list = e
                    .read("owned", &format!("{prefix}dvb1.m3u8"))
                    .await
                    .unwrap();
                for file in std::str::from_utf8(&list)
                    .unwrap()
                    .lines()
                    .filter(|l| l.ends_with(".vtt"))
                {
                    let vtt = e.read("owned", &format!("{prefix}{file}")).await.unwrap();
                    assert!(!std::str::from_utf8(&vtt).unwrap().contains("-->"));
                }
            }
            let stats = p.worker.stats();
            assert_eq!(stats["hls_captions"]["status"], "degraded", "{stats}");
            assert_eq!(stats["hls_captions"]["last_error"], reason, "{stats}");
            assert!(p.worker.alive.load(std::sync::atomic::Ordering::Relaxed));
            e.stop_all().await;
        }
    }
}
#[tokio::test]
async fn mixed_teletext_and_dvb_share_one_input_without_text_leakage() {
    let _serial = SERIAL.acquire().await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::new(dir.path(), "ffmpeg");
    let cfg = json!({"inputs":[{"url":"publish://"}],"flussonix_hls_captions":[{"dvb_page":1,"ocr_language":"eng","language":"en","name":"English bitmap"},{"teletext_page":888,"language":"de","name":"German teletext"}]});
    let mut p = e
        .publish_guarded("owned", &cfg, std::future::ready(true))
        .await
        .unwrap();
    p.stdin
        .as_mut()
        .unwrap()
        .write_all(&fixture::mixed_transport())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let mut ready = true;
            for prefix in ["", "fmp4/"] {
                ready &= e
                    .read("owned", &format!("{prefix}av.m3u8"))
                    .await
                    .is_ok_and(|b| String::from_utf8_lossy(&b).matches("#EXTINF:").count() >= 5);
            }
            if ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await
        }
    })
    .await
    .unwrap();
    for prefix in ["", "fmp4/"] {
        for (key, wanted, other) in [
            ("dvb1", "EUROPE DVB", "TELETEXT"),
            ("ttx888", "TELETEXT", "EUROPE DVB"),
        ] {
            let list = e
                .read("owned", &format!("{prefix}{key}.m3u8"))
                .await
                .unwrap();
            let mut words = String::new();
            for file in std::str::from_utf8(&list)
                .unwrap()
                .lines()
                .filter(|l| l.ends_with(".vtt"))
            {
                words += std::str::from_utf8(
                    &e.read("owned", &format!("{prefix}{file}")).await.unwrap(),
                )
                .unwrap();
            }
            assert!(words.contains(wanted), "{words}");
            assert!(!words.contains(other), "{words}");
        }
    }
    e.stop_all().await;
}
#[tokio::test]
async fn ancillary_rebinding_cancels_and_reaps_in_flight_recognition() {
    use flussonix::{
        caption_hls::State,
        captions::{Decoder, Service},
    };
    use std::os::unix::fs::PermissionsExt;
    let _serial = SERIAL.acquire().await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let executable = dir.path().join("ocr");
    let pidfile = dir.path().join("pid");
    std::fs::write(
        &executable,
        format!(
            "#!/bin/sh\necho $$ > '{}'\nexec /bin/sleep 60\n",
            pidfile.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    let services: Vec<Service> = serde_json::from_value(
        json!([{"dvb_page":1,"ocr_language":"eng","language":"en","name":"English"}]),
    )
    .unwrap();
    let state = Arc::new(State::new(Decoder::new(services), "owned".into(), 0));
    let cancel = tokio_util::sync::CancellationToken::new();
    let task = tokio::spawn(
        state
            .clone()
            .ocr(executable.to_str().unwrap().into(), cancel.clone()),
    );
    let mut transport = flussonix::caption_transport::Transport::default();
    let mut vc = 0;
    let mut dc = 0;
    let mut data = fixture::tables(&[(0x121, 1, 11)], 0);
    data.extend(fixture::carrier::video(10000, &mut vc));
    data.extend(fixture::carrier::pes(
        0x121,
        100000,
        &fixture::tiny(1),
        &mut dc,
    ));
    data.extend(fixture::carrier::video(200000, &mut vc));
    data.extend(fixture::carrier::video(210000, &mut vc));
    state.push_source(&data, &mut transport);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !pidfile.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await
        }
    })
    .await
    .unwrap();
    let mut data = fixture::tables(&[(0x121, 1, 12)], 1);
    data.extend(fixture::carrier::video(220000, &mut vc));
    state.push_source(&data, &mut transport);
    let pid = std::fs::read_to_string(pidfile).unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while std::path::Path::new(&format!("/proc/{}", pid.trim())).exists() {
            tokio::time::sleep(Duration::from_millis(10)).await
        }
    })
    .await
    .unwrap();
    assert!(state.decoder.lock().unwrap().snapshot().is_empty());
    cancel.cancel();
    task.await.unwrap();
}
#[tokio::test]
async fn unannounced_dvb_page_reports_degradation_while_av_progresses() {
    let _serial = SERIAL.acquire().await.unwrap();
    let d = tempfile::tempdir().unwrap();
    let e = Engine::new(d.path(), "ffmpeg");
    let cfg = json!({"inputs":[{"url":"publish://"}],"flussonix_hls_captions":[{"dvb_page":777,"ocr_language":"eng","language":"en","name":"Missing page"}]});
    let mut p = e
        .publish_guarded("owned", &cfg, std::future::ready(true))
        .await
        .unwrap();
    p.stdin
        .as_mut()
        .unwrap()
        .write_all(&fixture::transport())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let mut ready = true;
            for prefix in ["", "fmp4/"] {
                ready &= e
                    .read("owned", &format!("{prefix}av.m3u8"))
                    .await
                    .is_ok_and(|b| String::from_utf8_lossy(&b).matches("#EXTINF:").count() >= 5);
            }
            if ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await
        }
    })
    .await
    .unwrap();
    let stats = p.worker.stats();
    assert_eq!(stats["hls_captions"]["status"], "degraded", "{stats}");
    assert_eq!(stats["hls_captions"]["last_error"], "dvb_page_unannounced");
    assert_eq!(stats["hls_captions"]["dvb_pages"][0]["announced"], false);
    e.stop_all().await;
}
