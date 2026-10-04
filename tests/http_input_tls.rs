//! Owned HTTPS input qualification; no vendor process, file or public service.
use axum::serve::ListenerExt;
use axum::{
    body::Body,
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::IntoResponse,
};
use flussonix::{
    http_tls,
    media::Engine,
    server::{App, Options, router},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::net::TcpListener;
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
#[path = "support/tls.rs"]
mod tls_fixture;
use tls_fixture::Certificates;
struct Source {
    app: Arc<App>,
    cert: Certificates,
    url: String,
    hits: Arc<AtomicUsize>,
    cancel: CancellationToken,
    task: AbortOnDropHandle<()>,
    _dir: tempfile::TempDir,
}
async fn source(expired: bool, redirect: Option<String>) -> Source {
    let dir = tempfile::tempdir().unwrap();
    let cert = Certificates::new();
    if expired {
        cert.expire()
    }
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-peer-secret".into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.config.put("streams","owned",json!({"static":false,"inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":"libx264","vb":400},"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-viewer"))})).unwrap();
    let tcp = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let port = tcp.local_addr().unwrap().port();
    let url = format!("https://127.0.0.1:{port}");
    let hits = Arc::new(AtomicUsize::new(0));
    let count = hits.clone();
    let routes = router(app.clone()).layer(middleware::from_fn(
        move |mut req: Request<Body>, next: Next| {
            let count = count.clone();
            let redirect = redirect.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                assert!(
                    !req.headers().contains_key("X-Flussonix-Peer"),
                    "external inputs sent a cluster credential"
                );
                assert!(
                    !req.headers().contains_key("Authorization"),
                    "external inputs sent a management credential"
                );
                if req.uri().path() == "/bad.m3u8" {
                    return (
                        [("Content-Type", "application/vnd.apple.mpegurl")],
                        redirect.unwrap(),
                    )
                        .into_response();
                }
                if req.uri().path() == "/entry" {
                    return (
                        StatusCode::FOUND,
                        [(
                            "Location",
                            redirect.unwrap_or("/owned/index.m3u8?token=owned-viewer".into()),
                        )],
                    )
                        .into_response();
                }
                if req.uri().path() == "/transport" {
                    let query = req.uri().query().unwrap_or("");
                    *req.uri_mut() = format!("/owned/mpegts?{query}").parse().unwrap();
                }
                next.run(req).await
            }
        },
    ));
    let cancel = CancellationToken::new();
    let stop = cancel.clone();
    let tls = http_tls::Listener::new(tcp, cert.server()).tap_io(|_| {});
    let task = AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(
            tls,
            routes.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(stop.cancelled_owned())
        .await
        .unwrap()
    }));
    Source {
        app,
        cert,
        url,
        hits,
        cancel,
        task,
        _dir: dir,
    }
}
impl Source {
    async fn stop(self) {
        self.app.media.stop_all().await;
        self.cancel.cancel();
        tokio::time::timeout(Duration::from_secs(6), self.task)
            .await
            .unwrap()
            .unwrap();
    }
}
fn config(src: &Source, scheme: &str, ca: Option<&std::path::Path>) -> Value {
    let resource = if scheme == "hlss" {
        "entry"
    } else {
        "transport?token=owned-viewer"
    };
    let mut cfg = json!({"inputs":[{"url":format!("{scheme}://{}/{resource}",src.url.strip_prefix("https://").unwrap())}]});
    if let Some(ca) = ca {
        cfg["inputs"][0]["flussonix_tls_ca"] = json!(ca)
    }
    cfg
}
async fn delivered(engine: &Engine, name: &str) -> Vec<u8> {
    tokio::time::timeout(Duration::from_secs(18), async {
        loop {
            if let Ok(list) = engine.read(name, "index.m3u8").await {
                if let Some(segment) = String::from_utf8_lossy(&list)
                    .lines()
                    .find(|s| !s.is_empty() && !s.starts_with('#'))
                {
                    if let Ok(data) = engine.read(name, segment).await {
                        return data.to_vec();
                    }
                }
            }
            let stats = engine.stats(name).await;
            assert_ne!(stats["status"], "failed", "{stats}");
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap_or_else(|err| panic!("{name}: {err}"))
}
async fn decode(bytes: &[u8]) {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("delivered.ts");
    std::fs::write(&file, bytes).unwrap();
    let output = tokio::process::Command::new("ffmpeg")
        .args(["-v", "error", "-i"])
        .arg(&file)
        .args(["-map", "0:v:0", "-map", "0:a:0", "-f", "null", "-"])
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
async fn pull(scheme: &str, cpu: bool, fmp4: bool) {
    let src = source(
        false,
        fmp4.then(|| "/owned/fmp4/index.m3u8?token=owned-viewer".into()),
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let mut cfg = config(&src, scheme, Some(&src.cert.ca));
    if cpu {
        cfg["transcoder"] = json!({"encoder":"libx264","vb":300})
    }
    engine.ensure("owned", &cfg).await.unwrap();
    let bytes = delivered(&engine, "owned").await;
    decode(&bytes).await;
    assert!(src.hits.load(Ordering::SeqCst) > 0);
    assert_eq!(engine.stats("owned").await["input_protocol"], scheme);
    assert_eq!(src.app.media.count().await, 1);
    engine.stop_all().await;
    src.stop().await;
}
#[tokio::test]
async fn private_ca_hlss_copy_delivers_decodable_audio_and_video() {
    pull("hlss", false, false).await
}
#[tokio::test]
async fn private_ca_hlss_cpu_delivers_decodable_audio_and_video() {
    pull("hlss", true, false).await
}
#[tokio::test]
async fn private_ca_tshttps_copy_accepts_an_arbitrary_transport_path() {
    pull("tshttps", false, false).await
}
#[tokio::test]
async fn private_ca_tshttps_cpu_delivers_decodable_audio_and_video() {
    pull("tshttps", true, false).await
}
async fn failed(engine: &Engine, name: &str) {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if !engine.stats(name).await["status"]
                .as_str()
                .is_some_and(|s| s == "running" || s == "starting")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await
        }
    })
    .await
    .expect("untrusted upstream must stop promptly");
    assert_eq!(engine.stats(name).await["bytes_in"], 0);
}
#[tokio::test]
async fn secure_inputs_reject_wrong_public_only_identity_and_expired_trust_before_http() {
    for scheme in ["hlss", "tshttps", "https"] {
        for case in 0..4 {
            let src = source(case == 3, None).await;
            let wrong = Certificates::new();
            let dir = tempfile::tempdir().unwrap();
            let engine = Engine::new(dir.path(), "ffmpeg");
            let ca = match case {
                0 => None,
                1 => Some(wrong.ca.as_path()),
                _ => Some(src.cert.ca.as_path()),
            };
            let mut cfg = config(&src, scheme, ca);
            if case == 2 {
                cfg["inputs"][0]["url"] = json!(
                    cfg["inputs"][0]["url"]
                        .as_str()
                        .unwrap()
                        .replace("127.0.0.1", "127.0.0.2")
                );
            }
            engine.ensure("owned", &cfg).await.unwrap();
            failed(&engine, "owned").await;
            assert_eq!(
                src.hits.load(Ordering::SeqCst),
                0,
                "{scheme} case {case} sent HTTP despite TLS rejection"
            );
            assert_eq!(src.app.media.count().await, 0);
            engine.stop_all().await;
            src.stop().await;
        }
    }
}
async fn foreign() -> (String, Arc<AtomicUsize>, AbortOnDropHandle<()>) {
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let hits = count.clone();
    let task = AbortOnDropHandle::new(tokio::spawn(async move {
        loop {
            let (socket, _) = tcp.accept().await.unwrap();
            hits.fetch_add(1, Ordering::SeqCst);
            drop(socket)
        }
    }));
    (format!("{address}"), count, task)
}
#[tokio::test]
async fn secure_input_redirects_never_contact_foreign_or_plaintext_endpoints() {
    for scheme in ["http", "https"] {
        let (endpoint, hits, task) = foreign().await;
        let src = source(false, Some(format!("{scheme}://{endpoint}/foreign"))).await;
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "ffmpeg");
        let cfg = config(&src, "hlss", Some(&src.cert.ca));
        engine.ensure("owned", &cfg).await.unwrap();
        failed(&engine, "owned").await;
        assert!(src.hits.load(Ordering::SeqCst) > 0);
        assert_eq!(hits.load(Ordering::SeqCst), 0);
        engine.stop_all().await;
        src.stop().await;
        task.abort();
        let _ = task.await;
    }
}
#[tokio::test]
async fn untrusted_https_input_recovers_to_the_next_input_without_insecure_retry() {
    let src = source(false, None).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let mut cfg = config(&src, "tshttps", None);
    cfg["inputs"]
        .as_array_mut()
        .unwrap()
        .push(json!({"url":"testsrc://"}));
    engine.ensure("owned", &cfg).await.unwrap();
    failed(&engine, "owned").await;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if engine.recover("owned", &cfg).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await
        }
    })
    .await
    .unwrap();
    let bytes = delivered(&engine, "owned").await;
    decode(&bytes).await;
    assert_eq!(engine.stats("owned").await["input_index"], 1);
    assert_eq!(src.hits.load(Ordering::SeqCst), 0);
    engine.stop_all().await;
    src.stop().await;
}

#[tokio::test]
async fn private_ca_hlss_fetches_fmp4_init_and_segments_without_peer_headers() {
    pull("hlss", false, true).await
}
#[tokio::test]
async fn external_hls_rejects_foreign_variant_segment_key_and_map_uris() {
    for tag in ["variant", "segment", "key", "map"] {
        let (endpoint, hits, task) = foreign().await;
        let remote = format!("https://{endpoint}/foreign");
        let line = match tag {
            "variant" => format!("#EXT-X-STREAM-INF:BANDWIDTH=100000\n{remote}\n"),
            "segment" => format!("#EXT-X-TARGETDURATION:2\n#EXTINF:2,\n{remote}\n"),
            "key" => format!(
                "#EXT-X-TARGETDURATION:2\n#EXT-X-KEY:METHOD=AES-128,URI=\"{remote}\"\n#EXTINF:2,\nsegment.ts\n"
            ),
            "map" => format!(
                "#EXT-X-TARGETDURATION:2\n#EXT-X-MAP:URI=\"{remote}\"\n#EXTINF:2,\nsegment.m4s\n"
            ),
            _ => unreachable!(),
        };
        let src = source(false, Some(format!("#EXTM3U\n{line}"))).await;
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "ffmpeg");
        let mut cfg = config(&src, "hlss", Some(&src.cert.ca));
        cfg["inputs"][0]["url"] = json!(format!(
            "{}/bad.m3u8",
            src.url.replacen("https://", "hlss://", 1)
        ));
        engine.ensure("owned", &cfg).await.unwrap();
        failed(&engine, "owned").await;
        assert!(src.hits.load(Ordering::SeqCst) > 0);
        assert_eq!(
            hits.load(Ordering::SeqCst),
            0,
            "foreign {tag} reached a socket"
        );
        engine.stop_all().await;
        src.stop().await;
        task.abort();
        let _ = task.await;
    }
}
#[tokio::test]
async fn same_origin_redirect_cycles_stop_after_three_requests() {
    let src = source(false, Some("/entry".into())).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    engine
        .ensure("owned", &config(&src, "hlss", Some(&src.cert.ca)))
        .await
        .unwrap();
    failed(&engine, "owned").await;
    assert_eq!(src.hits.load(Ordering::SeqCst), 3);
    engine.stop_all().await;
    src.stop().await;
}

async fn raw_https_pull(hls: bool) {
    let src = source(false, None).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "ffmpeg");
    let path = if hls {
        "/owned/index.m3u8?token=owned-viewer"
    } else {
        "/transport?token=owned-viewer"
    };
    let cfg =
        json!({"inputs":[{"url":format!("{}{path}",src.url),"flussonix_tls_ca":src.cert.ca}]});
    engine.ensure("owned", &cfg).await.unwrap();
    let bytes = delivered(&engine, "owned").await;
    decode(&bytes).await;
    assert!(src.hits.load(Ordering::SeqCst) > 0);
    engine.stop_all().await;
    src.stop().await;
}
#[tokio::test]
async fn raw_https_playlist_urls_use_verified_trust() {
    raw_https_pull(true).await
}
#[tokio::test]
async fn raw_https_transport_urls_use_verified_trust() {
    raw_https_pull(false).await
}
