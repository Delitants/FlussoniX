use super::{Vendor, app, request, tls_fixture};
use flussonix::{http_tls, server::App};
use serde_json::{Value, json};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
const PATH: &str = "/flussonix/api/v1/srt-lookup";
struct Https {
    url: String,
    client: reqwest::Client,
    stop: CancellationToken,
    task: AbortOnDropHandle<()>,
    _cert: tls_fixture::Certificates,
}
impl Https {
    async fn new(app: Arc<App>) -> Self {
        let cert = tls_fixture::Certificates::new();
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("https://{}", tcp.local_addr().unwrap());
        let stop = CancellationToken::new();
        let cancel = stop.clone();
        let tls = cert.server();
        let task = AbortOnDropHandle::new(tokio::spawn(async move {
            http_tls::serve(tcp, tls, app, cancel).await.unwrap()
        }));
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .use_preconfigured_tls((*cert.client()).clone())
            .build()
            .unwrap();
        Self {
            url,
            client,
            stop,
            task,
            _cert: cert,
        }
    }
    async fn lookup(&self, id: &str, encryption: Option<&str>) -> reqwest::Response {
        let mut req = self
            .client
            .get(format!("{}{PATH}", self.url))
            .query(&[("streamid", id)]);
        if let Some(value) = encryption {
            req = req.query(&[("encryption", value)])
        }
        req.send().await.unwrap()
    }
    async fn close(self) {
        self.stop.cancel();
        tokio::time::timeout(Duration::from_secs(6), self.task)
            .await
            .unwrap()
            .unwrap();
    }
}
fn peer(v: &Vendor) -> Value {
    let mut p = v.peer();
    p["flussonix_srt_url"] = json!("srt://owned-edge.invalid:9998");
    p["flussonix_srt_encrypted"] = json!(true);
    p
}
#[tokio::test]
async fn https_lookup_returns_mapped_connection_fields_with_opaque_viewer_token() {
    let v = Vendor::new().await;
    let (_dir, a) = app();
    a.config.put("peers", "edge", peer(&v)).unwrap();
    let https = Https::new(a.clone()).await;
    let r = https
        .lookup("#!::u=one%2B+é,r=local/news,a=owned", None)
        .await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["cache-control"], "no-store");
    let result: Value = r.json().await.unwrap();
    assert_eq!(
        result,
        json!({"address":"srt://owned-edge.invalid:9998","streamid":"#!::u=one%2B+é,r=remote/news,a=owned,m=request","encryption_required":true,"expires_in":5})
    );
    assert!(!result.to_string().contains("owned-vendor-cluster-key"));
    assert_eq!(a.media.count().await, 0);
    assert_eq!(a.playback_auth.live_grants(), 0);
    assert_eq!(v.state.calls.load(Ordering::SeqCst), 3);
    https.close().await;
}
#[tokio::test]
async fn srt_lookup_requires_real_https_and_srt_viewer_policy_not_admin_credentials() {
    use sha2::{Digest, Sha256};
    let v = Vendor::new().await;
    let (_dir, a) = app();
    a.config.put("peers", "edge", peer(&v)).unwrap();
    a.config
        .put(
            "streams",
            "local/news",
            json!({"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"viewer"))}),
        )
        .unwrap();
    assert_eq!(
        request(&a, &format!("{PATH}?streamid=%23!::r=local/news,u=viewer"))
            .await
            .status(),
        426
    );
    use tower::ServiceExt;
    let forged = flussonix::server::router(a.clone())
        .oneshot(
            axum::http::Request::builder()
                .uri(format!("{PATH}?streamid=%23!::r=local/news,u=viewer"))
                .header("x-forwarded-proto", "https")
                .header("forwarded", "proto=https")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(forged.status(), 426);
    let https = Https::new(a.clone()).await;
    let r = https
        .client
        .get(format!("{}{PATH}", https.url))
        .basic_auth("admin", Some("owned-admin"))
        .query(&[("streamid", "#!::r=local/news,u=wrong")])
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    let r = https
        .client
        .get(format!("{}{PATH}", https.url))
        .header("x-flussonix-peer", "owned-native-peer-key")
        .query(&[("streamid", "#!::r=local/news,u=wrong")])
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    assert_eq!(v.state.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        https
            .lookup("#!::r=local/news,u=viewer", None)
            .await
            .status(),
        200
    );
    https.close().await;
}

#[tokio::test]
async fn lookup_rejects_ambiguous_queries_without_probing_destinations() {
    let v = Vendor::new().await;
    let (_d, a) = app();
    a.config.put("peers", "edge", peer(&v)).unwrap();
    let https = Https::new(a.clone()).await;
    for qs in [
        "",
        "streamid=%FF",
        "streamid=%",
        "streamid=%23!::r=local/news&streamid=x",
        "streamid=%23!::r=local/news&%73treamid=x",
        "streamid=%23!::r=local/news&encryption=bad",
        "streamid=%23!::r=local/news&unknown=x",
        "streamid=%23!::r=local/news&encryption=any&encryption=required",
        "streamid=%23!::r=local/news,m=publish",
        "streamid=%23!::r=local/news,r=other",
    ] {
        assert_eq!(
            https
                .client
                .get(format!("{}{PATH}?{qs}", https.url))
                .send()
                .await
                .unwrap()
                .status(),
            400,
            "{qs}"
        );
    }
    for method in [reqwest::Method::POST, reqwest::Method::HEAD] {
        assert_eq!(
            https
                .client
                .request(method, format!("{}{PATH}", https.url))
                .send()
                .await
                .unwrap()
                .status(),
            405
        );
    }
    assert_eq!(v.state.calls.load(Ordering::SeqCst), 0);
    https.close().await;
}
#[tokio::test]
async fn lookup_defaults_to_encryption_and_filters_unsupported_peers_before_io() {
    let v = Vendor::new().await;
    let (_d, a) = app();
    let mut p = peer(&v);
    p["flussonix_srt_encrypted"] = json!(false);
    a.config.put("peers", "edge", p).unwrap();
    let https = Https::new(a.clone()).await;
    assert_eq!(https.lookup("#!::r=local/news", None).await.status(), 503);
    assert_eq!(v.state.calls.load(Ordering::SeqCst), 0);
    let r = https.lookup("#!::r=local/news", Some("any")).await;
    assert_eq!(r.status(), 200);
    assert_eq!(
        r.json::<Value>().await.unwrap()["encryption_required"],
        false
    );
    https.close().await;
}
#[tokio::test]
async fn srt_peer_requires_paired_vendor_fields_safe_explicit_port_and_representable_mapping() {
    let v = Vendor::new().await;
    let (_d, a) = app();
    for raw in [
        "srt://host",
        "srt://host:0",
        "srt://host:9998/path",
        "srt://user@host:9998",
        "srt://@host:9998",
        "srt://host:9998?passphrase=secret",
        "srt://host:9998#fragment",
        "srt://host:9998\\path",
        "srt://host:9998\n",
        "https://host:9998",
    ] {
        let mut p = peer(&v);
        p["flussonix_srt_url"] = json!(raw);
        assert!(a.config.put("peers", "edge", p).is_err(), "{raw}");
    }
    let mut p = peer(&v);
    p.as_object_mut().unwrap().remove("flussonix_srt_encrypted");
    assert!(a.config.put("peers", "edge", p).is_err());
    let mut p = peer(&v);
    p["flussonix_peer_kind"] = json!("native");
    p.as_object_mut().unwrap().remove("flussonix_stream_map");
    assert!(a.config.put("peers", "edge", p).is_err());
    let mut p = peer(&v);
    p["flussonix_stream_map"][0]["remote_name"] = json!("remote,news");
    assert!(a.config.put("peers", "edge", p).is_err());
}

#[tokio::test]
async fn srt_and_http_concurrent_placements_share_one_vendor_egress_hold() {
    let v = Vendor::new().await;
    v.state.config.lock().unwrap()["stats"]["output_kbit"] = json!(82000);
    let (_d, a) = app();
    a.config.put("peers", "edge", peer(&v)).unwrap();
    let https = Https::new(a.clone()).await;
    let mut tasks = vec![];
    for i in 0..12 {
        let a = a.clone();
        let client = https.client.clone();
        let url = https.url.clone();
        tasks.push(tokio::spawn(async move {
            if i % 2 == 0 {
                client
                    .get(format!("{url}{PATH}"))
                    .query(&[("streamid", "#!::r=local/news,u=viewer")])
                    .send()
                    .await
                    .unwrap()
                    .status()
                    .as_u16()
            } else {
                request(&a, "/local/news/m4s?token=viewer")
                    .await
                    .status()
                    .as_u16()
            }
        }));
    }
    let mut successes = 0;
    for task in tasks {
        match task.await.unwrap() {
            200 | 302 => successes += 1,
            503 => {}
            status => panic!("Unexpected {status}"),
        }
    }
    assert_eq!(successes, 1);
    assert_eq!(a.media.count().await, 0);
    https.close().await;
}
#[tokio::test]
async fn srt_lookup_rejects_changed_policy_while_vendor_probe_is_in_flight() {
    let v = Vendor::new().await;
    let (_d, a) = app();
    a.config.put("peers", "edge", peer(&v)).unwrap();
    let (entered, wait) = tokio::sync::oneshot::channel();
    let (release, gate) = tokio::sync::oneshot::channel();
    *v.state.hold.lock().unwrap() = Some((entered, gate));
    let https = Https::new(a.clone()).await;
    let client = https.client.clone();
    let url = https.url.clone();
    let task = tokio::spawn(async move {
        client
            .get(format!("{url}{PATH}"))
            .query(&[("streamid", "#!::r=local/news")])
            .send()
            .await
            .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(3), wait)
        .await
        .unwrap()
        .unwrap();
    a.config
        .put("streams", "local/news", json!({"disabled":true}))
        .unwrap();
    release.send(()).ok();
    let r = task.await.unwrap();
    assert!(matches!(r.status().as_u16(), 403 | 503));
    assert_eq!(a.playback_auth.active(), 0);
    assert_eq!(a.media.count().await, 0);
    https.close().await;
}
#[tokio::test]
async fn srt_lookup_preserves_exact_512_byte_id_and_filters_expansion_before_io() {
    let v = Vendor::new().await;
    let (_d, a) = app();
    a.config.put("peers", "edge", peer(&v)).unwrap();
    let https = Https::new(a.clone()).await;
    let prefix = "#!::r=local/news,m=request,u=";
    let id = format!("{prefix}{}", "x".repeat(512 - prefix.len() - 1)); // remote is one byte longer
    assert_eq!(id.len(), 511);
    let r = https.lookup(&id, None).await;
    assert_eq!(r.status(), 200);
    assert_eq!(
        r.json::<Value>().await.unwrap()["streamid"]
            .as_str()
            .unwrap()
            .len(),
        512
    );
    assert_eq!(https.lookup(&(id + "x"), None).await.status(), 503);
    assert_eq!(v.state.calls.load(Ordering::SeqCst), 3);
    https.close().await;
}
#[tokio::test]
async fn srt_lookup_callback_uses_srt_and_original_streamid_only_and_rejects_redirect() {
    use axum::{Router, extract::OriginalUri, routing::get};
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let requests = seen.clone();
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", tcp.local_addr().unwrap());
    let callback = AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(
            tcp,
            Router::new().fallback(get(move |OriginalUri(uri): OriginalUri| {
                let seen = requests.clone();
                async move {
                    seen.lock().unwrap().push(uri.query().unwrap().to_owned());
                    (
                        axum::http::StatusCode::FOUND,
                        [("location", "https://unrelated.invalid")],
                    )
                }
            })),
        )
        .await
        .unwrap()
    }));
    let v = Vendor::new().await;
    let (_d, a) = app();
    a.config.put("peers", "edge", peer(&v)).unwrap();
    a.config
        .put("streams", "local/news", json!({"on_play":url}))
        .unwrap();
    let https = Https::new(a.clone()).await;
    assert_eq!(
        https
            .lookup("#!::r=local/news,u=one%2B+é,s=claim", Some("any"))
            .await
            .status(),
        403
    );
    let rows = seen.lock().unwrap().clone();
    assert_eq!(rows.len(), 1);
    let q: std::collections::HashMap<String, String> =
        url::form_urlencoded::parse(rows[0].as_bytes())
            .into_owned()
            .collect();
    assert_eq!(q.get("proto").map(String::as_str), Some("srt"));
    assert_eq!(q.get("token").map(String::as_str), Some("one%2B+é"));
    let original: std::collections::HashMap<String, String> =
        url::form_urlencoded::parse(q["qs"].as_bytes())
            .into_owned()
            .collect();
    assert_eq!(original.len(), 1);
    assert_eq!(original["streamid"], "#!::r=local/news,u=one%2B+é,s=claim");
    assert_eq!(v.state.calls.load(Ordering::SeqCst), 0);
    https.close().await;
    callback.abort();
    let _ = callback.await;
}

#[tokio::test]
async fn mapped_srt_lookup_delivers_encrypted_av_and_destination_enforces_its_own_token() {
    use flussonix::{
        server::Options,
        srt_playback::{self, Listener, Settings},
    };
    use sha2::{Digest, Sha256};
    use std::process::Stdio;
    use tokio::process::Command;
    let v = Vendor::new().await;
    let (_d, a) = app();
    let cdn_dir = tempfile::tempdir().unwrap();
    let cdn = App::new(
        cdn_dir.path().join("config.json"),
        cdn_dir.path().join("media"),
        Options {
            role: "cdn".into(),
            peer_key: "owned-cdn-peer-key".into(),
            admin_password: "owned-admin".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let token = "opaque%2B+é";
    cdn.config.put("streams","remote/news",json!({"static":false,"inputs":[{"url":"testsrc://"}],"flussonix_token_sha256":format!("{:x}",Sha256::digest(token.as_bytes()))})).unwrap();
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        Settings::new(120, 4, "owned-srt-passphrase".into()).unwrap(),
    )
    .unwrap();
    let address = format!("srt://{}", listener.address());
    let stop = CancellationToken::new();
    let server = AbortOnDropHandle::new(tokio::spawn(srt_playback::serve(
        listener,
        cdn.clone(),
        stop.clone(),
    )));
    let mut p = peer(&v);
    p["flussonix_srt_url"] = json!(address);
    a.config.put("peers", "edge", p).unwrap();
    let https = Https::new(a.clone()).await;
    use futures_util::FutureExt;
    let outcome = std::panic::AssertUnwindSafe(async {
        for (index, viewer, pass, success) in [
            (0, "denied", "owned-srt-passphrase", false),
            (1, token, "owned-srt-passphrase", true),
            (2, token, "wrong-srt-passphrase", false),
        ] {
            let r = https
                .lookup(&format!("#!::r=local/news,u={viewer}"), None)
                .await;
            assert_eq!(r.status(), 200);
            let result: Value = r.json().await.unwrap();
            assert!(!result.to_string().contains("owned-srt-passphrase"));
            let capture = cdn_dir.path().join(format!("received-{index}.ts"));
            let mut child = Command::new("ffmpeg")
                .args([
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-nostdin",
                    "-y",
                    "-threads",
                    "1",
                    "-analyzeduration",
                    "1000000",
                    "-probesize",
                    "524288",
                    "-srt_streamid",
                    result["streamid"].as_str().unwrap(),
                    "-passphrase",
                    pass,
                    "-i",
                    &format!(
                        "{}?mode=caller&latency=120000&connect_timeout=1000&timeout=3000000",
                        result["address"].as_str().unwrap()
                    ),
                    "-t",
                    "1",
                    "-map",
                    "0",
                    "-c",
                    "copy",
                    "-f",
                    "mpegts",
                ])
                .arg(&capture)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true)
                .spawn()
                .unwrap();
            let finished = tokio::time::timeout(Duration::from_secs(16), child.wait()).await;
            let status = match finished {
                Ok(status) => Some(status.unwrap()),
                Err(_) => {
                    child.kill().await.unwrap();
                    let _ = child.wait().await;
                    None
                }
            };
            if success {
                assert!(
                    status.is_some_and(|s| s.success()),
                    "owned direct caller failed"
                );
                let mut decoder = Command::new("ffmpeg")
                    .args(["-v", "error", "-xerror", "-threads", "1", "-i"])
                    .arg(&capture)
                    .args(["-map", "0:v:0", "-map", "0:a:0", "-f", "null", "-"])
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .kill_on_drop(true)
                    .spawn()
                    .unwrap();
                let result = tokio::time::timeout(Duration::from_secs(8), decoder.wait()).await;
                if result.is_err() {
                    decoder.kill().await.unwrap();
                    let _ = decoder.wait().await;
                }
                assert!(
                    result.unwrap().unwrap().success(),
                    "owned video and audio decode failed"
                );
            } else {
                assert!(!status.is_some_and(|s| s.success()));
                assert_eq!(std::fs::metadata(&capture).map(|m| m.len()).unwrap_or(0), 0);
                if index == 0 {
                    assert_eq!(cdn.media.count().await, 0);
                    assert_eq!(cdn.srt_egress.load(Ordering::Relaxed), 0);
                }
            }
        }
        assert_eq!(a.media.count().await, 0);
        assert!(cdn.srt_egress.load(Ordering::Relaxed) > 0);
    })
    .catch_unwind()
    .await;
    https.close().await;
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(6), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    cdn.media.stop_all().await;
    if let Err(error) = outcome {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
async fn lookup_excludes_unmapped_and_identity_changed_destinations_and_accepts_configured_idle() {
    let v = Vendor::new().await;
    let (_d, a) = app();
    a.config.put("peers", "edge", v.peer()).unwrap();
    let https = Https::new(a.clone()).await;
    assert_eq!(https.lookup("#!::r=local/news", None).await.status(), 503);
    assert_eq!(v.state.calls.load(Ordering::SeqCst), 0);
    let mut p = peer(&v);
    p["flussonix_stream_map"][0]["name"] = json!("unmapped");
    a.config.put("peers", "edge", p).unwrap();
    assert_eq!(https.lookup("#!::r=local/news", None).await.status(), 503);
    assert_eq!(v.state.calls.load(Ordering::SeqCst), 0);
    a.config.put("peers", "edge", peer(&v)).unwrap();
    v.state.stream.lock().unwrap()["stats"]["running"] = json!(false);
    assert_eq!(https.lookup("#!::r=local/news", None).await.status(), 200);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    v.state.config.lock().unwrap()["stats"]["id"] = json!("changed-server");
    assert_eq!(https.lookup("#!::r=local/news", None).await.status(), 503);
    assert_eq!(a.media.count().await, 0);
    assert_eq!(a.playback_auth.live_grants(), 0);
    https.close().await;
}
#[tokio::test]
async fn lookup_rejects_non_lb_and_draining_roles_before_vendor_io() {
    use flussonix::server::Options;
    let v = Vendor::new().await;
    for (role, drain) in [("cdn", false), ("standalone", false), ("lb", true)] {
        let dir = tempfile::tempdir().unwrap();
        let a = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            Options {
                role: role.into(),
                drain,
                admin_password: "owned-admin".into(),
                peer_key: "owned-peer-key".into(),
                ..Default::default()
            },
        )
        .unwrap();
        a.config.put("peers", "edge", peer(&v)).unwrap();
        a.config.put("streams", "local/news", json!({})).unwrap();
        let https = Https::new(a.clone()).await;
        assert_eq!(https.lookup("#!::r=local/news", None).await.status(), 503);
        assert_eq!(a.media.count().await, 0);
        https.close().await;
    }
    assert_eq!(v.state.calls.load(Ordering::SeqCst), 0);
}
