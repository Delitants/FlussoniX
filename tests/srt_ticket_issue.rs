//! The ticket issuer is exercised through the public router with owned state.
use axum::{
    body::{Body, to_bytes},
    extract::ConnectInfo,
    http::{Method, Request, StatusCode},
};
use flussonix::{
    server::{App, Options, router},
    srt_playback::{Listener, Settings},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{net::SocketAddr, sync::Arc};
use tower::ServiceExt;
#[path = "support/tls.rs"]
mod tls_fixture;

struct Lab {
    _dir: tempfile::TempDir,
    app: Arc<App>,
    _listener: Listener,
}
impl Lab {
    fn new(encrypted: bool, public_url: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            Options {
                admin_password: "owned-admin-secret".into(),
                peer_key: "owned-cluster-secret".into(),
                srt_play_public_url: public_url
                    .then(|| url::Url::parse("srt://127.0.0.1:9000").unwrap()),
                ..Options::default()
            },
        )
        .unwrap();
        app.config
            .put("streams", "owned", json!({"inputs":[{"url":"testsrc://"}]}))
            .unwrap();
        let listener = Listener::bind(
            "127.0.0.1:0".parse().unwrap(),
            Settings::new(
                120,
                8,
                if encrypted {
                    "owned-test-passphrase"
                } else {
                    ""
                }
                .into(),
            )
            .unwrap(),
        )
        .unwrap();
        app.set_srt_playback(Some(&listener));
        app.sample_metrics();
        Self {
            _dir: dir,
            app,
            _listener: listener,
        }
    }
    async fn request(
        &self,
        body: &str,
        peer: Option<SocketAddr>,
        uri: &str,
    ) -> (StatusCode, Value, String) {
        let mut request = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header("content-type", "application/json")
            .header("x-forwarded-for", "203.0.113.71")
            .body(Body::from(body.to_owned()))
            .unwrap();
        if let Some(peer) = peer {
            request.extensions_mut().insert(ConnectInfo(peer));
        }
        let response = router(self.app.clone()).oneshot(request).await.unwrap();
        let status = response.status();
        let cache = response
            .headers()
            .get("cache-control")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned();
        let bytes = to_bytes(response.into_body(), 8192).await.unwrap();
        let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, value, cache)
    }
}
const BODY: &str =
    r##"{"streamid":"#!::r=owned,m=request,u=owned-secret","encryption":"required"}"##;
fn loopback() -> SocketAddr {
    "127.0.0.1:40111".parse().unwrap()
}

#[tokio::test]
async fn loopback_issue_returns_only_tokenless_five_second_ticket() {
    let lab = Lab::new(true, true);
    let (status, value, cache) = lab
        .request(BODY, Some(loopback()), "/flussonix/api/v1/srt-ticket")
        .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(cache, "private, no-store");
    assert_eq!(value["address"], "srt://127.0.0.1:9000");
    assert_eq!(value["encryption_required"], true);
    assert_eq!(value["expires_in"], 5);
    let id = value["streamid"].as_str().unwrap();
    assert!(id.starts_with("#!::r=owned,m=request,flussonix_ticket="));
    assert!(!id.contains("owned-secret"));
    assert!(!value.to_string().contains("owned-secret"));
}

#[tokio::test]
async fn request_is_strict_bounded_and_never_echoes_credentials() {
    let lab = Lab::new(true, true);
    let invalid = [
        r##"{"streamid":"#!::r=owned,m=request,u=owned-secret","encryption":"any"}"##.to_owned(),
        r##"{"streamid":"#!::r=owned,m=request,u=owned-secret","unknown":1}"##.to_owned(),
        r##"{"streamid":"#!::r=owned,m=request,u=owned-secret","streamid":"#!::r=owned"}"##
            .to_owned(),
        r##"{"streamid":"#!::r=owned,m=publish,u=owned-secret"}"##.to_owned(),
        "{".to_owned(),
        format!("{{\"streamid\":\"{}\"}}", "x".repeat(4100)),
    ];
    for body in invalid {
        let (status, value, cache) = lab
            .request(&body, Some(loopback()), "/flussonix/api/v1/srt-ticket")
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}: {value}");
        assert_eq!(cache, "private, no-store");
        assert!(!value.to_string().contains("owned-secret"));
    }
}

#[tokio::test]
async fn issuer_requires_actual_peer_and_encrypted_public_listener() {
    let lab = Lab::new(true, true);
    let (status, _, cache) = lab
        .request(BODY, None, "/flussonix/api/v1/srt-ticket")
        .await;
    assert_eq!(status, StatusCode::UPGRADE_REQUIRED);
    assert_eq!(cache, "private, no-store");
    let remote: SocketAddr = "198.51.100.7:41111".parse().unwrap();
    let (status, _, _) = lab
        .request(BODY, Some(remote), "/flussonix/api/v1/srt-ticket")
        .await;
    assert_eq!(status, StatusCode::UPGRADE_REQUIRED);
    let (status, _, _) = lab
        .request(
            BODY,
            Some(loopback()),
            "/flussonix/api/v1/srt-ticket?token=owned-secret",
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    for lab in [Lab::new(false, true), Lab::new(true, false)] {
        let (status, _, cache) = lab
            .request(BODY, Some(loopback()), "/flussonix/api/v1/srt-ticket")
            .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(cache, "private, no-store");
    }
}

#[tokio::test]
async fn owned_https_listener_issues_without_exposing_the_original_token() {
    let lab = Lab::new(true, true);
    let certs = tls_fixture::Certificates::new();
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    let cancel = tokio_util::sync::CancellationToken::new();
    let app = lab.app.clone();
    let stop = cancel.clone();
    let tls = certs.server();
    let task = tokio::spawn(async move { flussonix::http_tls::serve(tcp, tls, app, stop).await });
    let client = reqwest::Client::builder()
        .no_proxy()
        .add_root_certificate(
            reqwest::Certificate::from_pem(&std::fs::read(&certs.ca).unwrap()).unwrap(),
        )
        .build()
        .unwrap();
    let response = client
        .post(format!(
            "https://localhost:{}/flussonix/api/v1/srt-ticket",
            address.port()
        ))
        .header("content-type", "application/json")
        .body(BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "private, no-store");
    let text = response.text().await.unwrap();
    assert!(!text.contains("owned-secret"));
    assert!(text.contains("flussonix_ticket="));
    cancel.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn failed_viewer_authorization_leaves_no_public_ticket() {
    let lab = Lab::new(true, true);
    lab.app
        .config
        .put(
            "streams",
            "owned",
            json!({
                "inputs":[{"url":"testsrc://"}],
                "flussonix_token_sha256":format!("{:x}",Sha256::digest(b"different-token"))
            }),
        )
        .unwrap();
    let (status, value, cache) = lab
        .request(BODY, Some(loopback()), "/flussonix/api/v1/srt-ticket")
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(cache, "private, no-store");
    assert!(!value.to_string().contains("owned-secret"));
    assert!(!value.to_string().contains("flussonix_ticket"));
}
