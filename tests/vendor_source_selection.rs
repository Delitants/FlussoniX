//! Persisted vendor intent never enters native discovery or inherits peer authority.
use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Request, StatusCode},
    routing::get,
};
use flussonix::{
    config::ConfigStore,
    server::{App, Options, router},
    source_directory::{LookupFailure, modern_coordinator::ModernSourceCoordinator, query},
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tower::ServiceExt;
const KEY: &str = "m4s://owned.invalid:8080";
fn source() -> Value {
    json!({"url":KEY,"flussonix_source_kind":"flussonic","cluster_key":"owned-vendor-private-key","on_play":"true"})
}
fn store() -> (tempfile::TempDir, Arc<ConfigStore>) {
    let d = tempfile::tempdir().unwrap();
    let s = Arc::new(ConfigStore::open(d.path().join("config.json")).unwrap());
    (d, s)
}
#[test]
fn vendor_type_and_local_source_and_group_policies_persist_and_prepare() {
    let (d, s) = store();
    s.put(
        "auth_backends",
        "billing",
        json!({"url":"https://billing.invalid/check"}),
    )
    .unwrap();
    let mut row = source();
    row["on_play"] = json!({"url":"auth://billing","max_sessions":2});
    row["flussonix_token_sha256"] = json!("a".repeat(64));
    row["group_config"] = json!({"region/eu":{"name":"region/eu","on_play":"true"},"protected":{"flussonix_token_sha256":"b".repeat(64)}});
    let saved = s
        .put("sources", KEY, row)
        .expect("explicit vendor source must save");
    assert_eq!(saved["api_url"], "http://owned.invalid:8080/");
    assert_eq!(saved["flussonix_transport"], "m4s");
    assert_eq!(saved["group_config"]["region/eu"]["name"], "region/eu");
    assert_eq!(
        ConfigStore::open(d.path().join("config.json"))
            .unwrap()
            .snapshot(),
        s.snapshot()
    );
    let c = ModernSourceCoordinator::new(s, "owned-local").unwrap();
    c.install(
        c.capture(KEY)
            .unwrap()
            .prepare(Duration::from_secs(3))
            .expect("saved strict profile must prepare without manual policy injection"),
    )
    .unwrap();
}
#[test]
fn missing_viewer_policy_is_not_implicitly_public_or_replaced_by_peer_key() {
    let (_d, s) = store();
    let mut row = source();
    row.as_object_mut().unwrap().remove("on_play");
    s.put("sources", KEY, row)
        .expect("missing policy stays unresolved at admission");
    let c = ModernSourceCoordinator::new(s, "owned-local").unwrap();
    c.install(
        c.capture(KEY)
            .unwrap()
            .prepare(Duration::from_secs(3))
            .unwrap(),
    )
    .unwrap();
}
#[test]
fn invalid_vendor_profile_rejects_every_transaction_without_writing_or_secret_errors() {
    let (d, s) = store();
    s.put("sources", KEY, source()).unwrap();
    let saved = s.snapshot();
    let revision = s.revision();
    let disk = std::fs::read(d.path().join("config.json")).unwrap();
    for patch in [
        json!({"flussonix_source_kind":"other"}),
        json!({"flussonix_source_kind":true}),
        json!({"cluster_key":null}),
        json!({"cluster_key":"x".repeat(4097)}),
        json!({"drain":false}),
        json!({"public_payload_url":"https://unused.invalid"}),
        json!({"flussonix_source_group":"native-failover"}),
        json!({"flussonix_transport":"m4f"}),
        json!({"on_play":false}),
        json!({"on_play":"auth://missing"}),
        json!({"flussonix_token_sha256":"credential-not-a-hash"}),
        json!({"group_config":[]}),
        json!({"group_config":{"unused":{"on_play":true}}}),
        json!({"group_config":{"unused":{"name":"other","on_play":"true"}}}),
        json!({"group_config":{"unused":{"template":"ignored"}}}),
        json!({"group_config":{"unused":{"drain":true}}}),
        json!({"group_config":{"../bad":{"on_play":"true"}}}),
        json!({"api_url":"http://user:private-secret@owned.invalid"}),
        json!({"private_payload_url":"http://owned.invalid/media?token=private-secret"}),
    ] {
        let error = s
            .put("sources", KEY, patch.clone())
            .expect_err("invalid vendor patch accepted");
        assert!(
            !error.contains("private-secret") && !error.contains("credential-not-a-hash"),
            "secret diagnostic"
        );
        let row = flussonix::config::merge(&saved["sources"][0], &patch);
        assert!(s.validate(json!({"sources":[row.clone()]})).is_err());
        assert!(s.replace(json!({"sources":[row]})).is_err());
        assert_eq!(s.revision(), revision);
        assert_eq!(s.snapshot(), saved);
        assert_eq!(std::fs::read(d.path().join("config.json")).unwrap(), disk);
    }
}
#[test]
fn vendor_fields_are_source_only_and_never_enable_native_or_peer_policies() {
    let (_d, s) = store();
    for row in [
        json!({"flussonix_source_kind":"flussonic","cluster_key":"owned-vendor-private-key","on_play":"true"}),
        json!({"on_play":"true"}),
        json!({"flussonix_token_sha256":"a".repeat(64)}),
        json!({"group_config":{}}),
    ] {
        assert!(
            s.put(
                "sources",
                "named-native",
                flussonix::config::merge(&json!({"api_url":"http://native.invalid"}), &row)
            )
            .is_err()
        );
    }
    for kind in ["peers", "streams", "templates", "auth_backends"] {
        assert!(
            s.put(
                kind,
                "owned",
                json!({"api_url":"http://native.invalid","flussonix_source_kind":"native"})
            )
            .is_err()
        );
    }
    s.put("sources", KEY, source()).unwrap();
    assert!(
        s.put("sources", KEY, json!({"flussonix_source_kind":"native"}))
            .is_err()
    );
    assert!(
        s.put("sources", KEY, json!({"flussonix_source_kind":null}))
            .is_err()
    );
    let row = s
        .put(
            "sources",
            KEY,
            json!({"flussonix_source_kind":"native","on_play":null}),
        )
        .unwrap();
    assert_eq!(row["flussonix_source_kind"], "native");
    assert!(row.get("on_play").is_none());
    let coordinator = ModernSourceCoordinator::new(s.clone(), "owned-local").unwrap();
    assert_eq!(
        coordinator
            .capture(KEY)
            .unwrap()
            .prepare(Duration::from_secs(3))
            .err(),
        Some(flussonix::source_directory::modern_coordinator::CoordinationFailure::Unsupported),
        "marked native configuration was projected into vendor clients"
    );
    s.put(
        "sources",
        KEY,
        json!({"flussonix_source_kind":"flussonic","on_play":"true"}),
    )
    .unwrap();
}
#[test]
fn deleting_used_backend_is_atomic_for_persisted_vendor_policies() {
    let (_d, s) = store();
    s.put(
        "auth_backends",
        "billing",
        json!({"url":"https://billing.invalid/check"}),
    )
    .unwrap();
    s.put(
        "sources",
        KEY,
        flussonix::config::merge(
            &source(),
            &json!({"group_config":{"unused":{"on_play":"auth://billing"}}}),
        ),
    )
    .unwrap();
    let saved = s.snapshot();
    let revision = s.revision();
    assert!(s.delete("auth_backends", "billing").is_err());
    assert_eq!(s.snapshot(), saved);
    assert_eq!(s.revision(), revision);
    s.put("sources", KEY, json!({"group_config":null})).unwrap();
    s.delete("auth_backends", "billing").unwrap();
}
#[test]
fn vendor_limit_also_counts_on_demand_rows_and_reopen_revalidates() {
    let (d, s) = store();
    let rows:Vec<_>=(0..65).map(|i|json!({"url":format!("m4s://owned{i}.invalid"),"flussonix_source_kind":"flussonic","cluster_key":"owned-vendor-key","only":[]})).collect();
    assert!(s.replace(json!({"sources":rows})).is_err());
    s.replace(json!({"sources":&rows[..64]})).unwrap();
    assert_eq!(s.snapshot()["sources"].as_array().unwrap().len(), 64);
    let mut corrupt = s.snapshot();
    corrupt["sources"][0]["drain"] = json!(true);
    std::fs::write(d.path().join("config.json"), corrupt.to_string()).unwrap();
    assert!(ConfigStore::open(d.path().join("config.json")).is_err());
}
async fn serve() -> (
    String,
    Arc<Mutex<Vec<(String, HeaderMap)>>>,
    tokio::task::JoinHandle<()>,
) {
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", tcp.local_addr().unwrap());
    let calls = Arc::new(Mutex::new(vec![]));
    let c = calls.clone();
    let server = tokio::spawn(async move {
        axum::serve(tcp,Router::new().fallback(get(move |uri:axum::extract::OriginalUri,h:HeaderMap|{c.lock().unwrap().push((uri.0.path().to_owned(),h));async{axum::Json(json!({"name":"owned","static":true,"disabled":false,"streams":[{"name":"owned","static":true,"disabled":false}]}))}}))).await.unwrap();
    });
    (url, calls, server)
}
#[tokio::test]
async fn native_directory_rejects_vendor_and_unknown_kind_before_sending_any_headers() {
    let (url, calls, server) = serve().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    for kind in [json!("flussonic"), json!("typo"), json!(null), json!(false)] {
        let result = query(
            &client,
            &json!({"api_url":url,"flussonix_source_kind":kind}),
            "owned",
            "owned-native-peer-key",
        )
        .await;
        assert_eq!(result.err(), Some(LookupFailure::Invalid));
    }
    assert!(calls.lock().unwrap().is_empty());
    for kind in [None, Some("native")] {
        let mut row = json!({"api_url":url});
        if let Some(k) = kind {
            row["flussonix_source_kind"] = json!(k);
        }
        assert!(
            query(&client, &row, "owned", "owned-native-peer-key")
                .await
                .is_ok()
        );
    }
    assert_eq!(calls.lock().unwrap().len(), 2);
    server.abort();
    let _ = server.await;
}
#[tokio::test]
async fn configured_vendor_does_not_enter_native_static_activation_or_playback_lookup() {
    let (url, calls, server) = serve().await;
    let d = tempfile::tempdir().unwrap();
    let app = App::new(
        d.path().join("config.json"),
        d.path().join("media"),
        Options {
            admin_password: "owned-admin-key".into(),
            peer_key: "owned-native-peer-key".into(),
            ffmpeg: "/owned-no-process".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let key = url.replacen("http", "m4s", 1);
    app.config.put("sources",&key,json!({"flussonix_source_kind":"flussonic","cluster_key":"owned-vendor-private-key","on_play":"true"})).unwrap();
    app.reconcile().await;
    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .uri("/owned/index.m3u8")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(
        calls.lock().unwrap().is_empty(),
        "native discovery sent credentials to vendor endpoint"
    );
    app.media.stop_all().await;
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn persisted_vendor_policy_controls_actual_app_authorizer_without_manual_injection() {
    use flussonix::{
        playback_auth::ViewerRequest,
        source_directory::modern_coordinator::{AdmissionFailure, AdmissionOutcome},
    };
    use sha2::{Digest, Sha256};
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", tcp.local_addr().unwrap());
    let groups = Arc::new(Mutex::new(json!([])));
    let remote_groups = groups.clone();
    let server = tokio::spawn(async move {
        axum::serve(tcp,Router::new().fallback(get(move |uri:axum::extract::OriginalUri,h:HeaderMap|{
        assert!(h.contains_key("authorization"));assert!(!h.contains_key("x-flussonix-peer"));assert!(!h.contains_key("cluster-auth-key"));
        let body=if uri.0.path().ends_with("/config") {json!({"stats":{"id":"owned-vendor-origin"}})} else {assert_eq!(uri.0.path(),"/streamer/api/v3/streams");json!({"server_id":"owned-vendor-origin","streams":[{"name":"owned","static":true,"named_by":"config","groups":remote_groups.lock().unwrap().clone()}]})};
        async{axum::Json(body)}
    }))).await.unwrap();
    });
    let d = tempfile::tempdir().unwrap();
    let app = App::new(
        d.path().join("config.json"),
        d.path().join("media"),
        Options {
            admin_password: "owned-admin-key".into(),
            peer_key: "owned-native-peer-key".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let key = url.replacen("http", "m4s", 1);
    let c = app.modern_source_coordinator().unwrap();
    let install = || {
        c.install(
            c.capture(&key)
                .unwrap()
                .prepare(Duration::from_secs(3))
                .unwrap(),
        )
        .unwrap()
    };
    let request = |token: &str| ViewerRequest {
        name: "owned".into(),
        proto: "hls".into(),
        ip: "127.0.0.1".into(),
        token: token.into(),
        ..Default::default()
    };
    app.config
        .put(
            "sources",
            &key,
            json!({"flussonix_source_kind":"flussonic","cluster_key":"owned-vendor-private-key"}),
        )
        .unwrap();
    let rows = c
        .inventory(&install(), Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        rows.candidate("owned").is_err(),
        "missing local viewer policy became public"
    );
    let token_hash = |text: &str| format!("{:x}", Sha256::digest(text.as_bytes()));
    app.config.put("sources",&key,json!({"on_play":"true","flussonix_token_sha256":token_hash("source-token"),"group_config":{"protected":{"flussonix_token_sha256":token_hash("group-token")}}})).unwrap();
    let rows = c
        .inventory(&install(), Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(
        c.admit(
            rows.candidate("owned").unwrap(),
            &app.playback_auth,
            request("wrong-token")
        )
        .await
        .unwrap(),
        AdmissionOutcome::Denied
    ));
    let AdmissionOutcome::Allowed(a) = c
        .admit(
            rows.candidate("owned").unwrap(),
            &app.playback_auth,
            request("source-token"),
        )
        .await
        .unwrap()
    else {
        panic!("saved source guard denied")
    };
    c.admitted_current(&a).unwrap();
    drop(a);
    *groups.lock().unwrap() = json!(["protected"]);
    let rows = c
        .inventory(&install(), Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(
        c.admit(
            rows.candidate("owned").unwrap(),
            &app.playback_auth,
            request("source-token")
        )
        .await
        .unwrap(),
        AdmissionOutcome::Denied
    ));
    assert!(matches!(
        c.admit(
            rows.candidate("owned").unwrap(),
            &app.playback_auth,
            request("group-token")
        )
        .await
        .unwrap(),
        AdmissionOutcome::Allowed(_)
    ));
    *groups.lock().unwrap() = json!(["unconfigured"]);
    let rows = c
        .inventory(&install(), Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(
        c.admit(
            rows.candidate("owned").unwrap(),
            &app.playback_auth,
            request("wrong-token")
        )
        .await
        .unwrap(),
        AdmissionOutcome::Denied
    ));
    assert!(matches!(
        c.admit(
            rows.candidate("owned").unwrap(),
            &app.playback_auth,
            request("source-token")
        )
        .await
        .unwrap(),
        AdmissionOutcome::Allowed(_)
    ));
    app.config
        .put(
            "sources",
            &key,
            json!({"on_play":null,"flussonix_token_sha256":null}),
        )
        .unwrap();
    let rows = c
        .inventory(&install(), Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        rows.candidate("owned").is_err(),
        "unconfigured group without source policy became public"
    );
    app.config
        .put(
            "sources",
            &key,
            json!({"on_play":"true","flussonix_token_sha256":token_hash("source-token")}),
        )
        .unwrap();
    *groups.lock().unwrap() = json!([]);
    app.config.put("streams","owned",json!({"disabled":true,"static":false,"inputs":[{"url":"http://owned.invalid/mpegts"}]})).unwrap();
    let rows = c
        .inventory(&install(), Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(
        c.admit(
            rows.candidate("owned").unwrap(),
            &app.playback_auth,
            request("source-token")
        )
        .await,
        Err(AdmissionFailure::LocalStream)
    ));
    app.media.stop_all().await;
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn public_crud_persists_vendor_fields_and_dry_run_errors_are_atomic() {
    use base64::{Engine, engine::general_purpose::STANDARD};
    let d = tempfile::tempdir().unwrap();
    let app = App::new(
        d.path().join("config.json"),
        d.path().join("media"),
        Options {
            admin_password: "owned-admin-key".into(),
            peer_key: "owned-native-peer-key".into(),
            view_user: Some("viewer".into()),
            view_password: Some("owned-view-key".into()),
            ffmpeg: "/owned-no-process".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let path = format!(
        "/streamer/api/v3/cluster/sources/{}",
        percent_encoding::utf8_percent_encode(KEY, percent_encoding::NON_ALPHANUMERIC)
    );
    let send = |method: &str, path: &str, body: Value, admin: bool| {
        let r = Request::builder()
            .method(method)
            .uri(path)
            .header(
                "authorization",
                format!(
                    "Basic {}",
                    STANDARD.encode(if admin {
                        "admin:owned-admin-key"
                    } else {
                        "viewer:owned-view-key"
                    })
                ),
            )
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        router(app.clone()).oneshot(r)
    };
    let response = send("PUT", &path, source(), false).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(
        app.config.snapshot()["sources"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let response = send("PUT", &path, source(), true).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = send("GET", &path, Value::Null, false).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), 100000)
        .await
        .unwrap();
    let saved: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(saved["flussonix_source_kind"], "flussonic");
    assert_eq!(saved["on_play"], "true");
    let snapshot = app.config.snapshot();
    let revision = app.config.revision();
    let response = send(
        "POST",
        "/streamer/api/v3/config",
        json!({"sources":[flussonix::config::merge(&saved,&json!({"on_play":false}))]}),
        true,
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let response = send(
        "PUT",
        &path,
        json!({"private_payload_url":"http://owned.invalid?token=private-secret"}),
        true,
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let bytes = axum::body::to_bytes(response.into_body(), 100000)
        .await
        .unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("private-secret"));
    assert_eq!(app.config.snapshot(), snapshot);
    assert_eq!(app.config.revision(), revision);
    app.media.stop_all().await;
}
