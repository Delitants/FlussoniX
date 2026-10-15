//! Owned control-only source fixture shared by ledger and LB fencing tests.
use super::*;
use crate::source_directory::modern_coordinator::{AdmissionOutcome, AdmittedCandidate};

pub(super) async fn admitted_source(
    lifetime: Duration,
    role: &str,
    peer: Option<Value>,
) -> (
    tempfile::TempDir,
    Arc<App>,
    Box<AdmittedCandidate>,
    tokio::task::JoinHandle<()>,
    String,
) {
    admitted_source_with_limit(lifetime, role, peer, 1000).await
}
pub(super) async fn admitted_source_with_limit(
    lifetime: Duration,
    role: &str,
    peer: Option<Value>,
    limit: u64,
) -> (
    tempfile::TempDir,
    Arc<App>,
    Box<AdmittedCandidate>,
    tokio::task::JoinHandle<()>,
    String,
) {
    use axum::{extract::OriginalUri, routing::get};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let key = format!("m4s://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener,Router::new().fallback(get(|OriginalUri(uri):OriginalUri,headers:HeaderMap| async move {
                assert!(headers.contains_key("authorization"));
                assert!(!headers.contains_key("x-flussonix-peer"));
                let value=if uri.path().ends_with("/config") {json!({"stats":{"id":"owned-origin"}})}
                    else {assert_eq!(uri.path(),"/streamer/api/v3/streams");json!({"server_id":"owned-origin","streams":[{"name":"owned","groups":[],"static":true,"named_by":"config"}]})};
                json_response(value)
            }))).await.unwrap()
    });
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            role: role.into(),
            client_limit: limit,
            admin_password: "owned-admin-key".into(),
            peer_key: "owned-peer-key".into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.config.put("sources",&key,json!({"flussonix_source_kind":"flussonic","cluster_key":"owned-vendor-key","on_play":"true"})).unwrap();
    if let Some(peer) = peer {
        let name = peer["hostname"].as_str().unwrap();
        app.config.put("peers", name, peer.clone()).unwrap();
    }
    let owner = app.modern_sources.as_ref().unwrap().clone();
    let handle = owner
        .install(
            owner
                .capture(&key)
                .unwrap()
                .prepare(Duration::from_secs(3))
                .unwrap(),
        )
        .unwrap();
    let rows = owner.inventory(&handle, lifetime).await.unwrap();
    let outcome = owner
        .admit(
            rows.candidate("owned").unwrap(),
            &app.playback_auth,
            ViewerRequest {
                name: "owned".into(),
                proto: "m4s".into(),
                ip: "127.0.0.1".into(),
                token: String::new(),
                qs: String::new(),
                user_agent: String::new(),
                referer: String::new(),
                host: String::new(),
            },
        )
        .await
        .unwrap();
    let AdmissionOutcome::Allowed(admitted) = outcome else {
        panic!("owned explicit policy rejected")
    };
    assert!(owner.admitted_current(&admitted).is_ok());
    (dir, app, admitted, server, key)
}
