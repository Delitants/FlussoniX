use axum::{
    body::Body,
    extract::{Query, State},
    http::{Request, StatusCode},
    routing::get,
};
use flussonix::server::{App, Options, router};
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tower::ServiceExt;
#[derive(Clone)]
struct Backend {
    calls: Arc<AtomicUsize>,
    queries: Arc<Mutex<Vec<HashMap<String, String>>>>,
    mode: Arc<AtomicUsize>,
}
async fn callback(
    State(b): State<Backend>,
    Query(q): Query<HashMap<String, String>>,
) -> impl axum::response::IntoResponse {
    b.calls.fetch_add(1, Ordering::SeqCst);
    b.queries.lock().unwrap().push(q);
    tokio::time::sleep(Duration::from_millis(60)).await;
    let status = match b.mode.load(Ordering::SeqCst) {
        1 => StatusCode::FORBIDDEN,
        2 => StatusCode::INTERNAL_SERVER_ERROR,
        3 => StatusCode::FOUND,
        _ => StatusCode::OK,
    };
    (
        status,
        [
            ("X-AuthDuration", "1"),
            ("X-UserId", "account"),
            ("X-Max-Sessions", "1"),
            ("Location", "https://player.example/alternate"),
        ],
    )
}
async fn setup() -> (
    tempfile::TempDir,
    Arc<App>,
    Backend,
    tokio::task::JoinHandle<()>,
    String,
) {
    let d = tempfile::tempdir().unwrap();
    let b = Backend {
        calls: Arc::new(AtomicUsize::new(0)),
        queries: Arc::new(Mutex::new(vec![])),
        mode: Arc::new(AtomicUsize::new(0)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/auth", listener.local_addr().unwrap());
    let backend = b.clone();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new()
                .route("/auth", get(callback))
                .with_state(backend),
        )
        .await
        .unwrap()
    });
    let app = App::new(
        d.path().join("c.json"),
        d.path().join("media"),
        Options {
            admin_password: "edit-secret".into(),
            view_user: Some("viewer".into()),
            view_password: Some("view-secret".into()),
            peer_key: "peer-secret-1234".into(),
            ..Default::default()
        },
    )
    .unwrap();
    for name in ["owned", "other"] {
        app.config
            .put(
                "streams",
                name,
                json!({"static":false,"inputs":[{"url":"testsrc://"}],"on_play":url}),
            )
            .unwrap();
    }
    (d, app, b, task, url)
}
async fn media(app: &Arc<App>, path: &str) -> axum::response::Response {
    router(app.clone())
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
}
#[tokio::test]
async fn callback_has_required_fields_uuid_and_actual_protocol() {
    let (_d, app, b, task, _) = setup().await;
    let response = media(&app, "/owned/mpegts?token=one&customer=demo").await;
    assert_eq!(response.status(), 200);
    let q = b.queries.lock().unwrap().last().unwrap().clone();
    app.media.stop_all().await;
    task.abort();
    assert_eq!(q.get("proto").map(String::as_str), Some("mpegts"));
    assert_eq!(
        q.get("request_type").map(String::as_str),
        Some("new_session")
    );
    assert_eq!(q.get("request_number").map(String::as_str), Some("0"));
    assert!(uuid::Uuid::parse_str(q.get("session_id").expect("session UUID")).is_ok());
    assert_eq!(
        q.get("qs").map(String::as_str),
        Some("token=one&customer=demo")
    );
    for field in ["stream_clients", "total_clients", "duration", "bytes"] {
        assert!(q.contains_key(field), "missing {field}");
    }
}
#[tokio::test]
async fn concurrent_initial_requests_share_one_callback() {
    let (_d, app, b, task, _) = setup().await;
    let responses =
        futures_util::future::join_all((0..8).map(|_| media(&app, "/owned/mpegts?token=shared")))
            .await;
    let calls = b.calls.load(Ordering::SeqCst);
    app.media.stop_all().await;
    task.abort();
    assert!(responses.iter().all(|r| r.status() == 200));
    assert_eq!(
        calls, 1,
        "a first viewer triggers only one backend decision"
    );
}
#[tokio::test]
async fn standard_max_sessions_header_limits_one_user_across_streams() {
    let (_d, app, _b, task, _) = setup().await;
    let first = media(&app, "/owned/mpegts?token=one").await;
    let second = media(&app, "/other/mpegts?token=two").await;
    app.media.stop_all().await;
    task.abort();
    assert_eq!(first.status(), 200);
    assert_eq!(second.status(), 403);
}
#[tokio::test]
async fn callback_redirect_is_cached_and_never_starts_a_worker() {
    let (_d, app, b, task, _) = setup().await;
    b.mode.store(3, Ordering::SeqCst);
    for _ in 0..2 {
        let r = media(&app, "/owned/index.m3u8?token=redirect").await;
        assert_eq!(r.status(), 302);
        assert_eq!(r.headers()["location"], "https://player.example/alternate");
    }
    assert_eq!(app.media.count().await, 0);
    assert_eq!(b.calls.load(Ordering::SeqCst), 1);
    task.abort();
}
