use axum::{
    Router,
    body::Body,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::get,
};
use flussonix::source_directory::{LookupFailure, query};
use serde_json::json;
use std::time::{Duration, Instant};
async fn serve(router: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (url, task)
}
fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}
#[tokio::test]
async fn query_preserves_prefix_encoded_name_and_source_specific_key() {
    let router = Router::new().route(
        "/prefix/flussonix/api/v1/stream/{*name}",
        get(
            |h: HeaderMap, axum::extract::Path(name): axum::extract::Path<String>| async move {
                assert_eq!(h["X-Flussonix-Peer"], "specific-owned-key");
                assert_eq!(name, "region/space name");
                axum::Json(json!({"inputs":[{"url":"testsrc://"}]}))
            },
        ),
    );
    let (url, task) = serve(router).await;
    let got = query(
        &client(),
        &json!({"api_url":format!("{url}/prefix"),"cluster_key":"specific-owned-key"}),
        "region/space name",
        "unused-default-key",
    )
    .await
    .unwrap();
    assert!(got.is_object());
    task.abort();
}
#[tokio::test]
async fn hung_lookup_is_bounded_even_with_a_large_client_timeout() {
    let (url, task) = serve(Router::new().fallback(get(|| async {
        tokio::time::sleep(Duration::from_secs(5)).await;
        axum::Json(json!({}))
    })))
    .await;
    let now = Instant::now();
    assert_eq!(
        query(
            &client(),
            &json!({"api_url":url}),
            "owned",
            "owned-peer-key"
        )
        .await
        .unwrap_err(),
        LookupFailure::Unavailable
    );
    assert!(now.elapsed() < Duration::from_millis(1500));
    task.abort();
}
#[tokio::test]
async fn excessive_and_malformed_metadata_are_rejected() {
    for body in [
        "x".repeat(1024 * 1024 + 1),
        "[]".into(),
        "invalid json".into(),
    ] {
        let b = body.clone();
        let (url, task) = serve(Router::new().fallback(get(move || {
            let b = b.clone();
            async move { Response::builder().status(200).body(Body::from(b)).unwrap() }
        })))
        .await;
        assert_eq!(
            query(
                &client(),
                &json!({"api_url":url}),
                "owned",
                "owned-peer-key"
            )
            .await
            .unwrap_err(),
            LookupFailure::Invalid
        );
        task.abort();
    }
}
#[tokio::test]
async fn not_found_is_authoritative_and_redirects_do_not_forward_peer_credentials() {
    let target = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let count = target.clone();
    let (url2, t2) = serve(Router::new().fallback(get(move || {
        count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        async { axum::Json(json!({})) }
    })))
    .await;
    let dest = url2.clone();
    let (url, task) = serve(
        Router::new()
            .route(
                "/flussonix/api/v1/stream/absent",
                get(|| async { StatusCode::NOT_FOUND }),
            )
            .fallback(get(move || {
                let dest = dest.clone();
                async move { (StatusCode::FOUND, [("Location", dest)]) }
            })),
    )
    .await;
    assert_eq!(
        query(
            &client(),
            &json!({"api_url":url}),
            "absent",
            "owned-peer-key"
        )
        .await
        .unwrap_err(),
        LookupFailure::Absent
    );
    assert_eq!(
        query(
            &client(),
            &json!({"api_url":url}),
            "redirect",
            "owned-peer-key"
        )
        .await
        .unwrap_err(),
        LookupFailure::Unavailable
    );
    assert_eq!(target.load(std::sync::atomic::Ordering::Relaxed), 0);
    task.abort();
    t2.abort();
}
