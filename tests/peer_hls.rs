use axum::{
    Router,
    body::Body,
    http::{HeaderMap, StatusCode},
    routing::get,
};
use flussonix::media::Engine;
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

async fn reject_foreign_credentials(redirect: bool) {
    let leaked = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let foreign = format!(
        "http://localhost:{}/foreign.m3u8",
        listener.local_addr().unwrap().port()
    );
    let count = leaked.clone();
    let fixture = Router::new()
        .route(
            "/foreign.m3u8",
            get(move |h: HeaderMap| {
                let count = count.clone();
                async move {
                    if h.contains_key("x-flussonix-peer") {
                        count.fetch_add(1, Ordering::Relaxed);
                    }
                    (StatusCode::OK, "#EXTM3U\n#EXT-X-ENDLIST\n")
                }
            }),
        )
        .route(
            "/entry.m3u8",
            get(move || {
                let foreign = foreign.clone();
                async move {
                    if redirect {
                        axum::response::Response::builder()
                            .status(302)
                            .header("location", foreign)
                            .body(Body::empty())
                            .unwrap()
                    } else {
                        axum::response::Response::builder()
                            .status(200)
                            .body(Body::from(format!(
                                "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=100000\n{foreign}\n"
                            )))
                            .unwrap()
                    }
                }
            }),
        );
    let entry = format!(
        "hlss://127.0.0.1:{}/entry.m3u8",
        listener.local_addr().unwrap().port()
    );
    // This owned fixture uses clear HTTP; hls:// selects HTTP in production.
    let entry = entry.replacen("hlss://", "hls://", 1);
    let server = tokio::spawn(async move {
        axum::serve(listener, fixture).await.unwrap();
    });
    let d = tempfile::tempdir().unwrap();
    let engine = Engine::new(d.path(), "ffmpeg");
    engine
        .ensure(
            "owned",
            &json!({"inputs":[{"url":entry}],"flussonix_peer_key":"owned-peer-secret"}),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    engine.stop_all().await;
    server.abort();
    assert_eq!(
        leaked.load(Ordering::Relaxed),
        0,
        "peer key reached a different origin"
    );
}
#[tokio::test]
async fn native_hls_never_forwards_peer_credentials_on_redirect() {
    reject_foreign_credentials(true).await;
}
#[tokio::test]
async fn native_hls_never_forwards_peer_credentials_to_foreign_playlist_resources() {
    reject_foreign_credentials(false).await;
}

#[tokio::test]
async fn native_hls_scopes_nested_playlists_keys_maps_and_byte_ranges() {
    use flussonix::peer_hls::PeerHls;
    let hits = Arc::new(AtomicUsize::new(0));
    let hit = hits.clone();
    let fixture = Router::new()
        .route("/root/index.m3u8", get(|| async { "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=100000\nvariant.m3u8\n" }))
        .route("/root/variant.m3u8", get(|| async { "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-KEY:METHOD=AES-128,URI=\"../key.bin?name=a,b\"\n#EXT-X-MAP:URI=\"init.mp4\",BYTERANGE=\"2@0\"\n#EXTINF:2,\nsegment.ts\n" }))
        .route("/key.bin", get(move |h: HeaderMap| { let hit = hit.clone(); async move {
            assert_eq!(h["x-flussonix-peer"], "owned-peer-secret");
            hit.fetch_add(1, Ordering::Relaxed);
            "owned-key"
        }}))
        .route("/root/init.mp4", get(|h: HeaderMap| async move {
            assert_eq!(h["x-flussonix-peer"], "owned-peer-secret");
            assert_eq!(h["range"], "bytes=0-1");
            (StatusCode::PARTIAL_CONTENT, [("content-range", "bytes 0-1/4")], "AB")
        }))
        .route("/root/segment.ts", get(|h: HeaderMap| async move { assert_eq!(h["x-flussonix-peer"], "owned-peer-secret"); "owned-segment" }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let input = format!("http://{}/root/index.m3u8", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, fixture).await.unwrap();
    });
    let proxy = PeerHls::start(&input, "owned-peer-secret").await.unwrap();
    let local = proxy.url.clone();
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let master = client
        .get(&local)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let variant = master
        .lines()
        .find(|l| !l.starts_with('#') && !l.is_empty())
        .unwrap();
    let playlist = client
        .get(variant)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let uris: Vec<_> = playlist
        .lines()
        .filter_map(|l| {
            l.split_once("URI=\"")
                .map(|(_, rest)| rest.split('"').next().unwrap())
        })
        .collect();
    assert_eq!(uris.len(), 2);
    assert_eq!(
        client
            .get(uris[0])
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
        "owned-key"
    );
    let map = client
        .get(uris[1])
        .header("range", "bytes=0-1")
        .send()
        .await
        .unwrap();
    assert_eq!(map.status(), 206);
    assert_eq!(map.headers()["content-range"], "bytes 0-1/4");
    assert_eq!(map.text().await.unwrap(), "AB");
    let segment = playlist
        .lines()
        .find(|l| !l.starts_with('#') && !l.is_empty())
        .unwrap();
    assert_eq!(
        client
            .get(segment)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
        "owned-segment"
    );
    assert_eq!(hits.load(Ordering::Relaxed), 1);
    drop(proxy);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(client.get(local).send().await.is_err());
    server.abort();
}
