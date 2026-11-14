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

#[cfg(target_os = "linux")]
#[tokio::test]
async fn protected_hls_proxy_capability_stays_out_of_ffmpeg_argv() {
    use axum::extract::Path;
    use bytes::Bytes;
    use std::collections::HashMap;
    use tokio::process::Command;

    let fixture = tempfile::tempdir().unwrap();
    let playlist_path = fixture.path().join("index.m3u8");
    let segment_pattern = fixture.path().join("segment%d.ts");
    let mut generator = Command::new("ffmpeg");
    generator.args([
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc=size=64x64:rate=10",
        "-t",
        "3",
        "-c:v",
        "libx264",
        "-g",
        "10",
        "-sc_threshold",
        "0",
        "-pix_fmt",
        "yuv420p",
        "-f",
        "hls",
        "-hls_time",
        "1",
        "-hls_list_size",
        "0",
        "-hls_segment_filename",
    ]);
    generator
        .arg(&segment_pattern)
        .arg(&playlist_path)
        .kill_on_drop(true);
    let generated = tokio::time::timeout(Duration::from_secs(15), generator.output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        generated.status.success(),
        "fixture generation failed: {}",
        String::from_utf8_lossy(&generated.stderr)
    );
    let playlist = std::fs::read_to_string(&playlist_path)
        .unwrap()
        .replace("#EXT-X-ENDLIST\n", "");
    let mut files = HashMap::new();
    files.insert("index.m3u8".to_owned(), Bytes::from(playlist));
    for name in ["segment0.ts", "segment1.ts", "segment2.ts"] {
        files.insert(
            name.to_owned(),
            Bytes::from(std::fs::read(fixture.path().join(name)).unwrap()),
        );
    }
    let files = Arc::new(files);
    let source = Router::new().route(
        "/{file}",
        get(move |Path(file): Path<String>, headers: HeaderMap| {
            let files = files.clone();
            async move {
                if headers
                    .get("x-flussonix-peer")
                    .and_then(|v| v.to_str().ok())
                    != Some("owned-peer-secret")
                {
                    return (StatusCode::FORBIDDEN, Bytes::new());
                }
                files
                    .get(&file)
                    .cloned()
                    .map_or((StatusCode::NOT_FOUND, Bytes::new()), |body| {
                        (StatusCode::OK, body)
                    })
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("hls://{}/index.m3u8", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, source).await.unwrap() });
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::new(directory.path(), "ffmpeg");
    let worker = engine
        .ensure(
            "owned",
            &json!({"inputs":[{"url":url}],"flussonix_peer_key":"owned-peer-secret"}),
        )
        .await
        .unwrap();
    let pid = worker.pid();
    assert!(pid > 0, "FFmpeg did not start");
    let argv = std::fs::read(format!("/proc/{pid}/cmdline")).unwrap();
    let argv = String::from_utf8_lossy(&argv);
    let leaked_proxy = argv.contains("resource.m3u8") || argv.contains("owned-peer-secret");
    let decoded = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if engine.read("owned", "index.m3u8").await.is_ok() {
                break true;
            }
            if worker.is_closed() {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or(false);
    engine.stop_all().await;
    server.abort();
    let _ = server.await;
    assert!(
        decoded,
        "protected HLS input did not produce an output playlist"
    );
    assert!(
        !leaked_proxy,
        "private HLS proxy capability leaked into FFmpeg argv"
    );
}

// Break caught: an uncredentialed HLS input lets FFmpeg fetch a segment from
// an unrelated HTTP origin instead of enforcing the configured source origin.
async fn reject_plain_hls_foreign_segment(scheme: &str, path: &str) {
    let foreign_hits = Arc::new(AtomicUsize::new(0));
    let observed = foreign_hits.clone();
    let foreign_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let foreign_url = format!(
        "http://{}/foreign.ts",
        foreign_listener.local_addr().unwrap()
    );
    let foreign = tokio::spawn(async move {
        axum::serve(
            foreign_listener,
            Router::new().route(
                "/foreign.ts",
                get(move || {
                    observed.fetch_add(1, Ordering::SeqCst);
                    async { vec![0x47; 188 * 10] }
                }),
            ),
        )
        .await
        .unwrap();
    });
    let source_hits = Arc::new(AtomicUsize::new(0));
    let seen = source_hits.clone();
    let source_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let input = format!("{scheme}://{}{path}", source_listener.local_addr().unwrap());
    let route = path.to_owned();
    let source = tokio::spawn(async move {
        axum::serve(
            source_listener,
            Router::new().route(
                &route,
                get(move || {
                    seen.fetch_add(1, Ordering::SeqCst);
                    let segment = foreign_url.clone();
                    async move {
                        (
                            [("content-type", "application/vnd.apple.mpegurl")],
                            format!("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:1.0,\n{segment}\n#EXT-X-ENDLIST\n"),
                        )
                    }
                }),
            ),
        )
        .await
        .unwrap();
    });
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::new(directory.path(), "ffmpeg");
    engine
        .ensure(
            "owned-plain-hls",
            &json!({"inputs":[{"url":input}],"flussonix_input_timeout":1}),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    engine.stop_all().await;
    source.abort();
    foreign.abort();
    assert!(
        source_hits.load(Ordering::SeqCst) > 0,
        "HLS input was not exercised"
    );
    assert_eq!(foreign_hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn plain_hls_input_never_fetches_a_foreign_segment() {
    reject_plain_hls_foreign_segment("hls", "/entry.m3u8").await;
}

#[tokio::test]
async fn plain_http_playlist_never_fetches_a_foreign_segment() {
    reject_plain_hls_foreign_segment("http", "/entry.m3u8").await;
}

#[tokio::test]
async fn plain_http_extensionless_playlist_never_fetches_a_foreign_segment() {
    reject_plain_hls_foreign_segment("http", "/entry").await;
}

// A response at an HLS URL may be a different manifest format. The decoder
// must not autodetect DASH and fetch its media outside the origin-scoped proxy.
async fn reject_dash_manifest_with_foreign_media_url(as_segment: bool) {
    let foreign_hits = Arc::new(AtomicUsize::new(0));
    let observed = foreign_hits.clone();
    let foreign_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let foreign_url = format!(
        "http://{}/foreign.mp4",
        foreign_listener.local_addr().unwrap()
    );
    let foreign = tokio::spawn(async move {
        axum::serve(
            foreign_listener,
            Router::new().route(
                "/foreign.mp4",
                get(move || {
                    observed.fetch_add(1, Ordering::SeqCst);
                    async { vec![0u8; 128] }
                }),
            ),
        )
        .await
        .unwrap();
    });
    let source_hits = Arc::new(AtomicUsize::new(0));
    let seen = source_hits.clone();
    let segment_hits = Arc::new(AtomicUsize::new(0));
    let segment_seen = segment_hits.clone();
    let source_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let input = format!("hls://{}/entry.m3u8", source_listener.local_addr().unwrap());
    let mpd = format!(
        "<?xml version=\"1.0\"?><MPD xmlns=\"urn:mpeg:dash:schema:mpd:2011\" type=\"static\" mediaPresentationDuration=\"PT1S\" minBufferTime=\"PT1S\" profiles=\"urn:mpeg:dash:profile:isoff-on-demand:2011\"><Period duration=\"PT1S\"><AdaptationSet mimeType=\"video/mp4\"><Representation id=\"1\" bandwidth=\"1000\" codecs=\"avc1.42E01E\" width=\"16\" height=\"16\"><BaseURL>{foreign_url}</BaseURL><SegmentBase indexRange=\"0-100\"/></Representation></AdaptationSet></Period></MPD>"
    );
    let entry = if as_segment {
        "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:1\n#EXTINF:1.0,\nsegment.bin\n#EXT-X-ENDLIST\n".to_owned()
    } else {
        mpd.clone()
    };
    let source = tokio::spawn(async move {
        axum::serve(
            source_listener,
            Router::new()
                .route(
                    "/entry.m3u8",
                    get(move || {
                        seen.fetch_add(1, Ordering::SeqCst);
                        let body = entry.clone();
                        async move { body }
                    }),
                )
                .route(
                    "/segment.bin",
                    get(move || {
                        segment_seen.fetch_add(1, Ordering::SeqCst);
                        let body = mpd.clone();
                        async move { body }
                    }),
                ),
        )
        .await
        .unwrap();
    });
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::new(directory.path(), "ffmpeg");
    engine
        .ensure("owned-dash", &json!({"inputs":[{"url":input}]}))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    engine.stop_all().await;
    source.abort();
    foreign.abort();
    assert!(
        source_hits.load(Ordering::SeqCst) > 0,
        "HLS input was not exercised"
    );
    if as_segment {
        assert!(
            segment_hits.load(Ordering::SeqCst) > 0,
            "HLS segment was not exercised"
        );
    }
    assert_eq!(
        foreign_hits.load(Ordering::SeqCst),
        0,
        "non-HLS manifest fetched media outside the configured origin"
    );
}

#[tokio::test]
async fn hls_input_rejects_a_dash_manifest_with_a_foreign_media_url() {
    reject_dash_manifest_with_foreign_media_url(false).await;
}

#[tokio::test]
async fn hls_segment_rejects_a_dash_manifest_with_a_foreign_media_url() {
    reject_dash_manifest_with_foreign_media_url(true).await;
}

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
