//! One unavailable startup announcement must not become a new viewer request.
use axum::{
    Router,
    body::Body,
    extract::OriginalUri,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::get,
};
use flussonix::{
    m4_ingest::flussonic::{InputFailure, MediaInput, Transport},
    m4f::Frame,
    m4s::Track,
    wire,
};
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn skipped_name_is_not_refetched_after_the_normal_dedup_window_rotates() {
    let mut signals = String::from("1 2026/10/17/00/00/00-2000\n");
    for i in 1..=20 {
        signals.push_str(&format!("{} 2026/10/17/00/00/{i:02}-2000\n", i + 1));
    }
    signals.push_str("22 2026/10/17/00/00/00-2000\n");
    let result = observe(
        signals.as_bytes(),
        &[0],
        StatusCode::BAD_REQUEST,
        true,
        Some("no_dvr"),
    )
    .await;
    assert!(result.metadata && result.bytes > 0);
    assert_eq!(
        result
            .paths
            .iter()
            .filter(|p| p.ends_with("/00.m4f"))
            .count(),
        1,
        "a late duplicate refetched the unavailable startup object"
    );
    assert_eq!(result.paths.len(), 22);
}

fn media(number: usize) -> Vec<u8> {
    let track = Track {
        id: 1,
        codec: "mp3".into(),
        config: vec![],
    };
    let frames = (0..77)
        .map(|i| Frame {
            track_id: 1,
            dts: 90_000 + number as u64 * 180_000 + i * 2351,
            pts_offset: 0,
            key: true,
            body: include_bytes!("fixtures/codecs/mp3.bin").to_vec(),
        })
        .collect::<Vec<_>>();
    flussonix::m4f::pack(&[track], &frames, 180_000).unwrap()
}

struct Observation {
    kind: InputFailure,
    paths: Vec<String>,
    metadata: bool,
    bytes: usize,
    stored: Vec<bool>,
}
// A real HTTP source emits literal announcements. Negative bodies never end,
// so passing proves the reader does not consume unavailable media bodies.
async fn observe(
    signals: &[u8],
    failures: &[usize],
    status: StatusCode,
    ack: bool,
    reason: Option<&'static str>,
) -> Observation {
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let root = format!("http://{}", tcp.local_addr().unwrap());
    let paths = Arc::new(Mutex::new(Vec::new()));
    let requests = paths.clone();
    let identity = Arc::new(Mutex::new(None::<String>));
    let signals = signals.to_vec();
    let failures = failures.to_vec();
    let service = Router::new().fallback(get(
        move |OriginalUri(uri): OriginalUri, headers: HeaderMap| {
            let requests = requests.clone();
            let identity = identity.clone();
            let signals = signals.clone();
            let failures = failures.clone();
            async move {
                assert!(headers.contains_key("cluster-auth-key"));
                assert!(!headers.contains_key("authorization"));
                let id = headers["x-playback-session-id"].to_str().unwrap();
                let mut session = identity.lock().unwrap();
                if let Some(current) = session.as_ref() {
                    assert_eq!(current, id)
                } else {
                    *session = Some(id.to_owned())
                }
                drop(session);
                requests.lock().unwrap().push(uri.path().to_owned());
                let control = uri.path() == "/owned/m4f";
                let number = if control {
                    0
                } else {
                    uri.path()
                        .trim_end_matches(".m4f")
                        .rsplit('/')
                        .next()
                        .unwrap()
                        .parse::<usize>()
                        .unwrap()
                };
                let missing = !control && failures.contains(&number);
                let body = if control {
                    Body::from(signals)
                } else if missing {
                    Body::from_stream(futures_util::stream::pending::<
                        Result<bytes::Bytes, std::io::Error>,
                    >())
                } else {
                    Body::from(media(number))
                };
                let mut response = Response::new(body);
                if missing {
                    *response.status_mut() = status;
                    if let Some(reason) = reason {
                        response
                            .headers_mut()
                            .insert("x-reason", reason.parse().unwrap());
                    }
                }
                if control || !missing || ack {
                    response
                        .headers_mut()
                        .insert("cluster-auth", "yes".parse().unwrap());
                }
                response
            }
        },
    ));
    let server = tokio::spawn(async move { axum::serve(tcp, service).await.unwrap() });
    let input = MediaInput::new(&root, "owned", Transport::M4f, "owned-key", None).unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let mut output = Vec::new();
    let hub = wire::Hub::new();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        input.pull_ready_classified(&mut output, Some(&hub), tx),
    )
    .await;
    server.abort();
    let _ = server.await;
    let kind = result
        .expect("reader consumed a nonterminating error body")
        .unwrap_err();
    assert!(
        output
            .chunks(188)
            .all(|packet| packet.len() == 188 && packet[0] == 0x47)
    );
    let stored = (0..3)
        .map(|n| {
            hub.segment(&format!("2026/10/17/00/00/{n:02}.m4f"))
                .is_some()
        })
        .collect();
    let paths = paths.lock().unwrap().clone();
    Observation {
        kind,
        paths,
        metadata: rx.await.is_ok(),
        bytes: output.len(),
        stored,
    }
}
const NEXT: &[u8] =
    b"1 2026/10/17/00/00/00-2000\n1 2026/10/17/00/00/00-2000\n2 2026/10/17/00/00/01-2000\n";
#[tokio::test]
async fn first_missing_private_segment_waits_for_next_distinct_announcement() {
    for (status, reason) in [
        (StatusCode::NOT_FOUND, None),
        (StatusCode::BAD_REQUEST, Some("no_dvr")),
    ] {
        let result = observe(NEXT, &[0], status, true, reason).await;
        assert!(
            result.metadata && result.bytes > 0,
            "an unavailable first announcement discarded ready next media"
        );
        assert_eq!(
            result.paths,
            vec![
                "/owned/m4f",
                "/owned/2026/10/17/00/00/00.m4f",
                "/owned/2026/10/17/00/00/01.m4f"
            ],
            "never refetch the unavailable object or open another control"
        );
        assert_eq!(result.stored, vec![false, true, false]);
        assert_eq!(
            result.kind,
            InputFailure::Transient,
            "only final control EOF terminates the successful fixture"
        );
    }
}
#[tokio::test]
async fn second_missing_startup_segment_still_ends_the_pull() {
    let result = observe(
        b"1 2026/10/17/00/00/00-2000\n2 2026/10/17/00/00/01-2000\n3 2026/10/17/00/00/02-2000\n",
        &[0, 1],
        StatusCode::BAD_REQUEST,
        true,
        Some("no_dvr"),
    )
    .await;
    assert_eq!(result.kind, InputFailure::Transient);
    assert!(!result.metadata && result.bytes == 0 && result.stored == vec![false, false, false]);
    assert_eq!(
        result.paths.len(),
        3,
        "bounded skip must reach second announcement, then stop"
    );
}
#[tokio::test]
async fn missing_segment_after_metadata_keeps_existing_failover_behavior() {
    let result = observe(
        b"1 2026/10/17/00/00/00-2000\n2 2026/10/17/00/00/01-2000\n3 2026/10/17/00/00/02-2000\n",
        &[1],
        StatusCode::BAD_REQUEST,
        true,
        Some("no_dvr"),
    )
    .await;
    assert_eq!(result.kind, InputFailure::Transient);
    assert!(result.metadata && result.bytes > 0);
    assert_eq!(result.paths.len(), 3);
    assert_eq!(result.stored, vec![true, false, false]);
}
#[tokio::test]
async fn rejected_startup_announcements_never_reach_later_media() {
    for (status, ack, reason) in [
        (StatusCode::BAD_REQUEST, true, None),
        (StatusCode::BAD_REQUEST, true, Some("No_Dvr")),
        (StatusCode::BAD_REQUEST, false, Some("no_dvr")),
        (StatusCode::NOT_FOUND, false, None),
        (StatusCode::FORBIDDEN, true, Some("no_dvr")),
        (StatusCode::UNAUTHORIZED, true, None),
        (StatusCode::TOO_MANY_REQUESTS, true, None),
    ] {
        let result = observe(NEXT, &[0], status, ack, reason).await;
        assert_eq!(result.kind, InputFailure::Rejected);
        assert!(
            !result.metadata && result.bytes == 0 && result.stored == vec![false, false, false]
        );
        assert_eq!(result.paths.len(), 2);
    }
}
