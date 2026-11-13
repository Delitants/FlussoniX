use axum::{Router, body::Body, routing::get};
use bytes::Bytes;
use flussonix::{
    m4f::{self, Frame},
    m4s::Track,
};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
pub struct Source {
    pub url: String,
    pub calls: Arc<AtomicUsize>,
    pub active: Arc<AtomicUsize>,
    started: Arc<OnceLock<Instant>>,
    epoch: Arc<Mutex<CancellationToken>>,
    server: tokio::task::JoinHandle<()>,
}
impl Source {
    pub async fn new(seconds: u64) -> Self {
        let tracks = vec![Track {
            id: 1,
            codec: "mp3".into(),
            config: vec![],
        }];
        let mut segments = HashMap::new();
        let mut signals = String::new();
        for n in 0..seconds.div_ceil(6) {
            let start = n * 6 * 90000;
            let duration = (seconds - n * 6).min(6) * 90000;
            let frames: Vec<_> = (0..)
                .map(|i| 90000 + i * 2351)
                .take_while(|d| *d < 90000 + start + duration)
                .filter(|d| *d >= 90000 + start)
                .map(|dts| Frame {
                    track_id: 1,
                    dts,
                    pts_offset: 0,
                    key: true,
                    body: include_bytes!("../fixtures/codecs/mp3.bin").to_vec(),
                })
                .collect();
            let stamp = chrono::DateTime::from_timestamp(1700000000 + n as i64 * 6, 0)
                .unwrap()
                .format("%Y/%m/%d/%H/%M/%S")
                .to_string();
            signals += &format!("{n} {stamp}-{}\n", duration / 90);
            segments.insert(
                format!("owned/{stamp}.m4f"),
                Bytes::from(m4f::pack(&tracks, &frames, duration).unwrap()),
            );
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let epoch = Arc::new(Mutex::new(CancellationToken::new()));
        let (count, readers, current) = (calls.clone(), active.clone(), epoch.clone());
        let started = Arc::new(OnceLock::new());
        let clock = started.clone();
        let signals = Bytes::from(signals);
        let router = Router::new()
            .route(
                "/owned/m4f",
                get(move || {
                    let (signals, count, readers, current, clock) = (
                        signals.clone(),
                        count.clone(),
                        readers.clone(),
                        current.clone(),
                        clock.clone(),
                    );
                    async move {
                        clock.get_or_init(Instant::now);
                        count.fetch_add(1, Ordering::SeqCst);
                        readers.fetch_add(1, Ordering::SeqCst);
                        let stop = current.lock().unwrap().clone();
                        Body::from_stream(futures_util::stream::unfold(
                            (Some(signals), Active(readers), stop),
                            |(mut data, guard, stop)| async move {
                                if let Some(bytes) = data.take() {
                                    Some((Ok::<_, std::io::Error>(bytes), (data, guard, stop)))
                                } else {
                                    stop.cancelled().await;
                                    None
                                }
                            },
                        ))
                    }
                }),
            )
            .route(
                "/{*path}",
                get(
                    move |axum::extract::Path(path): axum::extract::Path<String>| {
                        let data = segments.get(&path).cloned();
                        async move { data.ok_or(axum::http::StatusCode::NOT_FOUND) }
                    },
                ),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("m4f://{}/owned", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        Self {
            url,
            calls,
            active,
            epoch,
            started,
            server,
        }
    }
    pub fn elapsed(&self) -> Duration {
        self.started.get().expect("source control opened").elapsed()
    }
    pub fn disconnect(&self) {
        let mut epoch = self.epoch.lock().unwrap();
        epoch.cancel();
        *epoch = CancellationToken::new();
    }
    pub async fn stop(mut self) {
        self.disconnect();
        tokio::time::timeout(Duration::from_secs(3), async {
            while self.active.load(Ordering::SeqCst) != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("owned source bodies reaped");
        self.server.abort();
        let _ = (&mut self.server).await;
    }
}
impl Drop for Source {
    fn drop(&mut self) {
        self.epoch.lock().unwrap().cancel();
        self.server.abort();
    }
}
