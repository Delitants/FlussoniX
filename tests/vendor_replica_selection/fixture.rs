use super::*;

pub(super) struct Origin {
    pub(super) key: String,
    pub(super) status: Arc<Mutex<StatusCode>>,
    pub(super) rows: Arc<Mutex<Value>>,
    pub(super) calls: Arc<AtomicUsize>,
    pub(super) private_calls: Arc<AtomicUsize>,
    pub(super) secret: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Origin {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Origin {
    pub(super) async fn start(id: &'static str) -> Self {
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let key = format!("m4s://{}", tcp.local_addr().unwrap());
        let secret = format!("owned-{id}-key");
        let status = Arc::new(Mutex::new(StatusCode::OK));
        let rows = Arc::new(Mutex::new(
            json!([{"name":NAME,"named_by":"config","static":true,"groups":[]}]),
        ));
        let calls = Arc::new(AtomicUsize::new(0));
        let private_calls = Arc::new(AtomicUsize::new(0));
        let (s, r, c, p, secret_copy) = (
            status.clone(),
            rows.clone(),
            calls.clone(),
            private_calls.clone(),
            secret.clone(),
        );
        let router = Router::new().fallback(get(
            move |OriginalUri(uri): OriginalUri, headers: HeaderMap| {
                let (s, r, c, p, secret) = (
                    s.clone(),
                    r.clone(),
                    c.clone(),
                    p.clone(),
                    secret_copy.clone(),
                );
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    if uri.path().ends_with("/m4s") {
                        p.fetch_add(1, Ordering::SeqCst);
                        assert!(!headers.contains_key("authorization"));
                        assert!(!headers.contains_key("x-flussonix-peer"));
                        assert!(uri.query().is_none());
                        let time = headers["cluster-auth-time"].to_str().unwrap();
                        let expected = ring::digest::digest(
                            &ring::digest::SHA1_FOR_LEGACY_USE_ONLY,
                            format!("{time}:{secret}:{NAME}").as_bytes(),
                        );
                        let expected: String = expected
                            .as_ref()
                            .iter()
                            .map(|b| format!("{b:02x}"))
                            .collect();
                        assert_eq!(headers["cluster-auth-key"].to_str().unwrap(), expected);
                        let track = flussonix::m4s::Track {
                            id: 1,
                            codec: "mp3".into(),
                            config: vec![],
                        };
                        let mut wire =
                            flussonix::wire::encode_info(std::slice::from_ref(&track)).unwrap();
                        for n in 0..30 {
                            wire.extend(
                                flussonix::wire::encode_frame(
                                    &track,
                                    &flussonix::m4f::Frame {
                                        track_id: 1,
                                        dts: 90_000 + n * 2160,
                                        pts_offset: 0,
                                        key: true,
                                        body: include_bytes!("../fixtures/codecs/mp3.bin").to_vec(),
                                    },
                                )
                                .unwrap(),
                            );
                        }
                        let stream = futures_util::stream::iter([Ok::<_, std::io::Error>(
                            bytes::Bytes::from(wire),
                        )])
                        .chain(futures_util::stream::pending());
                        return Response::builder()
                            .header("cluster-auth", "yes")
                            .body(Body::from_stream(stream))
                            .unwrap();
                    }
                    assert!(headers.contains_key("authorization"));
                    assert!(!headers.contains_key("cluster-auth-key"));
                    assert!(!headers.contains_key("x-flussonix-peer"));
                    let authorization = headers["authorization"].to_str().unwrap();
                    let (timestamp, hash) = authorization
                        .strip_prefix("Cluster ")
                        .unwrap()
                        .split_once(':')
                        .unwrap();
                    let digest = ring::digest::digest(
                        &ring::digest::SHA1_FOR_LEGACY_USE_ONLY,
                        format!("{timestamp}:{secret}").as_bytes(),
                    );
                    assert_eq!(
                        hash,
                        digest
                            .as_ref()
                            .iter()
                            .map(|b| format!("{b:02x}"))
                            .collect::<String>()
                    );
                    let status = *s.lock().unwrap();
                    if status != StatusCode::OK {
                        return Response::builder()
                            .status(status)
                            .body(Body::empty())
                            .unwrap();
                    }
                    let value = if uri.path().ends_with("/config") {
                        json!({"stats":{"id":id}})
                    } else {
                        assert_eq!(uri.path(), "/streamer/api/v3/streams");
                        json!({"server_id":id,"streams":r.lock().unwrap().clone()})
                    };
                    Response::builder()
                        .header("Content-Type", "application/json")
                        .body(Body::from(value.to_string()))
                        .unwrap()
                }
            },
        ));
        let task = tokio::spawn(async move {
            axum::serve(tcp, router).await.unwrap();
        });
        Self {
            key,
            status,
            rows,
            task,
            calls,
            private_calls,
            secret,
        }
    }
    pub(super) fn source(&self) -> Value {
        json!({"url":self.key,"flussonix_source_kind":"flussonic","cluster_key":self.secret,"on_play":"true"})
    }
}
pub(super) struct Fixture {
    pub(super) _dir: tempfile::TempDir,
    pub(super) config: Arc<ConfigStore>,
    pub(super) owner: Arc<ModernSourceCoordinator>,
    pub(super) origins: Vec<Origin>,
}
impl Fixture {
    pub(super) async fn new() -> Self {
        let origins = vec![
            Origin::start("replica-a").await,
            Origin::start("replica-b").await,
        ];
        let dir = tempfile::tempdir().unwrap();
        let config = Arc::new(ConfigStore::open(dir.path().join("config.json")).unwrap());
        for origin in &origins {
            config.put("sources", &origin.key, origin.source()).unwrap();
        }
        let owner =
            Arc::new(ModernSourceCoordinator::new(config.clone(), "owned-selector").unwrap());
        Self {
            _dir: dir,
            config,
            owner,
            origins,
        }
    }
    pub(super) fn selector(&self) -> Arc<ReplicaSelector> {
        Arc::new(
            ReplicaSelector::new(
                self.owner.clone(),
                "owned-replicas",
                self.origins
                    .iter()
                    .map(|origin| ReplicaMember {
                        source_key: origin.key.clone(),
                        content_id: "owned-music-namespace".into(),
                    })
                    .collect(),
            )
            .unwrap(),
        )
    }
    pub(super) fn handle(&self, index: usize) -> SourceHandle {
        self.owner
            .install(
                self.owner
                    .capture(&self.origins[index].key)
                    .unwrap()
                    .prepare(Duration::from_secs(3))
                    .unwrap(),
            )
            .unwrap()
    }
}
