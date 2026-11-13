//! Owned encrypted protocol controls, not a sustained media/link qualification.
use super::*;
use crate::fxlink::MAX_BLOCKS_PER_RECORD;
use crate::fxlink::transport::State;
use crate::fxlink::{Budget, Capabilities, ClientPool, Features, Receiver};
use bytes::Bytes;
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use std::sync::{Mutex, OnceLock, Weak};
use tokio::sync::Notify;
struct Observation {
    raw: u64,
    done: u64,
    state: Weak<State>,
    changed: Arc<Notify>,
}
static HINT_OBSERVER: OnceLock<Mutex<Option<Observation>>> = OnceLock::new();
pub(crate) fn observe_hint(state: &Arc<State>, done: bool) {
    let mut observer = HINT_OBSERVER.get_or_init(Default::default).lock().unwrap();
    if let Some(observer) = observer.as_mut() {
        observer.state = Arc::downgrade(state);
        if done {
            observer.done += 1;
        } else {
            observer.raw += 1;
        }
        observer.changed.notify_one();
    }
}
struct Observe;
impl Drop for Observe {
    fn drop(&mut self) {
        *HINT_OBSERVER.get_or_init(Default::default).lock().unwrap() = None;
    }
}
async fn seen_hint(count: u64, done: bool) -> Arc<State> {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let changed = {
                let guard = HINT_OBSERVER.get().unwrap().lock().unwrap();
                let observer = guard.as_ref().unwrap();
                if (if done { observer.done } else { observer.raw }) >= count {
                    return observer.state.upgrade().unwrap();
                }
                observer.changed.clone()
            };
            changed.notified().await;
        }
    })
    .await
    .expect("hint routing checkpoint not reached")
}
pub(crate) fn features() -> Features {
    Features {
        trimmed_records: true,
        repair_parity: true,
        bootstrap_repair: true,
        geometry_hints: true,
    }
}
fn record(length: usize) -> Bytes {
    let mut raw = vec![8; length];
    raw[..4].copy_from_slice(&((length - 4) as u32).to_be_bytes());
    raw.into()
}
struct Feed(Vec<Bytes>);
impl Provider for Feed {
    fn subscribe(&self, request: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
        let records = self.0.clone();
        Box::pin(async move {
            if request.peer_key != "owned-peer" || request.token != "owned-viewer" {
                return Err(403);
            }
            Ok(Source {
                pending_records: None,
                records: Box::pin(
                    futures_util::stream::iter(records.into_iter().map(Ok))
                        .chain(futures_util::stream::pending()),
                ),
                current: Arc::new(|| true),
                cancelled: CancellationToken::new(),
            })
        })
    }
}
fn forced_server(cert: &test_tls::Certificates, version: u8) -> quinn::ServerConfig {
    let mut tls = (*cert.server()).clone();
    tls.alpn_protocols = vec![format!("fxlink/{version}").into_bytes()];
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap(),
    ));
    config.transport_config(transport_config());
    config
}
async fn raw_client(
    address: SocketAddr,
    cert: &test_tls::Certificates,
    version: u8,
) -> (quinn::Endpoint, quinn::Connection) {
    let mut tls = (*cert.client()).clone();
    tls.alpn_protocols = vec![format!("fxlink/{version}").into_bytes()];
    let mut config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap(),
    ));
    config.transport_config(transport_config());
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(config);
    let connection = endpoint
        .connect(address, "localhost")
        .unwrap()
        .await
        .unwrap();
    assert_eq!(control::version(&connection).unwrap(), version);
    (endpoint, connection)
}
#[test]
fn geometry_advertisement_requires_all_four_and_preserves_exact_old_lists() {
    for (mask, old_highest) in [
        (0, 5),
        (1, 6),
        (2, 5),
        (3, 7),
        (4, 5),
        (5, 6),
        (6, 5),
        (7, 8),
    ] {
        for geometry in [false, true] {
            let features = Features {
                trimmed_records: mask & 1 != 0,
                repair_parity: mask & 2 != 0,
                bootstrap_repair: mask & 4 != 0,
                geometry_hints: geometry,
            };
            let highest = if mask == 7 && geometry {
                9
            } else {
                old_highest
            };
            let expected = (1..=highest)
                .rev()
                .map(|version| format!("fxlink/{version}").into_bytes())
                .collect::<Vec<_>>();
            assert_eq!(
                control::protocols_with_features(features),
                expected,
                "mask{mask}/geometry{geometry}"
            );
        }
    }
    assert_eq!(
        control::protocols_with_features(Features::default()),
        [
            b"fxlink/5".to_vec(),
            b"fxlink/4".to_vec(),
            b"fxlink/3".to_vec(),
            b"fxlink/2".to_vec(),
            b"fxlink/1".to_vec()
        ]
    );
    assert_eq!(Capabilities::for_version(9), Capabilities::V7);
}
#[tokio::test]
async fn all_eighteen_version_direction_exchanges_preserve_native_bytes() {
    let _serial = test_lock().lock().await;
    let baseline = global().used();
    for version in 1..=9 {
        for old_client in [false, true] {
            let cert = test_tls::Certificates::new();
            let expected = vec![record(12), record(500), record(9781)];
            let listener = Listener::bind_with_features(
                "127.0.0.1:0".parse().unwrap(),
                &cert.cert,
                &cert.key,
                Settings::default(),
                features(),
            )
            .unwrap();
            if !old_client {
                listener
                    .endpoint
                    .set_server_config(Some(forced_server(&cert, version)));
            }
            let address = listener.address();
            let stop = CancellationToken::new();
            let server =
                tokio::spawn(listener.serve(Arc::new(Feed(expected.clone())), stop.clone()));
            if old_client {
                let (endpoint, connection) = raw_client(address, &cert, version).await;
                let (mut send, mut recv) = connection.open_bi().await.unwrap();
                let id = send.id().index();
                control::write(
                    &mut send,
                    &Control::Subscribe {
                        version,
                        id,
                        generation: 1,
                        profile: control::profile(&Settings::default()),
                        name: "owned".into(),
                        peer_key: "owned-peer".into(),
                        token: "owned-viewer".into(),
                    },
                )
                .await
                .unwrap();
                assert!(matches!(
                    control::read(&mut recv).await.unwrap(),
                    Control::Accepted
                ));
                let budget = Budget::new(4 << 20);
                let mut decoder = Receiver::new(Settings::default(), id, 1, budget.clone())
                    .unwrap()
                    .with_capabilities(Capabilities::for_version(version));
                tokio::time::timeout(Duration::from_secs(3), async {
                    let mut received = 0;
                    while received < expected.len() {
                        let packet = connection.read_datagram().await.unwrap();
                        if packet.starts_with(b"FXGH") {
                            assert_eq!(version, 9);
                            let frame = crate::fxlink::geometry::wire::decode(
                                &packet,
                                &Settings::default(),
                            )
                            .unwrap();
                            assert_eq!((frame.subscription, frame.generation), (id, 1));
                            continue;
                        }
                        for native in decoder.ingest_charged(&packet, 0).unwrap() {
                            assert_eq!(native.bytes, expected[received]);
                            received += 1;
                        }
                    }
                })
                .await
                .unwrap();
                drop(decoder);
                assert_eq!(budget.used(), 0);
                connection.close(0u32.into(), b"done");
                endpoint.wait_idle().await;
            } else {
                let pool = ClientPool::with_features(features());
                let mut subscription = pool
                    .subscribe(
                        &format!("fxlink://{address}/owned"),
                        "owned-peer",
                        "owned-viewer",
                        Some(&cert.ca),
                        Settings::default(),
                        CancellationToken::new(),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    crate::fxlink::client::negotiated_version_for_test(&subscription),
                    version
                );
                for native in &expected {
                    assert_eq!(
                        tokio::time::timeout(Duration::from_secs(3), subscription.recv())
                            .await
                            .unwrap()
                            .unwrap(),
                        *native
                    );
                }
                drop(subscription);
                pool.close().await;
            }
            stop.cancel();
            server.await.unwrap().unwrap();
        }
    }
    assert_eq!(global().used(), baseline);
}

#[tokio::test]
async fn authenticated_hint_before_acceptance_has_no_state_or_replay_authority() {
    use crate::fxlink::geometry::wire::{self, Frame, Summary};
    let _serial = test_lock().lock().await;
    let cert = Arc::new(test_tls::Certificates::new());
    let server =
        quinn::Endpoint::server(forced_server(&cert, 9), "127.0.0.1:0".parse().unwrap()).unwrap();
    let address = server.local_addr().unwrap();
    let pool = Arc::new(ClientPool::with_features(features()));
    let client_pool = pool.clone();
    let client_cert = cert.clone();
    let connecting = tokio::spawn(async move {
        client_pool
            .subscribe(
                &format!("fxlink://{address}/owned"),
                "owned-peer",
                "owned-viewer",
                Some(&client_cert.ca),
                Settings::default(),
                CancellationToken::new(),
            )
            .await
    });
    let connection = server.accept().await.unwrap().await.unwrap();
    let (mut send, mut recv) = connection.accept_bi().await.unwrap();
    let Control::Subscribe {
        version,
        id,
        generation,
        ..
    } = control::read(&mut recv).await.unwrap()
    else {
        panic!("subscribe");
    };
    assert_eq!((version, generation), (9, 1));
    let _observe = Observe;
    *HINT_OBSERVER.get_or_init(Default::default).lock().unwrap() = Some(Observation {
        raw: 0,
        done: 0,
        state: Weak::new(),
        changed: Arc::new(Notify::new()),
    });
    let frame = Frame {
        subscription: id,
        generation,
        sequence: 1,
        count: 1,
        entries: std::array::from_fn(|i| {
            if i == 0 {
                Summary {
                    record_id: 0,
                    record_len: 4835,
                }
            } else {
                Summary::default()
            }
        }),
    };
    let wire = wire::encode(&frame, &Settings::default()).unwrap();
    let before = global().used();
    connection.send_datagram_wait(wire.clone()).await.unwrap();
    let state = seen_hint(1, false).await;
    assert!(!state.admitted.load(Ordering::Acquire));
    assert!(state.geometry.lock().unwrap().is_none());
    assert_eq!(global().used(), before);
    control::write(&mut send, &Control::Accepted).await.unwrap();
    let subscription = connecting.await.unwrap().unwrap();
    assert!(
        state.admitted.load(Ordering::Acquire),
        "Accepted did not activate the hint route"
    );
    let ingress = state
        .geometry
        .lock()
        .unwrap()
        .clone()
        .expect("healthy admitted route has no weak table");
    let baseline = global().used();
    connection.send_datagram_wait(wire).await.unwrap();
    seen_hint(2, false).await;
    seen_hint(1, true).await;
    assert_eq!(
        ingress.snapshot(now(), 0).unwrap().shape(0),
        Some(4835),
        "pre-Accepted hint poisoned replay"
    );
    assert_eq!(
        global().used(),
        baseline,
        "shape allocated a native media body"
    );
    assert_eq!(state.telemetry.stats.lock().unwrap().expired_records, 0);
    drop(subscription);
    pool.close().await;
    assert!(!state.admitted.load(Ordering::Acquire));
    assert_eq!(
        ingress.offer(&frame, now(), 0),
        crate::fxlink::geometry::receive::OfferResult::Disabled
    );
    connection.close(0u32.into(), b"done");
    server.close(0u32.into(), b"done");
    server.wait_idle().await;
}

struct RawSource {
    cert: Arc<test_tls::Certificates>,
    endpoint: quinn::Endpoint,
    connection: quinn::Connection,
    send: quinn::SendStream,
    _recv: quinn::RecvStream,
    pool: Arc<ClientPool>,
    subscription: crate::fxlink::Subscription,
    id: u64,
}
impl RawSource {
    async fn new(settings: Settings) -> Self {
        let cert = Arc::new(test_tls::Certificates::new());
        let endpoint =
            quinn::Endpoint::server(forced_server(&cert, 9), "127.0.0.1:0".parse().unwrap())
                .unwrap();
        let address = endpoint.local_addr().unwrap();
        let pool = Arc::new(ClientPool::with_features(features()));
        let child = pool.clone();
        let client_cert = cert.clone();
        let connect = tokio::spawn(async move {
            child
                .subscribe(
                    &format!("fxlink://{address}/owned"),
                    "owned-peer",
                    "owned-viewer",
                    Some(&client_cert.ca),
                    settings,
                    CancellationToken::new(),
                )
                .await
        });
        let connection = endpoint.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let Control::Subscribe {
            version: 9,
            id,
            generation: 1,
            ..
        } = control::read(&mut recv).await.unwrap()
        else {
            panic!("v9 subscribe")
        };
        control::write(&mut send, &Control::Accepted).await.unwrap();
        let subscription = connect.await.unwrap().unwrap();
        Self {
            cert,
            endpoint,
            connection,
            send,
            _recv: recv,
            pool,
            subscription,
            id,
        }
    }
    async fn finish(self) {
        drop(self.subscription);
        self.pool.close().await;
        self.connection.close(0u32.into(), b"done");
        self.endpoint.close(0u32.into(), b"done");
        self.endpoint.wait_idle().await;
    }
    async fn hints(&self, sequence: u64, entries: &[(u64, u32)]) {
        use crate::fxlink::geometry::wire::{self, Frame, Summary};
        let frame = Frame {
            subscription: self.id,
            generation: 1,
            sequence,
            count: entries.len(),
            entries: std::array::from_fn(|index| {
                entries
                    .get(index)
                    .map_or(Summary::default(), |(record_id, record_len)| Summary {
                        record_id: *record_id,
                        record_len: *record_len,
                    })
            }),
        };
        self.connection
            .send_datagram_wait(wire::encode(&frame, &Settings::default()).unwrap())
            .await
            .unwrap();
    }
}
#[tokio::test]
async fn malformed_replay_and_foreign_hints_are_atomic_and_strong_control_wins() {
    use crate::fxlink::geometry::wire::{self, Frame, Summary};
    let _serial = test_lock().lock().await;
    let mut source = RawSource::new(Settings::default()).await;
    let _observe = Observe;
    *HINT_OBSERVER.get_or_init(Default::default).lock().unwrap() = Some(Observation {
        raw: 0,
        done: 0,
        state: Weak::new(),
        changed: Arc::new(Notify::new()),
    });
    let frame = Frame {
        subscription: source.id,
        generation: 1,
        sequence: 10,
        count: 2,
        entries: std::array::from_fn(|i| {
            if i < 2 {
                Summary {
                    record_id: i as u64,
                    record_len: 4835,
                }
            } else {
                Summary::default()
            }
        }),
    };
    let valid = wire::encode(&frame, &Settings::default()).unwrap();
    let mut malformed = valid.to_vec();
    *malformed.last_mut().unwrap() = 0;
    // Make the last descriptor length zero, leaving the first valid.
    let end = malformed.len();
    malformed[end - 4..].fill(0);
    source
        .connection
        .send_datagram_wait(malformed.into())
        .await
        .unwrap();
    let state = seen_hint(1, true).await;
    let ingress = state.geometry.lock().unwrap().clone().unwrap();
    assert_eq!(
        ingress.snapshot(now(), 0).unwrap().shape(0),
        None,
        "partially applied malformed tail"
    );
    let baseline = global().used();
    for (id, generation) in [(source.id + 700, 1), (source.id, 2)] {
        let foreign = Frame {
            subscription: id,
            generation,
            ..frame.clone()
        };
        source
            .connection
            .send_datagram_wait(wire::encode(&foreign, &Settings::default()).unwrap())
            .await
            .unwrap();
    }
    source
        .connection
        .send_datagram_wait(valid.clone())
        .await
        .unwrap();
    seen_hint(2, true).await;
    assert_eq!(
        ingress.snapshot(now(), 0).unwrap().shape(0),
        Some(4835),
        "malformed or foreign frame consumed replay/work authority"
    );
    assert_eq!(global().used(), baseline);
    tokio::time::sleep(Duration::from_millis(21)).await;
    source.connection.send_datagram_wait(valid).await.unwrap();
    seen_hint(3, true).await;
    assert_eq!(ingress.snapshot(now(), 0).unwrap().shape(1), Some(4835));
    assert_eq!(ingress.diagnostics()["received_frames"], 3);
    assert_eq!(ingress.diagnostics()["received_application_bytes"], 168);
    assert_eq!(ingress.diagnostics()["usable_entries"], 2);
    assert_eq!(ingress.diagnostics()["malformed_frames"], 1);
    assert_eq!(ingress.diagnostics()["replay_frames"], 1);
    assert!(state.telemetry.snapshot()["geometry_hints"].is_object());
    control::write(
        &mut source.send,
        &Control::Announce {
            record: 0,
            len: 500,
        },
    )
    .await
    .unwrap();
    // Counters follows Announce on the same ordered stream: a real owner checkpoint.
    control::write(
        &mut source.send,
        &Control::Counters {
            data: 123,
            parity: 0,
            repair: 0,
        },
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.telemetry.sent_data.load(Ordering::Relaxed) != 123 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        ingress.snapshot(now(), 0).unwrap().shape(0),
        None,
        "strong Control did not invalidate conflicting weak geometry"
    );
    let mut encoder = Encoder::new(Settings::default(), source.id, 1, Budget::new(4 << 20))
        .unwrap()
        .with_capabilities(Capabilities::V7);
    drop(encoder.encode(record(500), now()).unwrap());
    let encoded = encoder.encode(record(500), now()).unwrap();
    source
        .connection
        .send_datagram_wait(encoded.datagrams[0].clone())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.routed_packets.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        ingress.snapshot(now(), 0).unwrap().shape(1),
        None,
        "strong native header did not invalidate weak geometry"
    );
    assert_eq!(ingress.diagnostics()["authority_conflicts"], 2);
    source.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hint_ingress_runs_while_receiver_decode_and_progress_writer_are_held() {
    use crate::fxlink::client::review_tests::{PauseGuard, ProgressGuard};
    let _serial = test_lock().lock().await;
    let mut source = RawSource::new(Settings::default()).await;
    let _observe = Observe;
    *HINT_OBSERVER.get_or_init(Default::default).lock().unwrap() = Some(Observation {
        raw: 0,
        done: 0,
        state: Weak::new(),
        changed: Arc::new(Notify::new()),
    });
    let mut encoder = Encoder::new(Settings::default(), source.id, 1, Budget::new(4 << 20))
        .unwrap()
        .with_capabilities(Capabilities::V7);
    let progress = ProgressGuard::new();
    let first = encoder.encode(record(387), now()).unwrap();
    source
        .connection
        .send_datagram_wait(first.datagrams[0].clone())
        .await
        .unwrap();
    source.subscription.recv().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !progress.entered() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let pause = PauseGuard::new();
    drop(encoder.encode(record(4835), now()).unwrap());
    let successor = encoder.encode(record(387), now()).unwrap();
    source
        .connection
        .send_datagram_wait(successor.datagrams[0].clone())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !pause.entered() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    source.hints(1, &[(1, 4835)]).await;
    let state = seen_hint(1, true).await;
    let ingress = state.geometry.lock().unwrap().clone().unwrap();
    assert_eq!(ingress.snapshot(now(), 1).unwrap().shape(1), Some(4835));
    assert!(state.current());
    drop((pause, progress));
    source.finish().await;
}

#[tokio::test]
async fn retired_route_and_foreign_generation_do_not_change_a_live_sibling() {
    use crate::fxlink::geometry::wire::{self, Frame, Summary};
    let _serial = test_lock().lock().await;
    let mut source = RawSource::new(Settings::default()).await;
    let _observe = Observe;
    *HINT_OBSERVER.get_or_init(Default::default).lock().unwrap() = Some(Observation {
        raw: 0,
        done: 0,
        state: Weak::new(),
        changed: Arc::new(Notify::new()),
    });
    source.hints(1, &[(0, 4835)]).await;
    let old = seen_hint(1, true).await;
    let ingress = old.geometry.lock().unwrap().clone().unwrap();
    let pool = source.pool.clone();
    let cert = source.cert.clone();
    let address = source.endpoint.local_addr().unwrap();
    let connecting = tokio::spawn(async move {
        pool.subscribe(
            &format!("fxlink://{address}/sibling"),
            "owned-peer",
            "owned-viewer",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new(),
        )
        .await
    });
    let (mut send, mut recv) = source.connection.accept_bi().await.unwrap();
    let Control::Subscribe {
        id,
        generation: 1,
        version: 9,
        ..
    } = control::read(&mut recv).await.unwrap()
    else {
        panic!("sibling")
    };
    control::write(&mut send, &Control::Accepted).await.unwrap();
    let sibling = connecting.await.unwrap().unwrap();
    assert_eq!(source.subscription.connection_id(), sibling.connection_id());
    drop(std::mem::replace(&mut source.subscription, sibling));
    assert!(!old.admitted.load(Ordering::Acquire));
    assert!(ingress.snapshot(now(), 0).is_none());
    // Retired source and wrong-generation sibling traffic cannot use sibling work/replay state.
    source.hints(2, &[(0, 9781)]).await;
    let mut frame = Frame {
        subscription: id,
        generation: 2,
        sequence: 1,
        count: 1,
        entries: std::array::from_fn(|i| {
            if i == 0 {
                Summary {
                    record_id: 0,
                    record_len: 9781,
                }
            } else {
                Summary::default()
            }
        }),
    };
    source
        .connection
        .send_datagram_wait(wire::encode(&frame, &Settings::default()).unwrap())
        .await
        .unwrap();
    frame.generation = 1;
    frame.entries[0].record_len = 4835;
    source
        .connection
        .send_datagram_wait(wire::encode(&frame, &Settings::default()).unwrap())
        .await
        .unwrap();
    let live = seen_hint(2, true).await;
    assert!(live.current());
    assert!(!Arc::ptr_eq(&old, &live));
    let table = live.geometry.lock().unwrap().clone().unwrap();
    assert_eq!(table.snapshot(now(), 0).unwrap().shape(0), Some(4835));
    assert_eq!(table.diagnostics()["received_frames"], 1);
    assert_eq!(table.diagnostics()["replay_frames"], 0);
    source.finish().await;
}
#[tokio::test]
async fn useful_hint_needs_strong_successor_then_requests_exact_shapes_and_decodes_native_bytes() {
    let _serial = test_lock().lock().await;
    let baseline = global().used();
    let settings = Settings::default();
    let mut source = RawSource::new(settings.clone()).await;
    let _observe = Observe;
    *HINT_OBSERVER.get_or_init(Default::default).lock().unwrap() = Some(Observation {
        raw: 0,
        done: 0,
        state: Weak::new(),
        changed: Arc::new(Notify::new()),
    });
    let expected = [record(4835), record(9781), record(387)];
    let budget = Budget::new(4 << 20);
    let mut encoder = Encoder::new(settings.clone(), source.id, 1, budget.clone())
        .unwrap()
        .with_capabilities(Capabilities::V7);
    let encoded = expected
        .iter()
        .map(|bytes| encoder.encode(bytes.clone(), now()).unwrap())
        .collect::<Vec<_>>();
    source.hints(1, &[(0, 4835), (1, 9781)]).await;
    let state = seen_hint(1, true).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(40), source.connection.read_datagram())
            .await
            .is_err(),
        "weak shapes invented a native gap"
    );
    for packet in &encoded[2].datagrams {
        source
            .connection
            .send_datagram_wait(packet.clone())
            .await
            .unwrap();
    }
    let routes = super::super::feedback::Routes::default();
    let mut route = routes.admit(source.id, 9, &budget).unwrap();
    let mut repaired = [false; 2];
    tokio::time::timeout(Duration::from_secs(2), async {
        while !repaired.iter().all(|done| *done) {
            let frame = source.connection.read_datagram().await.unwrap();
            assert_eq!(&frame[..8], b"FXRQ\x04\x00\x00\x20");
            routes.receive(frame, now());
            // One bounded frame may contain both record requests.
            while let Some(request) = route.next(&settings).now_or_never() {
                let super::super::feedback::DatagramRequest::Ordinary(request) = request else {
                    panic!("useful shape remained a bootstrap")
                };
                let record = (request.block_id / MAX_BLOCKS_PER_RECORD as u64) as usize;
                if record > 1 || repaired[record] {
                    continue;
                }
                if record == 0 {
                    assert_eq!(
                        request.shard_indices,
                        vec![0, 1, 2, 3, 4, 5],
                        "hint was not used for the unknown head"
                    );
                }
                for packet in &encoded[record].datagrams {
                    let header = Header::parse(packet).unwrap();
                    if header.block_id == request.block_id
                        && request.shard_indices.contains(&header.shard_index)
                    {
                        source
                            .connection
                            .send_datagram_wait(packet.clone())
                            .await
                            .unwrap();
                    }
                }
                repaired[record] =
                    request.shard_indices.len() >= settings.fragment_count(expected[record].len());
            }
        }
        for expected in &expected {
            assert_eq!(source.subscription.recv().await.unwrap(), expected.clone());
        }
    })
    .await
    .unwrap();
    assert_eq!(state.telemetry.stats.lock().unwrap().expired_records, 0);
    drop((route, encoded, encoder));
    source.finish().await;
    drop(state);
    assert_eq!(budget.used(), 0);
    assert_eq!(global().used(), baseline);
}

#[tokio::test]
async fn preaccepted_media_queue_loss_cannot_enable_late_conflicting_hints() {
    use crate::fxlink::geometry::wire::{self, Frame, Summary};
    let _serial = test_lock().lock().await;
    let cert = Arc::new(test_tls::Certificates::new());
    let server =
        quinn::Endpoint::server(forced_server(&cert, 9), "127.0.0.1:0".parse().unwrap()).unwrap();
    let address = server.local_addr().unwrap();
    let pool = Arc::new(ClientPool::with_features(features()));
    let child = pool.clone();
    let client_cert = cert.clone();
    let connecting = tokio::spawn(async move {
        child
            .subscribe(
                &format!("fxlink://{address}/owned"),
                "owned-peer",
                "owned-viewer",
                Some(&client_cert.ca),
                Settings::default(),
                CancellationToken::new(),
            )
            .await
    });
    let connection = server.accept().await.unwrap().await.unwrap();
    let (mut send, mut recv) = connection.accept_bi().await.unwrap();
    let Control::Subscribe {
        id, generation: 1, ..
    } = control::read(&mut recv).await.unwrap()
    else {
        panic!("subscribe")
    };
    let _observe = Observe;
    *HINT_OBSERVER.get_or_init(Default::default).lock().unwrap() = Some(Observation {
        raw: 0,
        done: 0,
        state: Weak::new(),
        changed: Arc::new(Notify::new()),
    });
    let make_hint = |sequence, record_id, record_len| Frame {
        subscription: id,
        generation: 1,
        sequence,
        count: 1,
        entries: std::array::from_fn(|i| {
            if i == 0 {
                Summary {
                    record_id,
                    record_len,
                }
            } else {
                Summary::default()
            }
        }),
    };
    // The pre-admission hint is only a routing checkpoint: it has no authority.
    connection
        .send_datagram_wait(wire::encode(&make_hint(1, 0, 4835), &Settings::default()).unwrap())
        .await
        .unwrap();
    let state = seen_hint(1, false).await;
    assert!(!state.admitted.load(Ordering::Acquire));
    let mut encoder = Encoder::new(Settings::default(), id, 1, Budget::new(4 << 20))
        .unwrap()
        .with_capabilities(Capabilities::V7);
    let head = encoder.encode(record(4835), now()).unwrap();
    let middle = encoder.encode(record(9781), now()).unwrap();
    for sent in 1..=crate::fxlink::packet_queue::CAPACITY {
        connection
            .send_datagram_wait(head.datagrams[0].clone())
            .await
            .unwrap();
        if sent % 16 == 0 {
            tokio::time::timeout(Duration::from_secs(2), async {
                while state.routed_packets.load(Ordering::Acquire) < sent as u64 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
    }
    connection
        .send_datagram_wait(middle.datagrams[0].clone())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.telemetry.queue_dropped.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(state.geometry.lock().unwrap().is_none());
    control::write(&mut send, &Control::Accepted).await.unwrap();
    let subscription = connecting.await.unwrap().unwrap();
    connection
        .send_datagram_wait(wire::encode(&make_hint(2, 1, 4835), &Settings::default()).unwrap())
        .await
        .unwrap();
    seen_hint(2, false).await;
    seen_hint(1, true).await;
    let late_shape = state
        .geometry
        .lock()
        .unwrap()
        .as_ref()
        .and_then(|ingress| ingress.snapshot(now(), state.published()))
        .and_then(|snapshot| snapshot.shape(1));
    let version = subscription.telemetry().snapshot()["protocol_version"].clone();
    drop(subscription);
    pool.close().await;
    connection.close(0u32.into(), b"done");
    server.close(0u32.into(), b"done");
    server.wait_idle().await;
    assert_eq!(version, 9, "local fallback changed negotiation");
    assert_eq!(
        late_shape, None,
        "header lost before Accepted was replaced by a late weak hint"
    );
}
