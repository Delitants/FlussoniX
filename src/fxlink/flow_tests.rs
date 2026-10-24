//! Encrypted real-listener tests of record credit, independent of byte flow control.
use super::{
    Listener, Provider, Request, Settings, Source,
    control::{self, Control},
    packet::Header,
    transport,
};
use bytes::Bytes;
use futures_util::{StreamExt, future::BoxFuture};
use std::{
    collections::BTreeSet,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

fn record(sequence: usize) -> Bytes {
    let mut bytes = vec![sequence as u8; 1000];
    bytes[..4].copy_from_slice(&996u32.to_be_bytes());
    Bytes::from(bytes)
}
struct ReadyFeed(Arc<AtomicUsize>, Arc<AtomicBool>);
impl Provider for ReadyFeed {
    fn subscribe(&self, request: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
        let polled = self.0.clone();
        let alive = self.1.clone();
        Box::pin(async move {
            if request.peer_key != "owned-peer" || request.token != "owned-viewer" {
                return Err(403);
            }
            Ok(Source {
                pending_records: None,
                records: Box::pin(
                    futures_util::stream::iter(0..160)
                        .map(move |sequence| {
                            polled.fetch_add(1, Ordering::SeqCst);
                            Ok(record(sequence))
                        })
                        .chain(futures_util::stream::pending()),
                ),
                current: Arc::new(move || alive.load(Ordering::Acquire)),
                cancelled: CancellationToken::new(),
            })
        })
    }
}

async fn raw_client(
    cert: &crate::transport_tls_fixture::Certificates,
    remote: SocketAddr,
    protocols: &[&[u8]],
) -> (quinn::Endpoint, quinn::Connection, u8) {
    let mut tls = (*cert.client()).clone();
    tls.alpn_protocols = protocols.iter().map(|p| p.to_vec()).collect();
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap();
    let mut config = quinn::ClientConfig::new(Arc::new(crypto));
    config.transport_config(transport::config());
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(config);
    let connection = endpoint
        .connect(remote, "localhost")
        .unwrap()
        .await
        .unwrap();
    let data = connection
        .handshake_data()
        .unwrap()
        .downcast::<quinn::crypto::rustls::HandshakeData>()
        .unwrap();
    let version = match data.protocol.as_deref() {
        Some(b"fxlink/5") => 5,
        Some(b"fxlink/4") => 4,
        Some(b"fxlink/3") => 3,
        Some(b"fxlink/2") => 2,
        Some(b"fxlink/1") => 1,
        _ => panic!("unexpected ALPN"),
    };
    (endpoint, connection, version)
}

async fn progress(send: &mut quinn::SendStream, next: u64) {
    // Literal frame also lets this test run against the pre-v2 implementation.
    let body = serde_json::to_vec(&serde_json::json!({"Progress":{"next":next}})).unwrap();
    send.write_all(&(body.len() as u32).to_be_bytes())
        .await
        .unwrap();
    send.write_all(&body).await.unwrap();
}

async fn receive_for(connection: &quinn::Connection, duration: Duration) -> Vec<u64> {
    let deadline = tokio::time::Instant::now() + duration;
    let mut records = Vec::new();
    loop {
        tokio::select! {
            _=tokio::time::sleep_until(deadline)=>break,
            packet=connection.read_datagram()=>{
                let Ok(packet)=packet else {break};
                assert!(records.len()<512,"fixture datagram bound");
                records.push(Header::parse(&packet).unwrap().record_id);
            }
        }
    }
    records
}

struct SourcePeer {
    endpoint: quinn::Endpoint,
    connection: quinn::Connection,
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    stop: CancellationToken,
    serving: tokio::task::JoinHandle<Result<(), String>>,
    polled: Arc<AtomicUsize>,
    alive: Arc<AtomicBool>,
    baseline: usize,
}
impl SourcePeer {
    async fn new(protocols: &[&[u8]], latency_ms: u64) -> Self {
        let baseline = transport::global().used();
        let cert = crate::transport_tls_fixture::Certificates::new();
        let listener = Listener::bind(
            "127.0.0.1:0".parse().unwrap(),
            &cert.cert,
            &cert.key,
            Settings::default(),
        )
        .unwrap();
        let remote = listener.address();
        let stop = CancellationToken::new();
        let polled = Arc::new(AtomicUsize::new(0));
        let alive = Arc::new(AtomicBool::new(true));
        let serving = tokio::spawn(listener.serve(
            Arc::new(ReadyFeed(polled.clone(), alive.clone())),
            stop.clone(),
        ));
        let (endpoint, connection, version) = raw_client(&cert, remote, protocols).await;
        let (mut send, mut recv) = connection.open_bi().await.unwrap();
        let id = send.id().index();
        let settings = Settings {
            max_records: 4,
            latency_ms,
            ..Settings::default()
        };
        control::write(
            &mut send,
            &Control::Subscribe {
                version,
                id,
                generation: 1,
                profile: control::profile(&settings),
                name: "live".into(),
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
        Self {
            endpoint,
            connection,
            send,
            recv,
            stop,
            serving,
            polled,
            alive,
            baseline,
        }
    }
    async fn reset(&mut self, within: Duration) -> bool {
        tokio::time::timeout(within, async {
            while control::read(&mut self.recv).await.is_ok() {}
        })
        .await
        .is_ok()
    }
    async fn close(self) {
        let Self {
            endpoint,
            connection,
            send,
            recv,
            stop,
            serving,
            baseline,
            ..
        } = self;
        connection.close(0u32.into(), b"test complete");
        drop(send);
        drop(recv);
        endpoint.wait_idle().await;
        stop.cancel();
        serving.await.unwrap().unwrap();
        assert_eq!(
            transport::global().used(),
            baseline,
            "source peer leaked reservations"
        );
    }
}

#[tokio::test]
async fn legacy_v1_peer_keeps_media_without_record_progress() {
    let _serial = transport::test_lock().lock().await;
    let peer = SourcePeer::new(&[b"fxlink/1"], 1000).await;
    let received = receive_for(&peer.connection, Duration::from_millis(150)).await;
    let admitted = peer.polled.load(Ordering::SeqCst);
    peer.close().await;
    assert_eq!(
        admitted, 160,
        "legacy source incorrectly required new feedback"
    );
    assert_eq!(
        received.into_iter().collect::<BTreeSet<_>>(),
        (0..160).collect()
    );
}

#[tokio::test]
async fn invalid_progress_resets_only_its_subscription() {
    let _serial = transport::test_lock().lock().await;
    for decreasing in [false, true] {
        let mut peer = SourcePeer::new(&[b"fxlink/2"], 1000).await;
        let initial = receive_for(&peer.connection, Duration::from_millis(150)).await;
        if decreasing {
            progress(&mut peer.send, 4).await;
            receive_for(&peer.connection, Duration::from_millis(150)).await;
        }
        progress(&mut peer.send, if decreasing { 3 } else { 5 }).await;
        let reset = peer.reset(Duration::from_millis(400)).await;
        let connected = peer.connection.close_reason().is_none();
        let (mut send, mut recv) = peer.connection.open_bi().await.unwrap();
        let id = send.id().index();
        let settings = Settings {
            max_records: 4,
            ..Settings::default()
        };
        control::write(
            &mut send,
            &Control::Subscribe {
                version: 2,
                id,
                generation: 1,
                profile: control::profile(&settings),
                name: "sibling".into(),
                peer_key: "owned-peer".into(),
                token: "owned-viewer".into(),
            },
        )
        .await
        .unwrap();
        let accepted = matches!(control::read(&mut recv).await.unwrap(), Control::Accepted);
        let sibling = receive_for(&peer.connection, Duration::from_millis(150)).await;
        drop(send);
        drop(recv);
        peer.close().await;
        assert_eq!(
            initial.into_iter().collect::<BTreeSet<_>>(),
            (0..4).collect()
        );
        assert!(
            reset && connected && accepted,
            "bad progress affected the shared connection"
        );
        assert_eq!(
            sibling.into_iter().collect::<BTreeSet<_>>(),
            (0..4).collect()
        );
    }
}

#[tokio::test]
async fn delayed_credit_survives_cache_expiry_without_serving_expired_repairs() {
    let _serial = transport::test_lock().lock().await;
    let mut peer = SourcePeer::new(&[b"fxlink/2"], 500).await;
    let initial = receive_for(&peer.connection, Duration::from_millis(150)).await;
    tokio::time::sleep(Duration::from_millis(450)).await;
    control::write(
        &mut peer.send,
        &Control::Repair {
            block: 0,
            shards: vec![0],
        },
    )
    .await
    .unwrap();
    progress(&mut peer.send, 4).await;
    let resumed = receive_for(&peer.connection, Duration::from_millis(150)).await;
    let admitted = peer.polled.load(Ordering::SeqCst);
    peer.close().await;
    assert_eq!(
        initial.into_iter().collect::<BTreeSet<_>>(),
        (0..4).collect()
    );
    assert_eq!(
        resumed.into_iter().collect::<BTreeSet<_>>(),
        (4..8).collect(),
        "expired original was repaired or valid returning credit was fenced"
    );
    assert_eq!(admitted, 8);
}

#[tokio::test]
async fn duplicate_feedback_cannot_keep_a_full_window_alive() {
    let _serial = transport::test_lock().lock().await;
    let mut peer = SourcePeer::new(&[b"fxlink/2"], 500).await;
    receive_for(&peer.connection, Duration::from_millis(150)).await;
    for _ in 0..6 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        progress(&mut peer.send, 0).await;
    }
    let reset = peer.reset(Duration::from_millis(800)).await;
    let admitted = peer.polled.load(Ordering::SeqCst);
    peer.close().await;
    assert!(reset, "duplicate acknowledgements renewed the credit stall");
    assert_eq!(admitted, 4);
}

#[tokio::test]
async fn source_policy_revocation_remains_prompt_while_credit_is_full() {
    let _serial = transport::test_lock().lock().await;
    let mut peer = SourcePeer::new(&[b"fxlink/2"], 1000).await;
    receive_for(&peer.connection, Duration::from_millis(150)).await;
    peer.alive.store(false, Ordering::Release);
    let reset = peer.reset(Duration::from_millis(300)).await;
    let admitted = peer.polled.load(Ordering::SeqCst);
    peer.close().await;
    assert!(reset, "credit wait hid policy revocation");
    assert_eq!(admitted, 4);
}

#[tokio::test]
async fn subscribe_version_must_match_authenticated_alpn() {
    let _serial = transport::test_lock().lock().await;
    for version in [1u8, 2] {
        let protocol = if version == 1 {
            b"fxlink/1".as_slice()
        } else {
            b"fxlink/2".as_slice()
        };
        let peer = SourcePeer::new(&[protocol], 1000).await;
        receive_for(&peer.connection, Duration::from_millis(150)).await;
        let (mut send, mut recv) = peer.connection.open_bi().await.unwrap();
        let id = send.id().index();
        control::write(
            &mut send,
            &Control::Subscribe {
                version: 3 - version,
                id,
                generation: 1,
                profile: control::profile(&Settings::default()),
                name: "wrong-version".into(),
                peer_key: "owned-peer".into(),
                token: "owned-viewer".into(),
            },
        )
        .await
        .unwrap();
        let rejected = matches!(
            tokio::time::timeout(Duration::from_millis(300), control::read(&mut recv)).await,
            Ok(Err(_))
        );
        let connected = peer.connection.close_reason().is_none();
        drop(send);
        drop(recv);
        peer.close().await;
        assert!(
            rejected && connected,
            "version mismatch admitted media or closed unrelated subscriptions"
        );
    }
}

#[tokio::test]
async fn blocked_progress_writer_keeps_decoding_and_resumes_intact_feedback() {
    feedback_backpressure(true).await;
}

#[tokio::test]
async fn blocked_progress_writer_cannot_extend_record_lifetime() {
    feedback_backpressure(false).await;
}

async fn feedback_backpressure(resume: bool) {
    let _serial = transport::test_lock().lock().await;
    let before = transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let mut tls = (*cert.server()).clone();
    tls.alpn_protocols = vec![b"fxlink/2".to_vec()];
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    let mut wire = transport::config();
    // Reading Subscribe replenishes this one-byte credit. The first framed
    // Progress cannot finish until the peer starts reading feedback again.
    Arc::get_mut(&mut wire)
        .unwrap()
        .stream_receive_window(1u32.into());
    config.transport_config(wire);
    let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let url = format!("fxlink://{}/live", endpoint.local_addr().unwrap());
    let remote = endpoint.clone();
    let received = Arc::new(AtomicUsize::new(0));
    let delivered = received.clone();
    let completed = CancellationToken::new();
    let acknowledged = completed.clone();
    let server = tokio::spawn(async move {
        let connection = remote.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let Control::Subscribe {
            id,
            generation,
            version,
            ..
        } = control::read(&mut recv).await.unwrap()
        else {
            panic!("expected subscription")
        };
        assert_eq!(version, 2);
        control::write(&mut send, &Control::Accepted).await.unwrap();
        let began = tokio::time::Instant::now();
        let mut encoder = super::Encoder::new(
            Settings::default(),
            id,
            generation,
            super::Budget::new(1 << 20),
        )
        .unwrap();
        let mut missing = None;
        for sequence in 0..64 {
            tokio::time::sleep_until(began + Duration::from_millis(sequence as u64 * 5)).await;
            let encoded = encoder.encode(record(sequence), transport::now()).unwrap();
            control::write(
                &mut send,
                &Control::Announce {
                    record: sequence as u64,
                    len: 1000,
                },
            )
            .await
            .unwrap();
            if sequence == 50 {
                missing = Some(encoded);
                continue;
            }
            for packet in encoded.datagrams {
                connection.send_datagram_wait(packet).await.unwrap();
            }
        }
        tokio::time::sleep_until(began + Duration::from_millis(if resume { 420 } else { 1500 }))
            .await;
        let during_stall = delivered.load(Ordering::SeqCst);
        let mut last = 0;
        while let Ok(message) = control::read(&mut recv).await {
            match message {
                Control::Progress { next } => {
                    assert!(next >= last && next <= 64);
                    last = next;
                    if next == 64 {
                        acknowledged.cancel();
                    }
                }
                Control::Repair { block, shards } => {
                    assert_eq!(block, 50 * super::MAX_BLOCKS_PER_RECORD as u64);
                    assert_eq!(shards, [0]);
                    if let Some(encoded) = missing.take() {
                        let age = transport::now().saturating_sub(encoded.created_ms);
                        if age + connection.rtt().as_millis() as u64 + 5 < 1000 {
                            for packet in encoded.datagrams {
                                connection.send_datagram_wait(packet).await.unwrap();
                            }
                        }
                    }
                }
                _ => panic!("invalid feedback"),
            }
        }
        (during_stall, last)
    });
    let pool = super::ClientPool::default();
    let mut elapsed = Duration::ZERO;
    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        let mut sub = pool
            .subscribe(
                &url,
                "owned-peer",
                "owned-viewer",
                Some(&cert.ca),
                Settings::default(),
                CancellationToken::new(),
            )
            .await?;
        let began = std::time::Instant::now();
        let result = async {
            let mut records = Vec::new();
            for _ in 0..64 {
                records.push(sub.recv().await?);
                received.fetch_add(1, Ordering::SeqCst);
            }
            completed.cancelled().await;
            Ok::<_, String>(records)
        }
        .await;
        elapsed = began.elapsed();
        result
    })
    .await;
    pool.close().await;
    let (during_stall, last) = server.await.unwrap();
    endpoint.close(0u32.into(), b"test complete");
    endpoint.wait_idle().await;
    assert_eq!(
        transport::global().used(),
        before,
        "blocked feedback leaked reservations"
    );
    assert_eq!(
        during_stall, 50,
        "feedback backpressure blocked original decoding"
    );
    if resume {
        assert_eq!(
            outcome
                .expect("bounded feedback resume")
                .expect("partial progress/repair frames corrupted"),
            (0..64).map(record).collect::<Vec<_>>()
        );
        assert_eq!(last, 64);
    } else {
        assert!(outcome.unwrap().is_err());
        assert!(
            elapsed >= Duration::from_millis(800) && elapsed < Duration::from_millis(1300),
            "feedback stall changed original lifetime: {elapsed:?}"
        );
    }
}

#[tokio::test]
async fn full_record_credit_pauses_source_but_services_repair_and_resumes_on_progress() {
    let _serial = transport::test_lock().lock().await;
    for limit in [4, 64] {
        let before = transport::global().used();
        let cert = crate::transport_tls_fixture::Certificates::new();
        let listener = Listener::bind(
            "127.0.0.1:0".parse().unwrap(),
            &cert.cert,
            &cert.key,
            Settings::default(),
        )
        .unwrap();
        let remote = listener.address();
        let stop = CancellationToken::new();
        let polled = Arc::new(AtomicUsize::new(0));
        let serving = tokio::spawn(listener.serve(
            Arc::new(ReadyFeed(polled.clone(), Arc::new(AtomicBool::new(true)))),
            stop.clone(),
        ));
        let (endpoint, connection, version) =
            raw_client(&cert, remote, &[b"fxlink/2", b"fxlink/1"]).await;
        let (mut send, mut recv) = connection.open_bi().await.unwrap();
        let id = send.id().index();
        let settings = Settings {
            max_records: limit,
            ..Settings::default()
        };
        control::write(
            &mut send,
            &Control::Subscribe {
                version,
                id,
                generation: 1,
                profile: control::profile(&settings),
                name: "live".into(),
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
        let initial = receive_for(&connection, Duration::from_millis(150)).await;
        let first_polled = polled.load(Ordering::SeqCst);
        control::write(
            &mut send,
            &Control::Repair {
                block: 0,
                shards: vec![0],
            },
        )
        .await
        .unwrap();
        let repair = receive_for(&connection, Duration::from_millis(100)).await;
        let mut resumed = Vec::new();
        if version == 2 {
            progress(&mut send, limit as u64).await;
            resumed = receive_for(&connection, Duration::from_millis(150)).await;
        }
        let after_progress = polled.load(Ordering::SeqCst);
        connection.close(0u32.into(), b"test complete");
        drop(send);
        drop(recv);
        endpoint.wait_idle().await;
        stop.cancel();
        serving.await.unwrap().unwrap();
        assert_eq!(
            transport::global().used(),
            before,
            "record credit leaked reservations"
        );
        assert_eq!(
            first_polled, limit,
            "source ran past a stalled {limit}-record receive window"
        );
        assert_eq!(version, 2);
        assert_eq!(
            initial.into_iter().collect::<BTreeSet<_>>(),
            (0..limit as u64).collect()
        );
        assert_eq!(
            repair,
            vec![0],
            "full credit window blocked a useful repair"
        );
        assert_eq!(after_progress, limit * 2);
        assert_eq!(
            resumed.into_iter().collect::<BTreeSet<_>>(),
            (limit as u64..limit as u64 * 2).collect()
        );
    }
}

#[tokio::test]
async fn client_returns_cumulative_credit_to_a_v2_only_peer() {
    let _serial = transport::test_lock().lock().await;
    let before = transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let mut tls = (*cert.server()).clone();
    tls.alpn_protocols = vec![b"fxlink/2".to_vec()];
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    config.transport_config(transport::config());
    let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let url = format!("fxlink://{}/live", endpoint.local_addr().unwrap());
    let remote = endpoint.clone();
    let settings = Settings {
        max_records: 4,
        ..Settings::default()
    };
    let encoding = settings.clone();
    let completed = CancellationToken::new();
    let acknowledged_all = completed.clone();
    let server = tokio::spawn(async move {
        let Ok(connection) = remote.accept().await.unwrap().await else {
            return (0, Vec::new());
        };
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let Control::Subscribe {
            version,
            id,
            generation,
            profile,
            peer_key,
            token,
            ..
        } = control::read(&mut recv).await.unwrap()
        else {
            panic!("expected subscription")
        };
        assert_eq!(version, 2);
        assert_eq!(profile[5], 4);
        assert_eq!(
            (peer_key.as_str(), token.as_str()),
            ("owned-peer", "owned-viewer")
        );
        control::write(&mut send, &Control::Accepted).await.unwrap();
        let mut encoder =
            super::Encoder::new(encoding, id, generation, super::Budget::new(1 << 20)).unwrap();
        let (mut sent, mut acknowledged) = (0u64, 0u64);
        let mut progress = Vec::new();
        while acknowledged < 80 {
            while sent < 80 && sent - acknowledged < 4 {
                let encoded = encoder
                    .encode(record(sent as usize), transport::now())
                    .unwrap();
                control::write(
                    &mut send,
                    &Control::Announce {
                        record: sent,
                        len: 1000,
                    },
                )
                .await
                .unwrap();
                for packet in encoded.datagrams {
                    connection.send_datagram_wait(packet).await.unwrap();
                }
                sent += 1;
            }
            match tokio::time::timeout(Duration::from_millis(500), control::read(&mut recv)).await {
                Ok(Ok(Control::Progress { next })) => {
                    assert!(next >= acknowledged && next <= sent);
                    acknowledged = next;
                    progress.push(next);
                    assert!(progress.len() <= 80, "unbounded feedback");
                }
                Ok(Ok(Control::Repair { .. })) => {}
                _ => {
                    let _ = send.reset(1u32.into());
                    break;
                }
            }
        }
        if acknowledged == 80 {
            acknowledged_all.cancel();
        }
        // The client may have delivered the last record before its feedback
        // task runs; hold the stream until the test closes the pool.
        let _ = control::read(&mut recv).await;
        (sent, progress)
    });
    let pool = super::ClientPool::default();
    let outcome = tokio::time::timeout(Duration::from_secs(5), async {
        let mut sub = pool
            .subscribe(
                &url,
                "owned-peer",
                "owned-viewer",
                Some(&cert.ca),
                settings,
                CancellationToken::new(),
            )
            .await?;
        let mut received = Vec::new();
        for _ in 0..80 {
            received.push(sub.recv().await?);
        }
        completed.cancelled().await;
        Ok::<_, String>(received)
    })
    .await;
    pool.close().await;
    let (sent, progress) = server.await.unwrap();
    endpoint.close(0u32.into(), b"test complete");
    endpoint.wait_idle().await;
    assert_eq!(
        transport::global().used(),
        before,
        "v2 feedback leaked reservations"
    );
    eprintln!(
        "v2-only sent={sent} progress={progress:?} outcome={:?}",
        outcome.as_ref().map(|r| r.as_ref().map(Vec::len))
    );
    assert_eq!(
        outcome
            .expect("bounded v2 delivery")
            .expect("v2 peer could not obtain receiver credit"),
        (0..80).map(record).collect::<Vec<_>>()
    );
    assert_eq!(sent, 80);
    assert_eq!(progress.last(), Some(&80));
}

// A lost prefix of reliable feedback cannot block an urgent repair datagram.
#[tokio::test]
async fn v3_repairs_while_reliable_feedback_is_incomplete_and_fences_route_identity() {
    let _serial = transport::test_lock().lock().await;
    let mut peer = SourcePeer::new(&[b"fxlink/3", b"fxlink/2"], 500).await;
    let negotiated = control::version(&peer.connection).unwrap();
    let initial = receive_for(&peer.connection, Duration::from_millis(50)).await;
    // This reliable frame cannot be decoded until its missing suffix arrives.
    peer.send.write_all(&100u32.to_be_bytes()).await.unwrap();
    peer.send.write_all(b"{").await.unwrap();
    let frame = |id: u64, generation: u64| {
        let mut b = b"FXRQ\x01\x00\x00\x18".to_vec();
        b.extend_from_slice(&id.to_be_bytes());
        b.extend_from_slice(&generation.to_be_bytes());
        b.extend_from_slice(&0u64.to_be_bytes());
        b.extend_from_slice(&1u16.to_be_bytes());
        b.extend_from_slice(&0u16.to_be_bytes());
        Bytes::from(b)
    };
    for wrong in [frame(99, 1), frame(0, 2), frame(0, 1).slice(..35)] {
        peer.connection.send_datagram(wrong).unwrap();
    }
    let invalid = receive_for(&peer.connection, Duration::from_millis(40)).await;
    peer.connection.send_datagram(frame(0, 1)).unwrap();
    let repaired = receive_for(&peer.connection, Duration::from_millis(150)).await;
    // Repair credit for four originals permits exactly one repair shard.
    peer.connection.send_datagram(frame(0, 1)).unwrap();
    let replay = receive_for(&peer.connection, Duration::from_millis(40)).await;
    peer.alive.store(false, Ordering::Release);
    let revoked = peer.reset(Duration::from_millis(150)).await;
    peer.connection.send_datagram(frame(0, 1)).unwrap();
    let after_revoke = receive_for(&peer.connection, Duration::from_millis(30)).await;
    peer.close().await;
    assert_eq!(initial, vec![0, 1, 2, 3]);
    assert!(
        invalid.is_empty(),
        "unadmitted/stale/malformed route produced media"
    );
    assert_eq!(
        repaired,
        vec![0],
        "urgent repair was held behind reliable feedback"
    );
    assert_eq!(negotiated, 3, "v3 not negotiated");
    assert!(
        replay.is_empty(),
        "duplicate request exceeded original repair credit"
    );
    assert!(
        revoked && after_revoke.is_empty(),
        "revoked source served repair"
    );
}

#[tokio::test]
async fn compact_negotiation_prefers_five_and_preserves_four_fallback() {
    let _serial = transport::test_lock().lock().await;
    for (protocols, expected) in [
        (vec![b"fxlink/5".as_slice(), b"fxlink/4".as_slice()], 5u8),
        (vec![b"fxlink/4".as_slice()], 4),
    ] {
        let peer = SourcePeer::new(&protocols, 1000).await;
        let selected = control::version(&peer.connection).unwrap();
        let records = receive_for(&peer.connection, Duration::from_millis(150)).await;
        peer.close().await;
        assert_eq!(selected, expected);
        assert_eq!(
            records.into_iter().collect::<BTreeSet<_>>(),
            (0..4).collect()
        );
    }
}
