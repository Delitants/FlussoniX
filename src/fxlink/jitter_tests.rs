//! Real listener/client regression: an ordered cache burst followed by live records
//! must survive bounded UDP reordering without inventing packet loss.
use super::{ClientPool, Listener, Provider, Request, Settings, Source, transport};
use bytes::Bytes;
use futures_util::{StreamExt, future::BoxFuture};
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{net::UdpSocket, time::Instant};
use tokio_util::sync::CancellationToken;

fn native_record(sequence: usize) -> Bytes {
    let length: usize = if sequence == 0 {
        1000
    } else if sequence % 3 == 1 {
        4500
    } else {
        500
    };
    let mut bytes = vec![(sequence % 251) as u8; length];
    bytes[..4].copy_from_slice(&((length - 4) as u32).to_be_bytes());
    bytes[4..12].copy_from_slice(&(sequence as u64).to_be_bytes());
    Bytes::from(bytes)
}

const RECORDS: usize = 360;
struct Feed {
    bootstrap: usize,
    start: Option<CancellationToken>,
    cancel: CancellationToken,
    overflow: Arc<AtomicBool>,
    producers: Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}
impl Provider for Feed {
    fn subscribe(&self, request: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
        let start = self.start.clone();
        let cancel = self.cancel.child_token();
        let overflow = self.overflow.clone();
        let producers = self.producers.clone();
        let bootstrap_count = self.bootstrap;
        Box::pin(async move {
            if request.peer_key != "owned-peer" || request.token != "owned-viewer" {
                return Err(403);
            }
            let (tx, rx) = tokio::sync::mpsc::channel(128);
            let producer_start = start.clone();
            let producer_cancel = cancel.clone();
            let producer = tokio::spawn(async move {
                if let Some(start) = producer_start {
                    tokio::select! {_=producer_cancel.cancelled()=>return,_=start.cancelled()=>{}}
                }
                let began = Instant::now();
                for sequence in bootstrap_count..RECORDS {
                    let due = began
                        + Duration::from_micros((sequence - bootstrap_count + 1) as u64 * 13_333);
                    tokio::select! {_=producer_cancel.cancelled()=>return,_=tokio::time::sleep_until(due)=>{}}
                    match tx.try_send(Ok(native_record(sequence))) {
                        Ok(()) => {}
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                            eprintln!(
                                "FXLink test producer full at={:?} elapsed_ms={} bootstrap={bootstrap_count} next_record={sequence}",
                                Instant::now(),
                                began.elapsed().as_millis()
                            );
                            overflow.store(true, Ordering::Release);
                            producer_cancel.cancel();
                            return;
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => return,
                    }
                }
            });
            producers.lock().unwrap().push(producer);
            let bootstrap = futures_util::stream::unfold(
                (0usize, start),
                move |(sequence, start)| async move {
                    if sequence == bootstrap_count {
                        return None;
                    }
                    if sequence == 0
                        && let Some(start) = &start
                    {
                        start.cancelled().await;
                    }
                    Some((Ok(native_record(sequence)), (sequence + 1, start)))
                },
            );
            let live = futures_util::stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|record| (record, rx))
            });
            Ok(Source {
                pending_records: None,
                records: Box::pin(bootstrap.chain(live).chain(futures_util::stream::pending())),
                current: Arc::new(|| true),
                cancelled: cancel,
            })
        })
    }
}

struct MultiplexFeed {
    live: Feed,
    large: Bytes,
}
impl Provider for MultiplexFeed {
    fn subscribe(&self, request: Request, peer: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
        if request.name != "large" {
            return self.live.subscribe(request, peer);
        }
        Box::pin(async move {
            if request.peer_key != "owned-peer" || request.token != "owned-viewer" {
                return Err(403);
            }
            Ok(Source {
                pending_records: None,
                records: Box::pin(
                    futures_util::stream::iter(vec![Ok(self.large.clone()); 2])
                        .chain(futures_util::stream::pending()),
                ),
                current: Arc::new(|| true),
                cancelled: self.live.cancel.child_token(),
            })
        })
    }
}

#[derive(Default, Debug)]
struct ProxyStats {
    received: [u64; 2],
    forwarded: [u64; 2],
    dropped: [u64; 2],
    reordered: [u64; 2],
    peak_bytes: usize,
}

async fn proxy(
    socket: UdpSocket,
    remote: SocketAddr,
    active: Arc<AtomicUsize>,
    stop: CancellationToken,
) -> ProxyStats {
    let stats = proxy_impair(
        socket,
        remote,
        active,
        stop,
        Arc::new(AtomicUsize::new(0)),
        None,
    )
    .await;
    assert_eq!(stats.dropped, [0; 2], "zero-loss proxy dropped packets");
    stats
}

#[derive(Clone)]
struct ProxyProbe {
    min_drop_len: usize,
    dropped_len: Arc<AtomicUsize>,
    pending: Arc<AtomicUsize>,
}

async fn proxy_impair(
    socket: UdpSocket,
    remote: SocketAddr,
    active: Arc<AtomicUsize>,
    stop: CancellationToken,
    drop_down: Arc<AtomicUsize>,
    probe: Option<ProxyProbe>,
) -> ProxyStats {
    type Datagram = (Instant, u64, Bytes, SocketAddr, usize, u64);
    let mut pending: BinaryHeap<Reverse<Datagram>> = BinaryHeap::new();
    let mut client = None;
    let mut stats = ProxyStats::default();
    let mut queued = 0usize;
    let mut serial = 0u64;
    let mut previous = [0u64; 2];
    let mut random = [0x46584c4bu64, 0x46584c4cu64];
    let mut buffer = [0u8; 65536];
    let mut draining = false;
    let mut drain_limit = Instant::now();
    let mut quiet_at = Instant::now();
    loop {
        let due = pending
            .peek()
            .map(|Reverse((due, ..))| *due)
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(1));
        tokio::select! {
            _=stop.cancelled(),if !draining=>{
                // Forward every accepted datagram, including the close/ACK tail.
                // The client pool is closed first; this token also closes the listener.
                draining=true;
                quiet_at=Instant::now()+Duration::from_millis(130);
                drain_limit=Instant::now()+Duration::from_millis(600);
            },
            _=tokio::time::sleep_until(quiet_at),if draining && pending.is_empty()=>break,
            _=tokio::time::sleep_until(drain_limit),if draining=>panic!("owned jitter proxy did not drain"),
            _=tokio::time::sleep_until(due),if !pending.is_empty()=>{
                let Reverse((_,_,packet,to,direction,sequence))=pending.pop().unwrap();
                queued-=packet.len();
                if let Some(probe)=&probe {probe.pending.store(pending.len(),Ordering::Release);}
                socket.send_to(&packet,to).await.unwrap();
                stats.forwarded[direction]+=1;
                stats.reordered[direction]+=u64::from(sequence<previous[direction]);
                previous[direction]=sequence;
            },
            packet=socket.recv_from(&mut buffer)=>{
                let (length,from)=packet.unwrap();
                quiet_at=Instant::now()+Duration::from_millis(130);
                let (to,direction)=if from==remote {(client.expect("response before client"),1)} else {
                    assert!(client.is_none_or(|known|known==from),"unexpected second fixture client");
                    client=Some(from);(remote,0)
                };
                stats.received[direction]+=1;
                // This proxy is the sole decrementer; the fixture arms it once.
                if direction==1 && drop_down.load(Ordering::Relaxed)>0
                    && probe.as_ref().is_none_or(|probe|length>=probe.min_drop_len) {
                    drop_down.fetch_sub(1,Ordering::Relaxed);
                    if let Some(probe)=&probe {probe.dropped_len.store(length,Ordering::Release);}
                    stats.dropped[direction]+=1;
                    continue;
                }
                assert!(pending.len()<512 && queued+length<=8<<20,"owned jitter queue overloaded");
                random[direction]^=random[direction]<<13;
                random[direction]^=random[direction]>>7;
                random[direction]^=random[direction]<<17;
                let delay=if active.load(Ordering::Relaxed)&(1<<direction)!=0 {85+random[direction]%21} else {95};
                serial+=1;
                queued+=length;stats.peak_bytes=stats.peak_bytes.max(queued);
                pending.push(Reverse((Instant::now()+Duration::from_millis(delay),serial,Bytes::copy_from_slice(&buffer[..length]),to,direction,stats.received[direction])));
                if let Some(probe)=&probe {probe.pending.store(pending.len(),Ordering::Release);}
            }
        }
    }
    assert_eq!(
        stats.received,
        [
            stats.forwarded[0] + stats.dropped[0],
            stats.forwarded[1] + stats.dropped[1]
        ],
        "proxy discarded unconfigured datagrams"
    );
    stats
}

#[tokio::test]
async fn startup_loss_with_cached_and_live_media_keeps_record_window_bounded() {
    let _serial = transport::test_lock().lock().await;
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
    let start = CancellationToken::new();
    let feed_stop = CancellationToken::new();
    let overflow = Arc::new(AtomicBool::new(false));
    let producers = Arc::new(std::sync::Mutex::new(Vec::new()));
    let serving = tokio::spawn(listener.serve(
        Arc::new(Feed {
            bootstrap: 200,
            start: Some(start.clone()),
            cancel: feed_stop.child_token(),
            overflow: overflow.clone(),
            producers: producers.clone(),
        }),
        stop.clone(),
    ));
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let url = format!("fxlink://{}/live", socket.local_addr().unwrap());
    let drop_down = Arc::new(AtomicUsize::new(0));
    let relaying = tokio::spawn(proxy_impair(
        socket,
        remote,
        Arc::new(AtomicUsize::new(0)),
        stop.clone(),
        drop_down.clone(),
        None,
    ));
    let pool = ClientPool::default();
    let outcome = tokio::time::timeout(Duration::from_secs(12), async {
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
        tokio::time::sleep(Duration::from_millis(230)).await;
        // Lose an initial encrypted datagram after the authenticated setup;
        // later encrypted media can expose a gap before reliable retransmission.
        drop_down.store(1, Ordering::Relaxed);
        start.cancel();
        let mut received = Vec::new();
        for _ in 0..RECORDS {
            received.push(sub.recv().await?);
        }
        Ok::<_, String>((received, sub.telemetry().snapshot()))
    })
    .await;
    feed_stop.cancel();
    pool.close().await;
    stop.cancel();
    serving.await.unwrap().unwrap();
    let stats = relaying.await.unwrap();
    let producer_tasks = std::mem::take(&mut *producers.lock().unwrap());
    for task in producer_tasks {
        task.await.unwrap();
    }
    assert_eq!(
        transport::global().used(),
        before,
        "startup fixture leaked reservations"
    );
    assert!(
        !overflow.load(Ordering::Acquire),
        "independent live producer overflowed"
    );
    assert_eq!(
        stats.dropped,
        [0, 1],
        "intended encrypted loss was not exercised"
    );
    eprintln!(
        "startup loss proxy={stats:?}, outcome={:?}",
        outcome.as_ref().map(|r| r
            .as_ref()
            .map(|(records, telemetry)| (records.len(), telemetry)))
    );
    let (received, telemetry) = outcome
        .expect("bounded startup recovery")
        .expect("cached startup overran the receive window");
    assert_eq!(
        received,
        (0..RECORDS).map(native_record).collect::<Vec<_>>()
    );
    assert!(
        telemetry["repair_requested_shards"].as_u64().unwrap() > 0,
        "missing originals did not exercise repair"
    );
}

// Missing record 1 is first observable through later UDP media while record 0
// still blocks delivery. Its delayed reliable announcement must not restart
// the reordering grace and squander the source's last useful repair window.
#[tokio::test]
async fn delayed_future_announcement_keeps_first_observed_gap_age() {
    use super::{
        Budget, Encoder,
        control::{self, Control},
    };
    let _serial = transport::test_lock().lock().await;
    let before = transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let mut tls = (*cert.server()).clone();
    tls.alpn_protocols = vec![b"fxlink/1".to_vec()];
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    config.transport_config(transport::config());
    let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let url = format!("fxlink://{}/live", socket.local_addr().unwrap());
    let stop = CancellationToken::new();
    let relaying = tokio::spawn(proxy(
        socket,
        endpoint.local_addr().unwrap(),
        Arc::new(AtomicUsize::new(0)),
        stop.clone(),
    ));
    let settings = Settings::default();
    let source_settings = settings.clone();
    let source_endpoint = endpoint.clone();
    let expected: Vec<_> = (0u8..9)
        .map(|n| {
            let mut bytes = vec![n; 1000];
            bytes[..4].copy_from_slice(&996u32.to_be_bytes());
            Bytes::from(bytes)
        })
        .collect();
    let source_records = expected.clone();
    let server = tokio::spawn(async move {
        let connection = source_endpoint.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let Control::Subscribe {
            id,
            generation,
            peer_key,
            token,
            ..
        } = control::read(&mut recv).await.unwrap()
        else {
            panic!("expected subscription")
        };
        assert_eq!(
            (peer_key.as_str(), token.as_str()),
            ("owned-peer", "owned-viewer")
        );
        control::write(&mut send, &Control::Accepted).await.unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
        let mut encoder =
            Encoder::new(source_settings, id, generation, Budget::new(1 << 20)).unwrap();
        let records: Vec<_> = source_records
            .into_iter()
            .map(|bytes| encoder.encode(bytes, 0).unwrap())
            .collect();
        let began = Instant::now();
        control::write(
            &mut send,
            &Control::Announce {
                record: 0,
                len: 1000,
            },
        )
        .await
        .unwrap();
        let mut original_bytes = 0;
        for record in &records[2..] {
            for packet in &record.datagrams {
                connection.send_datagram_wait(packet.clone()).await.unwrap();
                original_bytes += packet.len();
            }
        }
        let connection2 = connection.clone();
        let head = records[0].datagrams.clone();
        let schedule = tokio::spawn(async move {
            tokio::time::sleep_until(began + Duration::from_millis(440)).await;
            control::write(
                &mut send,
                &Control::Announce {
                    record: 1,
                    len: 1000,
                },
            )
            .await
            .unwrap();
            tokio::time::sleep_until(began + Duration::from_millis(550)).await;
            for packet in head {
                let _ = connection2.send_datagram_wait(packet).await;
            }
            send
        });
        let mut attempts = Vec::new();
        let mut repair_bytes = 0;
        while let Ok(message) = control::read(&mut recv).await {
            if let Control::Repair { block, shards } = message {
                // Hold head 0 until the scheduled release. Only record 1 is
                // repairable, with the production source age/RTT guard and cap.
                if block != super::MAX_BLOCKS_PER_RECORD as u64 {
                    continue;
                }
                assert_eq!(shards, [0]);
                let age = began.elapsed().as_millis() as u64;
                let rtt = connection.rtt().as_millis() as u64;
                let eligible = age + rtt + 5 < 1000;
                attempts.push((age, rtt, eligible));
                if eligible && repair_bytes == 0 {
                    let packet = &records[1].datagrams[0];
                    assert!(4 * (repair_bytes + packet.len()) <= original_bytes);
                    connection.send_datagram_wait(packet.clone()).await.unwrap();
                    repair_bytes += packet.len();
                }
            }
        }
        drop(schedule.await.unwrap());
        (attempts, repair_bytes)
    });
    let pool = ClientPool::default();
    let outcome = tokio::time::timeout(Duration::from_secs(4), async {
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
        for _ in 0..9 {
            received.push(sub.recv().await?);
        }
        Ok::<_, String>(received)
    })
    .await;
    pool.close().await;
    let (attempts, repair_bytes) = server.await.unwrap();
    endpoint.close(0u32.into(), b"test done");
    endpoint.wait_idle().await;
    stop.cancel();
    relaying.await.unwrap();
    assert_eq!(
        transport::global().used(),
        before,
        "gap-age fixture leaked reservations"
    );
    eprintln!(
        "record 1 repair attempts (source age, RTT, eligible)={attempts:?}, repair_bytes={repair_bytes}, outcome={:?}",
        outcome.as_ref().map(|r| r.as_ref().map(Vec::len))
    );
    assert_eq!(
        outcome
            .expect("bounded gap recovery")
            .expect("late metadata lost an already observed repair opportunity"),
        expected
    );
    assert!(repair_bytes > 0);
}

#[tokio::test]
async fn zero_loss_udp_reordering_delivers_cached_bootstrap_and_live_records() {
    if std::env::var_os("FLUSSONIX_JITTER_TRACE").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                if std::env::var("FLUSSONIX_JITTER_TRACE").as_deref() == Ok("fxlink") {
                    "warn,flussonix::fxlink=trace"
                } else {
                    "warn,flussonix::fxlink=trace,quinn_proto::connection=trace"
                },
            )
            .with_test_writer()
            .try_init();
    }
    let _serial = transport::test_lock().lock().await;
    // Cover startup reordering, then isolate each direction after a fixed
    // handshake. The independent live clock never slows under backpressure.
    for (direction, gated, bootstrap) in [
        (0, false, 60),
        (3, false, 60),
        (1, true, 60),
        (2, true, 60),
        (3, true, 60),
        (3, false, 200),
    ] {
        let jitter = direction != 0;
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
        let start = CancellationToken::new();
        let feed_stop = CancellationToken::new();
        let overflow = Arc::new(AtomicBool::new(false));
        let producers = Arc::new(std::sync::Mutex::new(Vec::new()));
        let active = Arc::new(AtomicUsize::new(if gated { 0 } else { direction }));
        let serving = tokio::spawn(listener.serve(
            Arc::new(Feed {
                bootstrap,
                start: gated.then_some(start.clone()),
                cancel: feed_stop.child_token(),
                overflow: overflow.clone(),
                producers: producers.clone(),
            }),
            stop.clone(),
        ));
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let url = format!("fxlink://{}/live", socket.local_addr().unwrap());
        let relaying = tokio::spawn(proxy(socket, remote, active.clone(), stop.clone()));
        let pool = ClientPool::default();
        let outcome = tokio::time::timeout(Duration::from_secs(10), async {
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
            if gated {
                tokio::time::sleep(Duration::from_millis(230)).await;
                active.store(direction, Ordering::Relaxed);
                start.cancel();
            }
            let mut received = Vec::new();
            for _ in 0..RECORDS {
                match sub.recv().await {
                    Ok(record) => received.push(record),
                    Err(error) => {
                        eprintln!("FXLink test receive failed at={:?} bootstrap={bootstrap} delivered={} overflow={} telemetry={:?} error={error}", Instant::now(), received.len(), overflow.load(Ordering::Acquire), sub.telemetry().snapshot());
                        return Err(error);
                    }
                }
            }
            Ok::<_, String>((received, sub.telemetry().snapshot()))
        })
        .await;
        // Stop the independent producer before teardown can create new backlog.
        // Outcome and overflow above still preserve any delivery-time failure.
        feed_stop.cancel();
        pool.close().await;
        stop.cancel();
        serving.await.unwrap().unwrap();
        let stats = relaying.await.unwrap();
        let producer_tasks = std::mem::take(&mut *producers.lock().unwrap());
        for producer in producer_tasks {
            producer.await.unwrap();
        }
        assert_eq!(
            transport::global().used(),
            before,
            "jitter fixture leaked transport reservations"
        );
        eprintln!(
            "jitter={jitter} direction={direction} gated={gated} proxy={stats:?} result={:?}",
            outcome.as_ref().map(|r| r
                .as_ref()
                .map(|(records, telemetry)| (records.len(), telemetry)))
        );
        assert!(
            !overflow.load(Ordering::Acquire),
            "independent live producer overflowed its 128-record queue"
        );
        let (received, telemetry) = outcome
            .expect("bounded jitter delivery")
            .expect("zero-loss UDP reordering retired the generation");
        assert_eq!(
            received,
            (0..RECORDS).map(native_record).collect::<Vec<_>>()
        );
        assert!(
            telemetry["rtt_ms"].as_u64().unwrap() >= 150,
            "delay fixture was bypassed"
        );
        if jitter {
            for (index, reordered) in stats.reordered.iter().enumerate() {
                if direction & (1 << index) != 0 {
                    assert!(*reordered > 0, "requested direction was not reordered");
                }
            }
        }
    }
}

#[tokio::test]
async fn reordered_shared_connection_delivers_large_records_and_survives_sibling_cancel() {
    let _serial = transport::test_lock().lock().await;
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
    let start = CancellationToken::new();
    let overflow = Arc::new(AtomicBool::new(false));
    let producers = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut bytes = vec![0xa6; 64 * 1024];
    bytes[..4].copy_from_slice(&((64 * 1024 - 4) as u32).to_be_bytes());
    let large = Bytes::from(bytes);
    let serving = tokio::spawn(listener.serve(
        Arc::new(MultiplexFeed {
            live: Feed {
                bootstrap: 60,
                start: Some(start.clone()),
                cancel: stop.child_token(),
                overflow: overflow.clone(),
                producers: producers.clone(),
            },
            large: large.clone(),
        }),
        stop.clone(),
    ));
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let active = Arc::new(AtomicUsize::new(0));
    // The proxy rejects a second client UDP address: these subscriptions must
    // reuse the same authenticated connection and its small DATAGRAM FIFO.
    let relaying = tokio::spawn(proxy(socket, remote, active.clone(), stop.clone()));
    let pool = ClientPool::default();
    let outcome = tokio::time::timeout(Duration::from_secs(12), async {
        let mut live = pool
            .subscribe(
                &format!("fxlink://{address}/live"),
                "owned-peer",
                "owned-viewer",
                Some(&cert.ca),
                Settings::default(),
                CancellationToken::new(),
            )
            .await?;
        tokio::time::sleep(Duration::from_millis(230)).await;
        active.store(3, Ordering::Relaxed);
        start.cancel();
        let mut received = Vec::new();
        for _ in 0..50 {
            received.push(live.recv().await?);
        }
        let mut big = pool
            .subscribe(
                &format!("fxlink://{address}/large"),
                "owned-peer",
                "owned-viewer",
                Some(&cert.ca),
                Settings::default(),
                CancellationToken::new(),
            )
            .await?;
        assert_eq!(live.connection_id(), big.connection_id());
        let large_delivery = async {
            let record = big.recv().await?;
            drop(big);
            Ok::<_, String>(record)
        };
        let sibling_delivery = async {
            for _ in 50..RECORDS {
                received.push(live.recv().await?);
            }
            Ok::<_, String>((received, live.telemetry().snapshot()))
        };
        let (big, small) = tokio::join!(large_delivery, sibling_delivery);
        Ok::<_, String>((big?, small?))
    })
    .await;
    pool.close().await;
    stop.cancel();
    serving.await.unwrap().unwrap();
    let stats = relaying.await.unwrap();
    let producer_tasks = std::mem::take(&mut *producers.lock().unwrap());
    for producer in producer_tasks {
        producer.await.unwrap();
    }
    assert_eq!(
        transport::global().used(),
        before,
        "multiplex fixture leaked reservations"
    );
    assert!(
        !overflow.load(Ordering::Acquire),
        "independent sibling producer overflowed"
    );
    let (received_large, (received, telemetry)) = outcome
        .expect("bounded multiplex delivery")
        .expect("shared FIFO retired a subscription");
    assert_eq!(received_large, large);
    assert_eq!(
        received,
        (0..RECORDS).map(native_record).collect::<Vec<_>>()
    );
    assert_eq!(
        telemetry["active"], true,
        "sibling stopped after cancellation"
    );
    assert!(stats.reordered.iter().all(|count| *count > 0));
    eprintln!("large-record multiplex proxy={stats:?} sibling={telemetry}");
}

#[tokio::test]
async fn encrypted_packet_reordering_preserves_loss_signal_and_control_recovery() {
    if std::env::var_os("FLUSSONIX_RAW_QUIC_TRACE").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("warn,quinn_proto=trace")
            .with_test_writer()
            .try_init();
    }
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    #[derive(Debug)]
    struct Loss {
        packets: u64,
        bytes: u64,
        events: u64,
    }
    fn loss(after: &quinn::PathStats, before: &quinn::PathStats) -> Loss {
        Loss {
            packets: after.lost_packets - before.lost_packets,
            bytes: after.lost_bytes - before.lost_bytes,
            events: after.congestion_events - before.congestion_events,
        }
    }
    async fn exercise(old_packet_gap: bool) -> (Loss, Loss, Duration, ProxyStats, usize) {
        let cert = crate::transport_tls_fixture::Certificates::new();
        let mut wire = transport::config();
        let config = Arc::get_mut(&mut wire).unwrap();
        // Fix packet geometry and eliminate unrelated PMTU/keepalive probes.
        config.mtu_discovery_config(None).keep_alive_interval(None);
        if old_packet_gap {
            config.packet_threshold(3);
        }
        let mut tls = (*cert.server()).clone();
        tls.alpn_protocols = super::control::protocols();
        let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
        server_config.transport_config(wire.clone());
        let server =
            quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let proxy_address = socket.local_addr().unwrap();
        let stop = CancellationToken::new();
        let active = Arc::new(AtomicUsize::new(0));
        let drop_down = Arc::new(AtomicUsize::new(0));
        let probe = ProxyProbe {
            min_drop_len: 1000,
            dropped_len: Arc::new(AtomicUsize::new(0)),
            pending: Arc::new(AtomicUsize::new(0)),
        };
        let relaying = tokio::spawn(proxy_impair(
            socket,
            server.local_addr().unwrap(),
            active.clone(),
            stop.clone(),
            drop_down.clone(),
            Some(probe.clone()),
        ));
        let mut tls = (*cert.client()).clone();
        tls.alpn_protocols = super::control::protocols();
        let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap();
        let mut client_config = quinn::ClientConfig::new(Arc::new(crypto));
        client_config.transport_config(wire);
        let mut client = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        client.set_default_client_config(client_config);
        let phase = std::cell::Cell::new("connect");
        let enqueued = std::cell::Cell::new(0);
        let received_ids = std::cell::RefCell::new(std::collections::BTreeSet::new());
        let peers = std::cell::RefCell::new(None::<(quinn::Connection, quinn::Connection)>);
        let ports = [
            server.local_addr().unwrap().port(),
            proxy_address.port(),
            client.local_addr().unwrap().port(),
        ];
        // Linux queue/drop evidence for these owned IPv4 sockets. Other
        // platforms can still run the fixture without this optional snapshot.
        fn udp_snapshot(ports: &[u16]) -> Vec<String> {
            std::fs::read_to_string("/proc/net/udp")
                .unwrap_or_default()
                .lines()
                .filter(|line| {
                    line.split_whitespace()
                        .nth(1)
                        .and_then(|s| s.split_once(':'))
                        .and_then(|(_, p)| u16::from_str_radix(p, 16).ok())
                        .is_some_and(|port| ports.contains(&port))
                })
                .map(str::to_owned)
                .collect()
        }
        let udp_before = udp_snapshot(&ports);
        let outcome = tokio::time::timeout(Duration::from_secs(12), async {
            let connecting = client
                .connect(proxy_address, "localhost")
                .map_err(|e| e.to_string())?;
            let incoming = async {
                server
                    .accept()
                    .await
                    .ok_or("missing incoming")?
                    .await
                    .map_err(|e| e.to_string())
            };
            let (receiving, sending) = tokio::join!(connecting, incoming);
            let receiving = receiving.map_err(|e| e.to_string())?;
            let sending = sending?;
            *peers.borrow_mut() = Some((sending.clone(), receiving.clone()));
            phase.set("initial stream");
            let (mut control_send, mut source_feedback) =
                sending.open_bi().await.map_err(|e| e.to_string())?;
            control_send
                .write_all(b"r")
                .await
                .map_err(|e| e.to_string())?;
            let (mut client_feedback, mut control_recv) =
                receiving.accept_bi().await.map_err(|e| e.to_string())?;
            let mut ready = [0];
            control_recv
                .read_exact(&mut ready)
                .await
                .map_err(|e| e.to_string())?;
            assert_eq!(ready, *b"r");
            // Drain the fixed95ms TLS/control exchange before shaping media.
            tokio::time::sleep(Duration::from_millis(450)).await;
            let before = sending.stats().path;
            active.store(2, Ordering::Relaxed);
            fn payload(sequence: usize) -> Bytes {
                let mut bytes = vec![(sequence % 251) as u8; 1000];
                bytes[..8].copy_from_slice(&(sequence as u64).to_be_bytes());
                Bytes::from(bytes)
            }
            let send = async {
                for sequence in 0..256 {
                    sending
                        .send_datagram_wait(payload(sequence))
                        .await
                        .map_err(|e| e.to_string())?;
                    enqueued.set(sequence + 1);
                }
                Ok::<_, String>(())
            };
            let receive = async {
                for _ in 0..256 {
                    let bytes = receiving.read_datagram().await.map_err(|e| e.to_string())?;
                    if bytes.len() != 1000 {
                        return Err("unexpected raw datagram length".into());
                    }
                    let sequence = u64::from_be_bytes(bytes[..8].try_into().unwrap()) as usize;
                    if sequence >= 256
                        || bytes != payload(sequence)
                        || !received_ids.borrow_mut().insert(sequence)
                    {
                        return Err("corrupt/duplicate raw datagram".into());
                    }
                }
                Ok::<_, String>(())
            };
            phase.set("media exchange");
            tokio::try_join!(send, receive)?;
            phase.set("reliable barrier");
            active.store(0, Ordering::Relaxed);
            // Echo a reliable barrier after all256 datagrams arrived, then
            // observe a quiet, empty proxy queue for longer than base delay
            // plus delayedACK. No new application/PMTU/keepalive traffic remains.
            control_send
                .write_all(b"q")
                .await
                .map_err(|e| e.to_string())?;
            control_recv
                .read_exact(&mut ready)
                .await
                .map_err(|e| e.to_string())?;
            if ready != *b"q" {
                return Err("bad source barrier".into());
            }
            client_feedback
                .write_all(b"q")
                .await
                .map_err(|e| e.to_string())?;
            source_feedback
                .read_exact(&mut ready)
                .await
                .map_err(|e| e.to_string())?;
            if ready != *b"q" {
                return Err("bad returned barrier".into());
            }
            phase.set("proxy drain");
            let mut quiet = Instant::now();
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if probe.pending.load(Ordering::Acquire) > 0 {
                        quiet = Instant::now();
                    }
                    if quiet.elapsed() >= Duration::from_millis(150) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .map_err(|_| "proxy did not become quiescent")?;
            let after_reordering = sending.stats().path;
            // After all datagrams and their ACKs have drained, lose exactly one
            // source packet of a four-packet reliable write. Later ACKs must
            // still trigger real loss recovery within the minimum500ms budget.
            phase.set("control recovery");
            drop_down.store(1, Ordering::Relaxed);
            let body = vec![0xa5; 4096];
            let mut received = vec![0; body.len()];
            let began = Instant::now();
            let delivery = async {
                let write = async {
                    control_send
                        .write_all(&body)
                        .await
                        .map_err(|e| e.to_string())
                };
                let read = async {
                    control_recv
                        .read_exact(&mut received)
                        .await
                        .map_err(|e| e.to_string())
                };
                tokio::try_join!(write, read)?;
                Ok::<_, String>(())
            };
            tokio::time::timeout(Duration::from_millis(500), delivery)
                .await
                .map_err(|_| "reliable recovery exceeded500ms")??;
            let elapsed = began.elapsed();
            if received != body {
                return Err("reliable body corrupted".into());
            }
            tokio::time::sleep(Duration::from_millis(450)).await;
            Ok::<_, String>((
                loss(&after_reordering, &before),
                loss(&sending.stats().path, &after_reordering),
                elapsed,
            ))
        })
        .await;
        eprintln!(
            "RAW_DIAG old_packet_gap={old_packet_gap} phase={} enqueued={} received={} missing={:?} outcome={outcome:?} udp_before={udp_before:?} udp_after={:?} peers={:?}",
            phase.get(),
            enqueued.get(),
            received_ids.borrow().len(),
            (0..256)
                .filter(|id| !received_ids.borrow().contains(id))
                .collect::<Vec<_>>(),
            udp_snapshot(&ports),
            peers.borrow().as_ref().map(|(send, recv)| (
                send.stats(),
                recv.stats(),
                send.close_reason(),
                recv.close_reason()
            ))
        );
        server.close(0u32.into(), b"test done");
        client.close(0u32.into(), b"test done");
        tokio::join!(server.wait_idle(), client.wait_idle());
        stop.cancel();
        let stats = relaying.await.unwrap();
        let (false_loss, real_loss, elapsed) = outcome
            .unwrap_or_else(|e| panic!("bounded raw exchange: old_packet_gap={old_packet_gap}, phase={}, error={e}", phase.get()))
            .unwrap_or_else(|e| panic!("encrypted reordering/control fixture failed: old_packet_gap={old_packet_gap}, phase={}, error={e}", phase.get()));
        assert_eq!(stats.dropped, [0, 1]);
        assert!(
            stats.reordered[1] > 0,
            "media direction must actually reorder"
        );
        let dropped_len = probe.dropped_len.load(Ordering::Acquire);
        assert!(
            dropped_len >= 1000,
            "the dropped packet must carry the new reliable body"
        );
        assert_eq!(probe.pending.load(Ordering::Acquire), 0);
        (false_loss, real_loss, elapsed, stats, dropped_len)
    }
    let old = exercise(true).await;
    let candidate = exercise(false).await;
    eprintln!("packet-gap baseline={old:?}, candidate={candidate:?}");
    assert_eq!(transport::global().used(), baseline);
    assert!(
        old.0.packets > 0 && old.0.bytes > 0 && old.0.events > 0,
        "fixture must expose a congestion penalty for delivered datagrams"
    );
    assert_eq!(
        candidate.0.bytes, 0,
        "loss-free media reordering must not lose ack-eliciting bytes"
    );
    assert_eq!(
        candidate.0.events, 0,
        "loss-free media reordering must not become a congestion penalty"
    );
    assert!(
        candidate.1.packets > 0 && candidate.1.bytes > 0 && candidate.1.events > 0,
        "actual dropped control packet must be detected"
    );
    assert!(candidate.2 < Duration::from_millis(500));
}
