//! One actual encrypted UDP media loss; no feedback repair may mask protection.
use super::transport;
use super::*;
use bytes::Bytes;
use futures_util::{StreamExt, future::BoxFuture};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;
fn native() -> Bytes {
    let mut b = vec![83; 350];
    b[..4].copy_from_slice(&346u32.to_be_bytes());
    Bytes::from(b)
}
struct Feed(CancellationToken, CancellationToken);
impl Provider for Feed {
    fn subscribe(&self, r: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
        let start = self.0.clone();
        let rest = self.1.clone();
        Box::pin(async move {
            if r.peer_key != "owned-peer" || r.token != "owned-viewer" {
                return Err(403);
            }
            Ok(Source {
                pending_records: None,
                records: Box::pin(
                    futures_util::stream::once(async move {
                        start.cancelled().await;
                        Ok(native())
                    })
                    .chain(futures_util::stream::once(async move {
                        rest.cancelled().await;
                        Ok(native())
                    }))
                    .chain(futures_util::stream::pending()),
                ),
                current: Arc::new(|| true),
                cancelled: CancellationToken::new(),
            })
        })
    }
}
#[tokio::test]
async fn compact_media_recovers_one_udp_loss_before_any_feedback() {
    encrypted_case(1).await;
}
#[tokio::test]
async fn compact_media_shared_connection_keeps_both_subscriptions_healthy() {
    encrypted_case(2).await;
}
async fn encrypted_case(count: usize) {
    use std::{cmp::Reverse, collections::BinaryHeap};
    use tokio::{net::UdpSocket, time::Instant};
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        settings.clone(),
    )
    .unwrap();
    let remote = listener.address();
    let egress = listener.egress();
    let stop = CancellationToken::new();
    let start = CancellationToken::new();
    let rest = CancellationToken::new();
    let serving =
        tokio::spawn(listener.serve(Arc::new(Feed(start.clone(), rest.clone())), stop.clone()));
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let arm = armed.clone();
    let relay_stop = CancellationToken::new();
    let cancel = relay_stop.clone();
    let relay = tokio::spawn(async move {
        let mut client = None;
        let mut pending = BinaryHeap::new();
        let mut serial = 0u64;
        let mut buffer = [0u8; 65536];
        let mut dropped = Vec::new();
        let mut media_packets = 0usize;
        loop {
            let due = pending
                .peek()
                .map(|Reverse((at, _, _, _))| *at)
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(1));
            tokio::select! {
                _=cancel.cancelled()=>break,
                _=tokio::time::sleep_until(due),if !pending.is_empty()=>{
                    let Reverse((_,_,body,to)):Reverse<(Instant,u64,Vec<u8>,SocketAddr)>=pending.pop().unwrap();
                    socket.send_to(&body,to).await.unwrap();
                },
                result=socket.recv_from(&mut buffer)=>{
                    let (length,from)=result.unwrap();let to=if from==remote {client.unwrap()} else {
                        assert!(client.is_none_or(|known|known==from),"client pool opened another connection");client=Some(from);remote
                    };
                    if from==remote && (400..1200).contains(&length) {
                        media_packets+=1;
                        if arm.swap(false,Ordering::AcqRel) {dropped.push(length);continue;}
                    }
                    assert!(pending.len()<8192);serial+=1;
                    pending.push(Reverse((Instant::now()+Duration::from_millis(95),serial,buffer[..length].to_vec(),to)));
                }
            }
        }
        (dropped, media_packets)
    });
    let pool = ClientPool::default();
    let mut subscriptions = Vec::new();
    for index in 0..count {
        subscriptions.push(
            pool.subscribe(
                &format!("fxlink://{address}/owned-{index}"),
                "owned-peer",
                "owned-viewer",
                Some(&cert.ca),
                settings.clone(),
                CancellationToken::new(),
            )
            .await
            .unwrap(),
        );
    }
    tokio::time::sleep(Duration::from_millis(350)).await;
    armed.store(true, Ordering::Release);
    let began = std::time::Instant::now();
    start.cancel();
    let mut first_stats = Vec::new();
    let outcome = tokio::time::timeout(Duration::from_secs(3), async {
        for sub in &mut subscriptions {
            assert_eq!(sub.recv().await?, native());
            first_stats.push(sub.telemetry().snapshot());
        }
        rest.cancel();
        for sub in &mut subscriptions {
            assert_eq!(sub.recv().await?, native());
        }
        Ok::<_, String>(())
    })
    .await;
    let elapsed = began.elapsed();
    let stats = subscriptions
        .iter()
        .map(|s| s.telemetry().snapshot())
        .collect::<Vec<_>>();
    let telemetry = subscriptions
        .iter()
        .map(|s| s.telemetry())
        .collect::<Vec<_>>();
    drop(subscriptions);
    pool.close().await;
    stop.cancel();
    serving.await.unwrap().unwrap();
    relay_stop.cancel();
    let (dropped, packets) = relay.await.unwrap();
    for t in telemetry {
        assert_eq!(t.snapshot()["state_bytes"], 0);
    }
    assert_eq!(transport::global().used(), baseline);
    eprintln!(
        "compact UDP: count={count}, dropped={dropped:?}, packets={packets}, elapsed={elapsed:?}, stats={stats:?}"
    );
    assert_eq!(
        dropped.len(),
        1,
        "one encrypted UDP media packet must be lost"
    );
    outcome
        .expect("bounded compact recovery")
        .expect("lost short original needs proactive FEC");
    assert_eq!(first_stats.len(), count);
    assert!(
        first_stats
            .iter()
            .map(|s| s["fec_recovered_shards"].as_u64().unwrap())
            .sum::<u64>()
            >= 1
    );
    assert!(stats.iter().all(|s| s["repair_requested_shards"] == 0
        && s["expired_records"] == 0
        && s["queue_dropped_shards"] == 0));
    assert_eq!(
        egress.load(Ordering::Relaxed),
        count as u64 * 1736,
        "two records per subscription require exactly four compact datagrams"
    );
}

// Old-version validation must run before the route's capacity/drop branch.
#[tokio::test]
async fn compact_format_is_rejected_on_v4_even_with_a_full_packet_route() {
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let mut tls = (*cert.server()).clone();
    tls.alpn_protocols = vec![b"fxlink/4".to_vec()];
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    config.transport_config(transport::config());
    let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let url = format!("fxlink://{}/owned-invalid", endpoint.local_addr().unwrap());
    let send_bad = CancellationToken::new();
    let bad = send_bad.clone();
    let stop = CancellationToken::new();
    let halt = stop.clone();
    let server_endpoint = endpoint.clone();
    let server = tokio::spawn(async move {
        let connection = server_endpoint.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let super::control::Control::Subscribe {
            version,
            id,
            generation,
            ..
        } = super::control::read(&mut recv).await.unwrap()
        else {
            panic!("subscription")
        };
        assert_eq!(version, 4);
        super::control::write(&mut send, &super::control::Control::Accepted)
            .await
            .unwrap();
        let store = Budget::new(4 << 20);
        let mut encoder = Encoder::new(Settings::default(), id, generation, store).unwrap();
        let mut large = vec![73; 260 * 1024];
        let len = large.len();
        large[..4].copy_from_slice(&((len - 4) as u32).to_be_bytes());
        let frame = encoder.encode(Bytes::from(large), 0).unwrap();
        super::control::write(
            &mut send,
            &super::control::Control::Announce { record: 0, len },
        )
        .await
        .unwrap();
        for packet in &frame.datagrams {
            connection.send_datagram_wait(packet.clone()).await.unwrap();
        }
        bad.cancelled().await;
        let compact = encoder
            .with_short_record_fec(true)
            .encode(native(), 0)
            .unwrap();
        connection
            .send_datagram_wait(compact.datagrams[0].clone())
            .await
            .unwrap();
        halt.cancelled().await;
        connection.close(0u32.into(), b"test done");
    });
    let held = transport::fec_slots().acquire_many_owned(4).await.unwrap();
    let pool = ClientPool::default();
    let mut sub = pool
        .subscribe(
            &url,
            "owned-peer",
            "owned-viewer",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while sub.telemetry().snapshot()["queue_dropped_shards"]
            .as_u64()
            .unwrap()
            == 0
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("fixture must fill packet ingress");
    let before = sub.telemetry().snapshot()["received_data_bytes"].clone();
    send_bad.cancel();
    tokio::time::timeout(Duration::from_millis(500), async {
        while sub.current() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("v4 compact format must retire before decoder release");
    let state = sub.telemetry();
    assert_eq!(state.snapshot()["received_data_bytes"], before);
    assert!(sub.recv().await.is_err());
    drop(held);
    drop(sub);
    pool.close().await;
    stop.cancel();
    server.await.unwrap();
    endpoint.wait_idle().await;
    assert_eq!(state.snapshot()["state_bytes"], 0);
    assert_eq!(transport::global().used(), baseline);
}

#[tokio::test]
async fn compact_optional_metadata_exhaustion_accepts_legacy_media_after_pressure_clears() {
    let _serial = transport::test_lock().lock().await;
    let global = transport::global();
    let baseline = global.used();
    let remaining = 2 * 1024 * 1024 + 4 * 1024 * 1024 + 65536 + 16384 + 4096;
    let pressure = global
        .reserve(global.limit() - baseline - remaining)
        .unwrap();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let settings = Settings::default();
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        settings.clone(),
    )
    .unwrap();
    let remote = listener.address();
    let stop = CancellationToken::new();
    let start = CancellationToken::new();
    let rest = CancellationToken::new();
    let serving = tokio::spawn(listener.serve(Arc::new(Feed(start.clone(), rest)), stop.clone()));
    let mut tls = (*cert.client()).clone();
    tls.alpn_protocols = vec![b"fxlink/5".to_vec()];
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
    let (mut send, mut recv) = connection.open_bi().await.unwrap();
    let id = send.id().index();
    super::control::write(
        &mut send,
        &super::control::Control::Subscribe {
            version: 5,
            id,
            generation: 1,
            profile: super::control::profile(&settings),
            name: "owned-optional".into(),
            peer_key: "owned-peer".into(),
            token: "owned-viewer".into(),
        },
    )
    .await
    .unwrap();
    let accepted = matches!(
        tokio::time::timeout(Duration::from_secs(2), super::control::read(&mut recv)).await,
        Ok(Ok(super::control::Control::Accepted))
    );
    drop(pressure);
    let packet = if accepted {
        start.cancel();
        tokio::time::timeout(Duration::from_secs(1), connection.read_datagram())
            .await
            .ok()
            .and_then(Result::ok)
    } else {
        None
    };
    connection.close(0u32.into(), b"done");
    drop(send);
    drop(recv);
    endpoint.wait_idle().await;
    stop.cancel();
    serving.await.unwrap().unwrap();
    assert_eq!(global.used(), baseline);
    assert!(
        accepted,
        "optional metadata exhaustion must not abort a valid v5 subscription"
    );
    let packet = packet.expect("legacy media must resume after pressure clears");
    let header = packet::Header::parse(&packet).unwrap();
    assert_eq!(header.layout, super::Layout::Fixed);
    assert_eq!(packet.len(), 1088);
    assert_eq!(header.parity_count, 0);
}
