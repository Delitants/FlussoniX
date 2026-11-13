//! Real QUIC peers deliberately separate reliable announcements from UDP media.
use super::{
    Budget, ClientPool, Encoder, Settings,
    control::{self, Control},
    transport,
};
use bytes::Bytes;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

fn record(n: u8) -> Bytes {
    let mut bytes = vec![n; 1000];
    bytes[..4].copy_from_slice(&996u32.to_be_bytes());
    Bytes::from(bytes)
}

// A source with a queued native bootstrap can announce more than the receiver's
// incomplete-record allowance before the media path catches up. Neither limit
// nor deadline should need increasing when the UDP records arrive in order.
#[tokio::test]
async fn announcement_burst_waits_for_decode_without_overflowing_record_window() {
    exercise(80, false, End::Media).await;
}

// A reliable repair write can require more than 10 ms of peer flow-control
// credit. Receiving original media must continue while that write is pending.
#[tokio::test]
async fn repair_backpressure_does_not_block_original_media() {
    exercise(24, true, End::Media).await;
}

#[tokio::test]
async fn repair_frames_finish_intact_when_peer_flow_credit_resumes() {
    exercise(25, true, End::ResumeRepair).await;
}

#[tokio::test]
async fn blocked_repair_writer_cannot_extend_original_record_deadline() {
    exercise(24, true, End::Deadline).await;
}

#[tokio::test]
async fn full_record_window_still_observes_remote_reset_promptly() {
    exercise(64, false, End::Reset).await;
}

#[tokio::test]
async fn fragmented_control_frames_survive_timer_and_media_progress() {
    exercise(8, false, End::Fragmented).await;
}

#[tokio::test]
async fn legacy_blocked_repair_write_keeps_timely_originals_healthy() {
    exercise(24, true, End::LegacyHealthy).await;
}

#[derive(Clone, Copy)]
enum End {
    Media,
    ResumeRepair,
    Fragmented,
    Deadline,
    Reset,
    LegacyHealthy,
}

async fn exercise(count: u8, blocked_repairs: bool, end: End) {
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let mut tls = (*cert.server()).clone();
    tls.alpn_protocols = vec![b"fxlink/1".to_vec()];
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    let mut wire = transport::config();
    if blocked_repairs {
        Arc::get_mut(&mut wire)
            .unwrap()
            .stream_receive_window(512u32.into());
    }
    config.transport_config(wire);
    let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let url = format!("fxlink://{}/live", endpoint.local_addr().unwrap());
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let server_settings = settings.clone();
    let server_endpoint = endpoint.clone();
    let server = tokio::spawn(async move {
        let connection = server_endpoint.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let Control::Subscribe { id, .. } = control::read(&mut recv).await.unwrap() else {
            panic!("expected subscription");
        };
        control::write(&mut send, &Control::Accepted).await.unwrap();
        let originals = if matches!(end, End::ResumeRepair) {
            count - 1
        } else {
            count
        };
        for n in 0..originals {
            if matches!(end, End::Fragmented) {
                let body = serde_json::to_vec(&Control::Announce {
                    record: n.into(),
                    len: 1000,
                })
                .unwrap();
                let prefix = (body.len() as u32).to_be_bytes();
                send.write_all(&prefix[..2]).await.unwrap();
                tokio::time::sleep(Duration::from_millis(6)).await;
                send.write_all(&prefix[2..]).await.unwrap();
                send.write_all(&body[..9]).await.unwrap();
                tokio::time::sleep(Duration::from_millis(6)).await;
                send.write_all(&body[9..]).await.unwrap();
                continue;
            }
            if control::write(
                &mut send,
                &Control::Announce {
                    record: n.into(),
                    len: 1000,
                },
            )
            .await
            .is_err()
            {
                return;
            }
        }
        // The reader starts only after the original media is sent, exercising
        // an unfinished reliable write concurrently with record completion.
        tokio::time::sleep(Duration::from_millis(if matches!(end, End::Deadline) {
            700
        } else {
            150
        }))
        .await;
        if matches!(end, End::Reset) {
            send.reset(1u32.into()).unwrap();
            // Keep the reverse direction readable: receipt of RESET_STREAM,
            // rather than a failed repair write, must fence queued output.
        }
        if matches!(
            end,
            End::Media | End::ResumeRepair | End::Fragmented | End::LegacyHealthy
        ) {
            let mut encoder =
                Encoder::new(server_settings, id, 1, Budget::new(1024 * 1024)).unwrap();
            for n in 0..originals {
                let encoded = encoder.encode(record(n), 0).unwrap();
                for packet in encoded.datagrams {
                    if connection.send_datagram_wait(packet).await.is_err() {
                        return;
                    }
                }
                // Keep the test about record scheduling, not the independent
                // bounded datagram route or consumer output overflow guards.
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        if matches!(end, End::ResumeRepair) {
            control::write(
                &mut send,
                &Control::Announce {
                    record: originals.into(),
                    len: 1000,
                },
            )
            .await
            .unwrap();
        }
        if matches!(end, End::LegacyHealthy) {
            tokio::time::sleep(Duration::from_millis(700)).await;
        }
        while let Ok(message) = control::read(&mut recv).await {
            if let Control::Repair { block, .. } = message {
                if matches!(end, End::ResumeRepair)
                    && block == u64::from(originals) * super::MAX_BLOCKS_PER_RECORD as u64
                {
                    let mut encoder =
                        Encoder::new(Settings::default(), id, 1, Budget::new(1024 * 1024)).unwrap();
                    let mut encoded = encoder.encode(record(originals), 0).unwrap();
                    // Advance sequence using real encodes so the last record's
                    // native packet header has the requested record/block IDs.
                    for _ in 0..originals {
                        encoded = encoder.encode(record(originals), 0).unwrap();
                    }
                    for packet in encoded.datagrams {
                        if connection.send_datagram_wait(packet).await.is_err() {
                            return;
                        }
                    }
                }
            }
        }
    });
    let pool = ClientPool::default();
    let mut sub = pool
        .subscribe(
            &url,
            "peer",
            "viewer",
            Some(&cert.ca),
            settings,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let outcome = tokio::time::timeout(Duration::from_secs(2), async {
        let mut received = Vec::new();
        for _ in 0..count {
            received.push(sub.recv().await?);
        }
        if matches!(end, End::LegacyHealthy) {
            tokio::time::sleep(Duration::from_millis(600)).await;
        }
        Ok::<_, String>(received)
    })
    .await;
    let elapsed = started.elapsed();
    let active = sub.current();
    let telemetry = sub.telemetry();
    drop(sub);
    pool.close().await;
    endpoint.close(0u32.into(), b"test done");
    server.await.unwrap();
    endpoint.wait_idle().await;
    assert_eq!(telemetry.snapshot()["state_bytes"], 0);
    assert_eq!(
        transport::global().used(),
        baseline,
        "scheduling run leaked reservations"
    );
    if matches!(end, End::Reset) {
        assert!(outcome.unwrap().is_err());
        assert!(!active);
        assert!(
            elapsed < Duration::from_millis(350),
            "full control window hid remote reset: {elapsed:?}"
        );
    } else if matches!(end, End::Deadline) {
        assert!(outcome.unwrap().is_err());
        assert!(!active);
        assert!(
            elapsed >= Duration::from_millis(450),
            "retired before the original deadline: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_millis(700),
            "repair write extended the deadline: {elapsed:?}"
        );
    } else {
        let records = outcome
            .expect("bounded delivery time")
            .expect("healthy subscription retired during control backpressure");
        assert_eq!(records, (0..count).map(record).collect::<Vec<_>>());
        assert!(active);
    }
}

// A first repair for record 1 must not inherit record 0's retry cooldown.
// This custom peer tests the client window at 300 ms RTT / 500 ms budget,
// where a global retry cooldown would leave no first-request opportunity.
// It bypasses the real listener's stricter source-age guard; production repair
// feasibility at this setting is not established by this test.
#[tokio::test]
async fn new_losses_get_first_repair_without_waiting_for_an_older_records_rtt() {
    long_rtt_case(false).await;
}

#[tokio::test]
async fn originals_get_a_reordering_grace_before_repair_requests() {
    long_rtt_case(true).await;
}

async fn long_rtt_case(originals: bool) {
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let mut tls = (*cert.server()).clone();
    tls.alpn_protocols = vec![b"fxlink/1".to_vec()];
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    config.transport_config(transport::config());
    let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let remote = endpoint.local_addr().unwrap();
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let url = format!("fxlink://{}/live", socket.local_addr().unwrap());
    let stop = CancellationToken::new();
    let proxy = tokio::spawn(delay_packets(socket, remote, stop.clone()));
    let server_endpoint = endpoint.clone();
    let settings = Settings {
        latency_ms: if originals { 1000 } else { 500 },
        ..Settings::default()
    };
    let server_settings = settings.clone();
    let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let peer_requests = requests.clone();
    let server = tokio::spawn(async move {
        let connection = server_endpoint.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let Control::Subscribe { id, .. } = control::read(&mut recv).await.unwrap() else {
            panic!("subscription");
        };
        let mut encoder = Encoder::new(server_settings, id, 1, Budget::new(1024 * 1024)).unwrap();
        let records = [
            encoder.encode(record(0), 0).unwrap(),
            encoder.encode(record(1), 0).unwrap(),
        ];
        control::write(&mut send, &Control::Accepted).await.unwrap();
        control::write(
            &mut send,
            &Control::Announce {
                record: 0,
                len: 1000,
            },
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(60)).await;
        control::write(
            &mut send,
            &Control::Announce {
                record: 1,
                len: 1000,
            },
        )
        .await
        .unwrap();
        if originals {
            for encoded in &records {
                for packet in &encoded.datagrams {
                    connection.send_datagram_wait(packet.clone()).await.unwrap();
                }
            }
        }
        while let Ok(Control::Repair { block, .. }) = control::read(&mut recv).await {
            peer_requests.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let index = (block / super::MAX_BLOCKS_PER_RECORD as u64) as usize;
            for packet in &records[index].datagrams {
                if connection.send_datagram_wait(packet.clone()).await.is_err() {
                    return;
                }
            }
        }
    });
    let pool = ClientPool::default();
    let mut sub = pool
        .subscribe(
            &url,
            "peer",
            "viewer",
            Some(&cert.ca),
            settings,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        let a = sub.recv().await?;
        let b = sub.recv().await?;
        Ok::<_, String>((a, b))
    })
    .await;
    let rtt = sub.telemetry().snapshot()["rtt_ms"].as_u64().unwrap();
    drop(sub);
    pool.close().await;
    endpoint.close(0u32.into(), b"test done");
    server.await.unwrap();
    stop.cancel();
    proxy.await.unwrap();
    endpoint.wait_idle().await;
    assert_eq!(transport::global().used(), baseline);
    assert!(rtt >= 250, "test did not exercise long RTT: {rtt}");
    assert_eq!(
        result
            .unwrap()
            .expect("new losses missed their first repair window"),
        (record(0), record(1))
    );
    if originals {
        assert_eq!(
            requests.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "repair requested before timely originals could arrive"
        );
    }
}

async fn delay_packets(
    socket: tokio::net::UdpSocket,
    remote: std::net::SocketAddr,
    stop: CancellationToken,
) {
    delay_packets_by(socket, remote, stop, Duration::from_millis(150)).await;
}
async fn delay_packets_by(
    socket: tokio::net::UdpSocket,
    remote: std::net::SocketAddr,
    stop: CancellationToken,
    delay: Duration,
) {
    let mut client = None;
    let mut pending = std::collections::VecDeque::new();
    let mut buffer = [0; 65536];
    loop {
        let due = pending
            .front()
            .map(|(due, _, _)| *due)
            .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(1));
        tokio::select! {
            _=stop.cancelled()=>break,
            _=tokio::time::sleep_until(due),if !pending.is_empty()=>{
                let (_,bytes,to):(tokio::time::Instant,Bytes,std::net::SocketAddr)=pending.pop_front().unwrap();
                socket.send_to(&bytes,to).await.unwrap();
            },
            packet=socket.recv_from(&mut buffer)=>{
                let (n,from)=packet.unwrap();
                let to=if from==remote {client.unwrap()} else {client=Some(from);remote};
                assert!(pending.len()<512,"owned delay fixture overloaded");
                pending.push_back((tokio::time::Instant::now()+delay,Bytes::copy_from_slice(&buffer[..n]),to));
            }
        }
    }
}

// DATAGRAM can overtake a lost reliable-control packet. Complete future
// records must not prevent admission/repair of the missing record at the head.
#[tokio::test]
async fn udp_overtaking_control_keeps_room_for_the_missing_head_record() {
    overtaking_case(64, false).await;
}

#[tokio::test]
async fn single_record_window_recovers_when_future_udp_arrives_first() {
    overtaking_case(1, false).await;
}

#[tokio::test]
async fn discarded_future_records_keep_their_first_deadline() {
    overtaking_case(4, true).await;
}

#[tokio::test]
async fn single_record_window_keeps_discarded_future_deadline() {
    overtaking_case(1, true).await;
}

async fn overtaking_case(limit: u8, late_last: bool) {
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let mut tls = (*cert.server()).clone();
    tls.alpn_protocols = vec![b"fxlink/1".to_vec()];
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    config.transport_config(transport::config());
    let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let url = format!("fxlink://{}/live", endpoint.local_addr().unwrap());
    let settings = Settings {
        latency_ms: 500,
        max_records: limit.into(),
        ..Settings::default()
    };
    let server_settings = settings.clone();
    let server_endpoint = endpoint.clone();
    let server = tokio::spawn(async move {
        let connection = server_endpoint.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let Control::Subscribe { id, .. } = control::read(&mut recv).await.unwrap() else {
            panic!("subscription");
        };
        control::write(&mut send, &Control::Accepted).await.unwrap();
        let mut encoder = Encoder::new(server_settings, id, 1, Budget::new(1024 * 1024)).unwrap();
        let encoded: Vec<_> = (0..=limit)
            .map(|n| encoder.encode(record(n), 0).unwrap())
            .collect();
        let first_udp = tokio::time::Instant::now();
        let original_bytes: usize = encoded[1..]
            .iter()
            .flat_map(|r| &r.datagrams)
            .map(Bytes::len)
            .sum();
        let mut repaired = 0usize;
        for future in &encoded[1..] {
            for packet in &future.datagrams {
                connection.send_datagram_wait(packet.clone()).await.unwrap();
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        // Complete the future burst before any repair service, preserving the
        // receiver-ceiling/discard case. Delay forward control independently of
        // reverse repair reads, as with reliable control lost on the network.
        let announcements = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            for n in 0..=u64::from(limit) {
                if control::write(
                    &mut send,
                    &Control::Announce {
                        record: n,
                        len: 1000,
                    },
                )
                .await
                .is_err()
                {
                    break;
                }
            }
            // Keep the stream in the task result until repair service ends.
            // Dropping it here would finish control early and bypass expiry.
            send
        });
        let serviced = async {
            while let Ok(Control::Repair { block, shards }) = control::read(&mut recv).await {
                let index = (block / super::MAX_BLOCKS_PER_RECORD as u64) as usize;
                if late_last && index == limit as usize {
                    tokio::time::sleep_until(first_udp + Duration::from_millis(600)).await;
                }
                for packet in &encoded[index].datagrams {
                    let h = super::packet::Header::parse(packet).unwrap();
                    if h.block_id != block
                        || h.shard_index >= h.data_count
                        || !shards.contains(&h.shard_index)
                    {
                        continue;
                    }
                    repaired += packet.len();
                    if limit == 64 && repaired > original_bytes / 4 {
                        return Err("receiver recovery exceeded real listener repair-byte share");
                    }
                    if connection.send_datagram_wait(packet.clone()).await.is_err() {
                        return Ok(());
                    }
                }
            }
            Ok(())
        }
        .await;
        announcements.abort();
        let _ = announcements.await;
        serviced.unwrap();
    });
    let pool = ClientPool::default();
    let mut sub = pool
        .subscribe(
            &url,
            "peer",
            "viewer",
            Some(&cert.ca),
            settings,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let mut records = Vec::new();
    let outcome = tokio::time::timeout(Duration::from_secs(2), async {
        for _ in 0..=limit {
            records.push(sub.recv().await?);
        }
        Ok::<_, String>(())
    })
    .await;
    let elapsed = started.elapsed();
    let telemetry = sub.telemetry();
    drop(sub);
    pool.close().await;
    endpoint.close(0u32.into(), b"test done");
    server.await.unwrap();
    endpoint.wait_idle().await;
    assert_eq!(transport::global().used(), baseline);
    assert_eq!(telemetry.snapshot()["state_bytes"], 0);
    if late_last {
        assert!(
            outcome.unwrap().is_err(),
            "discarded record received a renewed deadline"
        );
        assert_eq!(records, (0..limit).map(record).collect::<Vec<_>>());
        assert!(
            elapsed >= Duration::from_millis(450) && elapsed < Duration::from_millis(580),
            "discarded-record expiry moved: {elapsed:?}"
        );
    } else {
        outcome
            .unwrap()
            .expect("future UDP records blocked head repair");
        assert_eq!(records, (0..=limit).map(record).collect::<Vec<_>>());
    }
}

// A known missing head must get a repair opportunity between decode jobs,
// even when every due timer poll collides with a short job owning Receiver.
#[tokio::test]
async fn due_repairs_run_between_contended_decode_jobs() {
    let _serial = transport::test_lock().lock().await;
    let baseline = transport::global().used();
    let cert = crate::transport_tls_fixture::Certificates::new();
    let mut tls = (*cert.server()).clone();
    tls.alpn_protocols = vec![b"fxlink/1".to_vec()];
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    let mut wire = transport::config();
    let mut congestion = quinn::congestion::NewRenoConfig::default();
    congestion.initial_window(128 * 1024);
    Arc::get_mut(&mut wire)
        .unwrap()
        .congestion_controller_factory(Arc::new(congestion));
    config.transport_config(wire);
    let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let url = format!("fxlink://{}/live", socket.local_addr().unwrap());
    let stop = CancellationToken::new();
    let proxy = tokio::spawn(delay_packets_by(
        socket,
        endpoint.local_addr().unwrap(),
        stop.clone(),
        Duration::from_millis(95),
    ));
    let (start, started) = tokio::sync::oneshot::channel();
    let server_endpoint = endpoint.clone();
    let requested = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let peer_requested = requested.clone();
    let first_head_age = Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX));
    let peer_first_head_age = first_head_age.clone();
    let head_decisions = Arc::new(std::array::from_fn::<_, 3, _>(|_| {
        std::sync::atomic::AtomicUsize::new(0)
    }));
    let peer_head_decisions = head_decisions.clone();
    let server = tokio::spawn(async move {
        let connection = server_endpoint.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let Control::Subscribe { id, .. } = control::read(&mut recv).await.unwrap() else {
            panic!("subscription")
        };
        control::write(&mut send, &Control::Accepted).await.unwrap();
        started.await.unwrap();
        let mut encoder =
            Encoder::new(Settings::default(), id, 1, Budget::new(1024 * 1024)).unwrap();
        let records: Vec<_> = (0..64)
            .map(|n| encoder.encode(record(n), 0).unwrap())
            .collect();
        let created = std::time::Instant::now();
        for n in 0..64 {
            control::write(
                &mut send,
                &Control::Announce {
                    record: n,
                    len: 1000,
                },
            )
            .await
            .unwrap();
        }
        let mut original_bytes = 0;
        // Deliberately lose record0's original; timely future UDP is retained.
        for encoded in &records[1..] {
            for packet in &encoded.datagrams {
                original_bytes += packet.len();
                connection.send_datagram_wait(packet.clone()).await.unwrap();
            }
        }
        let mut repaired = 0;
        while let Ok(Control::Repair { block, shards }) = control::read(&mut recv).await {
            let index = (block / super::MAX_BLOCKS_PER_RECORD as u64) as usize;
            if index == 0 {
                peer_requested.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                peer_first_head_age.fetch_min(
                    created.elapsed().as_millis() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            // Match the listener's independent original-age and byte-share guards.
            if created.elapsed().as_millis() + connection.rtt().as_millis() + 5 >= 1000 {
                if index == 0 {
                    peer_head_decisions[1].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                continue;
            }
            for packet in &records[index].datagrams {
                let h = super::packet::Header::parse(packet).unwrap();
                if shards.contains(&h.shard_index) && repaired + packet.len() <= original_bytes / 4
                {
                    repaired += packet.len();
                    if connection.send_datagram_wait(packet.clone()).await.is_err() {
                        return;
                    }
                    if index == 0 {
                        peer_head_decisions[0].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                } else if index == 0 && shards.contains(&h.shard_index) {
                    peer_head_decisions[2].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    });
    let pool = ClientPool::default();
    let mut sub = pool
        .subscribe(
            &url,
            "peer",
            "viewer",
            Some(&cert.ca),
            Settings::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let cadence = super::client::review_tests::DecodeCadence::new();
    start.send(()).unwrap();
    let mut delivered = Vec::new();
    let outcome = tokio::time::timeout(Duration::from_secs(3), async {
        for _ in 0..64 {
            delivered.push(sub.recv().await?);
        }
        Ok::<_, String>(())
    })
    .await;
    let skipped = cadence.skipped_polls();
    drop(cadence);
    let rtt = sub.telemetry().snapshot()["rtt_ms"].as_u64().unwrap();
    let telemetry = sub.telemetry();
    let decoded_shards = sub.stats().received_shards;
    drop(sub);
    pool.close().await;
    endpoint.close(0u32.into(), b"test done");
    server.await.unwrap();
    stop.cancel();
    proxy.await.unwrap();
    endpoint.wait_idle().await;
    assert_eq!(transport::global().used(), baseline);
    assert_eq!(telemetry.snapshot()["state_bytes"], 0);
    assert!(rtt >= 180, "RTT too short: {rtt}");
    assert!(
        skipped >= 20,
        "fixture did not hold receiver across enough repair polls: {skipped}"
    );
    eprintln!(
        "contention fixture: RTT={rtt} ms, skipped_polls={skipped}, first_head_request_source_age={} ms, decoded_shards={decoded_shards}, head_sent={}, head_age_rejected={}, head_quota_rejected={}, delivered={}",
        first_head_age.load(std::sync::atomic::Ordering::Relaxed),
        head_decisions[0].load(std::sync::atomic::Ordering::Relaxed),
        head_decisions[1].load(std::sync::atomic::Ordering::Relaxed),
        head_decisions[2].load(std::sync::atomic::Ordering::Relaxed),
        delivered.len()
    );
    outcome
        .unwrap()
        .expect("decoder ownership consumed the useful repair window");
    assert!(
        requested.load(std::sync::atomic::Ordering::Relaxed) > 0,
        "known head was never requested"
    );
    assert_eq!(delivered, (0..64).map(record).collect::<Vec<_>>());
}
