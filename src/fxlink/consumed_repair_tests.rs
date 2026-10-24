//! A consumed record must not spend the only repair before a still-needed one.
use super::*;
use crate::fxlink::Budget;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use std::time::Instant;

struct ChannelFeed(std::sync::Mutex<Option<tokio::sync::mpsc::Receiver<Result<Bytes, String>>>>);
impl Provider for ChannelFeed {
    fn subscribe(&self, _: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
        let rx = self.0.lock().unwrap().take();
        Box::pin(async move {
            let rx = rx.ok_or(409u16)?;
            Ok(Source {
                pending_records: None,
                records: Box::pin(futures_util::stream::unfold(rx, |mut rx| async move {
                    rx.recv().await.map(|record| (record, rx))
                })),
                current: Arc::new(|| true),
                cancelled: CancellationToken::new(),
            })
        })
    }
}
fn record(id: u8) -> Bytes {
    let mut bytes = vec![id; 1004];
    bytes[..4].copy_from_slice(&1000u32.to_be_bytes());
    Bytes::from(bytes)
}
async fn next_packet(connection: &quinn::Connection) -> Bytes {
    tokio::time::timeout(Duration::from_secs(2), connection.read_datagram())
        .await
        .expect("bounded original arrival")
        .expect("live source")
}

#[tokio::test]
async fn consumed_repair_cannot_exhaust_credit_for_the_unconsumed_boundary() {
    let _serial = super::super::transport::test_lock().lock().await;
    let before = global().used();
    for version in 1..=7 {
        let settings = Settings {
            max_records: 4,
            parity_shards: 0,
            ..Settings::default()
        };
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        for id in 0..5 {
            tx.send(Ok(record(id))).await.unwrap();
        }
        let cert = test_tls::Certificates::new();
        let listener = Listener::bind_with_features(
            "127.0.0.1:0".parse().unwrap(),
            &cert.cert,
            &cert.key,
            settings.clone(),
            super::super::Features {
                geometry_hints: false,
                trimmed_records: version >= 6,
                repair_parity: version == 7,
                bootstrap_repair: false,
            },
        )
        .unwrap();
        let remote = listener.address();
        let egress = listener.egress();
        let stop = CancellationToken::new();
        let started = Instant::now();
        let serving = tokio::spawn(listener.serve(
            Arc::new(ChannelFeed(std::sync::Mutex::new(Some(rx)))),
            stop.clone(),
        ));
        let mut tls = (*cert.client()).clone();
        tls.alpn_protocols = vec![format!("fxlink/{version}").into_bytes()];
        let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap();
        let mut config = quinn::ClientConfig::new(Arc::new(crypto));
        config.transport_config(super::super::transport::config());
        let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(config);
        let connection = endpoint
            .connect(remote, "localhost")
            .unwrap()
            .await
            .unwrap();
        let (mut send, mut recv) = connection.open_bi().await.unwrap();
        control::write(
            &mut send,
            &Control::Subscribe {
                version,
                id: 0,
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
        let budget = Budget::new(1 << 20);
        let mut receiver =
            super::super::Receiver::new(settings.clone(), 0, 1, budget.clone()).unwrap();
        let mut output = Vec::new();
        for expected in 0..4 {
            let packet = next_packet(&connection).await;
            assert_eq!(Header::parse(&packet).unwrap().record_id, expected);
            assert_eq!(packet.len(), 1088);
            // Drop record1 deliberately. Only record0 is acknowledged consumed.
            if expected != 1 {
                output.extend(receiver.ingest(&packet, 0).unwrap());
            }
        }
        if version >= 2 {
            control::write(&mut send, &Control::Progress { next: 1 })
                .await
                .unwrap();
        }
        // V2+ cannot admit this fifth original until it has processed Progress1.
        // This is a causal barrier, without sleeping or inspecting private state.
        let fifth = next_packet(&connection).await;
        assert_eq!(Header::parse(&fifth).unwrap().record_id, 4);
        assert_eq!(fifth.len(), 1088);
        assert_eq!(egress.load(Ordering::Relaxed), 5440);
        // Allowance starts after serving starts: this places all five creation
        // times in its first 1,000 ms cohort, independent of wall-clock phase.
        assert!(started.elapsed() < Duration::from_millis(500));
        output.extend(receiver.ingest(&fifth, 0).unwrap());
        let requests = [
            super::super::RepairRequest {
                block_id: 0,
                shard_indices: vec![0],
            },
            super::super::RepairRequest {
                block_id: 2048,
                shard_indices: vec![0],
            },
        ];
        if version < 3 {
            for request in &requests {
                control::write(
                    &mut send,
                    &Control::Repair {
                        block: request.block_id,
                        shards: request.shard_indices.clone(),
                    },
                )
                .await
                .unwrap();
            }
        } else {
            // One frame preserves stale-before-useful order in the source route.
            let frames = if version == 7 {
                super::super::feedback::encode_parity(0, &requests, &settings, &mut 1).unwrap()
            } else if version >= 4 {
                let mut sequence = 1;
                super::super::feedback::encode_sequenced(0, &requests, &settings, &mut sequence)
                    .unwrap()
            } else {
                super::super::feedback::encode(0, &requests, &settings).unwrap()
            };
            assert_eq!(frames.len(), 1);
            connection.send_datagram(frames[0].clone()).unwrap();
        }
        // Five originals fund1360bytes: exactly one1088-byte repair can fit.
        let response =
            tokio::time::timeout(Duration::from_secs(2), connection.read_datagram()).await;
        connection.close(0u32.into(), b"test complete");
        drop(send);
        drop(recv);
        endpoint.wait_idle().await;
        stop.cancel();
        serving.await.unwrap().unwrap();
        // Close the real listener before assertions that intentionally fail RED.
        let packet = response
            .expect("bounded repair arrival")
            .expect("healthy source");
        let repaired = Header::parse(&packet).unwrap().record_id;
        output.extend(receiver.ingest(&packet, 0).unwrap());
        drop(receiver);
        assert_eq!(budget.used(), 0);
        assert_eq!(
            global().used(),
            before,
            "version{version} leaked transport state"
        );
        assert_eq!(egress.load(Ordering::Relaxed), 6528);
        if version == 1 {
            assert_eq!(repaired, 0, "v1 has no consumption acknowledgement");
            assert_eq!(output, [record(0)]);
        } else {
            assert_eq!(
                repaired, 1,
                "v{version}: consumed repair spent the only credit"
            );
            assert_eq!(output, (0..5).map(record).collect::<Vec<_>>());
        }
        drop(tx);
    }
}
