// Catch source-age renewal, route-churn grants, busy timers and unfair dispatch.
use super::{
    send::{Hub, SourceHandle},
    wire,
};
use crate::fxlink::{Budget, Settings};
use bytes::Bytes;
use futures_util::FutureExt;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
fn settings() -> Settings {
    Settings {
        latency_ms: 500,
        ..Settings::default()
    }
}
fn register(hub: &Arc<Hub>, id: u64, budget: &Budget) -> SourceHandle {
    hub.register(
        id,
        1,
        settings(),
        budget,
        Arc::new(|| true),
        CancellationToken::new(),
    )
    .unwrap()
}
fn take(hub: &Hub, at: u64) -> Option<wire::Frame> {
    let mut bytes = None;
    let result = hub.attempt_send(at, |frame| {
        bytes = Some(frame);
        true
    });
    assert_eq!(result.sent, bytes.is_some());
    if let Some(bytes) = bytes {
        assert_eq!(result.application_bytes, bytes.len());
        assert_eq!(result.attempted_modeled_bytes, bytes.len() + 128);
        Some(wire::decode(&bytes, &settings()).unwrap())
    } else {
        assert_eq!(result.application_bytes, 0);
        None
    }
}
#[test]
fn newest_eight_summaries_are_sorted_and_copy_is_identical_once() {
    let budget = Budget::new(64 * 1024);
    let hub = Hub::new(&budget).unwrap();
    let route = register(&hub, 7, &budget);
    for id in 0..11 {
        route.encoded(id, 1000, 0);
    }
    let frame = take(&hub, 0).unwrap();
    assert_eq!(frame.subscription, 7);
    assert_eq!(frame.count, 8);
    assert_eq!(
        frame.entries.map(|entry| entry.record_id),
        [3, 4, 5, 6, 7, 8, 9, 10]
    );
    route.original_admitted(16384);
    assert!(take(&hub, 19).is_none());
    assert_eq!(hub.wake_at(19), Some(20));
    assert_eq!(take(&hub, 20).unwrap(), frame);
    route.original_admitted(usize::MAX);
    assert!(take(&hub, 40).is_none());
    assert_eq!(hub.wake_at(40), None);
}
#[test]
fn hint_diagnostics_separate_actual_bytes_from_attempt_cost_and_copies() {
    let budget = Budget::new(64 * 1024);
    let hub = Hub::new(&budget).unwrap();
    let route = register(&hub, 7, &budget);
    route.encoded(0, 4835, 0);
    assert!(!hub.attempt_send(0, |_| false).sent);
    assert!(!hub.attempt_send(1, |_| panic!("rate guard ignored")).sent);
    assert!(!hub.attempt_send(20, |_| panic!("unfunded retry")).sent);
    route.original_admitted(16384);
    assert!(hub.attempt_send(20, |_| true).sent);
    route.original_admitted(16384);
    assert!(hub.attempt_send(40, |_| true).sent);
    let counters = hub.diagnostics();
    assert_eq!(counters["application_bytes"], 88);
    assert_eq!(counters["attempted_modeled_bytes"], 516);
    assert_eq!(counters["sent_frames"], 2);
    assert_eq!(counters["sent_copies"], 1);
    assert_eq!(counters["skipped_quota"], 1);
    assert_eq!(counters["skipped_rate"], 1);
    assert_eq!(counters["skipped_queue"], 1);
    let held = budget.reserve(budget.limit() - budget.used()).unwrap();
    let mandatory = hub.scoped(&budget).reserve(1024).unwrap();
    drop((mandatory, held));
    hub.disable();
    assert_eq!(hub.diagnostics()["pressure_disable_events"], 1);
}
#[test]
fn source_strict_margin_and_consumption_fence_every_attempt() {
    for (at, allowed) in [(494, true), (495, false), (500, false), (u64::MAX, false)] {
        let budget = Budget::new(64 * 1024);
        let hub = Hub::new(&budget).unwrap();
        let route = register(&hub, 1, &budget);
        route.encoded(0, 1000, 0);
        assert_eq!(take(&hub, at).is_some(), allowed);
    }
    let budget = Budget::new(64 * 1024);
    let hub = Hub::new(&budget).unwrap();
    let route = register(&hub, 1, &budget);
    route.encoded(0, 1000, 0);
    route.encoded(1, 1000, 0);
    route.consumed(1);
    let frame = take(&hub, 0).unwrap();
    assert_eq!(frame.count, 1);
    assert_eq!(frame.entries[0].record_id, 1);
    route.original_admitted(usize::MAX);
    route.consumed(2);
    assert!(take(&hub, 20).is_none());
    assert_eq!(hub.wake_at(20), None);
}
#[test]
fn newer_snapshot_supersedes_copy_and_duplicate_id_cannot_renew_creation() {
    let budget = Budget::new(64 * 1024);
    let hub = Hub::new(&budget).unwrap();
    let route = register(&hub, 1, &budget);
    route.encoded(0, 1000, 0);
    let first = take(&hub, 0).unwrap();
    route.original_admitted(16384);
    route.encoded(1, 1000, 1);
    let next = take(&hub, 20).unwrap();
    assert!(next.sequence > first.sequence);
    assert_eq!(next.count, 2);
    route.original_admitted(16384);
    assert_eq!(take(&hub, 40).unwrap(), next);
    route.encoded(0, 1000, 490);
    route.original_admitted(16384);
    let surviving = take(&hub, 495).unwrap();
    assert_eq!(surviving.count, 1);
    assert_eq!(
        surviving.entries[0].record_id, 1,
        "duplicate renewed expired record0"
    );
    assert!(take(&hub, 496).is_none());
}
#[test]
fn shared_startup_credit_cannot_be_reset_by_retired_or_stale_handles() {
    let budget = Budget::new(64 * 1024);
    let hub = Hub::new(&budget).unwrap();
    let old = register(&hub, 1, &budget);
    for id in 0..8 {
        old.encoded(id, 1000, 0);
    }
    assert!(take(&hub, 0).is_some());
    old.retire();
    let replacement = register(&hub, 1, &budget);
    replacement.encoded(100, 1000, 20);
    old.encoded(200, 1000, 20);
    old.original_admitted(usize::MAX);
    old.consumed(u64::MAX);
    old.retire();
    assert!(take(&hub, 20).is_none());
    assert_eq!(hub.wake_at(20), None);
    replacement.original_admitted(11008);
    let actual = take(&hub, 20).unwrap();
    assert_eq!(actual.count, 1);
    assert_eq!(actual.entries[0].record_id, 100);
}
#[test]
fn sixteen_eligible_routes_rotate_and_unfunded_slots_do_not_spin() {
    let budget = Budget::new(128 * 1024);
    let hub = Hub::new(&budget).unwrap();
    let routes = (0..16)
        .map(|id| {
            let route = register(&hub, id, &budget);
            route.encoded(0, 1000, 0);
            route.original_admitted(16384);
            route
        })
        .collect::<Vec<_>>();
    assert!(
        hub.register(
            16,
            1,
            settings(),
            &budget,
            Arc::new(|| true),
            CancellationToken::new()
        )
        .is_none()
    );
    let mut seen = Vec::new();
    for step in 0..16 {
        let at = step * 10;
        for route in &routes {
            route.original_admitted(16384);
        }
        let frame = take(&hub, at).unwrap();
        seen.push(frame.subscription);
        assert!(hub.wake_at(at).is_none_or(|next| next > at));
    }
    assert_eq!(seen, (0..16).collect::<Vec<_>>());
    let budget = Budget::new(64 * 1024);
    let hub = Hub::new(&budget).unwrap();
    let a = register(&hub, 0, &budget);
    for id in 0..8 {
        a.encoded(id, 1000, 0);
    }
    take(&hub, 0).unwrap();
    let b = register(&hub, 1, &budget);
    b.encoded(0, 1000, 0);
    assert_eq!(hub.wake_at(100), None);
    assert!(take(&hub, 100).is_none());
    b.original_admitted(11008);
    assert_eq!(take(&hub, 100).unwrap().subscription, 1);
}
#[test]
fn failed_enqueue_is_charged_and_cancel_or_pressure_destroys_optional_payload() {
    let budget = Budget::new(64 * 1024);
    let hub = Hub::new(&budget).unwrap();
    let route = register(&hub, 1, &budget);
    for id in 0..8 {
        route.encoded(id, 1000, 0);
    }
    let attempted = hub.attempt_send(0, |_| false);
    assert!(!attempted.sent);
    assert_eq!(attempted.attempted_modeled_bytes, 256);
    assert!(take(&hub, 20).is_none());
    assert_eq!(hub.wake_at(20), None);
    let held = budget.used();
    assert!(held > 0 && held <= 4096 + 2048);
    hub.disable();
    assert!(budget.used() < held);
    route.original_admitted(usize::MAX);
    assert!(take(&hub, 40).is_none());
    drop(route);
    drop(hub);
    assert_eq!(budget.used(), 0);
    let hub = Hub::new(&budget).unwrap();
    let valid = Arc::new(AtomicBool::new(true));
    let current = valid.clone();
    let cancel = CancellationToken::new();
    let route = hub
        .register(
            1,
            1,
            settings(),
            &budget,
            Arc::new(move || current.load(Ordering::Acquire)),
            cancel.clone(),
        )
        .unwrap();
    route.encoded(0, 1000, 0);
    valid.store(false, Ordering::Release);
    assert!(take(&hub, 0).is_none());
    valid.store(true, Ordering::Release);
    cancel.cancel();
    assert!(take(&hub, 20).is_none());
    drop(route);
    drop(hub);
    assert_eq!(budget.used(), 0);
}

async fn pair() -> (
    quinn::Endpoint,
    quinn::Endpoint,
    quinn::Connection,
    quinn::Connection,
) {
    let cert = crate::transport_tls_fixture::Certificates::new();
    let mut transport = quinn::TransportConfig::default();
    transport
        .datagram_send_buffer_size(16 * 1024)
        .datagram_receive_buffer_size(Some(512 * 1024))
        .send_window(1024)
        .stream_receive_window(64u32.into());
    let transport = Arc::new(transport);
    let mut tls = (*cert.server()).clone();
    tls.alpn_protocols = crate::fxlink::control::protocols();
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    config.transport_config(transport.clone());
    let server = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut tls = (*cert.client()).clone();
    tls.alpn_protocols = crate::fxlink::control::protocols();
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap();
    let mut config = quinn::ClientConfig::new(Arc::new(crypto));
    config.transport_config(transport);
    let mut client = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    client.set_default_client_config(config);
    let (sender, receiver) = tokio::join!(
        client
            .connect(server.local_addr().unwrap(), "localhost")
            .unwrap(),
        async { server.accept().await.unwrap().await }
    );
    (client, server, sender.unwrap(), receiver.unwrap())
}

#[tokio::test]
async fn actual_non_evicting_poll_preserves_a_full_queue_of_originals() {
    let _serial = crate::fxlink::transport::test_lock().lock().await;
    let (client, server, sender, receiver) = pair().await;
    let budget = Budget::new(4 * 1024 * 1024 + 64 * 1024);
    let _transport = budget.reserve(4 * 1024 * 1024).unwrap();
    let hub = Hub::new(&budget).unwrap();
    let route = register(&hub, 7, &budget);
    for id in 0..8 {
        route.encoded(id, 1000, 0);
    }
    let mut originals = Vec::new();
    for sequence in 0..512 {
        let bytes = Bytes::from(vec![(sequence % 251) as u8; 128]);
        match sender.send_datagram_wait(bytes.clone()).now_or_never() {
            Some(Ok(())) => originals.push(bytes),
            None => break,
            Some(Err(error)) => panic!("fill: {error}"),
        }
    }
    assert!(!originals.is_empty() && originals.len() < 512);
    assert!(
        sender.datagram_send_buffer_space() < 44,
        "queue fixture left room for a hint"
    );
    let space = sender.datagram_send_buffer_space();
    let attempt = hub.attempt(&sender, 0);
    assert!(!attempt.sent);
    assert_eq!(attempt.attempted_modeled_bytes, 256);
    assert_eq!(
        sender.datagram_send_buffer_space(),
        space,
        "hint evicted original traffic"
    );
    let received = tokio::time::timeout(Duration::from_secs(2), async {
        let mut received = Vec::new();
        for _ in 0..originals.len() {
            received.push(receiver.read_datagram().await.unwrap());
        }
        received
    })
    .await
    .unwrap();
    assert_eq!(received, originals);
    route.original_admitted(16384);
    assert!(hub.attempt(&sender, 20).sent);
    let hint = tokio::time::timeout(Duration::from_secs(2), receiver.read_datagram())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(wire::decode(&hint, &settings()).unwrap().count, 8);
    route.retire();
    hub.disable();
    client.close(0u32.into(), b"done");
    server.close(0u32.into(), b"done");
    client.wait_idle().await;
    server.wait_idle().await;
}

#[tokio::test]
async fn published_geometry_dispatches_while_a_reliable_writer_is_stalled() {
    let _serial = crate::fxlink::transport::test_lock().lock().await;
    let (client, server, sender, receiver) = pair().await;
    let (mut send, _recv) = sender.open_bi().await.unwrap();
    let payload = vec![42; 4096];
    let mut writer = Box::pin(send.write_all(&payload));
    assert!(writer.as_mut().now_or_never().is_none());
    let budget = Budget::new(64 * 1024);
    let hub = Hub::new(&budget).unwrap();
    let route = register(&hub, 7, &budget);
    route.encoded(0, 1000, 0);
    assert!(hub.attempt(&sender, 0).sent);
    let bytes = tokio::time::timeout(Duration::from_secs(2), receiver.read_datagram())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        wire::decode(&bytes, &settings()).unwrap().entries[0].record_len,
        1000
    );
    assert!(
        writer.as_mut().now_or_never().is_none(),
        "writer fixture did not remain blocked"
    );
    drop(writer);
    send.reset(1u32.into()).unwrap();
    route.retire();
    hub.disable();
    client.close(0u32.into(), b"done");
    server.close(0u32.into(), b"done");
    client.wait_idle().await;
    server.wait_idle().await;
}

#[test]
fn release_dispatch_commits_both_spacing_fences_on_every_attempt() {
    let budget = Budget::new(64 * 1024);
    let hub = Hub::new(&budget).unwrap();
    let a = register(&hub, 0, &budget);
    let b = register(&hub, 1, &budget);
    a.encoded(0, 1000, 0);
    b.encoded(0, 1000, 0);
    a.original_admitted(usize::MAX);
    b.original_admitted(usize::MAX);
    assert_eq!(take(&hub, 0).unwrap().subscription, 0);
    assert!(take(&hub, 0).is_none(), "shared spacing was not committed");
    assert!(take(&hub, 9).is_none());
    assert_eq!(take(&hub, 10).unwrap().subscription, 1);
    b.retire();
    a.encoded(1, 1000, 10);
    a.original_admitted(usize::MAX);
    assert_eq!(take(&hub, 20).unwrap().count, 2);
    a.encoded(2, 1000, 21);
    a.original_admitted(usize::MAX);
    assert!(take(&hub, 30).is_none(), "route spacing was not committed");
    assert!(take(&hub, 39).is_none());
    assert_eq!(take(&hub, 40).unwrap().count, 3);
    assert!(take(&hub, 29).is_none());
    a.original_admitted(usize::MAX);
    assert_eq!(hub.wake_at(29), Some(60));
}

#[test]
fn in_flight_scope_retires_on_early_exit_and_commit_preserves_cached_route() {
    let budget = Budget::new(64 * 1024);
    let hub = Hub::new(&budget).unwrap();
    let route = register(&hub, 1, &budget);
    {
        let _flight = route.in_flight();
        route.encoded(0, 1000, 0);
    }
    assert!(
        take(&hub, 0).is_none(),
        "scope exit left an in-flight summary usable"
    );
    let cached = register(&hub, 2, &budget);
    {
        let flight = cached.in_flight();
        cached.encoded(0, 1000, 0);
        flight.commit();
    }
    assert_eq!(take(&hub, 0).unwrap().subscription, 2);
}

struct DropMode;
impl Drop for DropMode {
    fn drop(&mut self) {
        crate::fxlink::listener::TEST_DROP.store(0, Ordering::Relaxed);
    }
}
fn native(length: usize) -> Bytes {
    let mut raw = vec![42; length];
    raw[..4].copy_from_slice(&((length - 4) as u32).to_be_bytes());
    raw.into()
}

#[tokio::test]
async fn native_enqueue_grants_only_actual_original_admission_including_parity() {
    use crate::fxlink::{
        Capabilities, Encoder, Source,
        listener::{Admission, enqueue},
    };
    let _serial = crate::fxlink::transport::test_lock().lock().await;
    let (client, server, sender, receiver) = pair().await;
    let budget = Budget::new(8 * 1024 * 1024);
    let _transport = budget.reserve(4 * 1024 * 1024).unwrap();
    let hub = Hub::new(&budget).unwrap();
    let route = register(&hub, 7, &budget);
    let settings = settings();
    let cancel = CancellationToken::new();
    let mut source = Source {
        records: Box::pin(futures_util::stream::pending()),
        pending_records: None,
        current: Arc::new(|| true),
        cancelled: CancellationToken::new(),
    };
    let mut encoder = Encoder::new(settings.clone(), 7, 1, budget.clone())
        .unwrap()
        .with_capabilities(Capabilities::V7);
    let mut frames = Vec::new();
    for id in 0..8 {
        let length = if id == 7 { 14_000 } else { 1000 };
        let encoded = encoder.encode(native(length), 0).unwrap();
        route.encoded(id, length, 0);
        frames.push(encoded);
    }
    take(&hub, 0).unwrap(); // Consume the single connection startup grant.
    let egress = AtomicU64::new(0);
    let mut expected = Vec::new();
    for packet in &frames[7].datagrams {
        assert_eq!(
            enqueue(
                &sender,
                packet,
                &mut source,
                &cancel,
                crate::fxlink::transport::now() + 500,
                &settings,
                &egress,
                Some(&route)
            )
            .await
            .unwrap(),
            Admission::Queued
        );
        expected.push(packet.clone());
    }
    assert!(
        take(&hub, 20).is_some(),
        "successful originals did not replenish hint credit"
    );
    let next = encoder.encode(native(14_000), 20).unwrap();
    route.encoded(8, 14_000, 20);
    let mode = DropMode;
    crate::fxlink::listener::TEST_DROP.store(2, Ordering::Relaxed);
    for packet in &next.datagrams {
        enqueue(
            &sender,
            packet,
            &mut source,
            &cancel,
            crate::fxlink::transport::now() + 500,
            &settings,
            &egress,
            Some(&route),
        )
        .await
        .unwrap();
    }
    assert!(
        take(&hub, 40).is_none(),
        "synthetic drop before enqueue created original credit"
    );
    drop(mode);
    for packet in &frames[7].datagrams {
        enqueue(
            &sender,
            packet,
            &mut source,
            &cancel,
            crate::fxlink::transport::now() + 500,
            &settings,
            &egress,
            None,
        )
        .await
        .unwrap();
        expected.push(packet.clone());
    }
    assert!(
        take(&hub, 40).is_none(),
        "repair bytes created original credit"
    );
    for packet in &next.datagrams {
        enqueue(
            &sender,
            packet,
            &mut source,
            &cancel,
            crate::fxlink::transport::now() + 500,
            &settings,
            &egress,
            Some(&route),
        )
        .await
        .unwrap();
        expected.push(packet.clone());
    }
    assert!(take(&hub, 40).is_some());
    let received = tokio::time::timeout(Duration::from_secs(2), async {
        let mut received = Vec::new();
        for _ in 0..expected.len() {
            received.push(receiver.read_datagram().await.unwrap());
        }
        received
    })
    .await
    .unwrap();
    assert_eq!(received, expected);
    assert_eq!(
        egress.load(Ordering::Relaxed),
        expected.iter().map(|b| b.len() as u64).sum::<u64>()
    );
    route.retire();
    hub.disable();
    client.close(0u32.into(), b"done");
    server.close(0u32.into(), b"done");
    client.wait_idle().await;
    server.wait_idle().await;
}

#[test]
fn source_connection_pressure_drops_payload_before_one_mandatory_retry() {
    let root = Budget::new(16 * 1024);
    let a = root.child(16 * 1024);
    let b = root.child(16 * 1024);
    let hub = Hub::new(&root).unwrap();
    let first = register(&hub, 0, &a);
    let second = register(&hub, 1, &b);
    first.encoded(0, 1000, 0);
    second.encoded(0, 1000, 0);
    let pressure = root.reserve(root.limit() - root.used() - 1024).unwrap();
    let scoped = hub.scoped(&a);
    let mandatory = scoped
        .reserve(3000)
        .expect("optional source ownership blocked media");
    first.original_admitted(usize::MAX);
    second.original_admitted(usize::MAX);
    assert!(take(&hub, 0).is_none());
    assert_eq!(hub.wake_at(0), None);
    drop(mandatory);
    drop(pressure);
    drop(first);
    drop(second);
    drop(hub);
    assert_eq!(root.used(), 0);
}
