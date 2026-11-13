//! Exercise decode completion through the real encrypted client and its log boundary.
use super::*;
use crate::fxlink::{Encoder, packet::HEADER_LEN};
use std::sync::Mutex as StdMutex;
use tracing_subscriber::{Layer, layer::Context, prelude::*};

#[derive(Default)]
struct Fields {
    message: String,
    error: Option<String>,
    record: Option<u64>,
}
impl tracing::field::Visit for Fields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "message" => self.message = format!("{value:?}"),
            "error" => self.error = Some(format!("{value:?}")),
            _ => {}
        }
    }
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if field.name() == "record" {
            self.record = Some(value);
        }
    }
}
#[derive(Clone, Default)]
struct Capture(Arc<StdMutex<Vec<Fields>>>);
impl<S: tracing::Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        if event.metadata().target() != "flussonix::fxlink::client" {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        if matches!(
            fields.message.as_str(),
            "FXLink receive subscription ended" | "FXLink record announcement received"
        ) {
            let mut rows = self.0.lock().unwrap();
            assert!(rows.len() < 16, "unexpected diagnostic growth");
            rows.push(fields);
        }
    }
}
impl Capture {
    fn announced(&self, record: u64) -> bool {
        self.0
            .lock()
            .unwrap()
            .iter()
            .any(|row| row.record == Some(record))
    }
    fn errors(&self) -> Vec<String> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter_map(|row| row.error.clone())
            .collect()
    }
}

#[derive(Clone, Copy)]
enum Fault {
    None,
    Fragment,
    NativeLength,
}
fn record(sequence: u8) -> Bytes {
    let mut bytes = vec![sequence; 1000];
    bytes[..4].copy_from_slice(&996u32.to_be_bytes());
    Bytes::from(bytes)
}
async fn until(label: &str, mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_millis(750), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect(label);
}

// Break caught: a failed blocking batch is followed by Receiver::tick,
// replacing Corrupt with Retired before the receive task reports its cause.
#[tokio::test(flavor = "current_thread")]
async fn decode_corruption_keeps_first_error_before_receiver_tick() {
    completion_case(5, Fault::Fragment, None, Some("FXLink: Corrupt")).await;
}
// The same precedence must hold when an announcement was deferred while the
// decoder owned Receiver. Its invalid bounds must not hide the prior failure.
#[tokio::test(flavor = "current_thread")]
async fn decode_error_precedes_deferred_announcement() {
    completion_case(5, Fault::Fragment, Some(11), Some("FXLink: Corrupt")).await;
}
#[tokio::test(flavor = "current_thread")]
async fn v6_decode_error_precedes_deferred_announcement() {
    completion_case(6, Fault::Fragment, Some(11), Some("FXLink: Corrupt")).await;
}
#[tokio::test(flavor = "current_thread")]
async fn invalid_native_record_keeps_specific_first_error() {
    completion_case(
        5,
        Fault::NativeLength,
        None,
        Some("FXLink: InvalidRecord(\"native length prefix\")"),
    )
    .await;
}
// A successful decode does not exempt later validation, or discard its final
// counters when that validation retires the subscription.
#[tokio::test(flavor = "current_thread")]
async fn successful_decode_still_rejects_invalid_deferred_announcement() {
    completion_case(
        5,
        Fault::None,
        Some(11),
        Some("FXLink: InvalidRecord(\"announcement bounds\")"),
    )
    .await;
}
#[tokio::test(flavor = "current_thread")]
async fn successful_decode_keeps_deferred_announcement_and_record_order() {
    completion_case(5, Fault::None, Some(1000), None).await;
}

#[tokio::test(flavor = "current_thread")]
async fn v8_busy_decoder_keeps_deferred_announcement_and_record_order() {
    completion_case(8, Fault::None, Some(1000), None).await;
}
async fn completion_case(
    version: u8,
    fault: Fault,
    deferred_len: Option<usize>,
    expected_error: Option<&str>,
) {
    let _serial = test_lock().lock().await;
    let baseline = global().used();
    let slots = fec_slots().available_permits();
    let capture = Capture::default();
    // All client receive/router tasks run on this explicitly current-thread
    // runtime. The default is scoped to this test, never installed globally.
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let _dispatch = tracing::subscriber::set_default(subscriber);
    let cert = crate::transport_tls_fixture::Certificates::new();
    let mut tls = (*cert.server()).clone();
    tls.alpn_protocols = vec![format!("fxlink/{version}").into_bytes()];
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    config.transport_config(super::super::transport::config());
    let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let url = format!(
        "fxlink://{}/owned-completion",
        endpoint.local_addr().unwrap()
    );
    let remote = endpoint.clone();
    let server = tokio::spawn(async move {
        let connection = remote.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let Control::Subscribe {
            version: negotiated,
            id,
            generation,
            ..
        } = control::read(&mut recv).await.unwrap()
        else {
            panic!("subscription required")
        };
        assert_eq!((negotiated, id, generation), (version, 0, 1));
        control::write(&mut send, &Control::Accepted).await.unwrap();
        (connection, send, recv)
    });
    let settings = Settings {
        latency_ms: 1500,
        parity_shards: 0,
        ..Settings::default()
    };
    let pool = ClientPool::with_features(Features {
        geometry_hints: false,
        trimmed_records: version >= 6,
        repair_parity: version == 8,
        bootstrap_repair: version == 8,
    });
    let mut subscription = pool
        .subscribe(
            &url,
            "owned-peer",
            "owned-viewer",
            Some(&cert.ca),
            settings.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let (peer, mut send, recv) = server.await.unwrap();
    let mut encoder = Encoder::new(settings, 0, 1, Budget::new(1 << 20))
        .unwrap()
        .with_capabilities(Capabilities::for_version(version));
    let warm = encoder.encode(record(0), now()).unwrap();
    for packet in warm.datagrams {
        peer.send_datagram_wait(packet).await.unwrap();
    }
    until("warm output must be queued", || {
        subscription.state.queue.lock().unwrap().len() == 1
    })
    .await;
    let pause = review_tests::PauseGuard::new();
    let encoded = encoder.encode(record(1), now()).unwrap();
    assert_eq!(encoded.datagrams.len(), 1);
    let mut packet = encoded.datagrams[0].to_vec();
    match fault {
        Fault::None => {}
        // Header geometry is valid: only decoder-level fragment validation fails.
        Fault::Fragment => packet[HEADER_LEN + 18] = 1,
        // Fragment identity/length remain valid; the reassembled native prefix fails.
        Fault::NativeLength => {
            packet[HEADER_LEN + 20..HEADER_LEN + 24].copy_from_slice(&995u32.to_be_bytes())
        }
    }
    peer.send_datagram_wait(Bytes::from(packet)).await.unwrap();
    until("real blocking decoder must own Receiver", || {
        pause.entered()
    })
    .await;
    if let Some(len) = deferred_len {
        control::write(&mut send, &Control::Announce { record: 2, len })
            .await
            .unwrap();
        until(
            "announcement must be processed while decoder is held",
            || capture.announced(2),
        )
        .await;
    }
    drop(pause);
    let telemetry = subscription.telemetry();
    let mut delivered = Vec::new();
    if expected_error.is_some() {
        until("completion failure must retire", || !subscription.current()).await;
        assert!(subscription.recv().await.is_err());
        assert!(
            subscription.state.queue.lock().unwrap().is_empty(),
            "failure published or retained media"
        );
    } else {
        for _ in 0..2 {
            delivered.push(
                tokio::time::timeout(Duration::from_millis(750), subscription.recv())
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        assert!(
            subscription.current(),
            "valid deferred work retired healthy media"
        );
    }
    let errors = capture.errors();
    let received = subscription.stats().received_shards;
    let trimmed = subscription.stats().trimmed_shards;
    assert!(
        peer.close_reason().is_none(),
        "one subscription failure closed its peer"
    );
    drop(subscription);
    pool.close().await;
    drop((send, recv, peer));
    endpoint.close(0u32.into(), b"owned test complete");
    endpoint.wait_idle().await;
    assert_eq!(
        telemetry.snapshot()["state_bytes"],
        0,
        "completion leaked reservations"
    );
    assert_eq!(
        global().used(),
        baseline,
        "completion leaked ancestor charges"
    );
    assert_eq!(
        fec_slots().available_permits(),
        slots,
        "completion leaked decoder permit"
    );
    // Assert provenance only after cleanup, so canonical RED runs retain no workers.
    if let Some(expected) = expected_error {
        assert_eq!(
            errors,
            [expected],
            "specific first completion error was masked"
        );
    } else {
        assert!(errors.is_empty(), "unexpected receive error: {errors:?}");
        assert_eq!(
            delivered,
            [record(0), record(1)],
            "successful completion reordered output"
        );
    }
    assert_eq!(
        received, 2,
        "final decoder counters were lost on completion failure"
    );
    assert_eq!(
        trimmed,
        if version >= 6 { 2 } else { 0 },
        "negotiated record layout was not exercised"
    );
}
