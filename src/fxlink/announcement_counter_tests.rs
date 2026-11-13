//! Live-announcement operation uses the real receiver clock and API telemetry.
use super::*;
use crate::fxlink::Encoder;

fn native(len: usize, fill: u8) -> Bytes {
    let mut bytes = vec![fill; len];
    bytes[..4].copy_from_slice(&((len - 4) as u32).to_be_bytes());
    Bytes::from(bytes)
}

// Break caught: the live announcement handler propagates expiry before copying
// the final counters, leaving the API at expired=0 and losing missing shards.
#[test]
fn partial_record_expiry_publishes_loss_and_prior_recovery_at_original_deadline() {
    let settings = Settings {
        latency_ms: 500,
        data_shards: 3,
        parity_shards: 1,
        ..Settings::default()
    };
    let budget = Budget::new(1 << 20);
    let mut decoder = Receiver::new(settings.clone(), 0, 1, budget.clone()).unwrap();
    let mut encoder = Encoder::new(settings, 0, 1, Budget::new(1 << 20)).unwrap();
    let telemetry = Telemetry::default();
    let warm = encoder.encode(native(2200, 1), 900).unwrap();
    let mut output = Vec::new();
    for index in [0, 2, 3] {
        output.extend(decoder.ingest_charged(&warm.datagrams[index], 900).unwrap());
    }
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].bytes, native(2200, 1));
    drop(output);
    *telemetry.stats.lock().unwrap() = decoder.stats().clone();
    let partial = encoder.encode(native(2200, 2), 1000).unwrap();
    assert!(
        decoder
            .ingest_charged(&partial.datagrams[0], 1000)
            .unwrap()
            .is_empty()
    );
    // A repeated announcement must neither retire healthy state nor renew age.
    apply_announcement(&mut decoder, &telemetry, 1, 2200, 1499).unwrap();
    assert!(!decoder.is_retired());
    let result = apply_announcement(&mut decoder, &telemetry, 2, 1000, 1500);
    let snapshot = telemetry.snapshot();
    assert!(decoder.is_retired());
    assert_eq!(
        decoder
            .ingest_charged(&partial.datagrams[1], 1501)
            .unwrap_err(),
        super::super::Error::Retired
    );
    drop(decoder);
    assert_eq!(
        budget.used(),
        0,
        "announcement failure leaked receiver charges"
    );
    assert_eq!(result.unwrap_err(), "FXLink: Expired { record_id: 1 }");
    assert_eq!(snapshot["expired_records"], 1);
    assert_eq!(snapshot["original_lost_shards"], 3); // One recovered warm + two still missing.
    assert_eq!(snapshot["fec_recovered_shards"], 1);
    assert_eq!(snapshot["received_shards"], 4);
}

// A wholly absent head also expires, without inventing a known missing-shard
// count. v6 trimmed counters from its retained successor must survive the exit.
#[test]
fn absent_head_expiry_publishes_trimmed_successor_counters() {
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let budget = Budget::new(1 << 20);
    let mut decoder = Receiver::new(settings.clone(), 0, 1, budget.clone())
        .unwrap()
        .with_capabilities(Capabilities::for_version(6));
    let mut encoder = Encoder::new(settings, 0, 1, Budget::new(1 << 20))
        .unwrap()
        .with_capabilities(Capabilities::for_version(6));
    let _missing = encoder.encode(native(1000, 1), 1000).unwrap();
    let successor = encoder.encode(native(1000, 2), 1000).unwrap();
    assert_eq!(successor.datagrams.len(), 1);
    assert!(
        decoder
            .ingest_charged(&successor.datagrams[0], 1000)
            .unwrap()
            .is_empty()
    );
    let telemetry = Telemetry::default();
    let result = apply_announcement(&mut decoder, &telemetry, 2, 1000, 1501);
    let snapshot = telemetry.snapshot();
    assert!(decoder.is_retired());
    drop(decoder);
    assert_eq!(budget.used(), 0);
    assert_eq!(result.unwrap_err(), "FXLink: Expired { record_id: 0 }");
    assert_eq!(snapshot["expired_records"], 1);
    assert_eq!(snapshot["received_shards"], 1);
    assert_eq!(snapshot["trimmed_shards"], 1);
    assert_eq!(snapshot["original_lost_shards"], 0);
}

// Failure publication must retain prior counters for validation errors too,
// without replacing the specific announcement error or creating an expiry.
#[test]
fn invalid_announcement_publishes_prior_late_and_received_counters() {
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let budget = Budget::new(1 << 20);
    let mut decoder = Receiver::new(settings.clone(), 0, 1, budget.clone()).unwrap();
    let mut encoder = Encoder::new(settings, 0, 1, Budget::new(1 << 20)).unwrap();
    let encoded = encoder.encode(native(1000, 1), 1000).unwrap();
    assert_eq!(
        decoder
            .ingest_charged(&encoded.datagrams[0], 1000)
            .unwrap()
            .len(),
        1
    );
    assert!(
        decoder
            .ingest_charged(&encoded.datagrams[0], 1001)
            .unwrap()
            .is_empty()
    );
    let telemetry = Telemetry::default();
    let result = apply_announcement(&mut decoder, &telemetry, 1, 11, 1002);
    let snapshot = telemetry.snapshot();
    assert!(decoder.is_retired());
    drop(decoder);
    assert_eq!(budget.used(), 0);
    assert_eq!(
        result.unwrap_err(),
        "FXLink: InvalidRecord(\"announcement bounds\")"
    );
    assert_eq!(snapshot["late_shards"], 1);
    assert_eq!(snapshot["received_shards"], 1);
    assert_eq!(snapshot["expired_records"], 0);
}

#[test]
fn healthy_announcements_preserve_record_deadline_and_ordered_delivery() {
    let settings = Settings {
        latency_ms: 500,
        ..Settings::default()
    };
    let budget = Budget::new(1 << 20);
    let mut decoder = Receiver::new(settings.clone(), 0, 1, budget.clone()).unwrap();
    let mut encoder = Encoder::new(settings, 0, 1, Budget::new(1 << 20)).unwrap();
    let telemetry = Telemetry::default();
    apply_announcement(&mut decoder, &telemetry, 0, 1000, 1000).unwrap();
    apply_announcement(&mut decoder, &telemetry, 0, 1000, 1499).unwrap();
    let first = encoder.encode(native(1000, 1), 1000).unwrap();
    let output = decoder.ingest_charged(&first.datagrams[0], 1499).unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].bytes, native(1000, 1));
    assert_eq!(output[0].deadline_ms, 1500);
    drop(output);
    apply_announcement(&mut decoder, &telemetry, 1, 1000, 1500).unwrap();
    let second = encoder.encode(native(1000, 2), 1500).unwrap();
    let output = decoder.ingest_charged(&second.datagrams[0], 1500).unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].bytes, native(1000, 2));
    assert_eq!(output[0].deadline_ms, 2000);
    assert!(!decoder.is_retired());
    assert_eq!(decoder.stats().expired_records, 0);
    drop(output);
    drop(decoder);
    assert_eq!(budget.used(), 0);
}
