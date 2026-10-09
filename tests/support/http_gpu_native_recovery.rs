//! Native source loss must reap the old encoder and resume one shared publisher.
use super::*;
use std::time::Instant;

fn reset_receivers(receivers: &[Receiver; 2]) {
    for receiver in receivers {
        // Called only after the old worker and its uploads have closed.
        assert_eq!(receiver.capture.active.load(Ordering::SeqCst), 0);
        receiver.capture.data.lock().unwrap().clear();
        receiver.capture.paths.lock().unwrap().clear();
        receiver.capture.headers.lock().unwrap().clear();
        receiver.capture.requests.store(0, Ordering::SeqCst);
    }
}

async fn source_closed(source: &Source) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while source.capture.active.load(Ordering::SeqCst) != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("old native source body is released");
}

async fn recovery_case(
    fixture: &Fixture,
    protocol: &'static str,
    fallback: Option<&'static str>,
    encoder: &str,
) {
    let mut source = Source::new(fixture, protocol).await;
    let mut alternate = if let Some(protocol) = fallback {
        Some(Source::new(fixture, protocol).await)
    } else {
        None
    };
    let dir = tempfile::tempdir().unwrap();
    let engine = flussonix::media::Engine::new(dir.path().join("media"), "/usr/bin/ffmpeg");
    let mut receivers = [Receiver::new(false).await, Receiver::new(true).await];
    let outcome = std::panic::AssertUnwindSafe(async {
        let mut inputs = vec![source.input()];
        if let Some(alternate) = &alternate {
            inputs.push(alternate.input());
        }
        let store = ConfigStore::open(dir.path().join("config.json")).unwrap();
        let transcoder = if encoder == "h264_vaapi" {
            json!({"encoder":encoder,"qp":24,"acodec":"aac","ab":96})
        } else {
            json!({"encoder":encoder,"vb":1200,"acodec":"aac","ab":96})
        };
        store.put("templates", "recover-profile", json!({"transcoder":transcoder,
            "pushes":receivers.iter().map(destination).collect::<Vec<_>>()
        })).unwrap();
        store.put("streams", "owned-gpu", json!({"static":false,"template":"recover-profile",
            "inputs":inputs,"flussonix_input_timeout":10
        })).unwrap();
        let cfg = store.effective("owned-gpu").unwrap();
        let saved = store.snapshot();
        let (first, first_args) = generation(&engine, &receivers, &cfg, encoder).await;
        let first_driver = (encoder == "h264_vaapi").then(|| dependencies(&first));
        let first_pid = first.pid();
        assert!(first_pid > 0);
        assert_eq!(first.stats()["input_index"], 0);
        let initial_control = if protocol.starts_with("m4s") {"/native/m4s?token=owned-native-token"} else {"/native/m4f?token=owned-native-token"};
        assert_eq!(source.capture.paths.lock().unwrap().iter().filter(|p| p.as_str() == initial_control).count(), 1);
        assert_eq!(source.capture.active.load(Ordering::SeqCst), 1);
        if let Some(alternate) = &alternate {
            assert_eq!(alternate.capture.requests.load(Ordering::SeqCst), 0, "fallback must remain idle while the first source is healthy");
        }
        let fault = Instant::now();
        if alternate.is_some() {
            source.stop().await;
        } else {
            // Close only the current media response; keep the listener and
            // credentials available for a genuine retry of the same source.
            source.disconnect_current();
        }
        tokio::time::timeout(Duration::from_secs(8), first.closed()).await
            .expect("source loss terminates the old worker");
        tokio::time::timeout(Duration::from_secs(8), async {
            while first.alive.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("cancelled encoder and its owned tasks are reaped");
        assert!(!first.alive.load(Ordering::Relaxed));
        assert!(!Path::new(&format!("/proc/{first_pid}")).exists());
        source_closed(&source).await;
        let failed = first.stats();
        assert_eq!(failed["status"], "retrying");
        assert_eq!(failed["last_error"], "input_closed");
        let old_outputs = evidence(&receivers, dir.path(), "aac").await;
        let old_name = format!("recover-{encoder}-{protocol}-{}-before", fallback.unwrap_or("same"));
        retain(&old_name, dir.path(), &json!({"source":fixture.report,"outputs":old_outputs,
            "arguments":first_args,"dependencies":first_driver,"failure":"input_closed","encoder_reaped":true}));
        fixture.retain(&old_name);
        reset_receivers(&receivers);
        tokio::time::timeout(Duration::from_secs(4), async {
            while first.stats()["retry_in_ms"].as_u64().unwrap() > 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.unwrap();
        let (a,b) = tokio::join!(engine.recover("owned-gpu", &cfg), engine.recover("owned-gpu", &cfg));
        let recovered = a.unwrap();
        assert!(Arc::ptr_eq(&recovered, &b.unwrap()), "simultaneous recovery must create one worker");
        assert!(!Arc::ptr_eq(&first, &recovered));
        assert_eq!(recovered.stats()["restart_count"], 1);
        assert_eq!(recovered.stats()["input_index"], if fallback.is_some() {1} else {0});
        let (second, args) = generation(&engine, &receivers, &cfg, encoder).await;
        assert!(Arc::ptr_eq(&second, &recovered));
        assert_ne!(second.pid(), first_pid);
        let second_driver = (encoder == "h264_vaapi").then(|| dependencies(&second));
        assert!(args.windows(2).any(|v| v == ["-i", "pipe:0"]));
        assert!(args.windows(2).any(|v| v == ["-c:a", "aac"]));
        assert!(args.windows(2).any(|v| v == ["-b:a", "96k"]));
        let active = alternate.as_ref().unwrap_or(&source);
        assert_eq!(active.capture.active.load(Ordering::SeqCst), 1);
        let control = if active.protocol.starts_with("m4s") {"/native/m4s?token=owned-native-token"} else {"/native/m4f?token=owned-native-token"};
        let control_count = active.capture.paths.lock().unwrap().iter().filter(|p| p.as_str() == control).count();
        assert_eq!(control_count, if fallback.is_some() {1} else {2}, "one native pull per generation");
        assert!(active.capture.paths.lock().unwrap().iter().all(|p| p.ends_with("?token=owned-native-token")));
        assert!(active.capture.headers.lock().unwrap().iter().all(|h| h == &input_header()));
        assert_eq!(first.stats()["flussonix_pushes"], failed["flussonix_pushes"], "dead generation cannot publish again");
        assert_eq!(store.snapshot(), saved, "recovery must preserve saved stream/template configuration");
        for secret in ["native-input", "p:a@ss", "owned-native-token", "owned-publishing-only"] {
            assert!(!format!("{} {:?}", second.stats(), args).contains(secret));
        }
        let resumed_ms = fault.elapsed().as_millis();
        engine.stop_all().await;
        assert!(!Path::new(&format!("/proc/{}", second.pid())).exists());
        source_closed(active).await;
        let count = engine.http_push_egress.load(Ordering::Relaxed);
        let outputs = evidence(&receivers, dir.path(), "aac").await;
        assert_eq!(engine.http_push_egress.load(Ordering::Relaxed), count);
        assert_eq!(engine.count().await, 0);
        let name = format!("recover-{encoder}-{protocol}-{}-after", fallback.unwrap_or("same"));
        retain(&name, dir.path(), &json!({"source":fixture.report,"outputs":outputs,
            "arguments":args,"dependencies":second_driver,"input_protocol":active.protocol,
            "restart_count":1,"resumed_and_collected_ms":resumed_ms,
            "concurrent_recovery_coalesced":true,"old_encoder_reaped":true,"new_encoder_reaped":true}));
        fixture.retain(&name);
    }).catch_unwind().await;
    engine.stop_all().await;
    source.stop().await;
    if let Some(alternate) = &mut alternate {
        alternate.stop().await;
    }
    for r in &mut receivers {
        r.stop().await;
    }
    if let Err(p) = outcome {
        std::panic::resume_unwind(p);
    }
}

// Removing native failure cancellation or ordered input rotation must fail
// this ordinary decoded-media contract even on CI hosts without an Intel GPU.
#[tokio::test]
async fn native_source_loss_recovers_cpu_publishing_without_config_changes() {
    let fixture = Fixture::new("h264", "mp3").await;
    recovery_case(&fixture, "m4s", Some("m4f"), "libx264").await;
}

#[tokio::test]
#[ignore = "requires independent Intel H264 VAAPI renderD128 and installed iHD/GMM"]
async fn native_disconnect_reconnects_one_gpu_worker_and_both_http_outputs() {
    let fixture = Fixture::new("h264", "mp3").await;
    for protocol in ["m4s", "m4f"] {
        recovery_case(&fixture, protocol, None, "h264_vaapi").await;
    }
}

#[tokio::test]
#[ignore = "requires independent Intel H264 VAAPI renderD128 and installed iHD/GMM"]
async fn native_tls_source_loss_falls_back_with_one_gpu_worker_and_both_http_outputs() {
    let fixture = Fixture::new("hevc", "mp2").await;
    for (primary, fallback) in [("m4ss", "m4fs"), ("m4fs", "m4ss")] {
        recovery_case(&fixture, primary, Some(fallback), "h264_vaapi").await;
    }
}
