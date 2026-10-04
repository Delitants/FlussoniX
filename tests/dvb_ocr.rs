#[path = "support/dvb_fixture.rs"]
mod fixture;
use flussonix::{
    captions::Service,
    dvb::{Frame, Image},
    dvb_ocr::{Failure, Recognition, Store, parse_tsv, recognize},
};
use std::time::{Duration, Instant};
fn store() -> Store {
    Store::new(
        &serde_json::from_value::<Vec<Service>>(serde_json::json!([
            {"dvb_page":1,"ocr_language":"eng","language":"en","name":"English"},
            {"dvb_page":2,"ocr_language":"deu","language":"de","name":"German"}
        ]))
        .unwrap(),
    )
}
fn frame(page: u16, pts: u64, pixels: bool) -> Frame {
    Frame {
        page,
        pts,
        expires: pts + 180000,
        image: pixels.then(|| Image {
            width: 2,
            height: 2,
            pixels: vec![[255, 255]; 4],
        }),
    }
}
fn good(text: &str) -> Result<Recognition, Failure> {
    Ok(Recognition {
        text: text.into(),
        confidence: Some(96.0),
    })
}
#[test]
fn pending_clear_retains_source_interval_and_releases_frontier() {
    let mut s = store();
    s.ingest(frame(1, 100000, true), Instant::now());
    let j = s.take_job().unwrap();
    assert_eq!(s.frontier(500000), 100000);
    s.ingest(frame(1, 140000, false), Instant::now());
    s.complete(1, j.token, good("HELLO"));
    let c = s.snapshot();
    assert_eq!(c.len(), 1);
    assert_eq!((c[0].start, c[0].end), (100000, Some(140000)));
    assert_eq!(s.frontier(500000), 500000);
}
#[test]
fn reset_rejects_active_and_queued_tokens_without_losing_other_page() {
    let mut s = store();
    s.ingest(frame(1, 100000, true), Instant::now());
    let a = s.take_job().unwrap();
    s.ingest(frame(2, 110000, true), Instant::now());
    s.reset(&[1], 120000, "dvb_transport_gap");
    s.complete(1, a.token, good("STALE"));
    let b = s.take_job().unwrap();
    s.complete(2, b.token, good("GRÜSSE"));
    assert_eq!(s.snapshot().len(), 1);
    assert_eq!(s.snapshot()[0].channel, 65538);
}
#[test]
fn repeated_images_extend_pending_interval_but_failed_images_can_retry() {
    let mut s = store();
    let now = Instant::now();
    s.ingest(frame(1, 100000, true), now);
    let a = s.take_job().unwrap();
    s.ingest(frame(1, 130000, true), now);
    assert!(s.take_job().is_none());
    s.complete(1, a.token, good("HELLO"));
    assert_eq!(s.snapshot()[0].end, Some(310000));
    s.reset(&[1], 140000, "reset");
    s.ingest(frame(1, 150000, true), now);
    let b = s.take_job().unwrap();
    s.complete(1, b.token, Err(Failure::new("dvb_ocr_timeout")));
    s.ingest(frame(1, 160000, true), now);
    assert!(s.take_job().is_some());
}
#[test]
fn timeout_queue_pressure_and_history_are_bounded() {
    let mut s = store();
    let now = Instant::now();
    for i in 0..12 {
        let mut f = frame(1, 100000 + i * 1000, true);
        f.image.as_mut().unwrap().pixels[0][0] = i as u8;
        s.ingest(f, now);
    }
    let mut jobs = 0;
    while s.take_job().is_some() {
        jobs += 1
    }
    assert_eq!(jobs, 8);
    assert_eq!(s.frontier(500000), 100000);
    s.expire(now + Duration::from_secs(2));
    assert_eq!(s.frontier(500000), 500000);
    assert!(s.snapshot().is_empty());
    assert!(s.stats().to_string().contains("dvb_ocr_queue_limit"));
}
#[test]
fn late_historical_result_does_not_overwrite_current_failure() {
    let mut s = store();
    let now = Instant::now();
    s.ingest(frame(1, 100000, true), now);
    let a = s.take_job().unwrap();
    let mut f = frame(1, 110000, true);
    f.image.as_mut().unwrap().pixels[0][0] = 0;
    s.ingest(f, now);
    let b = s.take_job().unwrap();
    s.complete(
        1,
        b.token,
        Err(Failure {
            reason: "dvb_ocr_low_confidence",
            confidence: Some(22.0),
        }),
    );
    s.complete(1, a.token, good("EARLIER"));
    assert_eq!(s.snapshot()[0].end, Some(110000));
    assert_eq!(s.stats()[0]["last_error"], "dvb_ocr_low_confidence");
    assert_eq!(s.stats()[0]["confidence"], 22.0);
}
fn tsv(words: &str) -> String {
    format!(
        "level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext\n{words}"
    )
}
#[test]
fn tsv_is_plain_bounded_unicode_with_confidence_and_line_breaks() {
    let raw =
        tsv("5\t1\t1\t1\t1\t1\t0\t0\t3\t3\t90\tGRÜSSE\n5\t1\t1\t1\t2\t1\t0\t0\t3\t3\t95\t<&>\n");
    let r = parse_tsv(raw.as_bytes()).unwrap();
    assert_eq!(r.text, "GRÜSSE\n<&>");
    assert!(r.confidence.unwrap() > 90.0);
    assert_eq!(
        parse_tsv(tsv("5\t1\t1\t1\t1\t1\t0\t0\t3\t3\t12\tBAD\n").as_bytes())
            .unwrap_err()
            .reason,
        "dvb_ocr_low_confidence"
    );
    for malformed in [
        "oops",
        "5\t1\t1\t1\t1\t1\t0\t0\t3\t3\tNaN\tBAD\n",
        "5\t1\t1\t1\t1\t1\t0\t0\t3\t3\t101\tBAD\n",
    ] {
        assert_eq!(
            parse_tsv(tsv(malformed).as_bytes()).unwrap_err().reason,
            "dvb_ocr_output_invalid"
        );
    }
    assert_eq!(
        parse_tsv(&vec![b'a'; 65537]).unwrap_err().reason,
        "dvb_ocr_output_limit"
    );
    assert_eq!(
        parse_tsv(
            tsv(&format!(
                "5\t1\t1\t1\t1\t1\t0\t0\t3\t3\t95\t{}\n",
                "a".repeat(4097)
            ))
            .as_bytes()
        )
        .unwrap_err()
        .reason,
        "dvb_ocr_text_limit"
    );
}
#[tokio::test]
async fn independent_english_and_german_glyphs_are_recognized() {
    for (text, model) in [("EUROPE DVB", "eng"), ("GRÜSSE", "deu")] {
        let img = fixture::glyph(text);
        let result = recognize(
            "tesseract",
            &img,
            model,
            Instant::now() + Duration::from_millis(1500),
        )
        .await
        .unwrap();
        assert_eq!(result.text, text);
        assert!(result.confidence.unwrap() >= 60.0);
    }
}
#[tokio::test]
async fn optional_missing_executable_or_model_degrades_explicitly() {
    let image = fixture::glyph("HELLO");
    assert_eq!(
        recognize(
            "/no/flussonix/ocr",
            &image,
            "eng",
            Instant::now() + Duration::from_millis(1500)
        )
        .await
        .unwrap_err()
        .reason,
        "dvb_ocr_unavailable"
    );
    assert_eq!(
        recognize(
            "tesseract",
            &image,
            "missing_qualification_model",
            Instant::now() + Duration::from_millis(1500)
        )
        .await
        .unwrap_err()
        .reason,
        "dvb_ocr_model_unavailable"
    );
}
#[cfg(unix)]
#[tokio::test]
async fn stalled_and_oversized_processes_are_terminated_and_reaped() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let image = Image {
        width: 2,
        height: 2,
        pixels: vec![[255, 255]; 4],
    };
    for (name, script, reason) in [
        ("slow", "exec /bin/sleep 60", "dvb_ocr_timeout"),
        (
            "large",
            "exec /usr/bin/head -c 100000 /dev/zero",
            "dvb_ocr_output_limit",
        ),
    ] {
        let path = dir.path().join(name);
        let pidfile = dir.path().join(format!("{name}.pid"));
        std::fs::write(
            &path,
            format!("#!/bin/sh\necho $$ > '{}'\n{script}\n", pidfile.display()),
        )
        .unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        let start = Instant::now();
        let r = recognize(
            path.to_str().unwrap(),
            &image,
            "eng",
            start + Duration::from_millis(150),
        )
        .await
        .unwrap_err();
        assert_eq!(r.reason, reason);
        assert!(start.elapsed() < Duration::from_secs(1));
        let pid = std::fs::read_to_string(pidfile).unwrap();
        assert!(!std::path::Path::new(&format!("/proc/{}", pid.trim())).exists());
    }
}
#[test]
fn completed_history_is_capped_and_mixed_wire_services_are_isolated() {
    let services: Vec<Service> = serde_json::from_value(serde_json::json!([
        {"channel":1,"language":"en","name":"Analog"},
        {"service":1,"language":"en","name":"Digital"},
        {"teletext_page":888,"language":"de","name":"Teletext"},
        {"dvb_page":1,"ocr_language":"eng","language":"en","name":"Bitmap"}
    ]))
    .unwrap();
    let mut s = Store::new(&services);
    let now = Instant::now();
    for i in 0..100 {
        let mut f = frame(1, 100000 + i * 1000, true);
        f.image.as_mut().unwrap().pixels[0][0] = i as u8;
        s.ingest(f, now);
        let j = s.take_job().unwrap();
        s.complete(j.page, j.token, good("DVB"));
    }
    let cues = s.snapshot();
    assert_eq!(cues.len(), 64);
    assert_eq!(cues[0].start, 136000);
    assert!(cues.iter().all(|c| c.channel == 65537));
    assert_eq!(s.stats().as_array().unwrap().len(), 1);
}
#[tokio::test]
async fn explicit_cancellation_reaps_an_active_child() {
    use std::os::unix::fs::PermissionsExt;
    let d = tempfile::tempdir().unwrap();
    let file = d.path().join("sleep");
    let pidfile = d.path().join("pid");
    std::fs::write(
        &file,
        format!(
            "#!/bin/sh\necho $$ > '{}'\nexec /bin/sleep 60\n",
            pidfile.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o700)).unwrap();
    let cancel = tokio_util::sync::CancellationToken::new();
    let c = cancel.clone();
    let pidcopy = pidfile.clone();
    let stop = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !pidcopy.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await
            }
        })
        .await
        .unwrap();
        c.cancel();
    });
    let image = Image {
        width: 2,
        height: 2,
        pixels: vec![[255, 255]; 4],
    };
    let result = flussonix::dvb_ocr::recognize_cancellable(
        file.to_str().unwrap(),
        &image,
        "eng",
        Instant::now() + Duration::from_secs(2),
        cancel,
    )
    .await;
    stop.await.unwrap();
    assert_eq!(result.unwrap_err().reason, "dvb_ocr_canceled");
    let pid = std::fs::read_to_string(pidfile).unwrap();
    assert!(!std::path::Path::new(&format!("/proc/{}", pid.trim())).exists());
}
#[tokio::test]
async fn thin_or_inconsistent_images_fail_before_spawning_ocr() {
    for image in [
        Image {
            width: 1,
            height: 1048576,
            pixels: vec![[255, 255]; 1048576],
        },
        Image {
            width: 4096,
            height: 2304,
            pixels: vec![],
        },
    ] {
        assert_eq!(
            recognize(
                "/missing",
                &image,
                "eng",
                Instant::now() + Duration::from_millis(1500)
            )
            .await
            .unwrap_err()
            .reason,
            "dvb_ocr_image_limit"
        );
    }
}
