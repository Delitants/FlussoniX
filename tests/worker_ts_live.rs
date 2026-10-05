use flussonix::{
    m4f::Frame,
    m4s::{self, Track},
    media::Engine,
    worker_ts::Muxer,
};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::io::AsyncWriteExt;
fn fixture() -> (Vec<Track>, Vec<Frame>, Vec<u8>) {
    let tracks = vec![
        Track {
            id: 1,
            codec: "hevc".into(),
            config: include_bytes!("fixtures/codecs/hevc.hvcc").to_vec(),
        },
        Track {
            id: 2,
            codec: "m2a".into(),
            config: vec![],
        },
        Track {
            id: 3,
            codec: "mp3".into(),
            config: vec![],
        },
    ];
    let timing: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/codecs/hevc-timing.json")).unwrap();
    let mut frames = vec![];
    for cycle in 0..10u64 {
        for (i, t) in timing.iter().enumerate() {
            let d = t["dts"].as_i64().unwrap();
            let p = t["pts"].as_i64().unwrap();
            frames.push(Frame {
                track_id: 1,
                dts: 90000 + cycle * 43200 + ((d + 1024) * 90000 / 12800) as u64,
                pts_offset: (p - d) * 90000 / 12800,
                key: t["flags"].as_str().unwrap().contains('K'),
                body: std::fs::read(format!("tests/fixtures/codecs/hevc-{i:02}.bin")).unwrap(),
            });
        }
    }
    for i in 0..210u64 {
        for (id, rate, samples, body) in [
            (
                2,
                48000,
                1152,
                include_bytes!("fixtures/codecs/mp2.bin").as_slice(),
            ),
            (
                3,
                22050,
                576,
                include_bytes!("fixtures/codecs/mp3.bin").as_slice(),
            ),
        ] {
            frames.push(Frame {
                track_id: id,
                dts: 90000 + i * samples * 90000 / rate,
                pts_offset: 0,
                key: true,
                body: body.to_vec(),
            });
        }
    }
    frames.sort_by_key(|f| f.dts);
    let mut m = Muxer::new(&tracks).unwrap();
    let mut ts = m.tables();
    for f in &frames {
        ts.extend(m.frame(f).unwrap());
    }
    (tracks, frames, ts)
}
async fn publication(preserve: bool) {
    let (tracks, expected, ts) = fixture();
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "ffmpeg"));
    let cfg = json!({"inputs":[{"url":"publish://"}],"flussonix_subtitle_tracks":if preserve{"preserve"}else{"drop"},"flussonix_input_timeout":8});
    let mut publication = engine
        .publish_guarded("owned", &cfg, std::future::ready(true))
        .await
        .unwrap();
    let worker = publication.worker.clone();
    let mut rx = worker.wire.m4s.subscribe();
    let mut stdin = publication.stdin.take().unwrap();
    let writer = tokio::spawn(async move {
        for b in ts.chunks(188 * 32) {
            if stdin.write_all(b).await.is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        let _ = stdin.shutdown().await;
    });
    let result = tokio::time::timeout(Duration::from_secs(12), async {
        let mut decoder = m4s::Decoder::default();
        let mut tracks_out = vec![];
        let mut frames = vec![];
        loop {
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Ok(record)) => {
                    for e in decoder.push(&record).unwrap() {
                        match e {
                            m4s::Event::Info { tracks, .. } => tracks_out = tracks,
                            m4s::Event::Frame {
                                track_id,
                                dts,
                                pts_offset,
                                key,
                                body,
                                ..
                            } => frames.push(Frame {
                                track_id,
                                dts,
                                pts_offset,
                                key,
                                body: body.to_vec(),
                            }),
                            _ => {}
                        }
                    }
                }
                Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => break,
                Ok(Err(e)) => panic!("native queue lost records: {e}"),
                Err(_) => {
                    if worker.is_closed() {
                        break;
                    }
                }
            }
        }
        (tracks_out, frames)
    })
    .await;
    let stats = worker.stats();
    let pid = worker.pid();
    engine.stop_all().await;
    writer.abort();
    let _ = writer.await;
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    let (observed, frames) = result.expect("owned output deadline");
    assert_eq!(
        observed
            .iter()
            .map(|t| t.codec.as_str())
            .collect::<Vec<_>>(),
        ["hevc", "m2a", "mp3"],
        "worker stats {stats}"
    );
    assert!(
        frames.windows(2).all(|p| p[0].dts <= p[1].dts),
        "native publication must be globally interleaved"
    );
    let video: Vec<_> = frames
        .iter()
        .filter(|f| f.track_id == observed[0].id)
        .collect();
    assert_eq!(
        video.len(),
        120,
        "all coded pictures including the final zero-length PES must arrive; {stats}"
    );
    let vcl = |body: &[u8]| {
        let mut at = 0;
        let mut out = vec![];
        while at < body.len() {
            let n = u32::from_be_bytes(body[at..at + 4].try_into().unwrap()) as usize;
            at += 4;
            let nal = &body[at..at + n];
            if (nal[0] >> 1) & 63 <= 31 {
                out.extend_from_slice(&(n as u32).to_be_bytes());
                out.extend(nal);
            }
            at += n;
        }
        out
    };
    let expected_video: Vec<_> = expected.iter().filter(|f| f.track_id == 1).collect();
    let shift = video[0].dts as i64 - expected_video[0].dts as i64;
    for (a, b) in video.iter().zip(expected_video) {
        assert!(vcl(&a.body) == vcl(&b.body), "coded picture bytes changed");
        assert_eq!(a.dts as i64 - b.dts as i64, shift);
        assert_eq!(a.pts_offset, b.pts_offset);
    }
    for (i, codec) in [(1, "m2a"), (2, "mp3")] {
        let got: Vec<_> = frames
            .iter()
            .filter(|f| f.track_id == observed[i].id)
            .collect();
        assert_eq!(got.len(), 210, "all MPEG audio frames must survive");
        let body = if codec == "m2a" {
            include_bytes!("fixtures/codecs/mp2.bin").as_slice()
        } else {
            include_bytes!("fixtures/codecs/mp3.bin").as_slice()
        };
        assert!(got.iter().all(|f| f.body == body));
        let original = expected.iter().filter(|f| f.track_id == (i + 1) as u32);
        for (sample, original) in got.iter().zip(original) {
            assert_eq!(
                sample.dts as i64 - original.dts as i64,
                shift,
                "MPEG audio must use the same clock shift as video"
            );
            assert_eq!(sample.pts_offset, 0);
        }
    }
    // Read an actual originated M4F segment, not just reconstructed M4S.
    let signals = worker.wire.signal_subscribe().0;
    assert!(
        !signals.is_empty(),
        "native segment boundaries were never published"
    );
    let line = std::str::from_utf8(&signals[0]).unwrap();
    let stamp = line
        .split_whitespace()
        .nth(1)
        .unwrap()
        .split('-')
        .next()
        .unwrap();
    let segment = worker.wire.segment(&format!("{stamp}.m4f")).unwrap();
    let (packed_tracks, packed_frames) = flussonix::m4f::unpack(&segment).unwrap();
    assert_eq!(packed_tracks, observed);
    assert!(!packed_frames.is_empty());
    for sample in &packed_frames {
        let original = frames
            .iter()
            .find(|f| f.track_id == sample.track_id && f.dts == sample.dts)
            .expect("packed sample must match native wire clock");
        assert_eq!(sample.body, original.body);
        assert_eq!(sample.pts_offset, original.pts_offset);
    }
    let mut mux = Muxer::new(&observed).unwrap();
    let mut remux = mux.tables();
    for f in &frames {
        remux.extend(mux.frame(f).unwrap());
    }
    let file = dir.path().join("native.ts");
    std::fs::write(&file, remux).unwrap();
    let decoded = tokio::process::Command::new("ffmpeg")
        .args(["-nostdin", "-v", "error", "-i"])
        .arg(file)
        .args(["-map", "0", "-f", "null", "-"])
        .output()
        .await
        .unwrap();
    assert!(
        decoded.status.success() && decoded.stderr.is_empty(),
        "independent decode: {}",
        String::from_utf8_lossy(&decoded.stderr)
    );
    assert_eq!(tracks.len(), 3);
}
#[tokio::test]
async fn published_hevc_mpeg_audio_originates_complete_native_media_from_stdout() {
    publication(false).await;
}
#[tokio::test]
async fn preserved_transport_uses_av_channel_to_originate_hevc_mpeg_audio() {
    publication(true).await;
}

#[cfg(unix)]
async fn fake_output(preserve: bool, clean_av_eof: bool) {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let transport = dir.path().join("owned.ts");
    std::fs::write(&transport, fixture().2).unwrap();
    let script = dir.path().join("owned-packager.py");
    std::fs::write(
        &script,
        format!(
            r#"#!/usr/bin/python3
import os,re,socket,sys,time
if {preserve}:
    host,port=re.search(r'tcp://([0-9.]+):(\d+)', ' '.join(sys.argv)).groups()
    output=socket.create_connection((host,int(port)))
    output.sendall(open({transport:?},'rb').read() if {clean} else bytes([0])*188)
    output.close()
else:
    os.write(1,bytes([0])*188)
while True:
    os.write(1,bytes.fromhex('471fff10')+bytes([255])*184)
    time.sleep(0.05)
"#,
            preserve = if preserve { "True" } else { "False" },
            clean = if clean_av_eof { "True" } else { "False" },
            transport = transport.to_str().unwrap()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    let engine = Engine::new(dir.path().join("media"), script.to_str().unwrap());
    let worker=engine.ensure("owned",&json!({"inputs":[{"url":"testsrc://"}],"flussonix_subtitle_tracks":if preserve{"preserve"}else{"drop"},"flussonix_input_timeout":3})).await.unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(2), worker.closed()).await;
    let stats = worker.stats();
    engine.stop_all().await;
    assert!(!std::path::Path::new(&format!("/proc/{}", worker.pid())).exists());
    assert!(
        closed.is_ok(),
        "native output terminated while worker stayed alive: {stats}"
    );
    assert_eq!(
        stats["last_error"],
        if clean_av_eof {
            "input_closed"
        } else {
            "wire_decode_failed"
        }
    );
}
#[cfg(unix)]
#[tokio::test]
async fn malformed_stdout_closes_generation_and_reaps_packager() {
    fake_output(false, false).await;
}
#[cfg(unix)]
#[tokio::test]
async fn malformed_av_channel_closes_generation_and_reaps_packager() {
    fake_output(true, false).await;
}
#[cfg(unix)]
#[tokio::test]
async fn clean_av_eof_does_not_leave_a_healthy_transport_worker() {
    fake_output(true, true).await;
}

#[cfg(unix)]
async fn public_eof(abort: bool, wait_deadline: bool) {
    use axum::{body::Body, http::Request};
    use flussonix::server::{App, Options, router};
    use http_body_util::BodyExt;
    use std::os::unix::fs::PermissionsExt;
    use tower::ServiceExt;
    let dir = tempfile::tempdir().unwrap();
    let transport = dir.path().join("owned.ts");
    let (tracks, frames, _) = fixture();
    let frames = frames
        .into_iter()
        .filter(|f| f.dts < 110000)
        .collect::<Vec<_>>();
    let mut mux = Muxer::new(&tracks).unwrap();
    let mut ts = mux.tables();
    for f in &frames {
        ts.extend(mux.frame(f).unwrap());
    }
    std::fs::write(&transport, ts).unwrap();
    let script = dir.path().join("owned-eof.py");
    // Split small packet batches so the public route can establish an existing
    // viewer, then finish in the same poll as the last native samples.
    std::fs::write(&script,format!("#!/usr/bin/python3\nimport os,time\ndata=open({:?},'rb').read()\nfor i in range(0,len(data),188*8):\n os.write(1,data[i:i+188*8]);time.sleep(0.05)\n",transport.to_str().unwrap())).unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            ffmpeg: script.to_str().unwrap().into(),
            admin_password: "owned-admin".into(),
            peer_key: "owned-native-peer".into(),
            uplink_interface: "process".into(),
            ..Default::default()
        },
    )
    .unwrap();
    use sha2::{Digest, Sha256};
    app.config.put("streams","owned",json!({"static":false,"inputs":[{"url":"testsrc://"}],"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-viewer"))})).unwrap();
    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .uri("/owned/m4s?token=owned-viewer")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let worker = app
        .media
        .ensure("owned", &app.config.effective("owned").unwrap())
        .await
        .unwrap();
    let pid = worker.pid();
    // Delay this authorized consumer until the packager actually exits. Its
    // bounded queue is small enough to retain every sample without eviction.
    tokio::time::timeout(Duration::from_secs(2), async {
        while std::path::Path::new(&format!("/proc/{pid}")).exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("owned packager EOF");
    if abort {
        app.media.stop("owned").await;
    }
    if wait_deadline {
        tokio::time::timeout(Duration::from_secs(3), worker.closed())
            .await
            .expect("clean drain must have a deadline");
    }
    let result = tokio::time::timeout(Duration::from_secs(5), response.into_body().collect()).await;
    app.media.stop_all().await;
    let bytes = result
        .expect("bounded public native EOF")
        .unwrap()
        .to_bytes();
    let mut decoder = m4s::Decoder::default();
    let events = decoder.push(&bytes).unwrap();
    let count = events
        .iter()
        .filter(|e| matches!(e, m4s::Event::Frame { .. }))
        .count();
    assert_eq!(
        count,
        if abort || wait_deadline {
            0
        } else {
            frames.len()
        },
        "authorized public EOF drain and cancellation fence"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn authorized_public_m4s_drains_final_samples_on_clean_worker_eof() {
    public_eof(false, false).await;
}
#[cfg(unix)]
#[tokio::test]
async fn explicit_stop_fences_a_public_consumer_during_clean_eof_grace() {
    public_eof(true, false).await;
}
#[cfg(unix)]
#[tokio::test]
async fn a_public_consumer_cannot_hold_clean_eof_grace_open_forever() {
    public_eof(false, true).await;
}
