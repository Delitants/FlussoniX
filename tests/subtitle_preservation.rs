#[path = "support/subtitle_fixture.rs"]
mod fixture;
use flussonix::{
    m4f::{self, Frame},
    m4s::{self, Track},
    media::Engine,
    wire,
    worker_ts::Muxer,
};
use serde_json::{Value, json};
use std::{process::Command, sync::Arc, time::Duration};
use tokio::io::AsyncWriteExt;

fn probe(path: &std::path::Path) -> Value {
    let p = Command::new("ffprobe")
        .args(["-v", "error", "-show_streams", "-of", "json"])
        .arg(path)
        .output()
        .unwrap();
    assert!(p.status.success(), "{}", String::from_utf8_lossy(&p.stderr));
    serde_json::from_slice(&p.stdout).unwrap()
}
#[test]
fn owned_regional_fixture_has_independent_codec_and_descriptor_identity() {
    let ts = fixture::transport();
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("owned.ts");
    std::fs::write(&path, &ts).unwrap();
    let info = probe(&path);
    let streams = info["streams"].as_array().unwrap();
    assert_eq!(streams.len(), 4, "{info}");
    assert_eq!(streams[2]["codec_name"], "dvb_subtitle");
    assert_eq!(streams[2]["tags"]["language"], "eng");
    assert_eq!(streams[3]["codec_name"], "dvb_teletext");
    assert_eq!(streams[3]["tags"]["language"], "deu");
    let desc = fixture::descriptors(&ts);
    assert_eq!(desc[&0x120], fixture::DVB_DESC);
    assert_eq!(desc[&0x121], fixture::TTX_DESC);
    assert!(
        fixture::pes_bodies(&ts, 0x120)
            .iter()
            .all(|p| p == fixture::DVB_BODY)
    );
    assert!(
        fixture::pes_bodies(&ts, 0x121)
            .iter()
            .all(|p| p == &fixture::teletext_body())
    );
}
async fn run(encoder: Option<&str>, preserve: bool, passthrough: bool) {
    let input = fixture::transport();
    let d = tempfile::tempdir().unwrap();
    let e = Arc::new(Engine::new(d.path(), "ffmpeg"));
    let mut cfg = json!({"inputs":[{"url":"publish://"}],"flussonix_subtitle_tracks":if preserve {"preserve"} else {"drop"}});
    if passthrough {
        cfg["flussonix_hls_subtitles"] = json!("passthrough");
    }
    if let Some(encoder) = encoder {
        cfg["transcoder"] = json!({"encoder":encoder,"vb":300});
    }
    let mut p = e
        .publish_guarded("owned", &cfg, std::future::ready(true))
        .await
        .unwrap();
    let w = p.worker.clone();
    let mut rx = w.subscribe();
    let stop = tokio_util::sync::CancellationToken::new();
    let cancel = stop.clone();
    let capture = tokio::spawn(async move {
        let mut bytes = vec![];
        loop {
            tokio::select! {_=cancel.cancelled()=>break,result=rx.recv()=>match result {Ok(b)=>bytes.extend(b),Err(tokio::sync::broadcast::error::RecvError::Closed)=>break,Err(e)=>panic!("capture lagged: {e}")}}
        }
        bytes
    });
    p.stdin.as_mut().unwrap().write_all(&input).await.unwrap();
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            if e.read("owned", "index.m3u8").await.is_ok()
                && e.read("owned", "fmp4/index.m3u8").await.is_ok()
            {
                break;
            }
            assert!(
                w.alive.load(std::sync::atomic::Ordering::Relaxed),
                "{}",
                w.stats()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("both HLS muxers must publish");
    // Allow already encoded TS to drain without ending the publisher generation.
    tokio::time::sleep(Duration::from_millis(200)).await;
    stop.cancel();
    let ts = capture.await.unwrap();
    let desc = fixture::descriptors(&ts);
    let dvb = desc
        .iter()
        .find(|(_, v)| {
            v.windows(fixture::DVB_DESC.len())
                .any(|x| x == fixture::DVB_DESC)
        })
        .map(|(k, _)| *k);
    let ttx = desc
        .iter()
        .find(|(_, v)| {
            v.windows(fixture::TTX_DESC.len())
                .any(|x| x == fixture::TTX_DESC)
        })
        .map(|(k, _)| *k);
    if preserve {
        assert!(dvb.is_some(), "DVB subtitle descriptor missing: {desc:?}");
        assert!(ttx.is_some(), "Teletext descriptor missing: {desc:?}");
        for (id, body) in [
            (dvb.unwrap(), fixture::DVB_BODY.to_vec()),
            (ttx.unwrap(), fixture::teletext_body()),
        ] {
            let packets = fixture::pes_bodies(&ts, id);
            assert!(!packets.is_empty());
            assert!(
                packets.iter().all(|p| p == &body),
                "original PES must survive"
            );
        }
    } else {
        assert!(dvb.is_none());
        assert!(ttx.is_none());
    }
    for index in ["index.m3u8", "fmp4/index.m3u8"] {
        let list = String::from_utf8(e.read("owned", index).await.unwrap().to_vec()).unwrap();
        let root = if index.starts_with("fmp4") {
            "fmp4/"
        } else {
            ""
        };
        let name = list
            .lines()
            .find(|l| !l.is_empty() && !l.starts_with('#'))
            .unwrap();
        let segment = e.read("owned", &format!("{root}{name}")).await.unwrap();
        if root.is_empty() {
            let d = fixture::descriptors(&segment);
            assert_eq!(
                d.len(),
                if passthrough { 4 } else { 2 },
                "TS-HLS raw track policy"
            );
            if passthrough {
                for (descriptor, body) in [
                    (fixture::DVB_DESC, fixture::DVB_BODY.to_vec()),
                    (fixture::TTX_DESC, fixture::teletext_body()),
                ] {
                    let id = d
                        .iter()
                        .find(|(_, v)| v.windows(descriptor.len()).any(|x| x == descriptor))
                        .map(|(id, _)| *id)
                        .expect("TS-HLS must retain regional descriptors");
                    let packets = fixture::pes_bodies(&segment, id);
                    assert!(!packets.is_empty(), "TS-HLS must retain regional PES");
                    assert!(
                        packets.iter().all(|p| p == &body),
                        "TS-HLS must retain encoded payloads exactly"
                    );
                }
                let sequence: u64 = list
                    .lines()
                    .find_map(|l| l.strip_prefix("#EXT-X-MEDIA-SEQUENCE:"))
                    .unwrap()
                    .parse()
                    .unwrap();
                assert!(
                    sequence > i32::MAX as u64,
                    "public sequence must retain its epoch"
                );
            }
        } else {
            assert!(segment.windows(4).any(|w| w == b"moof"));
            let init = list
                .lines()
                .find(|l| l.starts_with("#EXT-X-MAP:"))
                .unwrap()
                .split('"')
                .nth(1)
                .unwrap();
            let mut joined = e
                .read("owned", &format!("fmp4/{init}"))
                .await
                .unwrap()
                .to_vec();
            joined.extend(&segment);
            let f = d.path().join("hls.mp4");
            std::fs::write(&f, joined).unwrap();
            let info = probe(&f);
            assert_eq!(info["streams"].as_array().unwrap().len(), 2);
        }
    }
    if preserve {
        // An actual policy edit replaces the publisher worker, not just its stats.
        cfg["flussonix_subtitle_tracks"] = json!("drop");
        let replacement = e
            .publish_guarded("owned", &cfg, std::future::ready(true))
            .await
            .unwrap();
        assert!(w.is_closed());
        assert!(!Arc::ptr_eq(&w, &replacement.worker));
        assert_eq!(replacement.worker.stats()["subtitle_tracks"], "drop");
        drop(replacement);
    }
    drop(p);
    e.stop_all().await;
}
#[tokio::test]
async fn copy_preserves_original_dvb_and_teletext_without_breaking_hls() {
    run(None, true, false).await
}
#[tokio::test]
async fn cpu_transcoding_preserves_original_dvb_and_teletext_without_breaking_hls() {
    run(Some("libx264"), true, false).await
}
#[tokio::test]
async fn drop_omits_separate_tracks_without_breaking_hls() {
    run(None, false, false).await
}

fn caption_nal(hevc: bool) -> (Vec<u8>, Vec<u8>) {
    // ATSC user_data_registered_itu_t_t35, 608 field-one pair + 708 packet start.
    // This tests exact payload survival, not decoded words/window semantics.
    let data = vec![
        0xb5, 0, 0x31, b'G', b'A', b'9', b'4', 3, 0x42, 0xff, 0xfc, 0x94, 0x20, 0xff, 0x02, 0x21,
        0xff,
    ];
    let mut n = if hevc { vec![0x4e, 1] } else { vec![6] };
    n.extend([4, data.len() as u8]);
    n.extend(&data);
    n.push(0x80);
    (n, data)
}
#[test]
fn native_framing_preserves_608_and_708_caption_payloads_for_h264_and_hevc() {
    for hevc in [false, true] {
        let track = if hevc {
            Track {
                id: 1,
                codec: "hevc".into(),
                config: include_bytes!("fixtures/codecs/hevc.hvcc").to_vec(),
            }
        } else {
            Track {
                id: 1,
                codec: "h264".into(),
                config: vec![
                    1, 100, 0, 31, 255, 225, 0, 4, 103, 100, 0, 31, 1, 0, 2, 104, 0,
                ],
            }
        };
        let (nal, payload) = caption_nal(hevc);
        let mut body = (nal.len() as u32).to_be_bytes().to_vec();
        body.extend(&nal);
        if hevc {
            body.extend(include_bytes!("fixtures/codecs/hevc-00.bin"));
        } else {
            body.extend([0, 0, 0, 2, 0x65, 0x88]);
        }
        let f = Frame {
            track_id: 1,
            dts: 90000,
            pts_offset: 0,
            key: true,
            body,
        };
        let mut records = wire::encode_info(std::slice::from_ref(&track)).unwrap();
        records.extend(wire::encode_frame(&track, &f).unwrap());
        let events = m4s::Decoder::default().push(&records).unwrap();
        let body = events
            .iter()
            .find_map(|e| {
                if let m4s::Event::Frame { body, .. } = e {
                    Some(body)
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(body, &f.body);
        let packed =
            m4f::pack(std::slice::from_ref(&track), std::slice::from_ref(&f), 3600).unwrap();
        let (_, frames) = m4f::unpack(&packed).unwrap();
        assert_eq!(frames[0].body, f.body);
        let mut mux = Muxer::new(std::slice::from_ref(&track)).unwrap();
        let ts = mux.frame(&frames[0]).unwrap();
        assert!(
            ts.windows(payload.len()).any(|w| w == payload),
            "GA94 caption data must survive native-to-TS framing"
        );
    }
}

async fn silent_tracks(encoder: Option<&str>, initial_cues: bool, passthrough: bool) {
    let original = fixture::transport_for("24");
    let mut seen = std::collections::HashSet::new();
    let input: Vec<u8> = original
        .chunks_exact(188)
        .filter(|p| {
            let id = fixture::pid(p);
            ![0x120, 0x121].contains(&id) || initial_cues && seen.insert(id)
        })
        .flatten()
        .copied()
        .collect();
    let d = tempfile::tempdir().unwrap();
    let e = Engine::new(d.path(), "ffmpeg");
    let mut cfg = json!({"inputs":[{"url":"publish://"}],"flussonix_subtitle_tracks":"preserve","flussonix_input_timeout":3});
    if passthrough {
        cfg["flussonix_hls_subtitles"] = json!("passthrough");
    }
    if let Some(encoder) = encoder {
        cfg["transcoder"] = json!({"encoder":encoder,"vb":300});
    }
    let mut p = e
        .publish_guarded("owned", &cfg, std::future::ready(true))
        .await
        .unwrap();
    let w = p.worker.clone();
    let mut stdin = p.stdin.take().unwrap();
    let producer = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(40));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        for packet in input.chunks_exact(188) {
            if fixture::pid(packet) == 256 && packet[1] & 0x40 != 0 {
                tick.tick().await;
            }
            if stdin.write_all(packet).await.is_err() {
                break;
            }
        }
    });
    let ready = tokio::time::timeout(Duration::from_secs(6), async {
        while w.alive.load(std::sync::atomic::Ordering::Relaxed) {
            if e.read("owned", "index.m3u8").await.is_ok()
                && e.read("owned", "fmp4/index.m3u8").await.is_ok()
            {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        false
    })
    .await
    .unwrap_or(false);
    let before = w.bytes.load(std::sync::atomic::Ordering::Relaxed);
    if ready {
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    let alive = w.alive.load(std::sync::atomic::Ordering::Relaxed);
    let progressed = w.bytes.load(std::sync::atomic::Ordering::Relaxed) > before;
    let stats = w.stats();
    producer.abort();
    let _ = producer.await;
    drop(p);
    e.stop_all().await;
    assert!(
        ready,
        "declared but silent subtitle tracks must not block HLS: {stats}"
    );
    assert!(
        alive && progressed,
        "AV must progress throughout subtitle silence: {stats}"
    );
}
#[tokio::test]
async fn absent_subtitle_packets_do_not_stall_copy_av() {
    silent_tracks(None, false, false).await;
}
#[tokio::test]
async fn absent_subtitle_packets_do_not_stall_cpu_av() {
    silent_tracks(Some("libx264"), false, false).await;
}
#[tokio::test]
async fn subtitle_silence_after_initial_cues_does_not_stall_copy_av() {
    silent_tracks(None, true, false).await;
}
#[tokio::test]
async fn subtitle_silence_after_initial_cues_does_not_stall_cpu_av() {
    silent_tracks(Some("libx264"), true, false).await;
}

#[tokio::test]
async fn raw_hls_copy_preserves_regional_tracks() {
    run(None, true, true).await;
}
#[tokio::test]
async fn raw_hls_cpu_preserves_regional_tracks() {
    run(Some("libx264"), true, true).await;
}
#[tokio::test]
async fn raw_hls_policy_is_independent_of_other_ts_outputs() {
    run(None, false, true).await;
}
#[tokio::test]
async fn raw_hls_silent_copy() {
    silent_tracks(None, false, true).await;
}
#[tokio::test]
async fn raw_hls_silent_cpu() {
    silent_tracks(Some("libx264"), false, true).await;
}
#[tokio::test]
async fn raw_hls_after_cues_copy() {
    silent_tracks(None, true, true).await;
}
#[tokio::test]
async fn raw_hls_after_cues_cpu() {
    silent_tracks(Some("libx264"), true, true).await;
}

#[tokio::test]
async fn raw_hls_window_is_bounded_and_replacement_does_not_reuse_sequences_or_files() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::new(d.path(), "ffmpeg");
    let cfg = json!({"inputs":[{"url":"publish://"}],"flussonix_hls_subtitles":"passthrough","flussonix_subtitle_tracks":"drop"});
    let input = fixture::transport_for("24");
    let mut publisher = e
        .publish_guarded("owned", &cfg, std::future::ready(true))
        .await
        .unwrap();
    publisher
        .stdin
        .as_mut()
        .unwrap()
        .write_all(&input)
        .await
        .unwrap();
    let engine = &e;
    let read = |retired_boundary: bool| async move {
        tokio::time::timeout(Duration::from_secs(12), async {
            loop {
                if let Ok(bytes) = engine.read("owned", "index.m3u8").await {
                    let list = String::from_utf8(bytes.to_vec()).unwrap();
                    let files: Vec<_> = list
                        .lines()
                        .filter(|l| !l.is_empty() && !l.starts_with('#'))
                        .map(str::to_owned)
                        .collect();
                    let sequence: u64 = list
                        .lines()
                        .find_map(|l| l.strip_prefix("#EXT-X-MEDIA-SEQUENCE:"))
                        .unwrap()
                        .parse()
                        .unwrap();
                    if files.len() == 6
                        && (!retired_boundary || list.contains("#EXT-X-DISCONTINUITY-SEQUENCE:1\n"))
                    {
                        return (list, files, sequence);
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap()
    };
    let (_, files, sequence) = read(false).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let list = String::from_utf8(e.read("owned", "index.m3u8").await.unwrap().to_vec()).unwrap();
    let last_sequence: u64 = list
        .lines()
        .find_map(|l| l.strip_prefix("#EXT-X-MEDIA-SEQUENCE:"))
        .unwrap()
        .parse()
        .unwrap();
    assert!(last_sequence >= sequence);
    let directory = d.path().join("owned");
    // Engine uses an encoded stream-directory name; locate the owned output.
    let directory = if directory.exists() {
        directory
    } else {
        std::fs::read_dir(d.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.join("index.m3u8").exists())
            .unwrap()
    };
    let retained = || {
        std::fs::read_dir(&directory)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|x| x == "ts")
            })
            .count()
    };
    // The public list is renamed before the asynchronous pruning pass completes.
    // Wait for that pass instead of assuming the earlier 200ms delay settled it.
    let pruned = tokio::time::timeout(Duration::from_secs(12), async {
        while retained() > 9 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        pruned.is_ok(),
        "raw segments failed to prune: {}",
        retained()
    );
    let old_worker = publisher.worker.clone();
    let mut changed = cfg.clone();
    changed["title"] = json!("replacement");
    changed["flussonix_subtitle_tracks"] = json!("preserve");
    let mut replacement = e
        .publish_guarded("owned", &changed, std::future::ready(true))
        .await
        .unwrap();
    assert!(old_worker.is_closed());
    // Hold the 25fps fixture just before fourteen seconds. The first six
    // finalized segments still include the generation's discontinuity marker.
    let split = input
        .chunks_exact(188)
        .enumerate()
        .filter(|(_, p)| ((u16::from(p[1] & 31) << 8) | u16::from(p[2])) == 256 && p[1] & 0x40 != 0)
        .nth(349)
        .unwrap()
        .0
        * 188;
    replacement
        .stdin
        .as_mut()
        .unwrap()
        .write_all(&input[..split])
        .await
        .unwrap();
    let (initial, _, _) = read(false).await;
    assert!(
        initial.contains("#EXT-X-DISCONTINUITY\n"),
        "initial replacement window: {initial}"
    );
    assert!(!initial.contains("#EXT-X-DISCONTINUITY-SEQUENCE:"));
    replacement
        .stdin
        .as_mut()
        .unwrap()
        .write_all(&input[split..])
        .await
        .unwrap();
    let (list, new_files, next) = read(true).await;
    assert!(
        next >= last_sequence + 6,
        "replacement reused a live sequence"
    );
    assert!(
        list.contains("#EXT-X-DISCONTINUITY-SEQUENCE:1\n"),
        "replacement discontinuity history was not observed: {list}"
    );
    assert!(!list.contains("#EXT-X-DISCONTINUITY\n"));
    assert!(new_files.iter().all(|f| !files.contains(f)));
    for file in files {
        assert!(
            e.read("owned", &file).await.is_err(),
            "old generation still served"
        );
    }
    drop(publisher);
    drop(replacement);
    e.stop_all().await;
}
