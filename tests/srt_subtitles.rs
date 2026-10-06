//! Owned CEA-608/708 and DVB/teletext fixtures through real encrypted SRT.
//! Copy-mode qualification; HLS policy and original TS tracks are independent.
#[allow(dead_code)]
#[path = "support/caption_fixture.rs"]
mod captions;

use flussonix::{
    server::{App, Options},
    srt_playback::{self, Listener, Settings},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, sync::atomic::Ordering, time::Duration};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

#[path = "support/srt_subtitle_oracle.rs"]
mod oracle;
use oracle::*;
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn run(push: bool, digital: bool, hevc: bool, input: &[u8], expected: &BTreeSet<Vec<u8>>) {
    for (hls, keep) in [("convert", true), ("drop", true), ("drop", false)] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("received.ts");
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            Options {
                admin_password: "owned-regional-admin".into(),
                peer_key: "owned-regional-peer".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let mut cfg = json!({"static":false,"inputs":[{"url":"publish://"}],
            "flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-viewer")),
            "flussonix_subtitle_tracks":if keep {"preserve"}else{"drop"},
            "flussonix_hls_subtitles":hls});
        if hls == "convert" {
            cfg["flussonix_hls_captions"] = if digital {
                json!([{"service":1,"language":"en","name":"English"}])
            } else {
                json!([{"channel":1,"language":"en","name":"English"}])
            };
        }
        let cancel = CancellationToken::new();
        let _cancel_on_drop = cancel.clone().drop_guard();
        let mut serving = None;
        let endpoint;
        if push {
            let reservation = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let address = reservation.local_addr().unwrap();
            drop(reservation);
            endpoint = format!(
                "srt://{address}?mode=listener&listen_timeout=15000000&timeout=10000000&enforced_encryption=1"
            );
            cfg["pushes"] = json!([{"url":format!("srt://{address}"),"passphrase":SECRET,"latency":120,"retry_timeout":1}]);
        } else {
            let listener = Listener::bind(
                "127.0.0.1:0".parse().unwrap(),
                Settings::new(120, 2, SECRET.into()).unwrap(),
            )
            .unwrap();
            endpoint = format!(
                "srt://{}?mode=caller&latency=120000&connect_timeout=2000&timeout=10000000&enforced_encryption=1",
                listener.address()
            );
            serving = Some(tokio::spawn(srt_playback::serve(
                listener,
                app.clone(),
                cancel.clone(),
            )));
        }
        app.config.put("streams", "owned", cfg.clone()).unwrap();
        let mut receiver = capture(&endpoint, push, &path).await;
        let mut publication = app
            .media
            .publish_guarded("owned", &cfg, std::future::ready(true))
            .await
            .unwrap();
        if !push {
            tokio::time::timeout(Duration::from_secs(3), async {
                while publication.worker.viewers.load(Ordering::Relaxed) != 1 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("viewer must attach before early caption events");
        }
        let feed = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(captions::paced(
            publication.stdin.take().unwrap(),
            input.to_vec(),
        )));
        // Data capture has no media timestamps. Stop through FFmpeg's normal
        // interactive quit after the paced fixture has supplied all cue events;
        // flush the untouched transport and require successful process exit.
        tokio::time::sleep(Duration::from_secs(12)).await;
        receiver
            .stdin
            .take()
            .unwrap()
            .write_all(b"q\n")
            .await
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(3), receiver.wait()).await;
        let words = if hls == "convert" {
            Some(converted_words(&app, if digital { "s1.m3u8" } else { "cc1.m3u8" }).await)
        } else {
            None
        };
        feed.abort();
        let _ = feed.await;
        cancel.cancel();
        if let Some(task) = serving {
            tokio::time::timeout(Duration::from_secs(3), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        app.media.stop_all().await;
        assert!(
            outcome.unwrap().unwrap().success(),
            "{}",
            std::fs::read_to_string(path.with_extension("log")).unwrap()
        );
        verify_original_tracks(&std::fs::read(&path).unwrap(), keep);
        verify_codecs(&path, hevc).await;
        let actual = caption_bodies(&path, hevc).await;
        assert!(
            expected.is_subset(&actual),
            "SRT must preserve every authored caption command independently of HLS policy; expected {} bodies, got {}",
            expected.len(),
            actual.len()
        );
        if let Some(words) = words {
            assert!(words.contains(if digital { "USA708" } else { "USA 608" }));
        }
        let decode_path = path.with_file_name("complete-pes.ts");
        std::fs::write(
            &decode_path,
            complete_sample(&std::fs::read(&path).unwrap()),
        )
        .unwrap();
        let decoded = strict_decode(&decode_path).await;
        assert!(
            clean_decode(&decoded),
            "{}",
            String::from_utf8_lossy(&decoded.stderr)
        );
        assert_eq!(app.media.count().await, 0);
        assert_eq!(publication.worker.viewers.load(Ordering::Relaxed), 0);
        println!(
            "qualified {} {} CEA-{} with DVB/teletext: HLS {hls}, originals {}",
            if push {
                "encrypted push"
            } else {
                "encrypted playback"
            },
            if hevc { "HEVC" } else { "H.264" },
            if digital { 708 } else { 608 },
            if keep { "kept" } else { "filtered" }
        );
    }
}

async fn regional(push: bool, hevc: bool) {
    let _guard = SERIAL.lock().await;
    for digital in [false, true] {
        let input = original::inject(&if hevc {
            captions::hevc_transport(digital)
        } else if digital {
            captions::digital_transport()
        } else {
            captions::transport()
        });
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("source.ts");
        std::fs::write(&file, &input).unwrap();
        verify_codecs(&file, hevc).await;
        let expected = caption_bodies(&file, hevc).await;
        assert!(
            expected.len() >= 4,
            "source oracle must contain distinct authored events"
        );
        let kinds: BTreeSet<_> = expected
            .iter()
            .flat_map(|body| {
                body[10..body.len() - 1]
                    .chunks_exact(3)
                    .map(|triple| triple[0] & 3)
            })
            .collect();
        assert_eq!(
            kinds,
            if digital {
                BTreeSet::from([2, 3])
            } else {
                BTreeSet::from([0])
            },
            "fixture must contain the stated regional caption format"
        );
        verify_original_tracks(&input, true);
        run(push, digital, hevc, &input, &expected).await;
    }
}

#[tokio::test]
async fn encrypted_listener_carries_608_708_dvb_teletext_independently_of_hls_policy() {
    regional(false, false).await;
}

#[tokio::test]
async fn encrypted_push_carries_608_708_dvb_teletext_independently_of_hls_policy() {
    regional(true, false).await;
}

#[tokio::test]
async fn encrypted_hevc_listener_preserves_regional_subtitles_and_hls_policy() {
    regional(false, true).await;
}

#[tokio::test]
async fn encrypted_hevc_push_preserves_regional_subtitles_and_hls_policy() {
    regional(true, true).await;
}

#[test]
fn filtered_transport_oracle_rejects_fragmented_unannounced_pes() {
    let av = captions::transport();
    let injected = original::inject(&av);
    let mut orphaned = av;
    for id in [0x120, 0x121] {
        let first = injected
            .chunks_exact(188)
            .find(|p| original::pid(p) == id)
            .unwrap();
        let pes = original::payload(first).unwrap();
        // The PES body is deliberately split by transport headers; neither its
        // descriptor nor a contiguous encoded body is present in this fixture.
        for (index, chunk) in pes.chunks(7).enumerate() {
            let mut packet = [0xff; 188];
            packet[0] = 0x47;
            packet[1] = (id >> 8) as u8 | if index == 0 { 0x40 } else { 0 };
            packet[2] = id as u8;
            packet[3] = 0x30 | (index as u8 & 15);
            packet[4] = (183 - chunk.len()) as u8;
            packet[5] = 0;
            packet[188 - chunk.len()..].copy_from_slice(chunk);
            orphaned.extend(packet);
        }
    }
    assert!(
        std::panic::catch_unwind(|| verify_original_tracks(&orphaned, false)).is_err(),
        "filter oracle accepted orphaned PES split across transport packets"
    );
}

#[tokio::test]
async fn finite_sample_omits_terminal_partial_pes_without_hiding_interior_corruption() {
    let _guard = SERIAL.lock().await;
    let bytes = original::inject(&captions::transport());
    let starts: Vec<_> = bytes
        .chunks_exact(188)
        .enumerate()
        .filter_map(|(i, p)| {
            let data = original::payload(p)?;
            if p[1] & 0x40 != 0
                && data.len() >= 6
                && data[..3] == [0, 0, 1]
                && (0xc0..=0xdf).contains(&data[3])
                && 6 + usize::from(u16::from_be_bytes([data[4], data[5]])) > data.len()
            {
                Some(i)
            } else {
                None
            }
        })
        .collect();
    let terminal = *starts.iter().find(|i| **i > bytes.len() / 188 / 2).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sample.ts");
    let partial = &bytes[..(terminal + 1) * 188];
    std::fs::write(&path, partial).unwrap();
    assert!(
        !clean_decode(&strict_decode(&path).await),
        "reproducer must end in an incomplete audio PES"
    );
    let complete = complete_sample(partial);
    let terminal_pid = original::pid(&partial[terminal * 188..]);
    let audio_packets: Vec<_> = partial
        .chunks_exact(188)
        .filter(|p| original::pid(p) == terminal_pid)
        .collect();
    let complete_audio: Vec<_> = complete
        .chunks_exact(188)
        .filter(|p| original::pid(p) == terminal_pid)
        .collect();
    assert!(audio_packets.len() > 1);
    assert_eq!(
        complete_audio,
        audio_packets[..audio_packets.len() - 1],
        "omit only the terminal partial audio PES, retaining every prior packet"
    );
    std::fs::write(&path, complete).unwrap();
    let decoded = strict_decode(&path).await;
    assert!(
        clean_decode(&decoded),
        "terminal capture boundary: {}",
        String::from_utf8_lossy(&decoded.stderr)
    );
    // Remove an interior continuation packet, not an unfinished terminal PES.
    let interior = *starts
        .iter()
        .find(|i| **i > bytes.len() / 188 / 4 && **i < terminal)
        .unwrap();
    let mut corrupt = bytes[..(interior + 1) * 188].to_vec();
    corrupt.extend(&bytes[(interior + 2) * 188..]);
    std::fs::write(&path, complete_sample(&corrupt)).unwrap();
    assert!(
        !clean_decode(&strict_decode(&path).await),
        "interior corruption must remain visible to -xerror"
    );
}
