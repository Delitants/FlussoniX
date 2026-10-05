//! Owned CEA-608/708 and DVB/teletext fixtures through real encrypted SRT.
//! Copy-mode qualification; HLS policy and original TS tracks are independent.
#[allow(dead_code)]
#[path = "support/caption_fixture.rs"]
mod captions;
#[allow(dead_code)]
#[path = "support/subtitle_fixture.rs"]
mod original;

use flussonix::{
    server::{App, Options},
    srt_playback::{self, Listener, Settings},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet, path::Path, process::Stdio, sync::atomic::Ordering, time::Duration,
};
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, Command};
use tokio_util::sync::CancellationToken;

const SECRET: &str = "owned-regional-srt-secret";
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn capture(endpoint: &str, listener: bool, path: &Path) -> Child {
    let log = std::fs::File::create(path.with_extension("log")).unwrap();
    let mut cmd = Command::new("ffmpeg");
    cmd.args(["-v", "error", "-y", "-threads", "1", "-passphrase", SECRET]);
    if !listener {
        cmd.args(["-srt_streamid", "#!::r=owned,m=request,u=owned-viewer"]);
    }
    cmd.args([
        "-f",
        "data",
        "-raw_packet_size",
        "1316",
        "-i",
        endpoint,
        "-map",
        "0",
        "-c",
        "copy",
        "-f",
        "data",
    ])
    .arg(path)
    .stdin(Stdio::piped())
    .stdout(Stdio::null())
    .stderr(log)
    .kill_on_drop(true)
    .spawn()
    .unwrap()
}

// Independently extract the video, then compare complete registered GA94 bodies,
// rather than checking for a magic marker or trusting server subtitle counters.
async fn caption_bodies(path: &Path) -> BTreeSet<Vec<u8>> {
    let out = Command::new("ffmpeg")
        .args(["-v", "error", "-nostdin", "-i"])
        .arg(path)
        .args(["-map", "0:v:0", "-c:v", "copy", "-f", "h264", "pipe:1"])
        .output()
        .await
        .unwrap();
    assert!(out.status.success(), "video extraction failed");
    let mut rbsp = Vec::new();
    let mut zeros = 0;
    for byte in out.stdout {
        if zeros >= 2 && byte == 3 {
            zeros = 0;
            continue;
        }
        rbsp.push(byte);
        zeros = if byte == 0 { zeros + 1 } else { 0 };
    }
    let mut bodies = BTreeSet::new();
    for (offset, part) in rbsp.windows(8).enumerate() {
        if part == b"\xb5\x00\x31GA94\x03" {
            let count = usize::from(rbsp[offset + 8] & 31);
            let end = offset + 11 + count * 3;
            if count > 0 && end <= rbsp.len() {
                bodies.insert(rbsp[offset..end].to_vec());
            }
        }
    }
    bodies
}

fn verify_original_tracks(bytes: &[u8], keep: bool) {
    let descriptors = original::descriptors(bytes);
    assert!(
        !descriptors.is_empty(),
        "receiver must contain valid program tables"
    );
    for (descriptor, body) in [
        (original::DVB_DESC, original::DVB_BODY.to_vec()),
        (original::TTX_DESC, original::teletext_body()),
    ] {
        let matches: Vec<_> = descriptors
            .iter()
            .filter(|(_, d)| d.windows(descriptor.len()).any(|v| v == descriptor))
            .collect();
        if keep {
            assert_eq!(matches.len(), 1, "regional descriptor must survive once");
            let payloads = original::pes_bodies(bytes, *matches[0].0);
            assert!(
                payloads.len() > 1,
                "receiver must see repeated original cues"
            );
            assert!(
                payloads.iter().all(|p| p == &body),
                "original subtitle payload changed"
            );
        } else {
            assert!(
                matches.is_empty(),
                "disabled original tracks escaped filtering"
            );
            // Detect orphan payloads even if their descriptor was removed.
            let pids: BTreeSet<_> = bytes.chunks_exact(188).map(original::pid).collect();
            for pid in pids {
                let payloads = original::pes_bodies(bytes, pid);
                assert!(
                    !payloads
                        .iter()
                        .any(|p| p.windows(body.len()).any(|b| b == body)),
                    "orphan subtitle payload on PID {pid}"
                );
            }
        }
    }
}

async fn converted_words(app: &App, file: &str) -> String {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(list) = app.media.read("owned", file).await {
                let list = String::from_utf8(list.to_vec()).unwrap();
                let mut words = String::new();
                for segment in list.lines().filter(|v| v.ends_with(".vtt")) {
                    if let Ok(cue) = app.media.read("owned", segment).await {
                        words.push_str(std::str::from_utf8(&cue).unwrap());
                    }
                }
                if words.contains("USA") {
                    return words;
                }
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .expect("HLS conversion must continue alongside encrypted SRT")
}

async fn run(push: bool, digital: bool, input: &[u8], expected: &BTreeSet<Vec<u8>>) {
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
        let actual = caption_bodies(&path).await;
        assert!(
            expected.is_subset(&actual),
            "SRT must preserve every authored caption command independently of HLS policy; expected {} bodies, got {}",
            expected.len(),
            actual.len()
        );
        if let Some(words) = words {
            assert!(words.contains(if digital { "USA708" } else { "USA 608" }));
        }
        let decoded = Command::new("ffmpeg")
            .args(["-v", "error", "-xerror", "-threads", "1", "-i"])
            .arg(&path)
            .args(["-map", "0:v:0", "-map", "0:a:0", "-f", "null", "-"])
            .output()
            .await
            .unwrap();
        assert!(
            decoded.status.success(),
            "{}",
            String::from_utf8_lossy(&decoded.stderr)
        );
        assert_eq!(app.media.count().await, 0);
        assert_eq!(publication.worker.viewers.load(Ordering::Relaxed), 0);
        println!(
            "qualified {} CEA-{} with DVB/teletext: HLS {hls}, originals {}",
            if push {
                "encrypted push"
            } else {
                "encrypted playback"
            },
            if digital { 708 } else { 608 },
            if keep { "kept" } else { "filtered" }
        );
    }
}

async fn regional(push: bool) {
    let _guard = SERIAL.lock().await;
    for digital in [false, true] {
        let input = original::inject(&if digital {
            captions::digital_transport()
        } else {
            captions::transport()
        });
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("source.ts");
        std::fs::write(&file, &input).unwrap();
        let expected = caption_bodies(&file).await;
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
        run(push, digital, &input, &expected).await;
    }
}

#[tokio::test]
async fn encrypted_listener_carries_608_708_dvb_teletext_independently_of_hls_policy() {
    regional(false).await;
}

#[tokio::test]
async fn encrypted_push_carries_608_708_dvb_teletext_independently_of_hls_policy() {
    regional(true).await;
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
