//! Independently encoded regional subtitles, through configured native direct transports.
#[allow(dead_code)]
#[path = "caption_fixture.rs"]
mod captions;
#[allow(dead_code)]
#[path = "srt_subtitle_oracle.rs"]
mod oracle;
use flussonix::{
    direct_rtp::crypto,
    server::{App, Options},
};
use oracle::*;
use serde_json::json;
use std::{net::Ipv4Addr, time::Duration};
use tokio::net::UdpSocket;
#[path = "direct_media.rs"]
mod media;
// Independent receiver framing: does not call FlussoniX's RTP parser/reorder.
pub async fn run(secure: bool) {
    for (hevc, digital, hls, keep) in [
        (false, false, "convert", true),
        (false, true, "drop", true),
        (true, false, "drop", false),
        (true, true, "passthrough", true),
    ] {
        let d = tempfile::tempdir().unwrap();
        let source = d.path().join("source.ts");
        let input = original::inject(&if hevc {
            captions::hevc_transport(digital)
        } else if digital {
            captions::digital_transport()
        } else {
            captions::transport()
        });
        std::fs::write(&source, &input).unwrap();
        let expected = caption_bodies(&source, hevc).await;
        verify_original_tracks(&input, true);
        let rx = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let output = rx.local_addr().unwrap().port();
        assert!(output < 65535);
        let control = UdpSocket::bind((Ipv4Addr::LOCALHOST, output + 1))
            .await
            .unwrap();
        let inbound = media::port();
        let scheme = if secure { "srtp" } else { "rtp" };
        let key = [0x39u8; 30];
        let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, key);
        let key_file = d.path().join("owned.key");
        std::fs::write(&key_file, &encoded).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key_file, std::fs::Permissions::from_mode(0o600)).unwrap();
        let opts = if secure {
            json!({"key_file":key_file})
        } else {
            json!({})
        };
        let app = App::new(
            d.path().join("config.json"),
            d.path().join("media"),
            Options {
                admin_password: "owned-rtp-admin".into(),
                peer_key: "owned-rtp-peer".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let mut cfg = json!({"inputs":[{"url":format!("{scheme}://127.0.0.1:{inbound}"),"flussonix_rtp":opts}],"flussonix_rtp_outputs":[{"url":format!("{scheme}://127.0.0.1:{output}"),"flussonix_rtp":opts}],"flussonix_hls_subtitles":hls,"flussonix_subtitle_tracks":if keep{"preserve"}else{"drop"},"flussonix_input_timeout":15});
        if hls == "convert" {
            cfg["flussonix_hls_captions"] = json!([{"channel":1,"language":"en","name":"English"}]);
        }
        app.config.put("streams", "owned", cfg.clone()).unwrap();
        let worker = app
            .media
            .ensure_guarded("owned", &cfg, true, std::future::ready(true))
            .await
            .unwrap();
        let mut capture = tokio::spawn(async move {
            let mut cipher = if secure {
                Some(crypto::Session::new(key, None).unwrap())
            } else {
                None
            };
            let mut out = vec![];
            let mut packet = [0u8; 1600];
            let deadline = tokio::time::Instant::now() + Duration::from_secs(13);
            loop {
                let got = tokio::time::timeout_at(deadline, rx.recv(&mut packet)).await;
                let Ok(Ok(n)) = got else { break };
                let mut bytes = packet[..n].to_vec();
                if let Some(c) = &mut cipher {
                    c.unprotect(&mut bytes, false).unwrap();
                }
                assert_eq!(&bytes[..2], &[0x80, 33]);
                assert_eq!((bytes.len() - 12) % 188, 0);
                out.extend_from_slice(&bytes[12..]);
            }
            out
        });
        // RTP MPEG-TS muxing in FFmpeg drops subtitle language metadata in its
        // nested TS context. Emit authored TS through a separate RFC2250 framer,
        // while FFmpeg's raw-data protocol supplies independent SRTP encryption.
        let mut cmd = tokio::process::Command::new("ffmpeg");
        cmd.args([
            "-v",
            "error",
            "-f",
            "data",
            "-raw_packet_size",
            "1328",
            "-i",
            "pipe:0",
            "-map",
            "0",
            "-c",
            "copy",
            "-flush_packets",
            "1",
        ]);
        if secure {
            cmd.args([
                "-srtp_out_suite",
                "AES_CM_128_HMAC_SHA1_80",
                "-srtp_out_params",
                &encoded,
            ]);
        }
        let mut sender = cmd
            .args([
                "-f",
                "data",
                &format!("{scheme}://127.0.0.1:{inbound}?pkt_size=1400"),
            ])
            .stdin(std::process::Stdio::piped())
            .stderr(std::fs::File::create(d.path().join("sender.log")).unwrap())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let writer = sender.stdin.take().unwrap();
        let feed = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(framed_source(
            writer,
            input.clone(),
        )));
        let received = tokio::time::timeout(Duration::from_secs(16), &mut capture)
            .await
            .unwrap()
            .unwrap();
        feed.abort();
        let _ = feed.await;
        let _ = sender.kill().await;
        let _ = sender.wait().await;
        let words = if hls == "convert" {
            Some(converted_words(&app, "cc1.m3u8").await)
        } else {
            None
        };
        app.media.stop_all().await;
        drop(control);
        let path = d.path().join("received.ts");
        std::fs::write(&path, &received).unwrap();
        if let Ok(root) = std::env::var("FLUSSONIX_DIRECT_ARTIFACT_DIR") {
            let root = std::path::Path::new(&root)
                .join(format!("{scheme}-subtitle-debug-{hevc}-{digital}-{hls}"));
            std::fs::create_dir_all(&root).unwrap();
            std::fs::write(root.join("source.ts"), &input).unwrap();
            std::fs::write(root.join("received.ts"), &received).unwrap();
            std::fs::copy(d.path().join("sender.log"), root.join("sender.log")).unwrap();
            std::fs::write(
                root.join("stats.json"),
                serde_json::to_vec_pretty(&worker.stats()).unwrap(),
            )
            .unwrap();
        }
        println!(
            "source descriptors {:?}, received {:?}",
            original::descriptors(&input),
            original::descriptors(&received)
        );
        verify_original_tracks(&received, keep);
        verify_codecs(&path, hevc).await;
        let actual = caption_bodies(&path, hevc).await;
        assert!(
            expected.is_subset(&actual),
            "missing authored caption bodies: expected {}, got {}",
            expected.len(),
            actual.len()
        );
        if let Some(words) = words {
            assert!(words.contains("USA 608"));
        }
        let clean = d.path().join("complete.ts");
        std::fs::write(&clean, complete_sample(&received)).unwrap();
        let decode = strict_decode(&clean).await;
        assert!(
            clean_decode(&decode),
            "{}",
            String::from_utf8_lossy(&decode.stderr)
        );
        assert_eq!(app.media.count().await, 0);
        if let Ok(root) = std::env::var("FLUSSONIX_DIRECT_ARTIFACT_DIR") {
            let root = std::path::Path::new(&root).join(format!(
                "{scheme}-subtitles-{}-{}-{hls}",
                if hevc { "hevc" } else { "h264" },
                if digital { 708 } else { 608 }
            ));
            std::fs::create_dir_all(&root).unwrap();
            std::fs::write(root.join("received.ts"), received).unwrap();
            std::fs::write(
                root.join("stats.json"),
                serde_json::to_vec_pretty(&worker.stats()).unwrap(),
            )
            .unwrap();
        }
    }
}

async fn framed_source(mut writer: tokio::process::ChildStdin, bytes: Vec<u8>) {
    use tokio::io::AsyncWriteExt;
    let clock = tokio::time::Instant::now();
    let mut initial = None;
    let mut current = 0;
    let mut sequence = 100u16;
    let mut null = vec![0xff; 188];
    null[..4].copy_from_slice(&[0x47, 0x1f, 0xff, 0x10]);
    for chunk in bytes.chunks(7 * 188) {
        for packet in chunk.chunks_exact(188) {
            if original::pid(packet) == 256 && packet[1] & 0x40 != 0 {
                if let Some(data) = original::payload(packet) {
                    if let Some(pts) = original::pts(data) {
                        let first = *initial.get_or_insert(pts);
                        current = pts.saturating_sub(first);
                    }
                }
            }
        }
        tokio::time::sleep_until(clock + Duration::from_micros(current * 1_000_000 / 90_000)).await;
        let mut packet = vec![0x80, 33];
        packet.extend(sequence.to_be_bytes());
        packet.extend((current as u32).to_be_bytes());
        packet.extend(0x19202342u32.to_be_bytes());
        packet.extend(chunk);
        while packet.len() < 1328 {
            packet.extend(&null);
        }
        if writer.write_all(&packet).await.is_err() {
            return;
        }
        sequence = sequence.wrapping_add(1);
    }
    std::future::pending::<()>().await;
}
