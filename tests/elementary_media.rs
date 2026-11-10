//! Independent plaintext RTP qualification.
#[path = "support/elementary_media.rs"]
mod support;
use serde_json::json;
use support::qualify;

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "current_thread")]
async fn owned_recorder_disk_stall_does_not_pause_media_runtime_or_truncate_chunk() {
    use std::{
        ffi::CString,
        fs::OpenOptions,
        io::Read,
        os::{fd::AsRawFd, unix::ffi::OsStrExt, unix::fs::OpenOptionsExt},
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::{Duration, Instant},
    };

    let owned = tempfile::tempdir().unwrap();
    let path = owned.path().join("blocked-recorder.fifo");
    let name = CString::new(path.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
    let writer = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let capacity = unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_GETPIPE_SZ) };
    assert!((1..=1024 * 1024).contains(&capacity));
    let channel = flussonix::media_queue::Channel::new(4, 2 * 1024 * 1024);
    let receiver = channel.subscribe();
    let cancel = tokio_util::sync::CancellationToken::new();
    let recording = support::start_worker_recording(receiver, writer, cancel.clone());
    let expected = vec![0x47; capacity as usize + 4096];
    let expected_len = expected.len();
    channel.send(bytes::Bytes::from(expected.clone())).unwrap();

    let (drain, release) = std::sync::mpsc::channel();
    let (complete, completed) = tokio::sync::oneshot::channel();
    let stop = Arc::new(AtomicBool::new(false));
    let reader_stop = stop.clone();
    let mut reader_file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(&path)
        .unwrap();
    let reader = std::thread::spawn({
        move || {
            // The timeout lets a deliberately blocking recorder finish and
            // exposes its stalled runtime instead of deadlocking the test.
            let _ = release.recv_timeout(Duration::from_secs(1));
            let mut actual = vec![0; expected_len];
            let mut filled = 0;
            while filled < actual.len() && !reader_stop.load(Ordering::Relaxed) {
                match reader_file.read(&mut actual[filled..]) {
                    Ok(0) => std::thread::sleep(Duration::from_millis(1)),
                    Ok(n) => filled += n,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(error) => return Err(error),
                }
            }
            if filled == actual.len() {
                let _ = complete.send(());
            }
            Ok((actual, filled))
        }
    });

    let began = Instant::now();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let responsive = began.elapsed() < Duration::from_millis(500);
    drain.send(()).unwrap();
    let drained = tokio::time::timeout(Duration::from_secs(5), completed)
        .await
        .is_ok();
    cancel.cancel();
    let recorded = recording.await;
    stop.store(true, Ordering::Relaxed);
    let result = tokio::task::spawn_blocking(move || reader.join().unwrap())
        .await
        .unwrap();
    recorded.unwrap();
    let (actual, filled) = result.unwrap();
    assert!(
        responsive,
        "a blocked recording write paused the media runtime"
    );
    assert!(drained, "recording stalled before exact drain");
    assert_eq!(filled, expected_len);
    assert_eq!(actual, expected);
}
#[tokio::test]
#[should_panic(expected = "video picture gap")]
async fn independent_decode_rejects_decodable_video_picture_gap() {
    let owned = tempfile::tempdir().unwrap();
    let media = owned.path().join("gap.ts");
    let encoded = tokio::process::Command::new("/usr/bin/ffmpeg")
        .args([
            "-nostdin",
            "-v",
            "error",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=duration=3:size=320x180:rate=25",
            "-vf",
            r"setpts=PTS+1.5/TB*gte(N\,40)",
            "-c:v",
            "libx264",
            "-threads",
            "1",
            "-preset",
            "ultrafast",
            "-g",
            "25",
            "-bf",
            "0",
            "-fps_mode",
            "passthrough",
            "-f",
            "mpegts",
        ])
        .arg(&media)
        .kill_on_drop(true)
        .output()
        .await
        .unwrap();
    assert!(
        encoded.status.success(),
        "{}",
        String::from_utf8_lossy(&encoded.stderr)
    );
    support::decode(&media, Some("h264"), &[]).await;
}
#[tokio::test]
async fn elementary_roundtrip_cpu_h264_hevc_and_all_audio_codecs_decode() {
    for video in ["libx264", "libx265"] {
        for audio in ["aac", "mp2", "libmp3lame"] {
            qualify(
                Some(video),
                &[audio],
                json!({"encoder":"copy","acodec":"copy"}),
                false,
                false,
            )
            .await;
        }
    }
}

#[tokio::test]
async fn direct_rtp_mp3_copy_delivers_fmp4_audio_and_video() {
    support::qualify_fmp4(
        Some("libx264"),
        &["libmp3lame"],
        json!({"encoder":"copy","acodec":"copy"}),
        false,
        false,
    )
    .await;
}

#[tokio::test]
async fn direct_rtp_aac_copy_delivers_fmp4_audio_and_video() {
    support::qualify_fmp4(
        Some("libx264"),
        &["aac"],
        json!({"encoder":"copy","acodec":"copy"}),
        false,
        false,
    )
    .await;
}

#[tokio::test]
async fn direct_rtp_mp2_copy_delivers_fmp4_audio_and_video() {
    support::qualify_fmp4(
        Some("libx264"),
        &["mp2"],
        json!({"encoder":"copy","acodec":"copy"}),
        false,
        false,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "isolated HEVC/MP3 intermittent-loss diagnostic; use a fresh evidence directory"]
async fn diagnostic_hevc_mp3_plaintext_input_once() {
    qualify(
        Some("libx265"),
        &["libmp3lame"],
        json!({"encoder":"copy","acodec":"copy"}),
        false,
        false,
    )
    .await;
}
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires CAP_NET_RAW, system FFmpeg and a fresh owned artifact directory"]
async fn elementary_handoff_capture_starts_before_public_release() {
    let base = std::env::var("FLUSSONIX_ELEMENTARY_ARTIFACT_DIR")
        .expect("set a fresh owner-only artifact directory");
    assert_eq!(
        std::env::var("FLUSSONIX_ELEMENTARY_PACKET_TAPS").as_deref(),
        Ok("1")
    );
    let case = std::path::Path::new(&base).join("libx265-mp2-copy-false-false");
    assert!(!case.exists(), "this case needs a fresh artifact directory");
    qualify(
        Some("libx265"),
        &["mp2"],
        json!({"encoder":"copy","acodec":"copy"}),
        false,
        false,
    )
    .await;
    let entries = std::fs::read_to_string(case.join("boundary-evidence.jsonl")).unwrap();
    let taps: Vec<serde_json::Value> = entries
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .flat_map(|entry| entry["packet_taps"].as_array().cloned().unwrap_or_default())
        .collect();
    let public = taps
        .iter()
        .find(|tap| tap["boundary"] == "source_to_public")
        .unwrap();
    let private = taps
        .iter()
        .find(|tap| tap["boundary"] == "relay_to_decoder")
        .unwrap();
    let ns = |tap: &serde_json::Value, name: &str| tap[name].as_u64().unwrap_or(0);
    assert!(
        ns(public, "coverage_start_ns") < ns(public, "sender_spawn_ns")
            && ns(public, "sender_spawn_ns") < ns(public, "sender_witness_ns")
            && ns(public, "sender_witness_ns") < ns(public, "reservation_release_ns")
            && ns(public, "reservation_release_ns") < ns(public, "native_owner_verified_ns")
            && ns(public, "native_owner_verified_ns") < ns(public, "coverage_end_ns"),
        "public capture must span reservation release and native bind: {public}"
    );
    assert!(ns(private, "coverage_start_ns") >= ns(public, "native_owner_verified_ns"));
    if public["observation"] == "unavailable" {
        assert!(!case.join("source_to_public.packets").exists());
    } else {
        assert_eq!(public["handoff_captured"], true);
        assert_eq!(public["source_prefix_unobserved"], true);
        assert_eq!(public["startup_not_observed"], true);
        assert_eq!(public["sender_prefix_attribution"], "candidate_only");
        assert_eq!(public["candidate_source_ports_match_sender_witness"], true);
        assert_eq!(public["incomplete"], false);
    }
}
#[tokio::test]
async fn elementary_audio_only_and_multiple_mpeg_audio_tracks_remain_distinct() {
    qualify(
        None,
        &["aac"],
        json!({"encoder":"copy","acodec":"copy"}),
        false,
        false,
    )
    .await;
    qualify(
        None,
        &["mp2", "libmp3lame"],
        json!({"encoder":"copy","acodec":"copy"}),
        false,
        false,
    )
    .await;
}
#[tokio::test]
async fn elementary_cpu_worker_changes_video_and_audio_codec_once() {
    qualify(
        Some("libx264"),
        &["aac"],
        json!({"encoder":"libx265","vb":300,"acodec":"mp3","ab":128}),
        false,
        false,
    )
    .await;
}
#[tokio::test]
#[ignore = "requires an independently installed working VAAPI render device and driver environment"]
async fn elementary_internal_vaapi_worker_output_decodes() {
    qualify(
        Some("libx264"),
        &["aac"],
        json!({"encoder":"h264_vaapi","qp":24,"acodec":"mp2a","ab":192}),
        false,
        false,
    )
    .await;
}
