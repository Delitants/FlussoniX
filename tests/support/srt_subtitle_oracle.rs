//! Independent raw-SRT receiver and broadcast subtitle oracles shared by delivery tests.
#[allow(dead_code)]
#[path = "subtitle_fixture.rs"]
pub mod original;
use flussonix::server::App;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    process::Stdio,
    time::Duration,
};
use tokio::process::{Child, Command};
pub const SECRET: &str = "owned-regional-srt-secret";

pub fn complete_sample(bytes: &[u8]) -> Vec<u8> {
    // Raw transport assertions remain untouched. Exclude only each PID's
    // unfinished terminal PES from a separate strict-decode copy.
    let mut last: BTreeMap<u16, (usize, Vec<u8>)> = BTreeMap::new();
    for (index, packet) in bytes.chunks_exact(188).enumerate() {
        assert_eq!(packet[0], 0x47);
        let Some(data) = original::payload(packet) else {
            continue;
        };
        let pid = original::pid(packet);
        if packet[1] & 0x40 != 0 {
            last.insert(pid, (index, data.to_vec()));
        } else if let Some((_, current)) = last.get_mut(&pid) {
            current.extend(data);
        }
    }
    let unfinished: BTreeMap<_, _> = last
        .into_iter()
        .filter_map(|(pid, (index, pes))| {
            if pes.len() < 6 || pes[..3] != [0, 0, 1] {
                return None;
            }
            let length = usize::from(u16::from_be_bytes([pes[4], pes[5]]));
            // An unbounded video PES is proven complete by its successor. The
            // final one has no successor in the sample, so omit it once.
            (length == 0 || pes.len() < 6 + length).then_some((pid, index))
        })
        .collect();
    let mut result = Vec::new();
    for (index, packet) in bytes.chunks_exact(188).enumerate() {
        if unfinished
            .get(&original::pid(packet))
            .is_none_or(|start| index < *start)
        {
            result.extend(packet);
        }
    }
    result
}

pub fn clean_decode(output: &std::process::Output) -> bool {
    // FFmpeg 6 may return zero after a decoder-thread error. With -v error,
    // stderr must also be empty to qualify a clean independent decode.
    output.status.success() && output.stderr.is_empty()
}

pub async fn strict_decode(path: &Path) -> std::process::Output {
    Command::new("ffmpeg")
        .args(["-v", "error", "-xerror", "-threads", "1", "-i"])
        .arg(path)
        .args(["-map", "0:v:0", "-map", "0:a:0", "-f", "null", "-"])
        .output()
        .await
        .unwrap()
}

pub async fn capture(endpoint: &str, listener: bool, path: &Path) -> Child {
    capture_as(endpoint, listener, path, "owned-viewer").await
}

pub async fn capture_as(endpoint: &str, listener: bool, path: &Path, token: &str) -> Child {
    let log = std::fs::File::create(path.with_extension("log")).unwrap();
    let mut cmd = Command::new("ffmpeg");
    cmd.args(["-v", "error", "-y", "-threads", "1", "-passphrase", SECRET]);
    if !listener {
        cmd.args(["-srt_streamid", &format!("#!::r=owned,m=request,u={token}")]);
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

// Codec identity must be checked independently of the requested output format.
pub async fn verify_codecs(path: &Path, hevc: bool) {
    let out = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "stream=codec_type,codec_name",
            "-of",
            "json",
        ])
        .arg(path)
        .output()
        .await
        .unwrap();
    assert!(
        clean_decode(&out),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let info: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let streams = info["streams"].as_array().unwrap();
    for (kind, codec) in [
        ("video", if hevc { "hevc" } else { "h264" }),
        ("audio", "aac"),
    ] {
        let actual: Vec<_> = streams
            .iter()
            .filter(|s| s["codec_type"] == kind)
            .map(|s| s["codec_name"].as_str().unwrap())
            .collect();
        assert_eq!(
            actual,
            [codec],
            "independent probe must verify the {kind} codec"
        );
    }
}

// Independently extract the video, then compare complete registered GA94 bodies.
pub async fn caption_bodies(path: &Path, hevc: bool) -> BTreeSet<Vec<u8>> {
    let out = Command::new("ffmpeg")
        .args(["-v", "error", "-nostdin", "-i"])
        .arg(path)
        .args([
            "-map",
            "0:v:0",
            "-c:v",
            "copy",
            "-f",
            if hevc { "hevc" } else { "h264" },
            "pipe:1",
        ])
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

pub fn verify_original_tracks(bytes: &[u8], keep: bool) {
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

pub async fn converted_words(app: &App, file: &str) -> String {
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
