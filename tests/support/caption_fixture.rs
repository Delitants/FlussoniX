//! Owned words and caption commands embedded in independently generated video.
use std::{path::Path, process::Command};
fn run(args: &[&str]) {
    let o = Command::new("ffmpeg").args(args).output().unwrap();
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
}
fn parity(b: u8) -> u8 {
    b | if b.count_ones() % 2 == 0 { 128 } else { 0 }
}
pub fn transport() -> Vec<u8> {
    make(false, false)
}
#[allow(dead_code)]
pub fn digital_transport() -> Vec<u8> {
    make(true, false)
}
#[allow(dead_code)]
pub fn hevc_transport(digital: bool) -> Vec<u8> {
    make(digital, true)
}
fn make(digital: bool, hevc: bool) -> Vec<u8> {
    let d = tempfile::tempdir().unwrap();
    let format = if hevc { "hevc" } else { "h264" };
    let raw = d.path().join(format!("raw.{format}"));
    run(&[
        "-y",
        "-v",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc2=size=64x48:rate=25",
        "-t",
        "16",
        "-threads",
        "1",
        "-c:v",
        if hevc { "libx265" } else { "libx264" },
        "-preset",
        "ultrafast",
        "-g",
        "50",
        "-bf",
        "0",
        if hevc { "-x265-params" } else { "-x264-params" },
        if hevc {
            "aud=1:pools=none:frame-threads=1:repeat-headers=1:log-level=error"
        } else {
            "aud=1"
        },
        "-f",
        format,
        raw.to_str().unwrap(),
    ]);
    let source = std::fs::read(raw).unwrap();
    let mut bounds = vec![];
    let mut i = 0;
    while i + 3 < source.len() {
        if source[i..i + 3] == [0, 0, 1] {
            bounds.push(i + 3);
            i += 3
        } else {
            i += 1
        }
    }
    bounds.push(source.len() + 3);
    let mut annotated = vec![];
    let mut frame = -1i32;
    for w in bounds.windows(2) {
        let nal = &source[w[0]..w[1] - 3];
        annotated.extend([0, 0, 0, 1]);
        annotated.extend(nal);
        let aud = if hevc {
            (nal[0] >> 1) & 63 == 35
        } else {
            nal[0] & 31 == 9
        };
        if !aud {
            continue;
        }
        frame += 1;
        let pairs: Vec<(u8, u8)> = match frame {
            25 | 26 => vec![(0x14, 0x20), (0x14, 0x2e)],
            27 => vec![(b'U', b'S'), (b'A', b' '), (b'6', b'0'), (b'8', b' ')],
            29 | 30 => vec![(0x14, 0x2f)],
            75 | 76 => vec![(0x14, 0x2c)],
            101 | 102 => vec![(0x14, 0x29)],
            103 => vec![(b'L', b'I'), (b'V', b'E')],
            150 | 151 => vec![(0x14, 0x2c)],
            _ => vec![],
        };
        let raw: Vec<(u8, u8, u8)> = if digital {
            let mut blocks = vec![];
            let sequence = match frame {
                29 => Some(0),
                75 => Some(1),
                103 => Some(2),
                150 => Some(3),
                _ => None,
            };
            if let Some(seq) = sequence {
                for (service, text) in [(1, b"USA708".as_slice()), (2, b"ESPA\xd1OL".as_slice())] {
                    let mut commands = vec![];
                    if frame == 29 || frame == 103 {
                        commands.extend([0x98, 0x18, 70, 80, 1, 31, 0]);
                        if frame == 103 {
                            commands.extend([0x88, 1, 0x92, 0, 0]);
                        }
                        commands.extend(if frame == 29 { text } else { b"LIVE708" });
                        commands.extend([0x89, 1]);
                    } else {
                        commands.extend([0x8a, 1]);
                    }
                    blocks.push((service << 5) | commands.len() as u8);
                    blocks.extend(commands);
                }
                if blocks.len() % 2 == 0 {
                    blocks.push(0);
                }
                let mut triples = vec![(3, (seq << 6) | blocks.len().div_ceil(2) as u8, blocks[0])];
                for b in blocks[1..].chunks_exact(2) {
                    triples.push((2, b[0], b[1]));
                }
                triples
            } else {
                vec![]
            }
        } else {
            pairs
                .into_iter()
                .map(|(a, b)| (0, parity(a), parity(b)))
                .collect()
        };
        if raw.is_empty() {
            continue;
        }
        let mut data = b"\xb5\x00\x31GA94\x03".to_vec();
        data.extend([0x40 | raw.len() as u8, 255]);
        for (kind, a, b) in raw {
            data.extend([0xfc | kind, a, b])
        }
        data.push(255);
        let mut rbsp = vec![4, data.len() as u8];
        rbsp.extend(data);
        rbsp.push(128);
        let mut escaped = vec![];
        let mut zeros = 0;
        for b in rbsp {
            if zeros >= 2 && b <= 3 {
                escaped.push(3);
                zeros = 0
            }
            escaped.push(b);
            zeros = if b == 0 { zeros + 1 } else { 0 }
        }
        if hevc {
            // HEVC prefix SEI, layer zero, temporal_id_plus1 one.
            annotated.extend([0, 0, 0, 1, 0x4e, 1]);
        } else {
            annotated.extend([0, 0, 0, 1, 6]);
        }
        annotated.extend(escaped);
    }
    let input = d.path().join(format!("owned.{format}"));
    std::fs::write(&input, annotated).unwrap();
    let output = d.path().join("owned.ts");
    remux(&input, &output);
    {
        let bytes = std::fs::read(output).unwrap();
        // Browser fixtures retain their established AVC format.
        if !hevc {
            if let Ok(path) = std::env::var(if digital {
                "FLUSSONIX_708_FIXTURE_FILE"
            } else {
                "FLUSSONIX_CAPTION_FIXTURE_FILE"
            }) {
                std::fs::write(path, &bytes).unwrap();
            }
        }
        bytes
    }
}
fn remux(input: &Path, output: &Path) {
    run(&[
        "-y",
        "-v",
        "error",
        "-r",
        "25",
        "-i",
        input.to_str().unwrap(),
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=700:sample_rate=48000",
        "-t",
        "16",
        "-threads",
        "1",
        "-c:v",
        "copy",
        "-c:a",
        "aac",
        "-max_interleave_delta",
        "0",
        "-f",
        "mpegts",
        output.to_str().unwrap(),
    ]);
}
pub async fn paced(mut stdin: impl tokio::io::AsyncWrite + Unpin + Send, bytes: Vec<u8>) {
    paced_observed(&mut stdin, bytes, None).await;
}
#[derive(Clone, Default, serde::Serialize)]
pub struct PacingProgress {
    pub phase: &'static str,
    pub total_bytes: usize,
    /// Completed write_all calls only; a pending write may have admitted a prefix.
    pub written_bytes: usize,
    pub pending_bytes: usize,
    pub first_pts: Option<u64>,
    pub target_pts: Option<u64>,
    pub last_written_pts: Option<u64>,
}
pub async fn paced_observed(
    stdin: &mut (impl tokio::io::AsyncWrite + Unpin + Send),
    bytes: Vec<u8>,
    progress: Option<std::sync::Arc<std::sync::Mutex<PacingProgress>>>,
) {
    use tokio::io::AsyncWriteExt;
    let clock = tokio::time::Instant::now();
    let mut first = None;
    let mut chunk = vec![];
    let mut chunk_pts = None;
    let update = |phase, pending, target| {
        if let Some(progress) = &progress {
            let mut p = progress.lock().unwrap();
            p.phase = phase;
            p.pending_bytes = pending;
            p.target_pts = target;
        }
    };
    let admitted = |size, pts, initial| {
        if let Some(progress) = &progress {
            let mut p = progress.lock().unwrap();
            p.written_bytes += size;
            p.pending_bytes = 0;
            p.last_written_pts = pts;
            p.first_pts = initial;
        }
    };
    for packet in bytes.chunks_exact(188) {
        let offset = if packet[3] & 0x20 != 0 {
            5 + usize::from(packet[4])
        } else {
            4
        };
        if packet[1] & 31 == 1 && packet[2] == 0 && packet[1] & 0x40 != 0 && offset + 14 <= 188 {
            if let Some(pts) = flussonix::caption_transport::pts(&packet[offset + 9..offset + 14]) {
                let initial = *first.get_or_insert(pts);
                update("writing", chunk.len(), chunk_pts);
                if stdin.write_all(&chunk).await.is_err() {
                    update("closed", chunk.len(), chunk_pts);
                    return;
                }
                admitted(chunk.len(), chunk_pts, first);
                chunk.clear();
                update("sleeping", 0, Some(pts));
                tokio::time::sleep_until(
                    clock
                        + std::time::Duration::from_micros(
                            pts.saturating_sub(initial) * 1000000 / 90000,
                        ),
                )
                .await;
                chunk_pts = Some(pts);
            }
        }
        chunk.extend(packet);
    }
    update("writing", chunk.len(), chunk_pts);
    if stdin.write_all(&chunk).await.is_err() {
        update("closed", chunk.len(), chunk_pts);
        return;
    }
    admitted(chunk.len(), chunk_pts, first);
    update("holding_open", 0, chunk_pts);
    std::future::pending::<()>().await;
}
