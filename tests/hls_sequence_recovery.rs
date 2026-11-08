#[allow(dead_code)]
#[path = "support/subtitle_fixture.rs"]
mod subtitle_fixture;

use flussonix::media::Engine;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{path::Path, process::Command, time::Duration};
use tokio::io::AsyncWriteExt;

struct Window {
    sequence: u64,
    segments: Vec<String>,
    init: Option<String>,
}

impl Window {
    fn from_playlist(playlist: &str) -> Self {
        let sequence = playlist
            .lines()
            .find_map(|line| line.strip_prefix("#EXT-X-MEDIA-SEQUENCE:"))
            .unwrap()
            .parse()
            .unwrap();
        let segments = playlist
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(str::to_owned)
            .collect();
        let init = playlist
            .lines()
            .find_map(|line| line.strip_prefix("#EXT-X-MAP:URI=\""))
            .map(|line| line.split('"').next().unwrap().to_owned());
        Self {
            sequence,
            segments,
            init,
        }
    }

    fn next(&self) -> u64 {
        self.sequence + self.segments.len() as u64
    }
}

async fn wait_windows(engine: &Engine) -> [Window; 2] {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let ts = engine.read("owned", "index.m3u8").await;
            let fmp4 = engine.read("owned", "fmp4/index.m3u8").await;
            if let (Ok(ts), Ok(fmp4)) = (ts, fmp4) {
                let ts = Window::from_playlist(std::str::from_utf8(&ts).unwrap());
                let fmp4 = Window::from_playlist(std::str::from_utf8(&fmp4).unwrap());
                if !ts.segments.is_empty() && !fmp4.segments.is_empty() {
                    return [ts, fmp4];
                }
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    })
    .await
    .expect("both public HLS variants must publish a segment")
}

fn media_dir(root: &Path) -> std::path::PathBuf {
    root.join(format!("{:x}", Sha256::digest(b"owned")))
}

fn strict_decode(playlist: &Path) {
    let output = Command::new("ffmpeg")
        .args(["-v", "error", "-i"])
        .arg(playlist)
        .args(["-frames:v", "10", "-f", "null", "-"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}: {}",
        playlist.display(),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn cold_reopen_uses_durable_floor_for_both_real_hls_variants() {
    let root = tempfile::tempdir().unwrap();
    let config = json!({"inputs":[{"url":"testsrc://"}]});
    let first = Engine::new(root.path(), "ffmpeg");
    first.ensure("owned", &config).await.unwrap();
    let old = wait_windows(&first).await;
    first.stop_all().await;

    // Simulate an earlier clock by placing the previous timeline in the
    // future. The new Engine must honor the durable floor, not wall time.
    let floor = old.iter().map(Window::next).max().unwrap() + 100_000;
    let record = root.path().join(".hls-sequences").join(format!(
        "{}.json",
        media_dir(root.path())
            .file_name()
            .unwrap()
            .to_string_lossy()
    ));
    std::fs::write(
        &record,
        serde_json::to_vec(&json!({"version":1,"name":"owned","next_sequence":floor})).unwrap(),
    )
    .unwrap();

    let reopened = Engine::new(root.path(), "ffmpeg");
    reopened.ensure("owned", &config).await.unwrap();
    let new = wait_windows(&reopened).await;
    assert!(new.iter().all(|window| window.sequence >= floor));
    for (index, prefix) in ["", "fmp4/"].iter().enumerate() {
        for file in &old[index].segments {
            assert!(
                reopened
                    .read("owned", &format!("{prefix}{file}"))
                    .await
                    .is_err()
            );
        }
        if let Some(init) = &old[index].init {
            assert!(
                reopened
                    .read("owned", &format!("{prefix}{init}"))
                    .await
                    .is_err()
            );
        }
    }
    let directory = media_dir(root.path());
    strict_decode(&directory.join("index.m3u8"));
    strict_decode(&directory.join("fmp4/index.m3u8"));
    reopened.stop_all().await;
}

#[tokio::test]
async fn passthrough_public_sequence_survives_cold_reopen() {
    let root = tempfile::tempdir().unwrap();
    let config = json!({
        "inputs":[{"url":"publish://"}],
        "flussonix_hls_subtitles":"passthrough",
        "flussonix_subtitle_tracks":"preserve"
    });
    let input = subtitle_fixture::transport_for("12");
    let first = Engine::new(root.path(), "ffmpeg");
    let mut publication = first
        .publish_guarded("owned", &config, std::future::ready(true))
        .await
        .unwrap();
    publication
        .stdin
        .as_mut()
        .unwrap()
        .write_all(&input)
        .await
        .unwrap();
    let old = wait_windows(&first).await;
    first.stop_all().await;

    let reopened = Engine::new(root.path(), "ffmpeg");
    let mut publication = reopened
        .publish_guarded("owned", &config, std::future::ready(true))
        .await
        .unwrap();
    publication
        .stdin
        .as_mut()
        .unwrap()
        .write_all(&input)
        .await
        .unwrap();
    let new = wait_windows(&reopened).await;
    assert!(new[0].sequence >= old.iter().map(Window::next).max().unwrap());
    assert!(new[1].sequence >= old.iter().map(Window::next).max().unwrap());
    let private = std::fs::read_to_string(media_dir(root.path()).join("passthrough.m3u8")).unwrap();
    let private = Window::from_playlist(&private);
    assert!(private.sequence < new[0].sequence);
    strict_decode(&media_dir(root.path()).join("index.m3u8"));
    strict_decode(&media_dir(root.path()).join("fmp4/index.m3u8"));
    reopened.stop_all().await;
}
