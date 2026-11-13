//! Bounded artifacts for owned caption fixtures only; no production diagnostics.
use serde_json::{Value, json};
use std::{io::Read, path::Path};

const FILES: usize = 64;
const FILE_BYTES: usize = 2 * 1024 * 1024;
const ARCHIVE_BYTES: usize = 16 * 1024 * 1024;

fn files(root: &Path) -> Vec<std::path::PathBuf> {
    let mut pending = vec![(root.to_path_buf(), 0)];
    let mut out = vec![];
    while let Some((dir, depth)) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(kind) = entry.file_type() else {
                continue;
            };
            if kind.is_dir() && depth < 3 && pending.len() < FILES {
                pending.push((entry.path(), depth + 1));
            } else if kind.is_file() && out.len() < FILES {
                out.push(entry.path());
            }
        }
    }
    out.sort();
    out
}
fn read(path: &Path, limit: usize) -> std::io::Result<Vec<u8>> {
    let mut bytes = vec![];
    std::fs::File::open(path)?
        .take(limit as u64)
        .read_to_end(&mut bytes)?;
    Ok(bytes)
}
pub fn playlists(root: &Path) -> Vec<Value> {
    files(root)
        .iter()
        .filter(|p| p.extension().is_some_and(|x| x == "m3u8"))
        .filter_map(|p| {
            let b = read(p, 8192).ok()?;
            let text = String::from_utf8_lossy(&b);
            Some(json!({"path":p.strip_prefix(root).ok()?,"bytes":b.len(),
                "segments":text.matches("#EXTINF:").count(),
                "media_sequence":text.lines().find_map(|l|l.strip_prefix("#EXT-X-MEDIA-SEQUENCE:")),
                "last_uri":text.lines().rev().find(|l|!l.is_empty()&&!l.starts_with('#'))}))
        })
        .collect()
}
pub fn process(pid: u32) -> Value {
    let root = std::path::PathBuf::from(format!("/proc/{pid}"));
    let state = read(&root.join("stat"), 4096).ok().and_then(|b| {
        String::from_utf8_lossy(&b)
            .rsplit_once(')')
            .map(|(_, rest)| rest.split_whitespace().next().unwrap_or("").to_owned())
    });
    let wait = read(&root.join("wchan"), 128)
        .ok()
        .map(|b| String::from_utf8_lossy(&b).into_owned());
    json!({"pid":pid,"state":state,"wait":wait})
}
pub struct Snapshot {
    files: Vec<(std::path::PathBuf, Vec<u8>, Option<u64>)>,
    responses: Vec<Value>,
    bytes: usize,
}
impl Snapshot {
    fn add(&mut self, path: std::path::PathBuf, mut bytes: Vec<u8>, size: Option<u64>) {
        if self.files.len() == FILES || self.bytes == ARCHIVE_BYTES {
            return;
        }
        bytes.truncate(FILE_BYTES.min(ARCHIVE_BYTES - self.bytes));
        self.bytes += bytes.len();
        self.files.push((path, bytes, size));
    }
}
/// Freeze owned resources before Engine::stop_all removes the stream directory.
/// This is a bounded sequential observation, not an atomic playlist transaction.
pub async fn snapshot(engine: &flussonix::media::Engine, root: &Path) -> Snapshot {
    let mut snapshot = Snapshot {
        files: vec![],
        responses: vec![],
        bytes: 0,
    };
    for prefix in ["", "fmp4/"] {
        let mut wanted = vec![];
        for file in ["index.m3u8", "av.m3u8", "s1.m3u8", "s2.m3u8"] {
            wanted.push(format!("{prefix}{file}"));
        }
        let mut at = 0;
        while at < wanted.len() && at < 32 {
            let file = wanted[at].clone();
            at += 1;
            match tokio::time::timeout(
                std::time::Duration::from_millis(100),
                engine.read("group/owned", &file),
            )
            .await
            {
                Ok(Ok(bytes)) => {
                    if file.ends_with(".m3u8") {
                        for uri in String::from_utf8_lossy(&bytes).lines() {
                            if wanted.len() < 32
                                && uri.ends_with(".vtt")
                                && uri.len() < 128
                                && !uri.contains("..")
                                && uri
                                    .bytes()
                                    .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
                            {
                                let path = format!("{prefix}{uri}");
                                if !wanted.contains(&path) {
                                    wanted.push(path);
                                }
                            }
                        }
                    }
                    snapshot
                        .responses
                        .push(json!({"path":file,"status":"ready","bytes":bytes.len()}));
                    snapshot.add(
                        Path::new("responses").join(file),
                        bytes.to_vec(),
                        Some(bytes.len() as u64),
                    );
                }
                Ok(Err(error)) => snapshot
                    .responses
                    .push(json!({"path":file,"status":"unavailable","error":error})),
                Err(_) => snapshot
                    .responses
                    .push(json!({"path":file,"status":"snapshot_timeout"})),
            }
        }
    }
    for path in files(root) {
        if snapshot.files.len() == FILES || snapshot.bytes == ARCHIVE_BYTES {
            break;
        }
        if let Ok(bytes) = read(&path, FILE_BYTES.min(ARCHIVE_BYTES - snapshot.bytes)) {
            let size = std::fs::metadata(&path).ok().map(|m| m.len());
            snapshot.add(
                Path::new("disk").join(path.strip_prefix(root).unwrap()),
                bytes,
                size,
            );
        }
    }
    snapshot
}
pub fn save(
    snapshot: Snapshot,
    cpu: bool,
    input: &[u8],
    captured: &[u8],
    report: &Value,
) -> std::io::Result<()> {
    let Ok(destination) = std::env::var("FLUSSONIX_CAPTION_ARTIFACT_DIR") else {
        return Ok(());
    };
    let out = Path::new(&destination).join(if cpu { "cea708-cpu" } else { "cea708-copy" });
    std::fs::create_dir_all(&out)?;
    std::fs::write(out.join("input.ts"), &input[..input.len().min(FILE_BYTES)])?;
    std::fs::write(
        out.join("captured.ts"),
        &captured[..captured.len().min(FILE_BYTES)],
    )?;
    let mut manifest = vec![];
    for (relative, bytes, size) in snapshot.files {
        let destination = out.join("media").join(&relative);
        std::fs::create_dir_all(destination.parent().unwrap())?;
        std::fs::write(destination, &bytes)?;
        manifest.push(
            json!({"path":relative,"retained_bytes":bytes.len(),"source_bytes_at_snapshot":size}),
        );
    }
    let mut report = report.clone();
    report["archive"] = json!({"files":manifest,"media_bytes":snapshot.bytes,"responses":snapshot.responses,
        "snapshot_phase":"before_feed_abort_and_worker_stop; sequential, not atomic",
        "input_bytes":input.len(),"retained_input_bytes":input.len().min(FILE_BYTES),
        "captured_bytes":captured.len(),"retained_captured_bytes":captured.len().min(FILE_BYTES)});
    std::fs::write(out.join("report.json"), serde_json::to_vec_pretty(&report)?)
}
