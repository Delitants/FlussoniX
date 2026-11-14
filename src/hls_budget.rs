use std::path::{Path, PathBuf};
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;

const MAX_SEGMENT: u64 = 64 * 1024 * 1024;
const MAX_TOTAL: u64 = 256 * 1024 * 1024;
const MAX_FILES: usize = 64;

fn owned_segment(name: &str, generation: &str, fmp4: bool) -> bool {
    let prefix = format!("g{generation}_");
    let Some(name) = name.strip_prefix(&prefix) else {
        return false;
    };
    let name = name.strip_suffix(".tmp").unwrap_or(name);
    if fmp4 && name == "init.mp4" {
        return true;
    }
    let numbered = if fmp4 {
        name.strip_suffix(".m4s")
    } else {
        name.strip_suffix(".ts")
    };
    numbered
        .map(|n| {
            if fmp4 {
                n
            } else {
                n.strip_prefix('p').unwrap_or(n)
            }
        })
        .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
}

async fn inspect_dir(
    dir: &Path,
    generation: &str,
    fmp4: bool,
    files: &mut usize,
    bytes: &mut u64,
) -> Result<(), String> {
    let mut entries = tokio::fs::read_dir(dir)
        .await
        .map_err(|_| "cannot inspect HLS segment budget")?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|_| "cannot inspect HLS segment budget")?
    {
        let name = entry.file_name();
        if !name
            .to_str()
            .is_some_and(|s| owned_segment(s, generation, fmp4))
        {
            continue;
        }
        let metadata = match tokio::fs::symlink_metadata(entry.path()).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return Err("cannot inspect HLS segment budget".into()),
        };
        *files += 1;
        *bytes = bytes.saturating_add(metadata.len());
        if !metadata.is_file()
            || metadata.len() > MAX_SEGMENT
            || *files > MAX_FILES
            || *bytes > MAX_TOTAL
        {
            return Err("HLS segment budget exceeded".into());
        }
    }
    Ok(())
}

async fn check_generation_budget(dir: &Path, generation: &str) -> Result<(), String> {
    let mut files = 0;
    let mut bytes = 0;
    inspect_dir(dir, generation, false, &mut files, &mut bytes).await?;
    inspect_dir(&dir.join("fmp4"), generation, true, &mut files, &mut bytes).await
}

#[derive(Default)]
struct MuxProgress {
    last_segment: Option<u64>,
    bytes_at_last_segment: u64,
}

impl MuxProgress {
    fn observe(
        &mut self,
        latest_segment: Option<u64>,
        stdout_bytes: u64,
        limit: u64,
    ) -> Result<(), String> {
        if let Some(segment) = latest_segment
            && self.last_segment.is_none_or(|last| segment > last)
        {
            self.last_segment = Some(segment);
            self.bytes_at_last_segment = stdout_bytes;
        }
        if stdout_bytes.saturating_sub(self.bytes_at_last_segment) > limit {
            return Err("HLS mux buffer budget exceeded".into());
        }
        Ok(())
    }
}

async fn latest_segment(
    dir: &Path,
    generation: &str,
    raw_hls: bool,
) -> Result<Option<u64>, String> {
    let filename = if raw_hls {
        "passthrough.m3u8"
    } else {
        "index.m3u8"
    };
    let file = match tokio::fs::File::open(dir.join(filename)).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("cannot inspect HLS playlist progress".into()),
    };
    let mut raw = Vec::new();
    file.take(65537)
        .read_to_end(&mut raw)
        .await
        .map_err(|_| "cannot inspect HLS playlist progress")?;
    if raw.len() > 65536 {
        return Err("HLS playlist progress exceeds size limit".into());
    }
    let text = std::str::from_utf8(&raw).map_err(|_| "invalid HLS playlist progress")?;
    let prefix = if raw_hls {
        format!("g{generation}_p")
    } else {
        format!("g{generation}_")
    };
    Ok(text
        .lines()
        .filter_map(|line| {
            line.strip_prefix(&prefix)?
                .strip_suffix(".ts")?
                .parse::<u64>()
                .ok()
        })
        .max())
}

pub async fn watch<F>(
    dir: PathBuf,
    generation: String,
    raw_hls: bool,
    output_bytes: F,
    cancel: CancellationToken,
) -> Result<(), String>
where
    F: Fn() -> u64 + Send + Sync,
{
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut progress = MuxProgress::default();
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            _ = tick.tick() => {}
        }
        let result = check_generation_budget(&dir, &generation).await;
        if cancel.is_cancelled() {
            return Ok(());
        }
        result?;
        let latest = latest_segment(&dir, &generation, raw_hls).await?;
        if cancel.is_cancelled() {
            return Ok(());
        }
        progress.observe(latest, output_bytes(), MAX_SEGMENT)?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sparse(path: &Path, size: u64) {
        std::fs::File::create(path).unwrap().set_len(size).unwrap();
    }

    #[tokio::test]
    async fn unlisted_ts_temp_segment_is_bounded() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir(d.path().join("fmp4")).unwrap();
        sparse(&d.path().join("gcurrent_7.ts.tmp"), MAX_SEGMENT + 1);
        assert!(check_generation_budget(d.path(), "current").await.is_err());
    }

    #[tokio::test]
    async fn unlisted_fmp4_temp_segment_is_bounded() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir(d.path().join("fmp4")).unwrap();
        sparse(&d.path().join("fmp4/gcurrent_7.m4s.tmp"), MAX_SEGMENT + 1);
        assert!(check_generation_budget(d.path(), "current").await.is_err());
    }

    #[tokio::test]
    async fn count_limit_covers_both_variants() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir(d.path().join("fmp4")).unwrap();
        for n in 0..MAX_FILES {
            sparse(&d.path().join(format!("gcurrent_{n}.ts")), 0);
        }
        sparse(&d.path().join("fmp4/gcurrent_0.m4s"), 0);
        assert!(check_generation_budget(d.path(), "current").await.is_err());
    }

    #[tokio::test]
    async fn total_bytes_limit_covers_both_variants() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir(d.path().join("fmp4")).unwrap();
        for n in 0..4 {
            sparse(&d.path().join(format!("gcurrent_{n}.ts")), MAX_SEGMENT);
        }
        sparse(&d.path().join("fmp4/gcurrent_0.m4s.tmp"), 1);
        assert!(check_generation_budget(d.path(), "current").await.is_err());
    }

    #[tokio::test]
    async fn unrelated_generation_and_nonsegment_files_are_ignored() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir(d.path().join("fmp4")).unwrap();
        sparse(&d.path().join("gprevious_0.ts.tmp"), MAX_SEGMENT + 1);
        sparse(&d.path().join("gcurrent_0.log"), MAX_SEGMENT + 1);
        sparse(&d.path().join("fmp4/gprevious_0.m4s.tmp"), MAX_SEGMENT + 1);
        assert!(check_generation_budget(d.path(), "current").await.is_ok());
    }

    #[tokio::test]
    async fn segment_and_total_limits_are_inclusive() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir(d.path().join("fmp4")).unwrap();
        for n in 0..2 {
            sparse(&d.path().join(format!("gcurrent_{n}.ts")), MAX_SEGMENT);
            sparse(
                &d.path().join(format!("fmp4/gcurrent_{n}.m4s")),
                MAX_SEGMENT,
            );
        }
        assert_eq!(MAX_TOTAL, 4 * MAX_SEGMENT);
        assert!(check_generation_budget(d.path(), "current").await.is_ok());
    }

    #[tokio::test]
    async fn cancelled_watcher_exits_without_waiting_for_next_tick() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir(d.path().join("fmp4")).unwrap();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(watch(
            d.path().to_path_buf(),
            "current".into(),
            false,
            || 0,
            cancel.clone(),
        ));
        cancel.cancel();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), task)
                .await
                .unwrap()
                .unwrap()
                .is_ok()
        );
    }

    #[test]
    fn unclosed_mux_bytes_are_bounded_even_without_a_playlist() {
        let mut progress = MuxProgress::default();
        assert!(progress.observe(None, MAX_SEGMENT, MAX_SEGMENT).is_ok());
        assert!(
            progress
                .observe(None, MAX_SEGMENT + 1, MAX_SEGMENT)
                .is_err()
        );
    }

    #[test]
    fn only_a_newer_completed_segment_rearms_mux_budget() {
        let mut progress = MuxProgress::default();
        assert!(progress.observe(None, MAX_SEGMENT - 1, MAX_SEGMENT).is_ok());
        assert!(progress.observe(Some(10), MAX_SEGMENT, MAX_SEGMENT).is_ok());
        assert!(
            progress
                .observe(Some(10), MAX_SEGMENT * 2, MAX_SEGMENT)
                .is_ok()
        );
        assert!(
            progress
                .observe(Some(9), MAX_SEGMENT * 2 + 1, MAX_SEGMENT)
                .is_err()
        );
        assert!(
            progress
                .observe(Some(11), MAX_SEGMENT * 2 + 1, MAX_SEGMENT)
                .is_ok()
        );
    }

    #[tokio::test]
    async fn playlist_progress_requires_a_current_generation_ts_segment() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(
            d.path().join("index.m3u8"),
            "#EXTM3U\n#EXTINF:2,\ngprevious_99.ts\n#EXTINF:2,\ngcurrent_42.ts\n",
        )
        .unwrap();
        assert_eq!(
            latest_segment(d.path(), "current", false).await.unwrap(),
            Some(42)
        );
    }
}
