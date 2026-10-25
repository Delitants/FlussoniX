//! Raw regional subtitle carriage in TS-HLS. FFmpeg's HLS mux routes separate
//! subtitles to WebVTT, so use its MPEG-TS segment mux and publish the live list.
use std::path::{Path, PathBuf};
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;

fn normalize<'a>(
    raw: &'a str,
    prefix: &str,
    epoch: u64,
    discontinuity: bool,
) -> Option<(String, u64, Vec<&'a str>)> {
    if !raw.starts_with("#EXTM3U\n") || !raw.ends_with('\n') {
        return None;
    }
    let start = raw
        .lines()
        .find_map(|l| l.strip_prefix("#EXT-X-MEDIA-SEQUENCE:"))?
        .parse::<u64>()
        .ok()?;
    let files: Vec<_> = raw
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .collect();
    if files.is_empty()
        || files.len() > 6
        || raw.lines().filter(|l| l.starts_with("#EXTINF:")).count() != files.len()
    {
        return None;
    }
    for (i, file) in files.iter().enumerate() {
        if *file != format!("{prefix}{}.ts", start.checked_add(i as u64)?) {
            return None;
        }
    }
    let sequence = epoch.checked_add(start)?;
    sequence
        .checked_add(files.len() as u64)
        .filter(|s| *s < i64::MAX as u64)?;
    let mut text = String::new();
    let mut first = true;
    for line in raw.lines() {
        if line.starts_with("#EXT-X-MEDIA-SEQUENCE:") {
            text.push_str(&format!("#EXT-X-MEDIA-SEQUENCE:{sequence}\n"));
            if discontinuity && start > 0 {
                text.push_str("#EXT-X-DISCONTINUITY-SEQUENCE:1\n");
            }
        } else {
            if first && line.starts_with("#EXTINF:") {
                if discontinuity && start == 0 {
                    text.push_str("#EXT-X-DISCONTINUITY\n");
                }
                first = false;
            }
            text.push_str(line);
            text.push('\n');
        }
    }
    Some((text, start, files))
}

async fn prune_before(
    dir: &Path,
    prefix: &str,
    start: u64,
    cancel: &CancellationToken,
) -> Result<(), String> {
    let mut entries = tokio::fs::read_dir(dir)
        .await
        .map_err(|_| "cannot retain raw HLS window")?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|_| "cannot retain raw HLS window")?
    {
        if cancel.is_cancelled() {
            return Ok(());
        }
        let name = entry.file_name();
        let Some(index) = name
            .to_str()
            .and_then(|s| s.strip_prefix(prefix))
            .and_then(|s| s.strip_suffix(".ts"))
            .and_then(|s| s.parse::<u64>().ok())
        else {
            continue;
        };
        if index < start.saturating_sub(2) {
            let _ = tokio::fs::remove_file(entry.path()).await;
        }
    }
    Ok(())
}

async fn check_generation_budget(dir: &Path, prefix: &str) -> Result<(), String> {
    const MAX_SEGMENT: u64 = 64 * 1024 * 1024;
    const MAX_TOTAL: u64 = 256 * 1024 * 1024;
    const MAX_FILES: usize = 64;
    let mut entries = tokio::fs::read_dir(dir)
        .await
        .map_err(|_| "cannot inspect raw HLS segment budget")?;
    let mut files = 0usize;
    let mut bytes = 0u64;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|_| "cannot inspect raw HLS segment budget")?
    {
        let name = entry.file_name();
        if !name
            .to_str()
            .and_then(|s| s.strip_prefix(prefix))
            .and_then(|s| s.strip_suffix(".ts"))
            .is_some_and(|s| s.parse::<u64>().is_ok())
        {
            continue;
        }
        let metadata = match tokio::fs::metadata(entry.path()).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return Err("cannot inspect raw HLS segment budget".into()),
        };
        files += 1;
        bytes = bytes.saturating_add(metadata.len());
        if !metadata.is_file()
            || metadata.len() > MAX_SEGMENT
            || files > MAX_FILES
            || bytes > MAX_TOTAL
        {
            return Err("raw HLS segment budget exceeded".into());
        }
    }
    Ok(())
}

pub async fn watch(
    dir: PathBuf,
    generation: String,
    epoch: u64,
    discontinuity: bool,
    cancel: CancellationToken,
) -> Result<(), String> {
    watch_inner(
        dir,
        generation,
        epoch,
        discontinuity,
        cancel,
        #[cfg(test)]
        std::time::Duration::ZERO,
    )
    .await
}

async fn watch_inner(
    dir: PathBuf,
    generation: String,
    epoch: u64,
    discontinuity: bool,
    cancel: CancellationToken,
    #[cfg(test)] list_work_delay: std::time::Duration,
) -> Result<(), String> {
    let prefix = format!("g{generation}_p");
    let mut last = Vec::new();
    let mut published_start = None::<u64>;
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(100));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut next_budget = tokio::time::Instant::now();
    loop {
        tokio::select! {
            biased;
            _=cancel.cancelled()=>return Ok(()),
            _=tick.tick()=>{},
        }
        #[cfg(test)]
        tokio::time::sleep(list_work_delay).await;
        process_playlist(
            &dir,
            &prefix,
            epoch,
            discontinuity,
            &cancel,
            &mut last,
            &mut published_start,
        )
        .await?;
        // Always inspect a due budget after the list pass, including a missing,
        // unchanged, malformed, or incomplete list. A slow pass cannot starve it.
        if tokio::time::Instant::now() >= next_budget {
            check_generation_budget(&dir, &prefix).await?;
            next_budget = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        }
    }
}

async fn process_playlist(
    dir: &Path,
    prefix: &str,
    epoch: u64,
    discontinuity: bool,
    cancel: &CancellationToken,
    last: &mut Vec<u8>,
    published_start: &mut Option<u64>,
) -> Result<(), String> {
    let Ok(file) = tokio::fs::File::open(dir.join("passthrough.m3u8")).await else {
        return Ok(());
    };
    let mut raw = Vec::new();
    if file.take(65537).read_to_end(&mut raw).await.is_err() || raw.len() > 65536 || raw == *last {
        return Ok(());
    }
    let Ok(value) = std::str::from_utf8(&raw) else {
        return Ok(());
    };
    let Some((text, start, files)) = normalize(value, prefix, epoch, discontinuity) else {
        return Ok(());
    };
    // An invalid new list must not remove files still named by the last
    // public list. The independent budget check bounds that backlog.
    prune_before(
        dir,
        prefix,
        published_start.map_or(start, |old| old.min(start)),
        cancel,
    )
    .await?;
    for name in files {
        if !tokio::fs::metadata(dir.join(name))
            .await
            .is_ok_and(|m| m.is_file() && m.len() > 0 && m.len() <= 32 * 1024 * 1024)
        {
            return Ok(());
        }
    }
    // Only finalized, generation-scoped files enter an atomic public list.
    tokio::fs::write(dir.join(".passthrough-index.tmp"), text)
        .await
        .map_err(|_| "cannot publish raw HLS list")?;
    tokio::fs::rename(dir.join(".passthrough-index.tmp"), dir.join("index.m3u8"))
        .await
        .map_err(|_| "cannot publish raw HLS list")?;
    *last = raw;
    *published_start = Some(start);
    prune_before(dir, prefix, start, cancel).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sliding_replacement_keeps_discontinuity_history_without_repeating_the_marker() {
        let raw = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-MEDIA-SEQUENCE:2\n#EXT-X-TARGETDURATION:2\n#EXTINF:2,\ngowned_p2.ts\n";
        let (text, _, _) = normalize(raw, "gowned_p", 9000000000, true).unwrap();
        assert!(text.contains("#EXT-X-MEDIA-SEQUENCE:9000000002\n"));
        assert!(text.contains("#EXT-X-DISCONTINUITY-SEQUENCE:1\n"));
        assert!(!text.contains("#EXT-X-DISCONTINUITY\n"));
    }

    #[tokio::test]
    async fn oversized_listed_segment_does_not_block_old_file_pruning() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        tokio::fs::write(path.join("gowned_p1.ts"), b"old")
            .await
            .unwrap();
        tokio::fs::write(path.join("gowned_p4.ts"), b"retained")
            .await
            .unwrap();
        tokio::fs::write(path.join("gother_p1.ts"), b"other")
            .await
            .unwrap();
        tokio::fs::write(path.join("gowned_p6.ts"), b"current")
            .await
            .unwrap();
        let oversized = path.join("gowned_p7.ts");
        let file = tokio::fs::File::create(&oversized).await.unwrap();
        file.set_len(32 * 1024 * 1024 + 1).await.unwrap();
        tokio::fs::write(
            path.join("passthrough.m3u8"),
            "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:6\n#EXTINF:2,\ngowned_p6.ts\n#EXTINF:2,\ngowned_p7.ts\n",
        )
        .await
        .unwrap();

        let cancel = CancellationToken::new();
        let watcher = tokio::spawn(watch(
            path.to_path_buf(),
            "owned".into(),
            0,
            false,
            cancel.clone(),
        ));
        let pruned = tokio::time::timeout(std::time::Duration::from_millis(600), async {
            while path.join("gowned_p1.ts").exists() {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .is_ok();
        cancel.cancel();
        watcher.await.unwrap().unwrap();
        assert!(pruned, "old same-generation file survived invalid list");
        assert!(path.join("gowned_p4.ts").exists());
        assert!(path.join("gowned_p6.ts").exists());
        assert!(oversized.exists());
        assert!(path.join("gother_p1.ts").exists());
        assert!(!path.join("index.m3u8").exists());
    }

    #[tokio::test]
    async fn unlisted_growing_segment_fails_the_worker_budget() {
        let dir = tempfile::tempdir().unwrap();
        let file = tokio::fs::File::create(dir.path().join("gowned_p0.ts"))
            .await
            .unwrap();
        file.set_len(64 * 1024 * 1024 + 1).await.unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            watch(
                dir.path().to_path_buf(),
                "owned".into(),
                0,
                false,
                CancellationToken::new(),
            ),
        )
        .await;
        assert!(
            matches!(result, Ok(Err(_))),
            "active segment escaped budget"
        );
    }

    #[tokio::test]
    async fn stalled_playlist_cannot_retain_unbounded_small_segments() {
        let dir = tempfile::tempdir().unwrap();
        for index in 0..65 {
            tokio::fs::write(dir.path().join(format!("gowned_p{index}.ts")), b"x")
                .await
                .unwrap();
        }
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            watch(
                dir.path().to_path_buf(),
                "owned".into(),
                0,
                false,
                CancellationToken::new(),
            ),
        )
        .await;
        assert!(matches!(result, Ok(Err(_))), "file count escaped budget");
    }

    #[tokio::test]
    async fn stalled_playlist_cannot_retain_excess_total_bytes() {
        let dir = tempfile::tempdir().unwrap();
        for index in 0..5 {
            let file = tokio::fs::File::create(dir.path().join(format!("gowned_p{index}.ts")))
                .await
                .unwrap();
            file.set_len(60 * 1024 * 1024).await.unwrap();
        }
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            watch(
                dir.path().to_path_buf(),
                "owned".into(),
                0,
                false,
                CancellationToken::new(),
            ),
        )
        .await;
        assert!(matches!(result, Ok(Err(_))), "total bytes escaped budget");
    }

    #[tokio::test]
    async fn playlist_prunes_obsolete_files_before_budget_check() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        for index in 0..5 {
            let file = tokio::fs::File::create(path.join(format!("gowned_p{index}.ts")))
                .await
                .unwrap();
            file.set_len(60 * 1024 * 1024).await.unwrap();
        }
        tokio::fs::write(path.join("gowned_p100.ts"), b"current")
            .await
            .unwrap();
        tokio::fs::write(
            path.join("passthrough.m3u8"),
            "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:100\n#EXTINF:2,\ngowned_p100.ts\n",
        )
        .await
        .unwrap();
        let cancel = CancellationToken::new();
        let watcher = tokio::spawn(watch(
            path.to_path_buf(),
            "owned".into(),
            0,
            false,
            cancel.clone(),
        ));
        let pruned = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while path.join("gowned_p0.ts").exists() {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .is_ok();
        cancel.cancel();
        let result = watcher.await.unwrap();
        assert!(pruned, "valid live list did not prune obsolete files");
        assert!(result.is_ok(), "budget rejected a prunable backlog");
        assert!(path.join("gowned_p100.ts").exists());
    }

    #[tokio::test]
    async fn invalid_new_list_preserves_files_in_last_published_list() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        for index in 0..2 {
            tokio::fs::write(path.join(format!("gowned_p{index}.ts")), b"published")
                .await
                .unwrap();
        }
        tokio::fs::write(
            path.join("passthrough.m3u8"),
            "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:2,\ngowned_p0.ts\n#EXTINF:2,\ngowned_p1.ts\n",
        )
        .await
        .unwrap();
        let cancel = CancellationToken::new();
        let watcher = tokio::spawn(watch(
            path.to_path_buf(),
            "owned".into(),
            0,
            false,
            cancel.clone(),
        ));
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !path.join("index.m3u8").exists() {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        tokio::fs::write(path.join("gowned_p6.ts"), b"new")
            .await
            .unwrap();
        let oversized = tokio::fs::File::create(path.join("gowned_p7.ts"))
            .await
            .unwrap();
        oversized.set_len(32 * 1024 * 1024 + 1).await.unwrap();
        tokio::fs::write(
            path.join("passthrough.m3u8"),
            "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:6\n#EXTINF:2,\ngowned_p6.ts\n#EXTINF:2,\ngowned_p7.ts\n",
        )
        .await
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        cancel.cancel();
        watcher.await.unwrap().unwrap();
        let published = tokio::fs::read_to_string(path.join("index.m3u8"))
            .await
            .unwrap();
        assert!(published.contains("gowned_p0.ts"));
        assert!(path.join("gowned_p0.ts").exists());
        assert!(path.join("gowned_p1.ts").exists());
    }

    #[tokio::test]
    async fn slow_playlist_checks_budget_after_a_segment_grows() {
        let dir = tempfile::tempdir().unwrap();
        let segment = dir.path().join("gowned_p0.ts");
        let file = tokio::fs::File::create(&segment).await.unwrap();
        file.set_len(1).await.unwrap();
        let watcher = tokio::spawn(watch_inner(
            dir.path().to_path_buf(),
            "owned".into(),
            0,
            false,
            CancellationToken::new(),
            std::time::Duration::from_millis(150),
        ));
        tokio::time::sleep(std::time::Duration::from_millis(350)).await;
        file.set_len(64 * 1024 * 1024 + 1).await.unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), watcher).await;
        assert!(
            matches!(result, Ok(Ok(Err(_)))),
            "slow list work starved the due budget check"
        );
    }

    #[tokio::test]
    async fn unchanged_published_playlist_still_checks_growing_segment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        let segment = path.join("gowned_p0.ts");
        let file = tokio::fs::File::create(&segment).await.unwrap();
        file.set_len(1).await.unwrap();
        tokio::fs::write(
            path.join("passthrough.m3u8"),
            "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:2,\ngowned_p0.ts\n",
        )
        .await
        .unwrap();
        let watcher = tokio::spawn(watch(
            path.to_path_buf(),
            "owned".into(),
            0,
            false,
            CancellationToken::new(),
        ));
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !path.join("index.m3u8").exists() {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        file.set_len(64 * 1024 * 1024 + 1).await.unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), watcher).await;
        assert!(
            matches!(result, Ok(Ok(Err(_)))),
            "unchanged list bypassed budget"
        );
    }

    #[tokio::test]
    async fn generation_budget_ignores_other_formats_and_generations() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["gowned_p0.m4s", "gother_p0.ts"] {
            let file = tokio::fs::File::create(dir.path().join(name))
                .await
                .unwrap();
            file.set_len(65 * 1024 * 1024).await.unwrap();
        }
        assert!(
            check_generation_budget(dir.path(), "gowned_p")
                .await
                .is_ok()
        );
    }
}
