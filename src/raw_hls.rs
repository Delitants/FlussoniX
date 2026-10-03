//! Raw regional subtitle carriage in TS-HLS. FFmpeg's HLS mux routes separate
//! subtitles to WebVTT, so use its MPEG-TS segment mux and publish the live list.
use std::path::PathBuf;
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

pub async fn watch(
    dir: PathBuf,
    generation: String,
    epoch: u64,
    discontinuity: bool,
    cancel: CancellationToken,
) -> Result<(), String> {
    let prefix = format!("g{generation}_p");
    let mut last = Vec::new();
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(100));
    loop {
        tokio::select! { biased; _=cancel.cancelled()=>return Ok(()), _=tick.tick()=>{} }
        let Ok(file) = tokio::fs::File::open(dir.join("passthrough.m3u8")).await else {
            continue;
        };
        let mut raw = Vec::new();
        if file.take(65537).read_to_end(&mut raw).await.is_err() || raw.len() > 65536 || raw == last
        {
            continue;
        }
        let Ok(value) = std::str::from_utf8(&raw) else {
            continue;
        };
        let Some((text, start, files)) = normalize(value, &prefix, epoch, discontinuity) else {
            continue;
        };
        let mut complete = true;
        for name in files {
            if !tokio::fs::metadata(dir.join(name))
                .await
                .is_ok_and(|m| m.is_file() && m.len() > 0 && m.len() <= 32 * 1024 * 1024)
            {
                complete = false;
                break;
            }
        }
        if !complete {
            continue;
        }
        // Only finalized, generation-scoped files enter an atomic public list.
        tokio::fs::write(dir.join(".passthrough-index.tmp"), text)
            .await
            .map_err(|_| "cannot publish raw HLS list")?;
        tokio::fs::rename(dir.join(".passthrough-index.tmp"), dir.join("index.m3u8"))
            .await
            .map_err(|_| "cannot publish raw HLS list")?;
        last = raw;
        let mut entries = tokio::fs::read_dir(&dir)
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
                .and_then(|s| s.strip_prefix(&prefix))
                .and_then(|s| s.strip_suffix(".ts"))
                .and_then(|s| s.parse::<u64>().ok())
            else {
                continue;
            };
            if index < start.saturating_sub(2) {
                let _ = tokio::fs::remove_file(entry.path()).await;
            }
        }
    }
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
}
