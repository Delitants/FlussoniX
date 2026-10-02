//! Sequence high-water and per-attempt names for generated HLS output.
use std::{
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::io::AsyncReadExt;
pub struct Epoch {
    high: AtomicU64,
}
impl Epoch {
    pub fn new() -> Self {
        Self {
            high: AtomicU64::new(0),
        }
    }
    pub async fn observe(&self, directory: &Path) -> Result<Option<u64>, String> {
        let mut window_next: Option<u64> = None;
        for name in ["index.m3u8", "fmp4/index.m3u8"] {
            let file = match tokio::fs::File::open(directory.join(name)).await {
                Ok(file) => file,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(_) => return Err("cannot inspect HLS sequence".into()),
            };
            let mut data = Vec::new();
            file.take(1024 * 1024 + 1)
                .read_to_end(&mut data)
                .await
                .map_err(|_| "cannot inspect HLS sequence")?;
            if data.len() > 1024 * 1024 {
                return Err("HLS playlist exceeds limit".into());
            }
            let text = std::str::from_utf8(&data).map_err(|_| "invalid generated HLS playlist")?;
            if let Some(sequence) = text
                .lines()
                .find_map(|l| l.strip_prefix("#EXT-X-MEDIA-SEQUENCE:"))
            {
                let sequence = sequence
                    .parse::<u64>()
                    .map_err(|_| "invalid HLS sequence")?;
                let segments = text
                    .lines()
                    .filter(|l| !l.is_empty() && !l.starts_with('#'))
                    .count() as u64;
                let next = sequence
                    .checked_add(segments)
                    .filter(|next| *next < i64::MAX as u64)
                    .ok_or("HLS sequence exhausted")?;
                self.high.fetch_max(next, Ordering::Relaxed);
                window_next = Some(window_next.map_or(next, |old| old.max(next)));
            }
        }
        Ok(window_next)
    }
    pub async fn next(&self, directory: &Path) -> Result<u64, String> {
        // Sequence holes are interpreted as missing media by players. Continue
        // this stream's own windows; unrelated streams cannot move its timeline.
        if let Some(next) = self.observe(directory).await? {
            return Ok(next);
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_micros().min(i64::MAX as u128 - 1) as u64);
        self.allocate_with_clock(now)
    }
    fn allocate_with_clock(&self, now: u64) -> Result<u64, String> {
        let mut old = self.high.load(Ordering::Relaxed);
        loop {
            let next = old
                .checked_add(1)
                .map(|next| if old == 0 { next.max(now) } else { next })
                .filter(|next| *next < i64::MAX as u64)
                .ok_or("HLS sequence exhausted")?;
            match self
                .high
                .compare_exchange_weak(old, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return Ok(next),
                Err(current) => old = current,
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn forward_clock_changes_do_not_create_sequence_holes() {
        let epoch = Epoch::new();
        assert_eq!(epoch.allocate_with_clock(1000).unwrap(), 1000);
        assert_eq!(epoch.allocate_with_clock(10_000_000).unwrap(), 1001);
    }
    #[tokio::test]
    async fn another_stream_cannot_create_a_replacement_sequence_hole() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let epoch = Epoch::new();
        tokio::fs::write(
            a.path().join("index.m3u8"),
            "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:1000\na0.ts\na1.ts\n",
        )
        .await
        .unwrap();
        tokio::fs::write(
            b.path().join("index.m3u8"),
            "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:1000000\nb0.ts\nb1.ts\n",
        )
        .await
        .unwrap();
        epoch.observe(b.path()).await.unwrap();
        assert_eq!(epoch.next(a.path()).await.unwrap(), 1002);
    }
    #[tokio::test]
    async fn rollback_and_old_playlist_windows_cannot_reuse_sequences() {
        let d = tempfile::tempdir().unwrap();
        let epoch = Epoch::new();
        assert_eq!(epoch.allocate_with_clock(1000).unwrap(), 1000);
        assert_eq!(epoch.allocate_with_clock(900).unwrap(), 1001);
        tokio::fs::write(
            d.path().join("index.m3u8"),
            "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:5000\nold0.ts\nold1.ts\n",
        )
        .await
        .unwrap();
        epoch.observe(d.path()).await.unwrap();
        assert_eq!(epoch.allocate_with_clock(800).unwrap(), 5003);
        epoch.high.store(i64::MAX as u64, Ordering::Relaxed);
        assert!(epoch.allocate_with_clock(1).is_err());
    }
}
