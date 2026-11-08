//! Sequence high-water and per-attempt names for generated HLS output.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::io::AsyncReadExt;
pub struct Epoch {
    high: AtomicU64,
    root: Option<PathBuf>,
    clock: Option<u64>,
}
impl Epoch {
    pub fn new() -> Self {
        Self {
            high: AtomicU64::new(0),
            root: None,
            clock: None,
        }
    }
    pub fn for_root(root: impl AsRef<Path>) -> Self {
        Self {
            high: AtomicU64::new(0),
            root: Some(root.as_ref().to_path_buf()),
            clock: None,
        }
    }
    #[cfg(test)]
    pub fn with_clock(root: impl AsRef<Path>, seed: u64) -> Self {
        Self {
            high: AtomicU64::new(0),
            root: Some(root.as_ref().to_path_buf()),
            clock: Some(seed),
        }
    }
    pub fn next_for(&self, name: &str, directory: &Path) -> Result<u64, String> {
        crate::config::valid_name(name)?;
        let (state, record) = self.record_path(name)?;
        let saved = read_record(&record, name)?;
        let visible = observe_public(directory)?;
        let start = match (saved, visible) {
            (Some(saved), Some(visible)) => saved.max(visible),
            (Some(saved), None) => saved,
            (None, Some(visible)) => visible,
            (None, None) => self.clock.unwrap_or_else(wall_clock_seed),
        };
        let floor = start
            .checked_add(1)
            .filter(|floor| *floor < i64::MAX as u64)
            .ok_or("HLS sequence exhausted")?;
        write_record(&state, &record, name, floor)?;
        Ok(start)
    }
    pub fn commit_observed(&self, name: &str, directory: &Path) -> Result<(), String> {
        crate::config::valid_name(name)?;
        let (state, record) = self.record_path(name)?;
        let saved = read_record(&record, name)?.ok_or("HLS sequence state missing")?;
        if let Some(visible) = observe_public(directory)? {
            if visible > saved {
                write_record(&state, &record, name, visible)?;
            }
        }
        Ok(())
    }
    fn record_path(&self, name: &str) -> Result<(PathBuf, PathBuf), String> {
        let root = self.root.as_ref().ok_or("HLS sequence root unavailable")?;
        fs::create_dir_all(root).map_err(|_| "cannot create HLS media root")?;
        let state = root.join(".hls-sequences");
        ensure_state_dir(&state)?;
        let hash = format!("{:x}", Sha256::digest(name.as_bytes()));
        let record = state.join(format!("{hash}.json"));
        Ok((state, record))
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

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SequenceRecord {
    version: u8,
    name: String,
    next_sequence: u64,
}

fn wall_clock_seed() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_micros().min(i64::MAX as u128 - 2) as u64)
}

fn ensure_state_dir(path: &Path) -> Result<(), String> {
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    match builder.create(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => return Err("cannot create HLS sequence state".into()),
    }
    let info = fs::symlink_metadata(path).map_err(|_| "cannot inspect HLS sequence state")?;
    if !info.is_dir()
        || info.uid() != unsafe { libc::geteuid() }
        || info.permissions().mode() & 0o777 != 0o700
    {
        return Err("unsafe HLS sequence state directory".into());
    }
    Ok(())
}

fn read_record(path: &Path, name: &str) -> Result<Option<u64>, String> {
    let info = match fs::symlink_metadata(path) {
        Ok(info) => info,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("cannot inspect HLS sequence record".into()),
    };
    if !info.is_file()
        || info.uid() != unsafe { libc::geteuid() }
        || info.permissions().mode() & 0o777 != 0o600
    {
        return Err("unsafe HLS sequence record".into());
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| "cannot read HLS sequence record")?;
    let mut bytes = Vec::new();
    file.take(16 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read HLS sequence record")?;
    if bytes.len() > 16 * 1024 {
        return Err("HLS sequence record exceeds limit".into());
    }
    let saved: SequenceRecord =
        serde_json::from_slice(&bytes).map_err(|_| "invalid HLS sequence record")?;
    if saved.version != 1
        || saved.name != name
        || saved.next_sequence == 0
        || saved.next_sequence >= i64::MAX as u64
    {
        return Err("invalid HLS sequence record".into());
    }
    Ok(Some(saved.next_sequence))
}

fn write_record(state: &Path, record: &Path, name: &str, next_sequence: u64) -> Result<(), String> {
    let data = serde_json::to_vec(&SequenceRecord {
        version: 1,
        name: name.to_owned(),
        next_sequence,
    })
    .map_err(|_| "cannot encode HLS sequence record")?;
    let temporary = state.join(format!(".{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
            .map_err(|_| "cannot create HLS sequence record")?;
        file.write_all(&data)
            .map_err(|_| "cannot write HLS sequence record")?;
        file.sync_all()
            .map_err(|_| "cannot sync HLS sequence record")?;
        fs::rename(&temporary, record).map_err(|_| "cannot replace HLS sequence record")?;
        File::open(state)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| "cannot sync HLS sequence state")?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn observe_public(directory: &Path) -> Result<Option<u64>, String> {
    let mut window_next: Option<u64> = None;
    for name in ["index.m3u8", "fmp4/index.m3u8"] {
        let file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(directory.join(name))
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return Err("cannot inspect HLS sequence".into()),
        };
        let mut data = Vec::new();
        file.take(1024 * 1024 + 1)
            .read_to_end(&mut data)
            .map_err(|_| "cannot inspect HLS sequence")?;
        if data.len() > 1024 * 1024 {
            return Err("HLS playlist exceeds limit".into());
        }
        let text = std::str::from_utf8(&data).map_err(|_| "invalid generated HLS playlist")?;
        if text.lines().next() != Some("#EXTM3U") {
            return Err("invalid generated HLS playlist".into());
        }
        let mut sequence = None;
        for line in text.lines() {
            if let Some(raw) = line.strip_prefix("#EXT-X-MEDIA-SEQUENCE:") {
                let value = raw.parse::<u64>().map_err(|_| "invalid HLS sequence")?;
                if sequence.replace(value).is_some() {
                    return Err("invalid HLS sequence".into());
                }
            }
        }
        let sequence = sequence.ok_or("missing HLS sequence")?;
        let segments = text
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .count() as u64;
        let next = sequence
            .checked_add(segments)
            .filter(|next| *next < i64::MAX as u64)
            .ok_or("HLS sequence exhausted")?;
        window_next = Some(window_next.map_or(next, |old| old.max(next)));
    }
    Ok(window_next)
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    fn saved_record(root: &Path) -> std::path::PathBuf {
        let mut files = std::fs::read_dir(root.join(".hls-sequences"))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(files.len(), 1);
        files.pop().unwrap().path()
    }

    #[test]
    fn first_generation_persists_a_private_stream_floor() {
        let root = tempfile::tempdir().unwrap();
        let media = root.path().join("stream-media");
        std::fs::create_dir(&media).unwrap();
        let epoch = Epoch::with_clock(root.path(), 1000);

        assert_eq!(epoch.next_for("a", &media).unwrap(), 1000);

        let state = root.path().join(".hls-sequences");
        assert_eq!(
            std::fs::metadata(&state).unwrap().permissions().mode() & 0o777,
            0o700
        );
        let files = std::fs::read_dir(&state)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(files.len(), 1);
        let record = files[0].path();
        assert_eq!(record.extension().and_then(|e| e.to_str()), Some("json"));
        assert_eq!(
            std::fs::metadata(&record).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let saved: serde_json::Value =
            serde_json::from_slice(&std::fs::read(record).unwrap()).unwrap();
        assert_eq!(saved["name"], "a");
        assert!(saved["next_sequence"].as_u64().unwrap() > 1000);
    }

    #[test]
    fn fresh_media_root_can_create_its_first_private_floor() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("new-media-root");
        let media = root.join("stream-media");
        let epoch = Epoch::with_clock(&root, 1000);

        assert_eq!(epoch.next_for("a", &media).unwrap(), 1000);
        assert!(saved_record(&root).is_file());
    }

    #[test]
    fn cold_reopen_with_earlier_clock_keeps_each_stream_independent() {
        let root = tempfile::tempdir().unwrap();
        let media = root.path().join("stream-media");
        std::fs::create_dir(&media).unwrap();
        let first = Epoch::with_clock(root.path(), 1000);
        assert_eq!(first.next_for("a", &media).unwrap(), 1000);
        let before = saved_record(root.path());
        let old_inode = std::fs::metadata(&before).unwrap().ino();
        assert_eq!(first.next_for("a", &media).unwrap(), 1001);
        assert_ne!(std::fs::metadata(&before).unwrap().ino(), old_inode);
        assert_eq!(
            std::fs::metadata(&before).unwrap().permissions().mode() & 0o777,
            0o600
        );

        let reopened = Epoch::with_clock(root.path(), 900);
        assert_eq!(reopened.next_for("a", &media).unwrap(), 1002);
        assert_eq!(reopened.next_for("b", &media).unwrap(), 900);
        assert_eq!(reopened.next_for("a", &media).unwrap(), 1003);
    }

    #[test]
    fn corrupt_or_foreign_record_never_falls_back_to_clock() {
        for bad in [
            b"{".to_vec(),
            b"".to_vec(),
            br#"{"version":1,"name":"other","next_sequence":1001}"#.to_vec(),
            br#"{"version":99,"name":"a","next_sequence":1001}"#.to_vec(),
        ] {
            let root = tempfile::tempdir().unwrap();
            let media = root.path().join("stream-media");
            std::fs::create_dir(&media).unwrap();
            let epoch = Epoch::with_clock(root.path(), 1000);
            epoch.next_for("a", &media).unwrap();
            let record = saved_record(root.path());
            std::fs::write(&record, &bad).unwrap();
            assert!(
                Epoch::with_clock(root.path(), 900)
                    .next_for("a", &media)
                    .is_err()
            );
            assert_eq!(std::fs::read(record).unwrap(), bad);
        }
    }

    #[test]
    fn linked_or_permission_damaged_state_fails_closed() {
        let root = tempfile::tempdir().unwrap();
        let media = root.path().join("stream-media");
        std::fs::create_dir(&media).unwrap();
        let epoch = Epoch::with_clock(root.path(), 1000);
        epoch.next_for("a", &media).unwrap();
        let record = saved_record(root.path());
        std::fs::set_permissions(&record, std::fs::Permissions::from_mode(0o200)).unwrap();
        assert!(epoch.next_for("a", &media).is_err(), "unreadable record");
        std::fs::set_permissions(&record, std::fs::Permissions::from_mode(0o400)).unwrap();
        assert!(epoch.next_for("a", &media).is_err(), "unwritable record");
        std::fs::set_permissions(&record, std::fs::Permissions::from_mode(0o600)).unwrap();
        let state = root.path().join(".hls-sequences");
        std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o500)).unwrap();
        assert!(
            epoch.next_for("a", &media).is_err(),
            "unwritable state directory"
        );
        std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o700)).unwrap();
        let original = record.with_extension("old");
        std::fs::rename(&record, &original).unwrap();
        std::os::unix::fs::symlink(&original, &record).unwrap();
        assert!(epoch.next_for("a", &media).is_err(), "linked record");
    }

    #[test]
    fn surviving_public_ts_and_fmp4_windows_advance_the_floor() {
        let root = tempfile::tempdir().unwrap();
        let media = root.path().join("stream-media");
        std::fs::create_dir(&media).unwrap();
        let epoch = Epoch::with_clock(root.path(), 1000);
        epoch.next_for("a", &media).unwrap();
        std::fs::write(
            media.join("index.m3u8"),
            "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:5000\na.ts\nb.ts\n",
        )
        .unwrap();
        std::fs::write(
            media.join("passthrough.m3u8"),
            "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:999999\nprivate.ts\n",
        )
        .unwrap();
        assert_eq!(epoch.next_for("a", &media).unwrap(), 5002);
        std::fs::create_dir(media.join("fmp4")).unwrap();
        std::fs::write(
            media.join("fmp4/index.m3u8"),
            "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:7000\n#EXT-X-MAP:URI=\"init.mp4\"\nchunk.m4s\n",
        )
        .unwrap();
        assert_eq!(
            Epoch::with_clock(root.path(), 800)
                .next_for("a", &media)
                .unwrap(),
            7001
        );
    }

    #[test]
    fn unreadable_public_sequence_fails_instead_of_reusing_the_clock() {
        for playlist in [
            "#EXTM3U\n#EXTINF:1,\na.ts\n",
            "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:100\n#EXT-X-MEDIA-SEQUENCE:101\na.ts\n",
            "#EXT-X-MEDIA-SEQUENCE:100\na.ts\n",
        ] {
            let root = tempfile::tempdir().unwrap();
            let media = root.path().join("stream-media");
            std::fs::create_dir(&media).unwrap();
            std::fs::write(media.join("index.m3u8"), playlist).unwrap();
            assert!(
                Epoch::with_clock(root.path(), 1000)
                    .next_for("a", &media)
                    .is_err()
            );
        }
    }

    #[test]
    fn observed_final_playlist_is_committed_without_lowering_the_floor() {
        let root = tempfile::tempdir().unwrap();
        let media = root.path().join("stream-media");
        std::fs::create_dir(&media).unwrap();
        let seed = Epoch::with_clock(root.path(), 1000);
        seed.next_for("a", &media).unwrap();
        std::fs::write(
            media.join("index.m3u8"),
            "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:5000\na.ts\nb.ts\n",
        )
        .unwrap();
        let persisted = Epoch::for_root(root.path());
        persisted.commit_observed("a", &media).unwrap();
        std::fs::remove_file(media.join("index.m3u8")).unwrap();
        assert_eq!(
            Epoch::with_clock(root.path(), 900)
                .next_for("a", &media)
                .unwrap(),
            5002
        );

        std::fs::write(
            media.join("index.m3u8"),
            "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:1000\nold.ts\n",
        )
        .unwrap();
        persisted.commit_observed("a", &media).unwrap();
        std::fs::remove_file(media.join("index.m3u8")).unwrap();
        assert_eq!(
            Epoch::with_clock(root.path(), 800)
                .next_for("a", &media)
                .unwrap(),
            5003
        );
    }

    #[test]
    fn exhausted_record_or_playlist_never_launches_a_reused_sequence() {
        let root = tempfile::tempdir().unwrap();
        let media = root.path().join("stream-media");
        std::fs::create_dir(&media).unwrap();
        let epoch = Epoch::with_clock(root.path(), 1000);
        epoch.next_for("a", &media).unwrap();
        let record = saved_record(root.path());
        std::fs::write(
            &record,
            format!(
                "{{\"version\":1,\"name\":\"a\",\"next_sequence\":{}}}",
                i64::MAX - 1
            ),
        )
        .unwrap();
        assert!(epoch.next_for("a", &media).is_err());

        let other_root = tempfile::tempdir().unwrap();
        let other_media = other_root.path().join("stream-media");
        std::fs::create_dir(&other_media).unwrap();
        std::fs::write(
            other_media.join("index.m3u8"),
            format!("#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:{}\nlast.ts\n", i64::MAX - 1),
        )
        .unwrap();
        assert!(
            Epoch::with_clock(other_root.path(), 1000)
                .next_for("a", &other_media)
                .is_err()
        );
    }
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
