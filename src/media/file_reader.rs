//! Read FFmpeg-generated HLS files without following a replaced segment or directory.
use sha2::{Digest, Sha256};
use std::{
    ffi::CString,
    io::Read,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::Path,
};
use tokio::sync::OwnedSemaphorePermit;

#[cfg(test)]
struct ReadPause {
    stream: String,
    file: String,
    entered: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
static READ_PAUSE: std::sync::OnceLock<std::sync::Mutex<Option<ReadPause>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
pub(crate) fn pause_once_for_test(
    stream: &str,
    file: &str,
) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let pause = ReadPause {
        stream: format!("{:x}", Sha256::digest(stream.as_bytes())),
        file: file.to_owned(),
        entered: entered_tx,
        release: release_rx,
    };
    let previous = READ_PAUSE
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .replace(pause);
    assert!(previous.is_none(), "overlapping test file-read pauses");
    (entered_rx, release_tx)
}

pub(super) async fn read(
    root: &Path,
    stream: &str,
    file: &str,
    max_bytes: usize,
) -> Result<Vec<u8>, String> {
    let root = root.to_path_buf();
    let stream = format!("{:x}", Sha256::digest(stream.as_bytes()));
    let file = file.to_owned();
    tokio::task::spawn_blocking(move || read_sync(&root, &stream, &file, max_bytes))
        .await
        .map_err(|_| "media not ready".to_owned())?
}

/// Keep selected-HLS admission owned by the blocking read itself. Aborting
/// its async caller must not admit another 32 MiB source while this task runs.
pub(super) async fn read_ts_with_permit(
    root: &Path,
    stream: &str,
    file: &str,
    max_bytes: usize,
    drop_captions: bool,
    permit: OwnedSemaphorePermit,
) -> Result<(OwnedSemaphorePermit, Vec<u8>), String> {
    let root = root.to_path_buf();
    let stream = format!("{:x}", Sha256::digest(stream.as_bytes()));
    let file = file.to_owned();
    let (permit, data) = tokio::task::spawn_blocking(move || {
        let data = read_sync(&root, &stream, &file, max_bytes).and_then(|mut data| {
            if drop_captions {
                crate::caption_filter::ts(&mut data)?;
            }
            Ok(data)
        });
        (permit, data)
    })
    .await
    .map_err(|_| "media not ready".to_owned())?;
    Ok((permit, data?))
}

/// Read all fMP4 caption dependencies under one cancellation-safe work slot.
pub(super) async fn read_mp4_with_permit(
    root: &Path,
    stream: &str,
    file: &str,
    permit: OwnedSemaphorePermit,
) -> Result<(OwnedSemaphorePermit, Vec<u8>), String> {
    let root = root.to_path_buf();
    let stream = format!("{:x}", Sha256::digest(stream.as_bytes()));
    let file = file.to_owned();
    let (permit, data) = tokio::task::spawn_blocking(move || {
        let data = (|| -> Result<Vec<u8>, String> {
            let mut data = read_sync(&root, &stream, &file, 32 * 1024 * 1024)?;
            let prefix = if file.starts_with("fmp4/") {
                "fmp4/"
            } else {
                ""
            };
            let list = read_sync(
                &root,
                &stream,
                &format!("{prefix}index.m3u8"),
                2 * 1024 * 1024,
            )?;
            let list = String::from_utf8(list).map_err(|_| "media not ready")?;
            let init = list
                .lines()
                .find_map(|line| line.strip_prefix("#EXT-X-MAP:URI=\""))
                .and_then(|line| line.split('"').next())
                .filter(|line| super::valid_file(line) && line.ends_with(".mp4"))
                .ok_or("caption initialization unavailable")?;
            let init = read_sync(&root, &stream, &format!("{prefix}{init}"), 2 * 1024 * 1024)?;
            crate::caption_filter::mp4(&init, &mut data)?;
            Ok(data)
        })();
        (permit, data)
    })
    .await
    .map_err(|_| "media not ready".to_owned())?;
    Ok((permit, data?))
}

fn read_sync(root: &Path, stream: &str, file: &str, max_bytes: usize) -> Result<Vec<u8>, String> {
    #[cfg(test)]
    let stream_name = stream;
    let root = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(root)
        .map_err(|_| "media not ready")?;
    safe_directory(&root)?;
    let stream = open_child(&root, stream, true)?;
    safe_directory(&stream)?;
    let (dir, leaf) = if let Some(leaf) = file.strip_prefix("fmp4/") {
        let fmp4 = open_child(&stream, "fmp4", true)?;
        safe_directory(&fmp4)?;
        (fmp4, leaf)
    } else {
        (stream, file)
    };
    if leaf.is_empty() || leaf.contains('/') || leaf == "." || leaf == ".." {
        return Err("media not ready".into());
    }
    let media = open_child(&dir, leaf, false)?;
    let meta = media.metadata().map_err(|_| "media not ready")?;
    if !meta.is_file() || meta.uid() != unsafe { libc::geteuid() } {
        return Err("media not ready".into());
    }
    if meta.len() > max_bytes as u64 {
        return Err("segment exceeds size limit".into());
    }
    #[cfg(test)]
    {
        let pause = {
            let mut pending = READ_PAUSE.get_or_init(Default::default).lock().unwrap();
            if pending
                .as_ref()
                .is_some_and(|pause| pause.stream == stream_name && pause.file == file)
            {
                pending.take()
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            let _ = pause.entered.send(());
            let _ = pause
                .release
                .recv_timeout(std::time::Duration::from_secs(5));
        }
    }
    let mut data = Vec::with_capacity(meta.len() as usize);
    media
        .take(max_bytes as u64 + 1)
        .read_to_end(&mut data)
        .map_err(|_| "media not ready")?;
    if data.len() > max_bytes {
        return Err("segment exceeds size limit".into());
    }
    Ok(data)
}

fn safe_directory(dir: &std::fs::File) -> Result<(), String> {
    let meta = dir.metadata().map_err(|_| "media not ready")?;
    if !meta.is_dir() || meta.uid() != unsafe { libc::geteuid() } || meta.mode() & 0o022 != 0 {
        return Err("media not ready".into());
    }
    Ok(())
}

fn open_child(dir: &std::fs::File, name: &str, directory: bool) -> Result<std::fs::File, String> {
    let name = CString::new(name).map_err(|_| "media not ready")?;
    let flags = libc::O_RDONLY
        | libc::O_CLOEXEC
        | libc::O_NOFOLLOW
        | libc::O_NONBLOCK
        | if directory { libc::O_DIRECTORY } else { 0 };
    let fd = unsafe { libc::openat(dir.as_raw_fd(), name.as_ptr(), flags) };
    if fd < 0 {
        return Err("media not ready".into());
    }
    Ok(unsafe { std::fs::File::from_raw_fd(fd) })
}
