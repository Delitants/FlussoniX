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

fn read_sync(root: &Path, stream: &str, file: &str, max_bytes: usize) -> Result<Vec<u8>, String> {
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
