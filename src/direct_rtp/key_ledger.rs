//! Durable, owner-only history of direct SRTP receiver key fingerprints.
use std::{
    collections::HashSet,
    ffi::CString,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::{
            ffi::OsStrExt,
            fs::{MetadataExt, OpenOptionsExt},
        },
    },
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

const HEADER: &[u8; 8] = b"FXSRTP1\n";
const MAX_ENTRIES: usize = 1_000_000;
const RECORD_BYTES: usize = 65;
const UNAVAILABLE: &str = "SRTP receiver ledger unavailable or unsafe";
const INVALID: &str = "SRTP receiver ledger is invalid";
const REUSED: &str = "SRTP receiver key already used; rotate key and restart sender";
type History = (File, Vec<[u8; 32]>, HashSet<[u8; 32]>);

#[cfg(test)]
thread_local! {
    static FAULT_POINT: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
}

fn check_fault(point: u8) -> Result<(), String> {
    #[cfg(test)]
    if FAULT_POINT.with(|fault| fault.get() == point) {
        return Err(UNAVAILABLE.into());
    }
    let _ = point;
    Ok(())
}

pub struct Ledger {
    path: PathBuf,
}

fn names(path: &Path) -> Result<(File, CString, CString), String> {
    if !path.is_absolute() {
        return Err(UNAVAILABLE.into());
    }
    let parent = path.parent().ok_or(UNAVAILABLE)?;
    let basename = path.file_name().ok_or(UNAVAILABLE)?.as_bytes();
    if basename.is_empty() || basename == b"." || basename == b".." {
        return Err(UNAVAILABLE.into());
    }
    let name = CString::new(basename).map_err(|_| UNAVAILABLE)?;
    let mut lock_name = basename.to_vec();
    lock_name.extend_from_slice(b".lock");
    let lock_name = CString::new(lock_name).map_err(|_| UNAVAILABLE)?;
    let dir = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(parent)
        .map_err(|_| UNAVAILABLE)?;
    let meta = dir.metadata().map_err(|_| UNAVAILABLE)?;
    if !meta.is_dir() || meta.uid() != unsafe { libc::geteuid() } || meta.mode() & 0o077 != 0 {
        return Err(UNAVAILABLE.into());
    }
    Ok((dir, name, lock_name))
}

fn open_at(dir: &File, name: &CString, flags: i32, mode: u32) -> Result<File, String> {
    let fd = unsafe {
        libc::openat(
            dir.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
            mode,
        )
    };
    if fd < 0 {
        return Err(UNAVAILABLE.into());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn private_file(file: &File) -> Result<(), String> {
    let meta = file.metadata().map_err(|_| UNAVAILABLE)?;
    if !meta.is_file()
        || meta.uid() != unsafe { libc::geteuid() }
        || meta.mode() & 0o777 != 0o600
        || meta.nlink() != 1
    {
        return Err(UNAVAILABLE.into());
    }
    Ok(())
}

fn same_name(dir: &File, name: &CString, held: &File) -> Result<(), String> {
    let current = open_at(dir, name, libc::O_RDONLY, 0)?;
    private_file(&current)?;
    let current = current.metadata().map_err(|_| UNAVAILABLE)?;
    let held = held.metadata().map_err(|_| UNAVAILABLE)?;
    if current.dev() != held.dev() || current.ino() != held.ino() {
        return Err(UNAVAILABLE.into());
    }
    Ok(())
}

fn lock(dir: &File, name: &CString) -> Result<File, String> {
    let file = open_at(dir, name, libc::O_RDWR, 0)?;
    private_file(&file)?;
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
            same_name(dir, name, &file)?;
            return Ok(file);
        }
        let error = std::io::Error::last_os_error();
        if !matches!(error.raw_os_error(), Some(code) if code == libc::EWOULDBLOCK || code == libc::EAGAIN)
        {
            return Err(UNAVAILABLE.into());
        }
        if Instant::now() >= deadline {
            return Err("SRTP receiver ledger lock timed out".into());
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn decode(hex: &[u8]) -> Result<[u8; 32], String> {
    if hex.len() != 64 {
        return Err(INVALID.into());
    }
    let mut result = [0; 32];
    for (index, pair) in hex.chunks_exact(2).enumerate() {
        let digit = |byte: u8| match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            _ => None,
        };
        result[index] = digit(pair[0]).ok_or(INVALID)? << 4 | digit(pair[1]).ok_or(INVALID)?;
    }
    Ok(result)
}

fn encode(fingerprint: &[u8; 32]) -> [u8; RECORD_BYTES] {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = [b'\n'; RECORD_BYTES];
    for (index, byte) in fingerprint.iter().enumerate() {
        out[index * 2] = HEX[usize::from(byte >> 4)];
        out[index * 2 + 1] = HEX[usize::from(byte & 15)];
    }
    out
}

fn read_history(dir: &File, name: &CString) -> Result<History, String> {
    let file = open_at(dir, name, libc::O_RDONLY, 0)?;
    private_file(&file)?;
    let length = file.metadata().map_err(|_| UNAVAILABLE)?.len();
    let max = HEADER.len() as u64 + (MAX_ENTRIES * RECORD_BYTES) as u64;
    if length < HEADER.len() as u64
        || length > max
        || (length - HEADER.len() as u64) % RECORD_BYTES as u64 != 0
    {
        return Err(INVALID.into());
    }
    let count = ((length - HEADER.len() as u64) / RECORD_BYTES as u64) as usize;
    let mut reader = BufReader::new(&file);
    let mut header = [0; 8];
    reader.read_exact(&mut header).map_err(|_| INVALID)?;
    if &header != HEADER {
        return Err(INVALID.into());
    }
    let mut rows = Vec::with_capacity(count);
    let mut seen = HashSet::with_capacity(count);
    for _ in 0..count {
        let mut row = [0; RECORD_BYTES];
        reader.read_exact(&mut row).map_err(|_| INVALID)?;
        if row[64] != b'\n' {
            return Err(INVALID.into());
        }
        let fingerprint = decode(&row[..64])?;
        if !seen.insert(fingerprint) {
            return Err(INVALID.into());
        }
        rows.push(fingerprint);
    }
    let mut extra = [0; 1];
    if reader.read(&mut extra).map_err(|_| INVALID)? != 0 {
        return Err(INVALID.into());
    }
    same_name(dir, name, &file)?;
    Ok((file, rows, seen))
}

fn replace(dir: &File, name: &CString, held: &File, rows: &[[u8; 32]]) -> Result<(), String> {
    let temp_name = CString::new(format!(".srtp-ledger-{}.tmp", uuid::Uuid::new_v4()))
        .map_err(|_| UNAVAILABLE)?;
    let fd = dir.as_raw_fd();
    let result = (|| {
        let temp = open_at(
            dir,
            &temp_name,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            0o600,
        )?;
        private_file(&temp)?;
        let mut writer = BufWriter::new(temp);
        writer.write_all(HEADER).map_err(|_| UNAVAILABLE)?;
        for row in rows {
            writer.write_all(&encode(row)).map_err(|_| UNAVAILABLE)?;
        }
        let temp = writer.into_inner().map_err(|_| UNAVAILABLE)?;
        check_fault(1)?;
        temp.sync_all().map_err(|_| UNAVAILABLE)?;
        same_name(dir, name, held)?;
        check_fault(2)?;
        if unsafe { libc::renameat(fd, temp_name.as_ptr(), fd, name.as_ptr()) } != 0 {
            return Err(UNAVAILABLE.into());
        }
        check_fault(3)?;
        dir.sync_all().map_err(|_| UNAVAILABLE)?;
        Ok(())
    })();
    if result.is_err() {
        unsafe { libc::unlinkat(fd, temp_name.as_ptr(), 0) };
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        os::unix::fs::{PermissionsExt, symlink},
    };

    fn private_history() -> (tempfile::TempDir, Ledger) {
        let directory = tempfile::tempdir().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let path = directory.path().join("history");
        Ledger::initialize(&path).unwrap();
        (directory, Ledger::new(path))
    }

    #[test]
    fn failed_precommit_steps_leave_the_entire_batch_unclaimed() {
        for point in [1, 2] {
            let (directory, ledger) = private_history();
            ledger.claim([1; 32]).unwrap();
            FAULT_POINT.with(|fault| fault.set(point));
            assert!(ledger.claim_many(&[[2; 32], [3; 32]]).is_err());
            FAULT_POINT.with(|fault| fault.set(0));
            assert!(fs::read_dir(directory.path()).unwrap().all(|entry| {
                !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".srtp-ledger-")
            }));
            ledger.claim_many(&[[2; 32], [3; 32]]).unwrap();
        }
    }

    #[test]
    fn failed_postrename_directory_sync_never_exposes_a_partial_batch() {
        let (_directory, ledger) = private_history();
        FAULT_POINT.with(|fault| fault.set(3));
        assert!(ledger.claim_many(&[[4; 32], [5; 32]]).is_err());
        FAULT_POINT.with(|fault| fault.set(0));
        ledger.probe().unwrap();
        assert!(ledger.claim([4; 32]).is_err());
        assert!(ledger.claim([5; 32]).is_err());
    }

    #[test]
    fn swapped_ledger_name_cannot_be_replaced_through_a_stale_descriptor() {
        let (directory, ledger) = private_history();
        let (dir, name, lock_name) = names(&ledger.path).unwrap();
        let _lock = lock(&dir, &lock_name).unwrap();
        let (held, mut rows, _seen) = read_history(&dir, &name).unwrap();
        let original = directory.path().join("original");
        fs::rename(&ledger.path, &original).unwrap();
        symlink(&original, &ledger.path).unwrap();
        rows.push([6; 32]);
        assert!(replace(&dir, &name, &held, &rows).is_err());
        assert_eq!(fs::read(&original).unwrap(), HEADER);
    }
}

impl Ledger {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Explicit one-shot initialization. An existing history is never reset.
    pub fn initialize(path: &Path) -> Result<(), String> {
        let (dir, name, lock_name) = names(path)?;
        let ledger = open_at(
            &dir,
            &name,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            0o600,
        )?;
        let result = (|| {
            private_file(&ledger)?;
            let mut lock = open_at(
                &dir,
                &lock_name,
                libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
                0o600,
            )?;
            let initialized = (|| {
                private_file(&lock)?;
                (&ledger).write_all(HEADER).map_err(|_| UNAVAILABLE)?;
                ledger.sync_all().map_err(|_| UNAVAILABLE)?;
                lock.flush().map_err(|_| UNAVAILABLE)?;
                lock.sync_all().map_err(|_| UNAVAILABLE)?;
                dir.sync_all().map_err(|_| UNAVAILABLE)?;
                Ok(())
            })();
            if initialized.is_err() {
                unsafe { libc::unlinkat(dir.as_raw_fd(), lock_name.as_ptr(), 0) };
            }
            initialized
        })();
        if result.is_err() {
            unsafe { libc::unlinkat(dir.as_raw_fd(), name.as_ptr(), 0) };
            let _ = dir.sync_all();
        }
        result
    }

    pub fn probe(&self) -> Result<(), String> {
        let (dir, name, lock_name) = names(&self.path)?;
        let _lock = lock(&dir, &lock_name)?;
        read_history(&dir, &name).map(|_| ())
    }

    pub fn claim(&self, fingerprint: [u8; 32]) -> Result<(), String> {
        self.claim_many(&[fingerprint])
    }

    /// Claim an entire 1..8-key receiver generation as one durable update.
    pub fn claim_many(&self, fingerprints: &[[u8; 32]]) -> Result<(), String> {
        if fingerprints.is_empty() || fingerprints.len() > 8 {
            return Err("SRTP receiver key claim requires 1..8 fingerprints".into());
        }
        if fingerprints.iter().collect::<HashSet<_>>().len() != fingerprints.len() {
            return Err("SRTP receiver key claim contains duplicates".into());
        }
        let (dir, name, lock_name) = names(&self.path)?;
        let _lock = lock(&dir, &lock_name)?;
        let (held, mut rows, seen) = read_history(&dir, &name)?;
        if fingerprints
            .iter()
            .any(|fingerprint| seen.contains(fingerprint))
        {
            return Err(REUSED.into());
        }
        if rows.len() + fingerprints.len() > MAX_ENTRIES {
            return Err("SRTP receiver ledger capacity exhausted".into());
        }
        rows.extend_from_slice(fingerprints);
        replace(&dir, &name, &held, &rows)
    }
}
