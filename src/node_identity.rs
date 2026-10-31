//! Per-installation management identity; independent of media and vendor files.
use std::{
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
use uuid::Uuid;

fn identity_path(config: &Path) -> Result<PathBuf, String> {
    let mut name = OsString::from(
        config
            .file_name()
            .ok_or("config path must include a file name")?,
    );
    name.push(".node-id");
    Ok(config.with_file_name(name))
}

fn existing(path: &Path) -> Result<Uuid, String> {
    let meta = fs::symlink_metadata(path).map_err(|e| e.to_string())?;
    if !meta.file_type().is_file() || !(36..=64).contains(&meta.len()) {
        return Err("node identity file must be a regular UUID file".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if meta.permissions().mode() & 0o077 != 0 {
            return Err("node identity file must be owner-only".into());
        }
    }
    let bytes = fs::read(path).map_err(|e| e.to_string())?;
    let text = std::str::from_utf8(&bytes).map_err(|_| "invalid node identity UUID")?;
    Uuid::parse_str(text.trim_end_matches('\n')).map_err(|_| "invalid node identity UUID".into())
}

pub(crate) fn load_or_create(config: &Path) -> Result<Uuid, String> {
    let path = identity_path(config)?;
    match fs::symlink_metadata(&path) {
        Ok(_) => return existing(&path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.to_string()),
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    let id = Uuid::new_v4();
    let mut name = path.as_os_str().to_owned();
    name.push(format!(".{}.tmp", Uuid::new_v4()));
    let temp = PathBuf::from(name);
    let result = (|| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp).map_err(|e| e.to_string())?;
        writeln!(file, "{id}").map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
        match fs::hard_link(&temp, &path) {
            Ok(()) => {
                File::open(parent)
                    .and_then(|directory| directory.sync_all())
                    .map_err(|e| e.to_string())?;
                Ok(id)
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => existing(&path),
            Err(e) => Err(e.to_string()),
        }
    })();
    let _ = fs::remove_file(temp);
    result
}
