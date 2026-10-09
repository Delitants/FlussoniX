//! Owned real-daemon fixture; no in-process App or manual reconciliation.
use super::*;
use std::{path::PathBuf, process::Stdio};
use tokio::{io::AsyncBufReadExt, process::Child};

pub struct Daemon {
    pub directory: tempfile::TempDir,
    pub cert: tls_fixture::Certificates,
    pub url: String,
    pub key: String,
    child: Option<Child>,
    config: Vec<u8>,
}
impl Daemon {
    pub fn new(name: &str) -> Self {
        Self {
            directory: tempfile::tempdir().unwrap(),
            cert: tls_fixture::Certificates::new(),
            url: String::new(),
            key: format!("owned-{name}-peer"),
            child: None,
            config: vec![],
        }
    }
    pub fn store(&self) -> flussonix::config::ConfigStore {
        flussonix::config::ConfigStore::open(self.directory.path().join("config.json")).unwrap()
    }
    pub async fn start(&mut self, name: &str, role: &str) {
        self.config = std::fs::read(self.directory.path().join("config.json")).unwrap();
        self.child = Some(
            tokio::process::Command::new(env!("CARGO_BIN_EXE_flussonix"))
                .args(["--https-only", "--https-listen", "127.0.0.1:0"])
                .arg("--https-cert")
                .arg(&self.cert.cert)
                .arg("--https-key")
                .arg(&self.cert.key)
                .arg("--config")
                .arg(self.directory.path().join("config.json"))
                .arg("--media-dir")
                .arg(self.directory.path().join("media"))
                .args([
                    "--ffmpeg",
                    "/usr/bin/ffmpeg",
                    "--role",
                    role,
                    "--node-name",
                    name,
                    "--uplink-interface",
                    "process",
                ])
                .env("FLUSSONIX_ADMIN_USER", "owned-cluster")
                .env("FLUSSONIX_ADMIN_PASSWORD", "owned-management")
                .env("FLUSSONIX_PEER_KEY", &self.key)
                .env("RUST_LOG", "error")
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(std::fs::File::create(self.directory.path().join("daemon.log")).unwrap())
                .kill_on_drop(true)
                .spawn()
                .unwrap(),
        );
        let stdout = self.child.as_mut().unwrap().stdout.take().unwrap();
        let mut lines = tokio::io::BufReader::new(stdout).lines();
        let line = tokio::time::timeout(Duration::from_secs(8), lines.next_line())
            .await
            .expect("owned daemon startup")
            .unwrap()
            .unwrap();
        let info: Value = serde_json::from_str(&line).unwrap();
        assert!(info["listen"].is_null(), "fixture must bind HTTPS only");
        self.url = format!("https://{}", info["https_listen"].as_str().unwrap());
    }
    pub async fn node(&self, client: &reqwest::Client) -> Value {
        client
            .get(format!("{}/flussonix/api/v1/node", self.url))
            .header("X-Flussonix-Peer", &self.key)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap()
    }
    pub fn owned_encoders(&self) -> Vec<u32> {
        let marker = self.directory.path().join("media");
        let marker = marker.as_os_str().as_encoded_bytes();
        let executable = std::fs::canonicalize("/usr/bin/ffmpeg").unwrap();
        std::fs::read_dir("/proc")
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let pid = entry.file_name().to_str()?.parse().ok()?;
                let path = entry.path();
                (std::fs::read_link(path.join("exe")).ok().as_ref() == Some(&executable)
                    && std::fs::read(path.join("cmdline")).is_ok_and(|args| {
                        args.split(|b| *b == 0)
                            .any(|arg| arg.windows(marker.len()).any(|part| part == marker))
                    }))
                .then_some(pid)
            })
            .collect()
    }
    pub async fn stop(&mut self) -> bool {
        let clean = if let Some(child) = &mut self.child {
            if let Some(pid) = child.id() {
                // SAFETY: this unreaped child belongs to this fixture.
                unsafe {
                    libc::kill(pid as i32, libc::SIGTERM);
                }
            }
            match tokio::time::timeout(Duration::from_secs(8), child.wait()).await {
                Ok(Ok(status)) => status.success(),
                _ => {
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    false
                }
            }
        } else {
            true
        };
        let leaked = self.owned_encoders();
        for pid in &leaked {
            // Owned executable plus unique output-directory marker, checked above.
            unsafe {
                libc::kill(*pid as i32, libc::SIGKILL);
            }
        }
        clean && leaked.is_empty()
    }
    pub fn unchanged(&self) -> bool {
        self.config == std::fs::read(self.directory.path().join("config.json")).unwrap()
    }
    pub fn playlist(&self) -> Option<String> {
        fn find(path: PathBuf) -> Option<String> {
            for entry in std::fs::read_dir(path).ok()?.filter_map(Result::ok) {
                if entry.path().is_dir() && entry.file_name() != "fmp4" {
                    if let Some(list) = find(entry.path()) {
                        return Some(list);
                    }
                } else if entry.file_name() == "index.m3u8" {
                    return std::fs::read_to_string(entry.path()).ok();
                }
            }
            None
        }
        find(self.directory.path().join("media"))
    }
}
impl Drop for Daemon {
    fn drop(&mut self) {
        if let Some(pid) = self.child.as_ref().and_then(Child::id) {
            unsafe {
                libc::kill(pid as i32, libc::SIGTERM);
            }
        }
    }
}
