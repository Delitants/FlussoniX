//! A disposable native PostgreSQL process owned by an integration test.
#![allow(dead_code)]
use std::{
    fs,
    net::{SocketAddr, TcpListener, TcpStream},
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

pub struct PgServer {
    child: Child,
    dir: tempfile::TempDir,
    port: u16,
}

fn binaries() -> Option<(PathBuf, PathBuf)> {
    if let Ok(bin) = std::env::var("FLUSSONIX_TEST_PG_BINDIR") {
        let bin = PathBuf::from(bin);
        let share = PathBuf::from(
            std::env::var("FLUSSONIX_TEST_PG_SHAREDIR")
                .expect("FLUSSONIX_TEST_PG_SHAREDIR is required with the binary override"),
        );
        assert!(bin.join("initdb").is_file() && bin.join("postgres").is_file());
        assert!(share.join("postgres.bki").is_file());
        return Some((bin, share));
    }
    for version in ["17", "16"] {
        let bin = PathBuf::from(format!("/usr/lib/postgresql/{version}/bin"));
        let share = PathBuf::from(format!("/usr/share/postgresql/{version}"));
        if bin.join("initdb").is_file()
            && bin.join("postgres").is_file()
            && share.join("postgres.bki").is_file()
        {
            return Some((bin, share));
        }
    }
    None
}

fn owned_command(binary: &Path) -> Command {
    let mut command = Command::new(binary);
    // PostgreSQL refuses to run as root. Only this child drops to the
    // unprivileged nobody account; the test process retains ownership of it.
    if unsafe { libc::geteuid() } == 0 {
        let uid = std::env::var("FLUSSONIX_TEST_PG_UID")
            .ok()
            .and_then(|raw| raw.parse::<u32>().ok())
            .unwrap_or(65534);
        command.uid(uid).gid(uid);
    }
    command.env("LC_ALL", "C");
    command
}

fn chown_for_database(path: &Path) {
    if unsafe { libc::geteuid() } != 0 {
        return;
    }
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let uid = std::env::var("FLUSSONIX_TEST_PG_UID")
        .ok()
        .and_then(|raw| raw.parse::<u32>().ok())
        .unwrap_or(65534);
    let name = CString::new(path.as_os_str().as_bytes()).unwrap();
    // SAFETY: CString remains alive during this direct OS call.
    assert_eq!(unsafe { libc::chown(name.as_ptr(), uid, uid) }, 0);
}

impl PgServer {
    pub fn start(certs: &super::tls_fixture::Certificates) -> Option<Self> {
        let (bin, share) = binaries()?;
        let dir = tempfile::Builder::new()
            .prefix("flussonix-owned-iptv-pg-")
            .tempdir_in("/tmp")
            .unwrap();
        chown_for_database(dir.path());
        let password_file = dir.path().join("password");
        fs::write(&password_file, "owned-test-secret\n").unwrap();
        fs::set_permissions(&password_file, fs::Permissions::from_mode(0o600)).unwrap();
        chown_for_database(&password_file);
        let cert_file = dir.path().join("server.pem");
        let key_file = dir.path().join("server.key");
        fs::copy(&certs.cert, &cert_file).unwrap();
        fs::copy(&certs.key, &key_file).unwrap();
        fs::set_permissions(&key_file, fs::Permissions::from_mode(0o600)).unwrap();
        chown_for_database(&cert_file);
        chown_for_database(&key_file);

        let data = dir.path().join("data");
        let init = owned_command(&bin.join("initdb"))
            .arg("-D")
            .arg(&data)
            .arg("-L")
            .arg(&share)
            .args([
                "--username=ha_test",
                "--auth-local=trust",
                "--auth-host=scram-sha-256",
                "--no-instructions",
            ])
            .arg("--pwfile")
            .arg(&password_file)
            .output()
            .unwrap();
        assert!(
            init.status.success(),
            "initdb failed: {}",
            String::from_utf8_lossy(&init.stderr)
        );

        let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = reservation.local_addr().unwrap().port();
        drop(reservation);
        let log = fs::File::create(dir.path().join("postgres.log")).unwrap();
        let child = owned_command(&bin.join("postgres"))
            .arg("-D")
            .arg(&data)
            .arg("-c")
            .arg("listen_addresses=127.0.0.1")
            .arg("-c")
            .arg(format!("port={port}"))
            .arg("-c")
            .arg("ssl=on")
            .arg("-c")
            .arg(format!("ssl_cert_file={}", cert_file.display()))
            .arg("-c")
            .arg(format!("ssl_key_file={}", key_file.display()))
            .arg("-c")
            .arg(format!("unix_socket_directories={}", dir.path().display()))
            .stdout(Stdio::null())
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        let mut server = Self { child, dir, port };
        let address = SocketAddr::from(([127, 0, 0, 1], port));
        let deadline = Instant::now() + Duration::from_secs(8);
        while TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_err() {
            if server.child.try_wait().unwrap().is_some() || Instant::now() >= deadline {
                let log =
                    fs::read_to_string(server.dir.path().join("postgres.log")).unwrap_or_default();
                panic!("owned PostgreSQL did not start: {log}");
            }
            thread::sleep(Duration::from_millis(50));
        }
        Some(server)
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub async fn local_client(&self) -> tokio_postgres::Client {
        let mut config = tokio_postgres::Config::new();
        config.host(self.dir.path().to_str().unwrap());
        config.port(self.port);
        config.user("ha_test");
        config.dbname("postgres");
        let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }
}

impl Drop for PgServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
