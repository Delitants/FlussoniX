//! A disposable native PostgreSQL process owned by an integration test.
#![allow(dead_code)]
use std::{
    fs,
    io::Write,
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
    fenced: bool,
}

/// Evidence that this fixture stopped its own primary and reaped the process.
/// This is test-local evidence, not a substitute for deployment fencing.
pub struct FenceEvidence {
    primary_port: u16,
}

/// The primary observed this standby replay its current flushed WAL while
/// remote-apply synchronous commit was active.
pub struct ReplayEvidence {
    primary_port: u16,
    standby_port: u16,
    flushed_lsn: String,
}

fn binaries() -> Option<(PathBuf, PathBuf)> {
    if let Ok(bin) = std::env::var("FLUSSONIX_TEST_PG_BINDIR") {
        let bin = PathBuf::from(bin);
        let share = PathBuf::from(
            std::env::var("FLUSSONIX_TEST_PG_SHAREDIR")
                .expect("FLUSSONIX_TEST_PG_SHAREDIR is required with the binary override"),
        );
        assert!(
            bin.join("initdb").is_file()
                && bin.join("postgres").is_file()
                && bin.join("psql").is_file()
        );
        assert!(share.join("postgres.bki").is_file());
        return Some((bin, share));
    }
    for version in ["17", "16"] {
        let bin = PathBuf::from(format!("/usr/lib/postgresql/{version}/bin"));
        let share = PathBuf::from(format!("/usr/share/postgresql/{version}"));
        if bin.join("initdb").is_file()
            && bin.join("postgres").is_file()
            && bin.join("psql").is_file()
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

fn accepts_authenticated_queries(bin: &Path, port: u16, home: &Path) -> bool {
    owned_command(&bin.join("psql"))
        .args(["-X", "-A", "-t", "-q", "-h", "127.0.0.1", "-p"])
        .arg(port.to_string())
        .args(["-U", "ha_test", "-d", "postgres", "-c", "SELECT 1"])
        .env("HOME", home)
        .env("PGPASSWORD", "owned-test-secret")
        .env("PGSSLMODE", "require")
        .env("PGCONNECT_TIMEOUT", "1")
        .output()
        .is_ok_and(|output| output.status.success() && output.stdout == b"1\n")
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
            .env("HOME", dir.path())
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
            .process_group(0)
            .stdout(Stdio::null())
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        let mut server = Self {
            child,
            dir,
            port,
            fenced: false,
        };
        let deadline = Instant::now() + Duration::from_secs(8);
        while !accepts_authenticated_queries(&bin, port, server.dir.path()) {
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

    pub fn backup(&self, certs: &super::tls_fixture::Certificates) -> PathBuf {
        let (bin, _) = binaries().unwrap();
        let path = self.dir.path().join("owner-only-backup.dump");
        fs::File::create(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        chown_for_database(&path);
        let output = owned_command(&bin.join("pg_dump"))
            .args(["-h", "127.0.0.1", "-p"])
            .arg(self.port.to_string())
            .args(["-U", "ha_test", "-d", "postgres", "-Fc", "-f"])
            .arg(&path)
            .env("HOME", self.dir.path())
            .env("PGPASSWORD", "owned-test-secret")
            .env("PGSSLMODE", "verify-full")
            .env("PGSSLROOTCERT", &certs.ca)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "owned pg_dump failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        path
    }

    pub fn restore(&self, certs: &super::tls_fixture::Certificates, backup: &Path) {
        let (bin, _) = binaries().unwrap();
        let output = owned_command(&bin.join("pg_restore"))
            .args(["-h", "127.0.0.1", "-p"])
            .arg(self.port.to_string())
            .args([
                "-U",
                "ha_test",
                "-d",
                "postgres",
                "--clean",
                "--if-exists",
                "--no-owner",
                "--no-privileges",
            ])
            .arg(backup)
            .env("HOME", self.dir.path())
            .env("PGPASSWORD", "owned-test-secret")
            .env("PGSSLMODE", "verify-full")
            .env("PGSSLROOTCERT", &certs.ca)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "owned pg_restore failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    pub fn fence(&mut self) -> Option<FenceEvidence> {
        if self.fenced {
            return None;
        }
        // SAFETY: this postmaster started in a distinct, test-owned group.
        if unsafe { libc::kill(-(self.child.id() as i32), libc::SIGKILL) } != 0 {
            return None;
        }
        self.fenced = true;
        self.child.wait().ok()?;
        let address = SocketAddr::from(([127, 0, 0, 1], self.port));
        let deadline = Instant::now() + Duration::from_secs(3);
        while TcpStream::connect_timeout(&address, Duration::from_millis(50)).is_ok() {
            if Instant::now() >= deadline {
                return None;
            }
            thread::sleep(Duration::from_millis(25));
        }
        Some(FenceEvidence {
            primary_port: self.port,
        })
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

pub struct PgReplica {
    child: Child,
    dir: tempfile::TempDir,
    port: u16,
    primary_port: u16,
}

impl PgReplica {
    pub fn start(primary: &PgServer, certs: &super::tls_fixture::Certificates) -> Self {
        let (bin, _) = binaries().expect("native PostgreSQL is required for replication test");
        let dir = tempfile::Builder::new()
            .prefix("flussonix-owned-iptv-standby-")
            .tempdir_in("/tmp")
            .unwrap();
        chown_for_database(dir.path());
        let data = dir.path().join("data");
        let backup = owned_command(&bin.join("pg_basebackup"))
            .args([
                "-h",
                "127.0.0.1",
                "-p",
                &primary.port.to_string(),
                "-U",
                "ha_test",
                "-D",
            ])
            .arg(&data)
            .args(["-X", "stream", "-R", "--no-password"])
            .env("HOME", dir.path())
            .env("PGPASSWORD", "owned-test-secret")
            .env("PGSSLMODE", "verify-full")
            .env("PGSSLROOTCERT", &certs.ca)
            .output()
            .unwrap();
        assert!(
            backup.status.success(),
            "pg_basebackup failed: {}",
            String::from_utf8_lossy(&backup.stderr)
        );
        let recovery = format!(
            "primary_conninfo = 'host=127.0.0.1 port={} user=ha_test password=owned-test-secret application_name=ha_standby sslmode=verify-full sslrootcert={}'\n",
            primary.port,
            certs.ca.display()
        );
        fs::OpenOptions::new()
            .append(true)
            .open(data.join("postgresql.auto.conf"))
            .unwrap()
            .write_all(recovery.as_bytes())
            .unwrap();
        let cert_file = dir.path().join("server.pem");
        let key_file = dir.path().join("server.key");
        fs::copy(&certs.cert, &cert_file).unwrap();
        fs::copy(&certs.key, &key_file).unwrap();
        fs::set_permissions(&key_file, fs::Permissions::from_mode(0o600)).unwrap();
        chown_for_database(&cert_file);
        chown_for_database(&key_file);
        let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = reservation.local_addr().unwrap().port();
        drop(reservation);
        let log = fs::File::create(dir.path().join("postgres.log")).unwrap();
        let child = owned_command(&bin.join("postgres"))
            .arg("-D")
            .arg(&data)
            .env("HOME", dir.path())
            .arg("-c")
            .arg("listen_addresses=127.0.0.1")
            .arg("-c")
            .arg(format!("port={port}"))
            .arg("-c")
            .arg("hot_standby=on")
            .arg("-c")
            .arg("ssl=on")
            .arg("-c")
            .arg(format!("ssl_cert_file={}", cert_file.display()))
            .arg("-c")
            .arg(format!("ssl_key_file={}", key_file.display()))
            .arg("-c")
            .arg(format!("unix_socket_directories={}", dir.path().display()))
            .process_group(0)
            .stdout(Stdio::null())
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        let mut replica = Self {
            child,
            dir,
            port,
            primary_port: primary.port,
        };
        let deadline = Instant::now() + Duration::from_secs(8);
        while !accepts_authenticated_queries(&bin, port, replica.dir.path()) {
            if replica.child.try_wait().unwrap().is_some() || Instant::now() >= deadline {
                let log =
                    fs::read_to_string(replica.dir.path().join("postgres.log")).unwrap_or_default();
                panic!("owned standby did not start: {log}");
            }
            thread::sleep(Duration::from_millis(50));
        }
        replica
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn diagnostic_log(&self) -> String {
        let text = fs::read_to_string(self.dir.path().join("postgres.log"))
            .unwrap_or_default()
            .replace("owned-test-secret", "[redacted]");
        text.chars()
            .rev()
            .take(4000)
            .collect::<String>()
            .chars()
            .rev()
            .collect()
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

    pub async fn confirmed_replay(&self, primary: &PgServer) -> Option<ReplayEvidence> {
        if primary.port != self.primary_port {
            return None;
        }
        let client = primary.local_client().await;
        let row = client
            .query_opt(
                "SELECT pg_current_wal_flush_lsn()::text
             FROM pg_stat_replication
             WHERE application_name = 'ha_standby'
               AND sync_state = 'sync'
               AND replay_lsn >= pg_current_wal_flush_lsn()
               AND current_setting('synchronous_commit') = 'remote_apply'",
                &[],
            )
            .await
            .ok()??;
        Some(ReplayEvidence {
            primary_port: primary.port,
            standby_port: self.port,
            flushed_lsn: row.get(0),
        })
    }

    pub async fn promote(&self, fence: &FenceEvidence, replay: &ReplayEvidence) -> bool {
        if fence.primary_port != self.primary_port
            || replay.primary_port != self.primary_port
            || replay.standby_port != self.port
        {
            return false;
        }
        let client = self.local_client().await;
        let caught_up = client
            .query_one(
                "SELECT pg_is_in_recovery() AND pg_last_wal_replay_lsn() >= $1::text::pg_lsn",
                &[&replay.flushed_lsn],
            )
            .await
            .ok()
            .is_some_and(|row| row.get::<_, bool>(0));
        if !caught_up {
            return false;
        }
        client
            .query_one("SELECT pg_promote(TRUE, 10)", &[])
            .await
            .ok()
            .is_some_and(|row| row.get::<_, bool>(0))
    }
}

impl Drop for PgReplica {
    fn drop(&mut self) {
        // SAFETY: this standby started in a distinct, test-owned group.
        unsafe { libc::kill(-(self.child.id() as i32), libc::SIGKILL) };
        let _ = self.child.wait();
    }
}

impl Drop for PgServer {
    fn drop(&mut self) {
        if !self.fenced {
            // SAFETY: this primary started in a distinct, test-owned group.
            unsafe { libc::kill(-(self.child.id() as i32), libc::SIGKILL) };
            let _ = self.child.wait();
        }
    }
}
