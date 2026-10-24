#![cfg(target_os = "linux")]

use serde_json::{Value, json};
use std::{
    fs,
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::fs::PermissionsExt,
    },
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

fn pidfd(pid: u32) -> OwnedFd {
    // SAFETY: Open a descriptor for the recorded, test-owned process.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    assert!(fd >= 0, "pidfd_open failed");
    // SAFETY: The syscall returned a new owned descriptor.
    unsafe { OwnedFd::from_raw_fd(fd as i32) }
}

fn exited(fd: &OwnedFd) -> bool {
    let mut poll = libc::pollfd {
        fd: fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: One valid pollfd, checked without blocking.
    assert!(unsafe { libc::poll(&mut poll, 1, 0) } >= 0);
    poll.revents & libc::POLLIN != 0
}

fn kill(fd: &OwnedFd, signal: i32) {
    // SAFETY: The pidfd identifies only a process owned by this fixture.
    let result = unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            fd.as_raw_fd(),
            signal,
            0usize,
            0,
        )
    };
    assert!(result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH));
}

struct Daemon {
    child: Child,
    main_fd: Option<OwnedFd>,
    worker: Option<(u32, OwnedFd)>,
    dir: tempfile::TempDir,
    base: String,
}

impl Daemon {
    async fn start(client: &reqwest::Client) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("worker.sh");
        // The stand-in deliberately resists graceful orphan termination. It
        // execs sleep directly, so it has no unowned wrapper descendants.
        fs::write(&script, "#!/bin/sh\ntrap '' TERM\nprintf '%s\\n' \"$$\" > \"$FLUSSONIX_TEST_WORKER_PID\"\nexec /bin/sleep 60\n").unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).unwrap();
        let log = fs::File::create(dir.path().join("daemon.log")).unwrap();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_flussonix"));
        for (key, _) in std::env::vars().filter(|(key, _)| key.starts_with("FLUSSONIX_")) {
            cmd.env_remove(key);
        }
        let child = cmd
            .args(["--listen", "127.0.0.1:0", "--ffmpeg"])
            .arg(&script)
            .arg("--config")
            .arg(dir.path().join("config.json"))
            .arg("--media-dir")
            .arg(dir.path().join("media"))
            .env("FLUSSONIX_ADMIN_PASSWORD", "owned-child-admin")
            .env("FLUSSONIX_PEER_KEY", "owned-child-peer-key")
            .env("FLUSSONIX_TEST_WORKER_PID", dir.path().join("worker.pid"))
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        let mut daemon = Self {
            child,
            main_fd: None,
            worker: None,
            dir,
            base: String::new(),
        };
        daemon.main_fd = Some(pidfd(daemon.child.id()));
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            assert!(
                daemon.child.try_wait().unwrap().is_none(),
                "fixture daemon exited"
            );
            if let Some(node) = fs::read_to_string(daemon.dir.path().join("daemon.log"))
                .unwrap()
                .lines()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .find(|value| value["listen"].is_string())
            {
                daemon.base = format!("http://{}", node["listen"].as_str().unwrap());
                break;
            }
            assert!(Instant::now() < deadline, "fixture startup timed out");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(client.put(format!("{}/streamer/api/v3/streams/owned", daemon.base))
            .basic_auth("admin", Some("owned-child-admin"))
            .json(&json!({"static":true,"inputs":[{"url":"testsrc://"}],"flussonix_input_timeout":300}))
            .send().await.unwrap().status().is_success());
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(text) = fs::read_to_string(daemon.dir.path().join("worker.pid")) {
                if let Ok(pid) = text.trim().parse::<u32>() {
                    let fd = pidfd(pid);
                    daemon.worker = Some((pid, fd));
                    break;
                }
            }
            assert!(Instant::now() < deadline, "fixture worker did not start");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        daemon
    }
}

impl Daemon {
    fn capture_worker(&mut self) {
        if self.worker.is_none() {
            if let Ok(text) = fs::read_to_string(self.dir.path().join("worker.pid")) {
                if let Ok(pid) = text.trim().parse::<u32>() {
                    let parent = fs::read_to_string(format!("/proc/{pid}/status"))
                        .ok()
                        .and_then(|status| {
                            status
                                .lines()
                                .find(|line| line.starts_with("PPid:"))
                                .and_then(|line| line.split_whitespace().nth(1))
                                .and_then(|value| value.parse::<u32>().ok())
                        });
                    if parent == Some(self.child.id()) || parent == Some(std::process::id()) {
                        // SAFETY: The private PID record and parent check fence this fixture child.
                        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
                        if fd >= 0 {
                            // SAFETY: A fresh successfully opened descriptor.
                            self.worker = Some((pid, unsafe { OwnedFd::from_raw_fd(fd as i32) }));
                        }
                    }
                }
            }
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        self.capture_worker();
        let _ = self.child.kill();
        let _ = self.child.wait();
        // A constructor error may race with the child's PID publication. Once
        // the daemon has stopped, also look for its newly adopted orphan.
        let lookup_deadline = Instant::now() + Duration::from_secs(1);
        while self.worker.is_none() && Instant::now() < lookup_deadline {
            self.capture_worker();
            if self.worker.is_none() {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        if let Some((pid, fd)) = &self.worker {
            // SAFETY: Only the captured test worker descriptor is signaled.
            unsafe {
                libc::syscall(
                    libc::SYS_pidfd_send_signal,
                    fd.as_raw_fd(),
                    libc::SIGKILL,
                    0usize,
                    0,
                );
            }
            let deadline = Instant::now() + Duration::from_secs(2);
            loop {
                // SAFETY: Reap only our adopted orphan, never any other child.
                let result =
                    unsafe { libc::waitpid(*pid as i32, std::ptr::null_mut(), libc::WNOHANG) };
                if result != 0 || Instant::now() >= deadline {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn daemon_crash_reaps_its_worker_and_preserves_a_sibling_daemon() {
    // SAFETY: This isolated integration-test process adopts only its orphaned
    // descendants, so the fixture can reap them even on container hosts.
    assert_eq!(
        unsafe {
            libc::prctl(
                libc::PR_SET_CHILD_SUBREAPER,
                1 as libc::c_ulong,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
            )
        },
        0
    );
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let mut first = Daemon::start(&client).await;
    let mut sibling = Daemon::start(&client).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!exited(&first.worker.as_ref().unwrap().1));
    assert!(!exited(&sibling.worker.as_ref().unwrap().1));
    kill(first.main_fd.as_ref().unwrap(), libc::SIGKILL);
    assert!(!first.child.wait().unwrap().success());
    let deadline = Instant::now() + Duration::from_secs(3);
    while !exited(&first.worker.as_ref().unwrap().1) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        exited(&first.worker.as_ref().unwrap().1),
        "orphan worker survived daemon crash"
    );
    assert!(!exited(sibling.main_fd.as_ref().unwrap()));
    assert!(!exited(&sibling.worker.as_ref().unwrap().1));
    assert!(
        client
            .get(format!("{}/health", sibling.base))
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    kill(sibling.main_fd.as_ref().unwrap(), libc::SIGTERM);
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = sibling.child.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "normal daemon shutdown timed out"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(status.success());
    assert!(exited(&sibling.worker.as_ref().unwrap().1));
}
