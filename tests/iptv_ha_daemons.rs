//! Two test-owned operating-system authority processes sharing one HA ledger.
#![cfg(target_os = "linux")]

use flussonix::iptv::ha::{
    catalog::{CatalogEdit, HaCatalog},
    connection::{HaOptions, HaPool},
    schema,
};
use serde_json::{Value, json};
use std::{
    fs,
    net::{IpAddr, Ipv4Addr},
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::Path,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
};
#[path = "support/iptv_postgres.rs"]
mod iptv_postgres;
#[path = "support/tls.rs"]
mod tls_fixture;

const CLUSTER: &str = "a3192376-6df5-4b76-a15c-43942fd3d466";
const PEER: &str = "owned-daemon-peer-key-123";
const VIEWERS: usize = 16;

struct OwnedAuthority {
    child: Child,
    base: String,
}

impl OwnedAuthority {
    async fn start(root: &Path, name: &str, db_config: &Path) -> Self {
        Self::start_with_tls(root, name, db_config, None).await
    }

    async fn start_tls(
        root: &Path,
        name: &str,
        db_config: &Path,
        certs: &tls_fixture::Certificates,
    ) -> Self {
        Self::start_with_tls(root, name, db_config, Some(certs)).await
    }

    async fn start_with_tls(
        root: &Path,
        name: &str,
        db_config: &Path,
        certs: Option<&tls_fixture::Certificates>,
    ) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_flussonix"));
        let log = root.join(format!("{name}-stderr.log"));
        let log_file = fs::File::create(&log).unwrap();
        fs::set_permissions(&log, fs::Permissions::from_mode(0o600)).unwrap();
        for (key, _) in std::env::vars().filter(|(key, _)| key.starts_with("FLUSSONIX_")) {
            command.env_remove(key);
        }
        command
            .args([
                "--listen",
                "127.0.0.1:0",
                "--role",
                "lb",
                "--node-name",
                name,
                "--iptv-lease-authority",
                "--iptv-peer-allow-insecure-loopback",
                "--iptv-public-origin",
                "http://127.0.0.1:18210/",
                // The CLI requires a viewer-admission origin on authority nodes.
                // This fixture does not serve media viewers, so keep it unused.
                "--iptv-authority",
                "http://127.0.0.1:9/",
                "--iptv-authority-allow-insecure-loopback",
            ])
            .arg("--iptv-ha-db-config")
            .arg(db_config)
            .arg("--config")
            .arg(root.join(format!("{name}-config.json")))
            .arg("--media-dir")
            .arg(root.join(format!("{name}-media")))
            .env("FLUSSONIX_ADMIN_PASSWORD", "owned-admin")
            .env("FLUSSONIX_PEER_KEY", PEER)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(log_file))
            .kill_on_drop(true);
        if let Some(certs) = certs {
            command
                .arg("--https-listen")
                .arg("127.0.0.1:0")
                .arg("--https-cert")
                .arg(&certs.cert)
                .arg("--https-key")
                .arg(&certs.key);
        }
        command.as_std_mut().process_group(0);
        let mut child = command.spawn().unwrap();
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let line = tokio::time::timeout(Duration::from_secs(8), lines.next_line())
            .await
            .expect("owned HA authority startup timed out")
            .unwrap()
            .unwrap_or_else(|| {
                let detail = fs::read_to_string(&log)
                    .unwrap_or_default()
                    .replace("owned-test-secret", "[redacted]");
                panic!("owned HA authority exited before readiness: {detail}")
            });
        let ready: Value = serde_json::from_str(&line).unwrap();
        let base = match certs {
            Some(_) => format!("https://{}", ready["https_listen"].as_str().unwrap()),
            None => format!("http://{}", ready["listen"].as_str().unwrap()),
        };
        Self { child, base }
    }

    fn pid(&self) -> u32 {
        self.child.id().expect("owned authority already exited")
    }

    async fn stop(&mut self) {
        let pgid = self.child.id().expect("owned authority already exited") as i32;
        // SAFETY: this process group was created for this test child only.
        assert_eq!(unsafe { libc::kill(-pgid, libc::SIGKILL) }, 0);
        tokio::time::timeout(Duration::from_secs(5), self.child.wait())
            .await
            .expect("owned authority did not stop")
            .unwrap();
    }
}

impl Drop for OwnedAuthority {
    fn drop(&mut self) {
        if let Some(pid) = self.child.id() {
            // SAFETY: the process group is owned by this disposable fixture.
            unsafe { libc::kill(-(pid as i32), libc::SIGKILL) };
        }
    }
}

async fn lease(
    client: &reqwest::Client,
    daemon: &OwnedAuthority,
    action: &str,
    body: Value,
) -> (reqwest::StatusCode, Value) {
    let (status, body, _) = lease_timed(client, &daemon.base, action, body).await;
    (status, body)
}

async fn lease_timed(
    client: &reqwest::Client,
    base: &str,
    action: &str,
    body: Value,
) -> (reqwest::StatusCode, Value, Duration) {
    let started = Instant::now();
    let response = client
        .post(format!("{}/flussonix/peer/v1/iptv/leases/{action}", base))
        .header("x-flussonix-peer", PEER)
        .json(&body)
        .send()
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(response.headers()["x-flussonix-iptv-cluster-id"], CLUSTER);
    let status = response.status();
    let body = response.json().await.unwrap_or(Value::Null);
    (status, body, elapsed)
}

#[tokio::test]
async fn two_real_authorities_share_grants_and_survive_one_process_loss() {
    let certs = tls_fixture::Certificates::new();
    let database = iptv_postgres::PgServer::start(&certs)
        .expect("native PostgreSQL is required for real-daemon HA test");
    let config = certs.dir.path().join("daemon-db.json");
    fs::write(
        &config,
        json!({
            "cluster_id":CLUSTER,"host":"127.0.0.1","port":database.port(),
            "database":"postgres","user":"ha_test","password":"owned-test-secret",
            "ca_pem":certs.ca,
        })
        .to_string(),
    )
    .unwrap();
    fs::set_permissions(&config, fs::Permissions::from_mode(0o600)).unwrap();
    let pool = Arc::new(HaPool::new(&HaOptions::load(&config).unwrap()).unwrap());
    schema::initialize(&pool, None).await.unwrap();
    let catalog = HaCatalog::new(pool, certs.dir.path().join("daemon-export.json"));
    catalog.edit(0, CatalogEdit::Enable(true)).await.unwrap();
    catalog
        .edit(
            1,
            CatalogEdit::PutPackage("basic".into(), json!({"channels":["news"]})),
        )
        .await
        .unwrap();
    catalog
        .edit(
            2,
            CatalogEdit::PutSubscriber(
                "alice".into(),
                json!({"token":"owned-daemon-token","packages":["basic"],"max_sessions":1}),
            ),
        )
        .await
        .unwrap();

    let mut first = OwnedAuthority::start(certs.dir.path(), "first", &config).await;
    let mut second = OwnedAuthority::start(certs.dir.path(), "second", &config).await;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(4))
        .build()
        .unwrap();
    for daemon in [&first, &second] {
        let (status, ready) = lease(&client, daemon, "status", json!({})).await;
        assert_eq!(status, reqwest::StatusCode::OK);
        assert_eq!(ready, json!({"ready":true}));
    }
    let boot = uuid::Uuid::new_v4().to_string();
    let session = uuid::Uuid::new_v4().to_string();
    let operation = uuid::Uuid::new_v4().to_string();
    let body = json!({"token":"owned-daemon-token","channel":"news","boot":boot,"session":session,"operation":operation,"units":1});
    let (status, grant) = lease(&client, &first, "acquire", body.clone()).await;
    assert_eq!(status, reqwest::StatusCode::OK);
    let mut competing = body.clone();
    competing["session"] = json!(uuid::Uuid::new_v4().to_string());
    competing["operation"] = json!(uuid::Uuid::new_v4().to_string());
    assert_eq!(
        lease(&client, &second, "acquire", competing).await.0,
        reqwest::StatusCode::CONFLICT
    );
    let mut granted = body.clone();
    granted["lease_id"] = grant["lease_id"].clone();
    assert_eq!(
        lease(&client, &second, "commit", granted.clone()).await.0,
        reqwest::StatusCode::OK
    );
    first.stop().await;
    assert_eq!(
        lease(&client, &second, "renew", granted).await.0,
        reqwest::StatusCode::OK
    );
    let release = json!({
        "subscriber_id":grant["subscriber_id"],"boot":boot,"session":session,
        "lease_id":grant["lease_id"],"operation":operation,
    });
    assert_eq!(
        lease(&client, &second, "release", release).await.0,
        reqwest::StatusCode::NO_CONTENT
    );
    let mut replacement = body;
    replacement["session"] = json!(uuid::Uuid::new_v4().to_string());
    replacement["operation"] = json!(uuid::Uuid::new_v4().to_string());
    assert_eq!(
        lease(&client, &second, "acquire", replacement).await.0,
        reqwest::StatusCode::OK
    );
    second.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "run explicitly with scripts/qualify-iptv-ha.sh"]
async fn two_real_authority_processes_sustain_competing_channels() {
    let seconds: u64 = std::env::var("FLUSSONIX_IPTV_HA_DAEMON_LOAD_SECONDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(60);
    assert!((5..=900).contains(&seconds));
    let summary = run_sustained_daemon_load(seconds).await;
    assert!(summary.cycles >= 16, "all 16 viewers must complete a cycle");
    assert_eq!(summary.excess_grants, 0);
    assert_eq!(summary.missed_renewals, 0);
    assert_eq!(summary.peak_active, 16);
    assert!(summary.peak_db_connections <= 36);
    assert!(summary.wal_bytes > 0);
    assert!(summary.acquire_p99_ms < 2_000);
    assert!(summary.renew_p99_ms < 2_000);
    assert!(summary.peak_authority_rss_kib.iter().all(|rss| *rss > 0));
    assert!(summary.peak_authority_fds.iter().all(|fds| *fds > 0));
}

struct LoadSummary {
    cycles: usize,
    excess_grants: usize,
    missed_renewals: usize,
    peak_active: i32,
    peak_db_connections: i64,
    wal_bytes: i64,
    acquire_p99_ms: u128,
    renew_p99_ms: u128,
    peak_authority_rss_kib: [u64; 2],
    peak_authority_fds: [usize; 2],
}

struct ProcessUsage {
    cpu_seconds: f64,
    rss_kib: u64,
    fds: usize,
}

fn process_usage(pid: u32) -> ProcessUsage {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    let after_name = stat.rsplit_once(") ").unwrap().1;
    let fields: Vec<&str> = after_name.split_whitespace().collect();
    let ticks: u64 = fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap();
    // SAFETY: sysconf reads the Linux clock-tick constant without touching memory.
    let hertz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    assert!(hertz > 0);
    let status = fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
    let rss_kib = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let fds = fs::read_dir(format!("/proc/{pid}/fd")).unwrap().count();
    ProcessUsage {
        cpu_seconds: ticks as f64 / hertz as f64,
        rss_kib,
        fds,
    }
}

fn percentile_ms(mut samples: Vec<Duration>, percentile: usize) -> u128 {
    samples.sort_unstable();
    samples[(samples.len() - 1) * percentile / 100].as_millis()
}

async fn run_sustained_daemon_load(seconds: u64) -> LoadSummary {
    let certs = tls_fixture::Certificates::new();
    let database = iptv_postgres::PgServer::start(&certs)
        .expect("native PostgreSQL is required for real-daemon HA load gate");
    let config = certs.dir.path().join("load-daemon-db.json");
    fs::write(
        &config,
        json!({
            "cluster_id":CLUSTER,"host":"127.0.0.1","port":database.port(),
            "database":"postgres","user":"ha_test","password":"owned-test-secret",
            "ca_pem":certs.ca,
        })
        .to_string(),
    )
    .unwrap();
    fs::set_permissions(&config, fs::Permissions::from_mode(0o600)).unwrap();
    let pool = Arc::new(HaPool::new(&HaOptions::load(&config).unwrap()).unwrap());
    schema::initialize(&pool, None).await.unwrap();
    let catalog = HaCatalog::new(pool, certs.dir.path().join("daemon-load-export.json"));
    let mut revision = catalog
        .edit(0, CatalogEdit::Enable(true))
        .await
        .unwrap()
        .revision;
    revision = catalog
        .edit(
            revision,
            CatalogEdit::PutPackage("basic".into(), json!({"channels":["news","sports"]})),
        )
        .await
        .unwrap()
        .revision;
    for index in 0..VIEWERS {
        revision = catalog
            .edit(
                revision,
                CatalogEdit::PutSubscriber(
                    format!("viewer-{index}"),
                    json!({"token":format!("owned-daemon-load-{index}"),"packages":["basic"],"max_sessions":1}),
                ),
            )
            .await
            .unwrap()
            .revision;
    }

    let mut first =
        OwnedAuthority::start_tls(certs.dir.path(), "load-first", &config, &certs).await;
    let mut second =
        OwnedAuthority::start_tls(certs.dir.path(), "load-second", &config, &certs).await;
    assert_ne!(first.pid(), second.pid());
    let pids = [first.pid(), second.pid()];
    let bases = [first.base.clone(), second.base.clone()];
    let before = [process_usage(pids[0]), process_usage(pids[1])];
    let clients: Vec<reqwest::Client> = (1..=2)
        .map(|octet| {
            reqwest::Client::builder()
                .no_proxy()
                .local_address(IpAddr::V4(Ipv4Addr::new(127, 0, 0, octet)))
                .add_root_certificate(
                    reqwest::Certificate::from_pem(&fs::read(&certs.ca).unwrap()).unwrap(),
                )
                .timeout(Duration::from_secs(4))
                .build()
                .unwrap()
        })
        .collect();
    for base in &bases {
        let (status, ready, _) = lease_timed(&clients[0], base, "status", json!({})).await;
        assert_eq!(status, reqwest::StatusCode::OK);
        assert_eq!(ready, json!({"ready":true}));
    }
    let racing: Vec<Value> = ["news", "sports"]
        .into_iter()
        .map(|channel| {
            json!({
                "token":"owned-daemon-load-0","channel":channel,
                "boot":uuid::Uuid::new_v4().to_string(),
                "session":uuid::Uuid::new_v4().to_string(),
                "operation":uuid::Uuid::new_v4().to_string(),"units":1,
            })
        })
        .collect();
    let (left, right) = tokio::join!(
        lease_timed(&clients[0], &bases[0], "acquire", racing[0].clone()),
        lease_timed(&clients[1], &bases[1], "acquire", racing[1].clone())
    );
    assert_eq!(
        usize::from(left.0 == reqwest::StatusCode::OK)
            + usize::from(right.0 == reqwest::StatusCode::OK),
        1,
        "cross-channel race must issue exactly one grant: {left:?} {right:?}"
    );
    let (winner, grant, denied) = if left.0 == reqwest::StatusCode::OK {
        (&racing[0], &left.1, right.0)
    } else {
        (&racing[1], &right.1, left.0)
    };
    assert_eq!(denied, reqwest::StatusCode::CONFLICT);
    let mut committed = winner.clone();
    committed["lease_id"] = grant["lease_id"].clone();
    assert_eq!(
        lease_timed(&clients[0], &bases[1], "commit", committed)
            .await
            .0,
        reqwest::StatusCode::OK
    );
    let release = json!({
        "subscriber_id":grant["subscriber_id"],"boot":winner["boot"],
        "session":winner["session"],"lease_id":grant["lease_id"],
        "operation":winner["operation"],
    });
    assert_eq!(
        lease_timed(&clients[1], &bases[0], "release", release)
            .await
            .0,
        reqwest::StatusCode::NO_CONTENT
    );
    let sql = database.local_client().await;
    let starting_lsn: String = sql
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .unwrap()
        .get(0);
    let stop = Arc::new(AtomicBool::new(false));
    let monitor_stop = stop.clone();
    let monitor = tokio::spawn(async move {
        let mut peak_active = 0_i32;
        let mut peak_connections = 0_i64;
        let mut peak_rss = [0_u64; 2];
        let mut peak_fds = [0_usize; 2];
        loop {
            let row = sql
                .query_one(
                    "SELECT (SELECT active_sessions FROM fx_iptv_capacity),
                        (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database())",
                    &[],
                )
                .await
                .unwrap();
            peak_active = peak_active.max(row.get(0));
            peak_connections = peak_connections.max(row.get(1));
            for (index, pid) in pids.into_iter().enumerate() {
                let usage = process_usage(pid);
                peak_rss[index] = peak_rss[index].max(usage.rss_kib);
                peak_fds[index] = peak_fds[index].max(usage.fds);
            }
            if monitor_stop.load(Ordering::Relaxed) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        (peak_active, peak_connections, peak_rss, peak_fds, sql)
    });

    let first_commits = Arc::new(tokio::sync::Barrier::new(VIEWERS + 1));
    let (start, started) = tokio::sync::watch::channel(None::<Instant>);
    let mut tasks = Vec::new();
    for index in 0..VIEWERS {
        let primary = bases[index % 2].clone();
        let alternate = bases[(index + 1) % 2].clone();
        let client = clients[index % 2].clone();
        let first_commits = first_commits.clone();
        let mut started = started.clone();
        tasks.push(tokio::spawn(async move {
            let mut acquire = Vec::new();
            let mut renew = Vec::new();
            let mut cycles = 0_usize;
            let mut missed_renewals = 0_usize;
            let boot = uuid::Uuid::new_v4().to_string();
            let session = uuid::Uuid::new_v4().to_string();
            let operation = uuid::Uuid::new_v4().to_string();
            let body = json!({
                "token":format!("owned-daemon-load-{index}"),
                "channel":if index % 2 == 0 {"news"} else {"sports"},
                "boot":boot,"session":session,"operation":operation,"units":1,
            });
            let (status, grant, elapsed) =
                lease_timed(&client, &primary, "acquire", body.clone()).await;
            assert_eq!(
                status,
                reqwest::StatusCode::OK,
                "acquire failed for viewer {index}"
            );
            acquire.push(elapsed);
            let mut granted = body.clone();
            granted["lease_id"] = grant["lease_id"].clone();
            let (status, _, _) = lease_timed(&client, &alternate, "commit", granted.clone()).await;
            assert_eq!(
                status,
                reqwest::StatusCode::OK,
                "commit failed for viewer {index}"
            );
            let mut last_renewal = Instant::now();
            tokio::time::timeout(Duration::from_secs(10), first_commits.wait())
                .await
                .expect("not all 16 process-load viewers committed");
            started.changed().await.unwrap();
            let deadline = (*started.borrow()).expect("load start signal is missing");
            while Instant::now() < deadline {
                let mut competing = body.clone();
                competing["channel"] = json!(if index % 2 == 0 { "sports" } else { "news" });
                competing["session"] = json!(uuid::Uuid::new_v4().to_string());
                competing["operation"] = json!(uuid::Uuid::new_v4().to_string());
                let (status, denial, _) =
                    lease_timed(&client, &alternate, "acquire", competing).await;
                assert_eq!(
                    status,
                    reqwest::StatusCode::CONFLICT,
                    "extra grant for viewer {index}: {denial}"
                );
                let (status, _, elapsed) =
                    lease_timed(&client, &primary, "renew", granted.clone()).await;
                assert_eq!(
                    status,
                    reqwest::StatusCode::OK,
                    "renew failed for viewer {index}"
                );
                renew.push(elapsed);
                if last_renewal.elapsed() >= Duration::from_secs(10) {
                    missed_renewals += 1;
                }
                last_renewal = Instant::now();
                cycles += 1;
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            let release = json!({
                "subscriber_id":grant["subscriber_id"],"boot":boot,"session":session,
                "lease_id":grant["lease_id"],"operation":operation,
            });
            let (status, _, _) = lease_timed(&client, &alternate, "release", release).await;
            assert_eq!(
                status,
                reqwest::StatusCode::NO_CONTENT,
                "release failed for viewer {index}"
            );
            (acquire, renew, cycles, missed_renewals)
        }));
    }
    tokio::time::timeout(Duration::from_secs(10), first_commits.wait())
        .await
        .expect("not all 16 process-load viewers committed");
    let snapshot = database.local_client().await;
    let row = snapshot
        .query_one(
            "SELECT (SELECT active_sessions FROM fx_iptv_capacity),
                (SELECT count(*) FROM fx_iptv_sessions)",
            &[],
        )
        .await
        .unwrap();
    let committed_sessions: i32 = row.get(0);
    let session_rows: i64 = row.get(1);
    assert_eq!(committed_sessions, VIEWERS as i32);
    assert_eq!(session_rows, VIEWERS as i64);
    drop(snapshot);
    start
        .send(Some(Instant::now() + Duration::from_secs(seconds)))
        .unwrap();
    let mut acquire = Vec::new();
    let mut renew = Vec::new();
    let mut cycles = 0_usize;
    let mut missed_renewals = 0_usize;
    for task in tasks {
        let (a, r, c, missed) = task.await.unwrap();
        acquire.extend(a);
        renew.extend(r);
        cycles += c;
        missed_renewals += missed;
    }
    stop.store(true, Ordering::Relaxed);
    let (observed_active, peak_db_connections, peak_authority_rss_kib, peak_authority_fds, sql) =
        monitor.await.unwrap();
    let peak_active = observed_active.max(committed_sessions);
    let row = sql
        .query_one(
            "SELECT (SELECT active_sessions FROM fx_iptv_capacity),
                (SELECT count(*) FROM fx_iptv_sessions),
                pg_wal_lsn_diff(pg_current_wal_lsn(), $1::text::pg_lsn)::bigint",
            &[&starting_lsn],
        )
        .await
        .unwrap();
    let remaining: i32 = row.get(0);
    let session_rows: i64 = row.get(1);
    let wal_bytes: i64 = row.get(2);
    let after = [process_usage(pids[0]), process_usage(pids[1])];
    let cpu_seconds = [
        after[0].cpu_seconds - before[0].cpu_seconds,
        after[1].cpu_seconds - before[1].cpu_seconds,
    ];
    let acquire_p50_ms = percentile_ms(acquire.clone(), 50);
    let acquire_p99_ms = percentile_ms(acquire, 99);
    let renew_p50_ms = percentile_ms(renew.clone(), 50);
    let renew_p99_ms = percentile_ms(renew, 99);
    let summary = LoadSummary {
        cycles,
        excess_grants: 0,
        missed_renewals,
        peak_active,
        peak_db_connections,
        wal_bytes,
        acquire_p99_ms,
        renew_p99_ms,
        peak_authority_rss_kib,
        peak_authority_fds,
    };
    println!(
        "IPTV_HA_DAEMON_LOAD_RESULT {}",
        json!({
            "seconds":seconds,"viewers":VIEWERS,"cycles":cycles,
            "acquire_p50_ms":acquire_p50_ms,"acquire_p99_ms":acquire_p99_ms,
            "renew_p50_ms":renew_p50_ms,"renew_p99_ms":renew_p99_ms,
            "missed_renewals":missed_renewals,"excess_grants":0,
            "peak_active":peak_active,"remaining_sessions":remaining,"session_rows":session_rows,
            "peak_database_connections":peak_db_connections,"wal_bytes":wal_bytes,
            "authority_cpu_seconds":cpu_seconds,
            "peak_authority_rss_kib":peak_authority_rss_kib,
            "peak_authority_fds":peak_authority_fds,
        })
    );
    assert_eq!(remaining, 0);
    assert_eq!(session_rows, 0);
    first.stop().await;
    second.stop().await;
    summary
}
