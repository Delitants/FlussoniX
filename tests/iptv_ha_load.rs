//! Explicit, bounded release gate for two independent IPTV authority instances.
#![cfg(unix)]

use axum::{
    Router,
    body::Body,
    extract::ConnectInfo,
    http::{Method, Request, StatusCode},
};
use flussonix::{
    iptv::ha::{
        catalog::{CatalogEdit, HaCatalog},
        connection::{HaOptions, HaPool},
        schema,
    },
    server::{App, Options, router},
};
use serde_json::{Value, json};
use std::{
    fs,
    net::SocketAddr,
    os::unix::fs::PermissionsExt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tower::ServiceExt;
#[path = "support/iptv_postgres.rs"]
mod iptv_postgres;
#[path = "support/tls.rs"]
mod tls_fixture;

const CLUSTER: &str = "a3192376-6df5-4b76-a15c-43942fd3d466";
const VIEWERS: usize = 16;

async fn call(
    app: Router,
    source: SocketAddr,
    action: &str,
    body: Value,
) -> (StatusCode, Value, Duration) {
    let mut request = Request::builder()
        .method(Method::POST)
        .uri(format!("/flussonix/peer/v1/iptv/leases/{action}"))
        .header("x-flussonix-peer", "owned-load-peer-key-123")
        .body(Body::from(body.to_string()))
        .unwrap();
    request.extensions_mut().insert(ConnectInfo(source));
    let started = Instant::now();
    let response = tokio::time::timeout(Duration::from_secs(2), app.oneshot(request))
        .await
        .expect("lease HTTP deadline missed")
        .unwrap();
    let elapsed = started.elapsed();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 8192)
        .await
        .unwrap();
    let json = serde_json::from_slice(&body).unwrap_or(Value::Null);
    (status, json, elapsed)
}

fn percentile(mut samples: Vec<Duration>, percentile: usize) -> u128 {
    samples.sort_unstable();
    samples[(samples.len() - 1) * percentile / 100].as_millis()
}

fn cpu_seconds() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage writes the complete rusage struct on success.
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
        0
    );
    let usage = unsafe { usage.assume_init() };
    (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) as f64
        + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) as f64 / 1_000_000.0
}

fn max_rss_kib() -> i64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage writes the complete rusage struct on success.
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
        0
    );
    unsafe { usage.assume_init() }.ru_maxrss
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "run explicitly with scripts/qualify-iptv-ha.sh"]
async fn two_authorities_sustain_competing_channels_without_extra_grants() {
    let seconds: u64 = std::env::var("FLUSSONIX_IPTV_HA_LOAD_SECONDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(60);
    assert!((5..=900).contains(&seconds));
    let certs = tls_fixture::Certificates::new();
    let db = iptv_postgres::PgServer::start(&certs)
        .expect("native PostgreSQL is required for the HA load gate");
    let path = certs.dir.path().join("load-db.json");
    fs::write(
        &path,
        json!({
            "cluster_id":CLUSTER, "host":"127.0.0.1", "port":db.port(),
            "database":"postgres", "user":"ha_test", "password":"owned-test-secret",
            "ca_pem":certs.ca,
        })
        .to_string(),
    )
    .unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    let ha = HaOptions::load(&path).unwrap();
    let pool = Arc::new(HaPool::new(&ha).unwrap());
    schema::initialize(&pool, None).await.unwrap();
    let catalog = HaCatalog::new(pool.clone(), certs.dir.path().join("load-export.json"));
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
        revision = catalog.edit(revision, CatalogEdit::PutSubscriber(
            format!("viewer-{index}"),
            json!({"token":format!("owned-load-token-{index}"),"packages":["basic"],"max_sessions":1}),
        )).await.unwrap().revision;
    }
    let apps: Vec<Router> = (0..2)
        .map(|index| {
            let app = App::new(
                certs
                    .dir
                    .path()
                    .join(format!("load-authority-{index}.json")),
                certs.dir.path().join(format!("media-{index}")),
                Options {
                    admin_password: "owned-admin".into(),
                    peer_key: "owned-load-peer-key-123".into(),
                    role: "lb".into(),
                    iptv_lease_authority: true,
                    iptv_peer_allow_insecure_loopback: true,
                    iptv_public_origin: Some(url::Url::parse("http://127.0.0.1:18210/").unwrap()),
                    iptv_ha: Some(ha.clone()),
                    ..Options::default()
                },
            )
            .unwrap();
            router(app)
        })
        .collect();
    let sql = db.local_client().await;
    let starting_lsn: String = sql
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .unwrap()
        .get(0);
    let starting_cpu = cpu_seconds();
    let stop = Arc::new(AtomicBool::new(false));
    let monitor_stop = stop.clone();
    let monitor = tokio::spawn(async move {
        let mut peak_active = 0_i32;
        let mut peak_connections = 0_i64;
        while !monitor_stop.load(Ordering::Relaxed) {
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
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        (peak_active, peak_connections, sql)
    });
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let mut tasks = Vec::new();
    for index in 0..VIEWERS {
        let primary = apps[index % 2].clone();
        let alternate = apps[(index + 1) % 2].clone();
        tasks.push(tokio::spawn(async move {
            let source = SocketAddr::from(([127, 0, 0, (index % 2 + 1) as u8], 41000));
            let mut acquire = Vec::new();
            let mut renew = Vec::new();
            let mut cycles = 0_usize;
            let mut missed_renewals = 0_usize;
            while Instant::now() < deadline {
                let boot = uuid::Uuid::new_v4().to_string();
                let session = uuid::Uuid::new_v4().to_string();
                let operation = uuid::Uuid::new_v4().to_string();
                let body = json!({
                    "token":format!("owned-load-token-{index}"),
                    "channel":if index % 2 == 0 {"news"} else {"sports"},
                    "boot":boot,"session":session,"operation":operation,"units":1,
                });
                let (status, grant, elapsed) =
                    call(primary.clone(), source, "acquire", body.clone()).await;
                assert_eq!(status, StatusCode::OK, "acquire failed for viewer {index}");
                acquire.push(elapsed);
                let mut competing = body.clone();
                competing["session"] = json!(uuid::Uuid::new_v4().to_string());
                competing["operation"] = json!(uuid::Uuid::new_v4().to_string());
                let (status, denial, _) =
                    call(alternate.clone(), source, "acquire", competing).await;
                assert_eq!(
                    status,
                    StatusCode::CONFLICT,
                    "extra grant for viewer {index}: {denial}"
                );
                let mut granted = body.clone();
                granted["lease_id"] = grant["lease_id"].clone();
                let (status, _, _) =
                    call(alternate.clone(), source, "commit", granted.clone()).await;
                assert_eq!(status, StatusCode::OK, "commit failed for viewer {index}");
                let committed_at = Instant::now();
                let (status, _, elapsed) = call(primary.clone(), source, "renew", granted).await;
                assert_eq!(status, StatusCode::OK, "renew failed for viewer {index}");
                renew.push(elapsed);
                if committed_at.elapsed() >= Duration::from_secs(10) {
                    missed_renewals += 1;
                }
                let release = json!({
                    "subscriber_id":grant["subscriber_id"],"boot":boot,"session":session,
                    "lease_id":grant["lease_id"],"operation":operation,
                });
                let (status, _, _) = call(alternate.clone(), source, "release", release).await;
                assert_eq!(
                    status,
                    StatusCode::NO_CONTENT,
                    "release failed for viewer {index}"
                );
                cycles += 1;
            }
            (acquire, renew, cycles, missed_renewals)
        }));
    }
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
    let (peak_active, peak_connections, sql) = monitor.await.unwrap();
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
    let rows: i64 = row.get(1);
    let wal_bytes: i64 = row.get(2);
    let acquire_p50 = percentile(acquire.clone(), 50);
    let acquire_p99 = percentile(acquire, 99);
    let renew_p50 = percentile(renew.clone(), 50);
    let renew_p99 = percentile(renew, 99);
    let cpu = cpu_seconds() - starting_cpu;
    let rss = max_rss_kib();
    println!(
        "IPTV_HA_LOAD_RESULT {}",
        json!({
            "seconds":seconds,"viewers":VIEWERS,"cycles":cycles,
            "acquire_p50_ms":acquire_p50,"acquire_p99_ms":acquire_p99,
            "renew_p50_ms":renew_p50,"renew_p99_ms":renew_p99,
            "missed_renewals":missed_renewals,"peak_active":peak_active,
            "remaining_sessions":remaining,"session_rows":rows,
            "peak_database_connections":peak_connections,"wal_bytes":wal_bytes,
            "cpu_seconds":cpu,"max_rss_kib":rss,
        })
    );
    assert!(
        cycles >= VIEWERS,
        "each viewer must complete at least one cycle"
    );
    assert_eq!(missed_renewals, 0);
    assert!(peak_active <= VIEWERS as i32);
    assert_eq!(remaining, 0);
    assert_eq!(rows, 0);
    assert!(acquire_p99 < 2_000 && renew_p99 < 2_000);
    assert!(peak_connections <= 40);
    assert!(rss < 2 * 1024 * 1024);
}
