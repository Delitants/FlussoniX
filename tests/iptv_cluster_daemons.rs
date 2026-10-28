#![cfg(target_os = "linux")]

use axum::Router;
use flussonix::{
    config::ConfigStore,
    server::{App, Options, router},
};
use futures_util::StreamExt;
use serde_json::json;
use std::{
    net::SocketAddr,
    os::unix::process::CommandExt,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpListener,
    process::{Child, Command},
};

const PEER_KEY: &str = "owned-cluster-peer-123";
const TOKEN: &str = "private-viewer-token";

struct Cdn {
    child: Child,
    base: String,
}

impl Cdn {
    async fn start(root: &std::path::Path, name: &str, authority: &url::Url) -> Self {
        let config = root.join(format!("{name}-config.json"));
        ConfigStore::open(&config)
            .unwrap()
            .put(
                "streams",
                "news",
                json!({
                    "inputs":[{"url":"testsrc://"}],
                    "on_play":"iptv://"
                }),
            )
            .unwrap();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_flussonix"));
        for (key, _) in std::env::vars().filter(|(key, _)| key.starts_with("FLUSSONIX_")) {
            cmd.env_remove(key);
        }
        cmd.args([
            "--listen",
            "127.0.0.1:0",
            "--role",
            "cdn",
            "--node-name",
            name,
        ])
        .arg("--config")
        .arg(config)
        .arg("--iptv-data")
        .arg(root.join(format!("{name}-iptv.json")))
        .arg("--media-dir")
        .arg(root.join(format!("{name}-media")))
        .args([
            "--iptv-authority",
            authority.as_str(),
            "--iptv-authority-allow-insecure-loopback",
        ])
        .env("FLUSSONIX_ADMIN_PASSWORD", "owned-admin")
        .env("FLUSSONIX_PEER_KEY", PEER_KEY)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
        cmd.as_std_mut().process_group(0);
        let mut child = cmd.spawn().unwrap();
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let line = tokio::time::timeout(Duration::from_secs(8), lines.next_line())
            .await
            .expect("CDN did not start")
            .unwrap()
            .unwrap();
        let status: serde_json::Value = serde_json::from_str(&line).unwrap();
        let base = format!("http://{}", status["listen"].as_str().unwrap());
        Self { child, base }
    }

    async fn terminate(&mut self) {
        let pgid = self.child.id().expect("owned CDN still running") as i32;
        // SAFETY: This process group was created only for this test child.
        assert_eq!(unsafe { libc::kill(-pgid, libc::SIGKILL) }, 0);
        tokio::time::timeout(Duration::from_secs(5), self.child.wait())
            .await
            .expect("owned CDN did not exit")
            .unwrap();
    }
}

impl Drop for Cdn {
    fn drop(&mut self) {
        if let Some(pgid) = self.child.id() {
            // SAFETY: Each CDN was started in a fresh, test-owned process group.
            unsafe { libc::kill(-(pgid as i32), libc::SIGKILL) };
        }
    }
}

async fn hls_segment(client: &reqwest::Client, cdn: &Cdn) -> Vec<u8> {
    let playlist_url = format!("{}/news/index.m3u8?token={TOKEN}", cdn.base);
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let response = client.get(&playlist_url).send().await.unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            let playlist = response.text().await.unwrap();
            if let Some(segment) = playlist
                .lines()
                .find(|line| !line.is_empty() && !line.starts_with('#'))
            {
                let segment_url = url::Url::parse(&playlist_url)
                    .unwrap()
                    .join(segment)
                    .unwrap();
                let media = client.get(segment_url).send().await.unwrap();
                if media.status() == reqwest::StatusCode::OK {
                    let bytes = media.bytes().await.unwrap();
                    if !bytes.is_empty() {
                        return bytes.to_vec();
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("HLS media did not become available")
}

fn rss_kib(pid: u32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse().ok())
}

fn group_rss_kib(pgid: u32) -> u64 {
    std::fs::read_dir("/proc")
        .unwrap()
        .filter_map(Result::ok)
        .filter_map(|entry| {
            entry
                .file_name()
                .to_str()
                .and_then(|s| s.parse::<u32>().ok())
        })
        .filter_map(|pid| {
            let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
            let fields = stat
                .rsplit_once(')')?
                .1
                .split_whitespace()
                .collect::<Vec<_>>();
            (fields.get(2)?.parse::<u32>().ok()? == pgid).then_some(pid)
        })
        .filter_map(rss_kib)
        .sum()
}

#[tokio::test]
async fn cdn_reports_authority_readiness_without_subscriber_traffic() {
    let dir = tempfile::tempdir().unwrap();
    let authority = App::new(
        dir.path().join("authority-config.json"),
        dir.path().join("authority-media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: PEER_KEY.into(),
            role: "lb".into(),
            iptv_lease_authority: true,
            iptv_peer_allow_insecure_loopback: true,
            ..Options::default()
        },
    )
    .unwrap();
    authority.iptv.set_enabled(true).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let server = tokio::spawn(
        axum::serve(
            listener,
            router(authority.clone())
                .layer(axum::middleware::map_response(
                    |mut response: axum::response::Response| async move {
                        response.headers_mut().insert(
                            axum::http::header::CONNECTION,
                            axum::http::HeaderValue::from_static("close"),
                        );
                        response
                    },
                ))
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .into_future(),
    );
    let mut cdn = Cdn::start(dir.path(), "cdn-readiness", &origin).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    async fn ready(client: &reqwest::Client, cdn: &Cdn) -> bool {
        let response = client
            .get(format!("{}/flussonix/api/v1/node", cdn.base))
            .header("x-flussonix-peer", PEER_KEY)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        response.json::<serde_json::Value>().await.unwrap()["iptv_cluster"]["ready"] == true
    }

    assert!(!ready(&client, &cdn).await);
    let became_ready = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if ready(&client, &cdn).await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    if became_ready.is_ok() {
        authority.iptv.set_enabled(false).unwrap();
    }
    let became_unready = if became_ready.is_ok() {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if !ready(&client, &cdn).await {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .is_ok()
    } else {
        false
    };
    if became_unready {
        authority.iptv.set_enabled(true).unwrap();
    }
    let recovered = if became_unready {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if ready(&client, &cdn).await {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .is_ok()
    } else {
        false
    };
    server.abort();
    let _ = server.await;
    let lost_authority = if recovered {
        tokio::time::timeout(Duration::from_secs(16), async {
            loop {
                if !ready(&client, &cdn).await {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .is_ok()
    } else {
        false
    };
    cdn.terminate().await;
    assert!(
        became_ready.is_ok(),
        "CDN never reported its ready authority"
    );
    assert!(
        became_unready,
        "CDN kept reporting a disabled authority as ready"
    );
    assert!(
        recovered,
        "CDN did not recover when authority was re-enabled"
    );
    assert!(
        lost_authority,
        "CDN kept reporting a lost authority as ready"
    );
}

#[tokio::test]
async fn two_cdn_daemons_share_one_iptv_limit_during_hls_and_continuous_playback() {
    let dir = tempfile::tempdir().unwrap();
    let authority = App::new(
        dir.path().join("authority-config.json"),
        dir.path().join("authority-media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: PEER_KEY.into(),
            role: "lb".into(),
            iptv_data: Some(dir.path().join("authority-iptv.json")),
            iptv_lease_authority: true,
            iptv_peer_allow_insecure_loopback: true,
            ..Options::default()
        },
    )
    .unwrap();
    authority.iptv.set_enabled(true).unwrap();
    authority
        .iptv
        .put_package("basic", json!({"channels":["news"]}))
        .unwrap();
    authority
        .iptv
        .put_subscriber(
            "alice",
            json!({
                "token":TOKEN,"packages":["basic"],"max_sessions":2
            }),
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let service: Router = router(authority.clone());
    let server = tokio::spawn(
        axum::serve(
            listener,
            service.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .into_future(),
    );
    let (first, second) = tokio::join!(
        Cdn::start(dir.path(), "cdn-first", &origin),
        Cdn::start(dir.path(), "cdn-second", &origin),
    );
    assert_ne!(first.child.id(), second.child.id());
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    tokio::time::sleep(Duration::from_secs(11)).await;

    let ts_url = format!("{}/news/mpegts?token={TOKEN}", first.base);
    let (ts, segment) = tokio::join!(
        async {
            tokio::time::timeout(Duration::from_secs(8), client.get(&ts_url).send())
                .await
                .unwrap()
                .unwrap()
        },
        hls_segment(&client, &second),
    );
    assert_eq!(ts.status(), reqwest::StatusCode::OK);
    assert!(!segment.is_empty());
    let mut ts_body = ts.bytes_stream();
    let first_ts = tokio::time::timeout(Duration::from_secs(8), ts_body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!first_ts.is_empty());
    assert_eq!(authority.iptv_leases.active_leases(), 2);

    let overflow = tokio::time::timeout(
        Duration::from_secs(8),
        client
            .get(format!("{}/news/mpegts?token={TOKEN}", second.base))
            .send(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(overflow.status(), reqwest::StatusCode::FORBIDDEN);
    assert_eq!(authority.iptv_leases.active_leases(), 2);
    drop((ts_body, first, second));
    server.abort();
}

#[tokio::test]
async fn daemon_closes_iptv_media_on_authority_partition_and_readmits_after_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let authority = App::new(
        dir.path().join("authority-config.json"),
        dir.path().join("authority-media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: PEER_KEY.into(),
            role: "lb".into(),
            iptv_data: Some(dir.path().join("authority-iptv.json")),
            iptv_lease_authority: true,
            iptv_peer_allow_insecure_loopback: true,
            ..Options::default()
        },
    )
    .unwrap();
    authority.iptv.set_enabled(true).unwrap();
    authority
        .iptv
        .put_package("basic", json!({"channels":["news"]}))
        .unwrap();
    authority
        .iptv
        .put_subscriber(
            "alice",
            json!({"token":TOKEN,"packages":["basic"],"max_sessions":1}),
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let origin = url::Url::parse(&format!("http://{address}/")).unwrap();
    let server = tokio::spawn(
        axum::serve(
            listener,
            router(authority.clone()).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .into_future(),
    );
    let cdn = Cdn::start(dir.path(), "cdn-partition", &origin).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    tokio::time::sleep(Duration::from_secs(11)).await;
    let ts_url = format!("{}/news/mpegts?token={TOKEN}", cdn.base);
    let response = tokio::time::timeout(Duration::from_secs(8), client.get(&ts_url).send())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let mut body = response.bytes_stream();
    let first = tokio::time::timeout(Duration::from_secs(8), body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!first.is_empty());
    assert_eq!(authority.iptv_leases.active_leases(), 1);

    server.abort();
    let _ = server.await;
    let outage_started = std::time::Instant::now();
    let closed = tokio::time::timeout(Duration::from_secs(9), async {
        while let Some(chunk) = body.next().await {
            if chunk.is_err() {
                break;
            }
        }
    })
    .await;
    assert!(
        closed.is_ok(),
        "CDN kept delivering IPTV after authority loss"
    );
    let hls = tokio::time::timeout(
        Duration::from_secs(8),
        client
            .get(format!("{}/news/index.m3u8?token={TOKEN}", cdn.base))
            .send(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(hls.status(), reqwest::StatusCode::FORBIDDEN);

    let listener = TcpListener::bind(address).await.unwrap();
    let server = tokio::spawn(
        axum::serve(
            listener,
            router(authority.clone()).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .into_future(),
    );
    tokio::time::sleep(Duration::from_secs(11).saturating_sub(outage_started.elapsed())).await;
    let response = tokio::time::timeout(Duration::from_secs(8), client.get(&ts_url).send())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let mut resumed = response.bytes_stream();
    let media = tokio::time::timeout(Duration::from_secs(8), resumed.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!media.is_empty(), "recovered CDN emitted no media");
    assert_eq!(authority.iptv_leases.active_leases(), 1);
    drop((body, resumed, cdn));
    server.abort();
}

/// Manual release gate: `FLUSSONIX_IPTV_SOAK_SECONDS=900 cargo test --locked
/// --test iptv_cluster_daemons sustained_iptv_cap_and_memory -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "15-minute, two-daemon live-media resource qualification"]
async fn sustained_iptv_cap_and_memory() {
    let duration = std::env::var("FLUSSONIX_IPTV_SOAK_SECONDS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(900)
        .clamp(1, 3600);
    let dir = tempfile::tempdir().unwrap();
    let authority = App::new(
        dir.path().join("authority-config.json"),
        dir.path().join("authority-media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: PEER_KEY.into(),
            role: "lb".into(),
            iptv_data: Some(dir.path().join("authority-iptv.json")),
            iptv_lease_authority: true,
            iptv_peer_allow_insecure_loopback: true,
            ..Options::default()
        },
    )
    .unwrap();
    authority.iptv.set_enabled(true).unwrap();
    authority
        .iptv
        .put_package("basic", json!({"channels":["news"]}))
        .unwrap();
    authority
        .iptv
        .put_subscriber(
            "alice",
            json!({"token":TOKEN,"packages":["basic"],"max_sessions":2}),
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let server = tokio::spawn(
        axum::serve(
            listener,
            router(authority.clone()).into_make_service_with_connect_info::<SocketAddr>(),
        )
        .into_future(),
    );
    let (mut first, mut second) = tokio::join!(
        Cdn::start(dir.path(), "cdn-soak-ts", &origin),
        Cdn::start(dir.path(), "cdn-soak-hls", &origin),
    );
    let first_pgid = first.child.id().unwrap();
    let second_pgid = second.child.id().unwrap();
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    tokio::time::sleep(Duration::from_secs(11)).await;
    let response = tokio::time::timeout(
        Duration::from_secs(8),
        client
            .get(format!("{}/news/mpegts?token={TOKEN}", first.base))
            .send(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let mut body = response.bytes_stream();
    let delivered = Arc::new(AtomicU64::new(0));
    let credit = delivered.clone();
    let consumer = tokio::spawn(async move {
        while let Some(chunk) = body.next().await {
            let bytes = chunk.expect("continuous IPTV playback ended during soak");
            credit.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
    });
    assert!(!hls_segment(&client, &second).await.is_empty());
    let baseline = [
        group_rss_kib(first_pgid),
        group_rss_kib(second_pgid),
        rss_kib(std::process::id()).unwrap(),
    ];
    let mut peak = baseline;
    let started = Instant::now();
    let mut cycles = 0u64;
    let mut last_media = delivered.load(Ordering::Relaxed);
    while started.elapsed() < Duration::from_secs(duration) {
        let (third_ts, third_m4s) = tokio::time::timeout(Duration::from_secs(8), async {
            tokio::join!(
                client
                    .get(format!("{}/news/mpegts?token={TOKEN}", second.base))
                    .send(),
                client
                    .get(format!("{}/news/m4s?token={TOKEN}", first.base))
                    .send(),
            )
        })
        .await
        .expect("IPTV churn admission stalled");
        assert_eq!(third_ts.unwrap().status(), reqwest::StatusCode::FORBIDDEN);
        assert_eq!(third_m4s.unwrap().status(), reqwest::StatusCode::FORBIDDEN);
        assert_eq!(authority.iptv_leases.active_leases(), 2);
        assert_eq!(authority.iptv_leases.subscriber_records(), 1);
        if cycles % 10 == 0 {
            assert!(!hls_segment(&client, &second).await.is_empty());
        }
        if cycles % 10 == 9 {
            let now = delivered.load(Ordering::Relaxed);
            assert!(now > last_media, "continuous IPTV media stalled");
            last_media = now;
        }
        let current = [
            group_rss_kib(first_pgid),
            group_rss_kib(second_pgid),
            rss_kib(std::process::id()).unwrap(),
        ];
        for i in 0..3 {
            peak[i] = peak[i].max(current[i]);
            assert!(
                peak[i].saturating_sub(baseline[i]) <= 128 * 1024,
                "component {i} RSS grew more than 128 MiB: baseline={} KiB peak={} KiB",
                baseline[i],
                peak[i]
            );
        }
        cycles += 1;
        if cycles % 15 == 0 {
            println!(
                "elapsed={}s churn={} ts_bytes={} rss_kib={peak:?}",
                started.elapsed().as_secs(),
                cycles,
                delivered.load(Ordering::Relaxed)
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    assert!(!consumer.is_finished(), "continuous IPTV consumer stopped");
    assert!(delivered.load(Ordering::Relaxed) > 0);
    println!(
        "soak_pass seconds={} churn={} ts_bytes={} baseline_kib={baseline:?} peak_kib={peak:?}",
        started.elapsed().as_secs(),
        cycles,
        delivered.load(Ordering::Relaxed)
    );
    consumer.abort();
    first.terminate().await;
    second.terminate().await;
    server.abort();
}
