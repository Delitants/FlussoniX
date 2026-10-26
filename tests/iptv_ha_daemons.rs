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
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::Path,
    process::Stdio,
    sync::Arc,
    time::Duration,
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

struct OwnedAuthority {
    child: Child,
    base: String,
}

impl OwnedAuthority {
    async fn start(root: &Path, name: &str, db_config: &Path) -> Self {
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
        let base = format!("http://{}", ready["listen"].as_str().unwrap());
        Self { child, base }
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
    let response = client
        .post(format!(
            "{}/flussonix/peer/v1/iptv/leases/{action}",
            daemon.base
        ))
        .header("x-flussonix-peer", PEER)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.headers()["x-flussonix-iptv-cluster-id"], CLUSTER);
    let status = response.status();
    let body = response.json().await.unwrap_or(Value::Null);
    (status, body)
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
