//! Two isolated CLI processes: HTTP discovery stays separate from FXLink media.
use futures_util::FutureExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{panic::AssertUnwindSafe, path::Path, time::Duration};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
#[path = "support/tls.rs"]
mod tls_fixture;
const PEER: &str = "owned-fxlink-cli-peer";
const ADMIN: &str = "owned-fxlink-cli-admin";
const VIEWER: &str = "owned-fxlink-cli-viewer";
struct Node {
    child: tokio::process::Child,
    url: String,
    stdout: Option<tokio::task::JoinHandle<()>>,
}
impl Drop for Node {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            if let Some(pid) = self.child.id() {
                let _ = std::process::Command::new("kill")
                    .args(["-KILL", "--", &format!("-{pid}")])
                    .status();
            }
        }
    }
}
impl Node {
    async fn start(path: &Path, name: &str, config: Value, args: &[&str]) -> Self {
        let config_path = path.join(format!("{name}.json"));
        tokio::fs::write(&config_path, serde_json::to_vec(&config).unwrap())
            .await
            .unwrap();
        let reference = std::env::var_os("FXLINK_V5_REFERENCE_BINARY");
        let binary = if !args.contains(&"--fxlink-trimmed-records") {
            reference
                .as_deref()
                .map(Path::new)
                .unwrap_or_else(|| Path::new(env!("CARGO_BIN_EXE_flussonix")))
        } else {
            Path::new(env!("CARGO_BIN_EXE_flussonix"))
        };
        assert!(
            binary.is_file(),
            "selected owned qualification executable is missing"
        );
        let mut command = tokio::process::Command::new(binary);
        if std::env::var_os("FXLINK_QUAL_TRACE").is_some() {
            command.env("RUST_LOG", "flussonix::fxlink::listener=trace");
        }
        let child = command
            .args(["--listen", "127.0.0.1:0", "--uplink-interface", "process"])
            .arg("--config")
            .arg(config_path)
            .arg("--media-dir")
            .arg(path.join(name))
            .args(args)
            .env("FLUSSONIX_ADMIN_USER", "admin")
            .env("FLUSSONIX_ADMIN_PASSWORD", ADMIN)
            .env("FLUSSONIX_PEER_KEY", PEER)
            .env_remove("FLUSSONIX_FXLINK_TRIMMED_RECORDS")
            .env_remove("FLUSSONIX_FXLINK_REPAIR_PARITY")
            .stdout(std::process::Stdio::piped())
            .stderr(std::fs::File::create(path.join(format!("{name}.log"))).unwrap())
            .process_group(0)
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut node = Self {
            child,
            url: String::new(),
            stdout: None,
        };
        let mut lines = BufReader::new(node.child.stdout.take().unwrap()).lines();
        let line = tokio::time::timeout(Duration::from_secs(10), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap_or_else(|| {
                panic!(
                    "daemon must report listeners: {}",
                    std::fs::read_to_string(path.join(format!("{name}.log"))).unwrap_or_default()
                )
            });
        let status: Value = serde_json::from_str(&line).unwrap();
        node.url = format!("http://{}", status["listen"].as_str().unwrap());
        let mut output = tokio::fs::File::create(path.join(format!("{name}.stdout.log")))
            .await
            .unwrap();
        node.stdout = Some(tokio::spawn(async move {
            while let Ok(Some(line)) = lines.next_line().await {
                output.write_all(line.as_bytes()).await.unwrap();
                output.write_all(b"\n").await.unwrap();
            }
            output.flush().await.unwrap();
        }));
        node
    }
    async fn stop(&mut self) {
        if let Some(pid) = self.child.id() {
            let _ = std::process::Command::new("kill")
                .args(["-TERM", &pid.to_string()])
                .status();
        }
        let status = tokio::time::timeout(Duration::from_secs(12), self.child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(
            status.success(),
            "owned daemon must shut down cleanly: {status}"
        );
        if let Some(mut stdout) = self.stdout.take() {
            match tokio::time::timeout(Duration::from_secs(5), &mut stdout).await {
                Ok(result) => result.unwrap(),
                Err(_) => {
                    stdout.abort();
                    panic!("owned stdout collector did not finish");
                }
            }
        }
    }
}
fn empty() -> Value {
    json!({"streams":[],"templates":[],"peers":[],"sources":[],"auth_backends":[]})
}
#[tokio::test]
async fn cli_nodes_discover_over_http_and_decode_fxlink_hls() {
    cli_nodes_case(false, false).await;
}
#[tokio::test]
async fn cli_nodes_opt_in_v6_on_source_and_client_only_cdn() {
    cli_nodes_case(true, true).await;
}
#[tokio::test]
async fn cli_nodes_one_sided_v6_opt_in_keeps_v5_in_both_directions() {
    for (source, client) in [(true, false), (false, true)] {
        cli_nodes_case(source, client).await;
    }
}
async fn cli_nodes_case(source_enabled: bool, client_enabled: bool) {
    let dir = tempfile::tempdir().unwrap();
    let cert = tls_fixture::Certificates::new();
    let mut origin_config = empty();
    origin_config["streams"] = json!([{"name":"region/news","static":false,
        "inputs":[{"url":"testsrc://"}],"flussonix_token_sha256":format!("{:x}",Sha256::digest(VIEWER.as_bytes()))}]);
    // The default96kbps AAC fixture has no records in the trimmed size range.
    // Exercise real trimmed audio while preserving the same decode/health gates.
    if source_enabled && client_enabled {
        origin_config["streams"][0]["transcoder"] = json!({"ab":192});
    }
    let mut origin_args = vec![
        "--fxlink-listen",
        "127.0.0.1:0",
        "--fxlink-cert",
        cert.cert.to_str().unwrap(),
        "--fxlink-key",
        cert.key.to_str().unwrap(),
        "--role",
        "source",
    ];
    if source_enabled {
        origin_args.push("--fxlink-trimmed-records");
    }
    let mut origin = Node::start(dir.path(), "origin", origin_config, &origin_args).await;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(40))
        .build()
        .unwrap();
    let mut cdn: Option<Node> = None;
    let outcome = AssertUnwindSafe(async {
        let report: Value = client
            .get(format!("{}/flussonix/api/v1/node", origin.url))
            .basic_auth("admin", Some(ADMIN))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(report["fxlink"]["enabled"], true);
        let endpoint = format!("fxlink://{}", report["fxlink"]["listen"].as_str().unwrap());
        let mut config = empty();
        config["sources"] = json!([{"hostname":"native-origin","api_url":origin.url,
            "cluster_key":PEER,"flussonix_transport":"fxlink","flussonix_fxlink_url":endpoint,
            "flussonix_media_tls_ca":cert.ca,"flussonix_fxlink_token":VIEWER}]);
        let mut client_args = vec!["--role", "cdn"];
        if client_enabled {
            client_args.push("--fxlink-trimmed-records");
        }
        cdn = Some(Node::start(dir.path(), "cdn", config, &client_args).await);
        let edge = &cdn.as_ref().unwrap().url;
        let path = format!("{edge}/region/news/index.m3u8?token={VIEWER}");
        let manifest = tokio::time::timeout(Duration::from_secs(40), async {
            loop {
                let response = client.get(&path).send().await.unwrap();
                if response.status().is_success() {
                    break response.text().await.unwrap();
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("native source discovery and FXLink media must produce HLS");
        let segment = manifest
            .lines()
            .find(|s| !s.is_empty() && !s.starts_with('#'))
            .unwrap();
        let segment_url = url::Url::parse(&path).unwrap().join(segment).unwrap();
        let bytes = client
            .get(segment_url)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let file = dir.path().join("received.ts");
        tokio::fs::write(&file, bytes).await.unwrap();
        let output = tokio::time::timeout(
            Duration::from_secs(15),
            tokio::process::Command::new("ffmpeg")
                .kill_on_drop(true)
                .args(["-nostdin", "-v", "error", "-xerror", "-i"])
                .arg(&file)
                .args([
                    "-map", "0:v:0", "-map", "0:a:0", "-t", "1", "-f", "null", "-",
                ])
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            output.status.success() && output.stderr.is_empty(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let status: Value = client
            .get(format!("{edge}/flussonix/api/v1/node"))
            .basic_auth("admin", Some(ADMIN))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        let status = status["streams"]
            .as_array()
            .unwrap()
            .iter()
            .find(|stream| stream["name"] == "region/news")
            .unwrap();
        assert_eq!(status["stats"]["input_protocol"], "fxlink");
        assert_eq!(status["stats"]["fxlink"]["active"], true);
        let link = &status["stats"]["fxlink"];
        let old_client =
            std::env::var_os("FXLINK_V5_REFERENCE_BINARY").is_some() && !client_enabled;
        if old_client {
            assert!(
                link["protocol_version"].is_null(),
                "reference binary unexpectedly exposes v6 telemetry"
            );
        } else {
            assert_eq!(
                link["protocol_version"],
                if source_enabled && client_enabled {
                    6
                } else {
                    5
                }
            );
        }
        if let Some(path) = std::env::var_os("FXLINK_QUAL_ARTIFACT_DIR") {
            let path = std::path::PathBuf::from(path);
            std::fs::create_dir_all(&path).unwrap();
            std::fs::write(
                path.join(format!(
                    "source{source_enabled}-client{client_enabled}-stats.json"
                )),
                serde_json::to_vec_pretty(link).unwrap(),
            )
            .unwrap();
            std::fs::copy(
                &file,
                path.join(format!("source{source_enabled}-client{client_enabled}.ts")),
            )
            .unwrap();
        }
        if source_enabled && client_enabled {
            assert!(
                link["trimmed_shards"].as_u64().unwrap() > 0,
                "v6 negotiated without exercising trimmed native media"
            );
        } else if !old_client {
            assert_eq!(link["trimmed_shards"], 0);
        }
        assert_eq!(link["expired_records"], 0);
        assert_eq!(link["queue_dropped_shards"], 0);
        assert_eq!(status["stats"]["restart_count"], 0);
    })
    .catch_unwind()
    .await;
    if outcome.is_err() {
        for name in ["origin", "cdn"] {
            eprintln!(
                "owned {name} daemon diagnostic: {}",
                std::fs::read_to_string(dir.path().join(format!("{name}.log"))).unwrap_or_default()
            );
        }
    }
    if let Some(node) = cdn.as_mut() {
        node.stop().await;
    }
    origin.stop().await;
    if let Some(path) = std::env::var_os("FXLINK_QUAL_ARTIFACT_DIR") {
        let path = std::path::PathBuf::from(path);
        std::fs::create_dir_all(&path).unwrap();
        for name in ["origin", "cdn"] {
            std::fs::copy(
                dir.path().join(format!("{name}.log")),
                path.join(format!(
                    "source{source_enabled}-client{client_enabled}-{name}.log"
                )),
            )
            .unwrap();
            std::fs::copy(
                dir.path().join(format!("{name}.stdout.log")),
                path.join(format!(
                    "source{source_enabled}-client{client_enabled}-{name}.stdout.log"
                )),
            )
            .unwrap();
        }
    }
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
