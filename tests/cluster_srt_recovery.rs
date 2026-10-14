//! Real HTTPS lookup + encrypted SRT reconnect across equivalent native origins.
use futures_util::FutureExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{path::Path, process::Stdio, time::Duration};
#[path = "support/cluster_daemon.rs"]
mod daemon;
#[path = "support/tls.rs"]
mod tls_fixture;
use daemon::Daemon;

const STREAM: &str = "owned/srt-recovery";
const TOKEN: &str = "owned%2B+viewer";
const PASSPHRASE: &str = "owned-encrypted-srt-recovery";

fn source(origin: &Daemon) -> Value {
    json!({"api_url":origin.url,"private_payload_url":origin.url,
        "flussonix_tls_ca":origin.cert.ca,"flussonix_media_tls_ca":origin.cert.ca,
        "cluster_key":origin.key,"flussonix_source_group":"owned-equivalent-origins"})
}
fn stats(n: &Value) -> Value {
    n["streams"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == STREAM)
        .map(|s| s["stats"].clone())
        .unwrap_or(Value::Null)
}
async fn lookup(client: &reqwest::Client, lb: &Daemon, token: &str) -> reqwest::Response {
    client
        .get(format!("{}/flussonix/api/v1/srt-lookup", lb.url))
        .query(&[("streamid", format!("#!::r={STREAM},u={token},a=opaque"))])
        .send()
        .await
        .unwrap()
}
async fn fresh(client: &reqwest::Client, lb: &Daemon, cdn: &Daemon) -> Value {
    let response = lookup(client, lb, TOKEN).await;
    assert_eq!(
        response.status(),
        200,
        "fresh lookup: {}",
        response.text().await.unwrap()
    );
    assert_eq!(response.headers()["cache-control"], "no-store");
    let value: Value = response.json().await.unwrap();
    assert_eq!(value["address"], cdn.srt_url.as_deref().unwrap());
    assert_eq!(value["encryption_required"], true);
    let id = value["streamid"].as_str().unwrap();
    assert!(id.contains(&format!("u={TOKEN},")));
    assert!(id.contains(",flussonix_ticket="));
    for secret in [lb.key.as_str(), cdn.key.as_str(), PASSPHRASE] {
        assert!(!value.to_string().contains(secret));
    }
    value
}
fn receiver(value: &Value, path: &Path, seconds: &str) -> tokio::process::Child {
    let log = std::fs::File::create(path.with_extension("log")).unwrap();
    tokio::process::Command::new("/usr/bin/ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-y",
            "-threads",
            "1",
            "-analyzeduration",
            // A reconnect joins a live GOP; cover the encoder's two-second GOP.
            "3000000",
            "-probesize",
            "1048576",
            "-srt_streamid",
            value["streamid"].as_str().unwrap(),
            "-passphrase",
            PASSPHRASE,
            "-i",
        ])
        .arg(format!(
            "{}?mode=caller&latency=120000&connect_timeout=1000&timeout=5000000",
            value["address"].as_str().unwrap()
        ))
        .args(["-t", seconds, "-map", "0", "-c", "copy", "-f", "mpegts"])
        .arg(path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log)
        .kill_on_drop(true)
        .spawn()
        .unwrap()
}
async fn receive(value: &Value, path: &Path) -> bool {
    let mut child = receiver(value, path, "3");
    match tokio::time::timeout(Duration::from_secs(20), child.wait()).await {
        Ok(result) => result.unwrap().success(),
        Err(_) => {
            child.kill().await.unwrap();
            child.wait().await.unwrap();
            false
        }
    }
}
async fn decode(path: &Path, audio: &str) {
    let diagnostic = path.with_extension("decode.log");
    let probe = tokio::time::timeout(
        Duration::from_secs(12),
        tokio::process::Command::new("/usr/bin/ffprobe")
            .args([
                "-v",
                "error",
                "-show_streams",
                "-count_frames",
                "-of",
                "json",
            ])
            .arg(path)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(probe.status.success() && probe.stderr.is_empty());
    let value: Value = serde_json::from_slice(&probe.stdout).unwrap();
    let streams = value["streams"].as_array().unwrap();
    assert_eq!(streams.len(), 2);
    assert!(
        streams.iter().all(|s| s["nb_read_frames"]
            .as_str()
            .and_then(|s| s.parse::<u64>().ok())
            .is_some_and(|frames| frames > 0)),
        "both tracks must decode nonzero frames"
    );
    assert_eq!(
        streams.iter().find(|s| s["codec_type"] == "video").unwrap()["codec_name"],
        "hevc"
    );
    assert_eq!(
        streams.iter().find(|s| s["codec_type"] == "audio").unwrap()["codec_name"],
        audio
    );
    let mut decoder = tokio::process::Command::new("/usr/bin/ffmpeg")
        .args(["-v", "error", "-xerror", "-threads", "1", "-i"])
        .arg(path)
        .args(["-map", "0:v:0", "-map", "0:a:0", "-f", "null", "-"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(&diagnostic).unwrap())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(12), decoder.wait()).await;
    if result.is_err() {
        decoder.kill().await.unwrap();
        decoder.wait().await.unwrap();
    }
    assert!(
        result.is_ok_and(|r| r.unwrap().success()),
        "strict independent A/V decode at {}: {}",
        path.display(),
        std::fs::read_to_string(diagnostic).unwrap()
    );
}
async fn recovered(client: &reqwest::Client, cdn: &Daemon, previous: &Value) -> Value {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let current = stats(&cdn.node(client).await);
            if current["upstream_source"] == "b"
                && current["source_switches"].as_u64().is_some_and(|n| n >= 1)
                && current["status"] == "running"
                && current["pid"].is_u64()
                && current["pid"] != previous["pid"]
                && current["bytes_in"].as_u64().is_some_and(|n| n > 100_000)
                && cdn
                    .playlist()
                    .is_some_and(|p| p.lines().any(|l| !l.is_empty() && !l.starts_with('#')))
            {
                return current;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("supervisor must recover fresh equivalent-origin media without a viewer request")
}
async fn run(
    nodes: &mut [Daemon; 4],
    transport: &str,
    audio: &str,
    active: &mut Option<tokio::process::Child>,
) {
    for (index, name) in [(0, "a"), (1, "b")] {
        nodes[index].store().put("streams",STREAM,json!({"static":false,"inputs":[{"url":"testsrc://"}],
            "transcoder":{"encoder":"libx265","vb":1200,"acodec":if audio=="mp2"{"mp2a"}else{"mp3"},"ab":if audio=="mp2"{192}else{128}},
            "flussonix_content_id":"owned-equivalent-srt-content",
            "flussonix_token_sha256":format!("{:x}",Sha256::digest(TOKEN.as_bytes()))})).unwrap();
        nodes[index].start(name, "source").await;
    }
    for (index, name, role) in [(2, "cdn", "cdn"), (3, "lb", "lb")] {
        for (origin, name) in [(0, "a"), (1, "b")] {
            let mut cfg = source(&nodes[origin]);
            cfg["flussonix_transport"] = json!(transport);
            nodes[index].store().put("sources", name, cfg).unwrap();
        }
        if index == 2 {
            nodes[index].start_srt(name, role, PASSPHRASE).await;
        } else {
            nodes[index].store().put("peers","cdn",json!({"api_url":nodes[2].url,"flussonix_tls_ca":nodes[2].cert.ca,
                "cluster_key":nodes[2].key,"flussonix_srt_url":nodes[2].srt_url,"flussonix_srt_encrypted":true})).unwrap();
            nodes[index].start(name, role).await;
        }
    }
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(16))
        .redirect(reqwest::redirect::Policy::none());
    for n in nodes.iter() {
        builder = builder.add_root_certificate(
            reqwest::Certificate::from_pem(&std::fs::read(&n.cert.ca).unwrap()).unwrap(),
        );
    }
    let client = builder.build().unwrap();
    assert!(
        reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .get(format!("{}/health", nodes[3].url))
            .send()
            .await
            .is_err()
    );
    assert_eq!(
        lookup(&client, &nodes[3], "wrong-viewer").await.status(),
        403
    );
    assert!(nodes.iter().all(|n| n.owned_encoders().is_empty()));
    tokio::time::timeout(Duration::from_secs(12), async {
        while !nodes[2].node(&client).await["cpu"]
            .as_f64()
            .is_some_and(|n| n < 0.9)
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    let first = fresh(&client, &nodes[3], &nodes[2]).await;
    assert!(
        nodes.iter().all(|n| n.owned_encoders().is_empty()),
        "lookup must reserve only"
    );
    let capture = nodes[2].directory.path().join("before.ts");
    assert!(
        receive(&first, &capture).await,
        "encrypted initial SRT delivery"
    );
    decode(&capture, audio).await;
    let live = nodes[2].directory.path().join("active-at-failure.ts");
    let active_ticket = fresh(&client, &nodes[3], &nodes[2]).await;
    *active = Some(receiver(&active_ticket, &live, "30"));
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            assert!(
                active.as_mut().unwrap().try_wait().unwrap().is_none(),
                "live SRT caller exited before the injected failure"
            );
            if std::fs::metadata(&live).is_ok_and(|m| m.len() > 32_768) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("independent caller must receive media before origin shutdown");
    let before = stats(&nodes[2].node(&client).await);
    assert_eq!(before["upstream_source"], "a");
    assert!(nodes[3].owned_encoders().is_empty());
    let before_pid = before["pid"].as_u64().unwrap();
    assert!(
        nodes[0].stop().await,
        "stop/reap only the owned failed origin"
    );
    let closing = async {
        let caller = active.as_mut().unwrap();
        let closed = tokio::time::timeout(Duration::from_secs(8), caller.wait()).await;
        if closed.is_err() {
            caller.kill().await.unwrap();
            caller.wait().await.unwrap();
        }
        assert!(
            closed.is_ok_and(|r| r.is_ok()),
            "old SRT caller must close before its normal capture deadline, without waiting for recovery"
        );
    };
    let (after, ()) = tokio::join!(recovered(&client, &nodes[2], &before), closing);
    active.take();
    assert!(
        !Path::new(&format!("/proc/{before_pid}")).exists(),
        "failed CDN worker reaped"
    );
    assert!(nodes[3].owned_encoders().is_empty());
    let replay = nodes[2].directory.path().join("spent.ts");
    assert!(!receive(&first, &replay).await);
    assert_eq!(std::fs::metadata(&replay).map(|m| m.len()).unwrap_or(0), 0);
    let second = fresh(&client, &nodes[3], &nodes[2]).await;
    assert_ne!(second["streamid"], first["streamid"]);
    let capture = nodes[2].directory.path().join("after.ts");
    assert!(
        receive(&second, &capture).await,
        "fresh encrypted SRT after automatic failover"
    );
    decode(&capture, audio).await;
    assert_eq!(
        stats(&nodes[2].node(&client).await)["pid"],
        after["pid"],
        "reconnect shares automatically recovered worker"
    );
    assert!(nodes[1].stop().await);
    tokio::time::timeout(Duration::from_secs(16), async {
        loop {
            if stats(&nodes[2].node(&client).await)["source_available"] == false
                && nodes[2].owned_encoders().is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("blackout stops stale native media");
    let unavailable = lookup(&client, &nodes[3], TOKEN).await;
    assert!(
        matches!(unavailable.status().as_u16(), 404 | 503),
        "lookup must reject unavailable source: {}",
        unavailable.status()
    );
    assert!(nodes[2].owned_encoders().is_empty());
    nodes[1].start("b", "source").await;
    recovered(&client, &nodes[2], &after).await;
    let restored = fresh(&client, &nodes[3], &nodes[2]).await;
    let capture = nodes[2].directory.path().join("restored.ts");
    assert!(
        receive(&restored, &capture).await,
        "fresh encrypted SRT after blackout restoration"
    );
    decode(&capture, audio).await;
    assert!(nodes[0].owned_encoders().is_empty());
    assert!(nodes[3].owned_encoders().is_empty());
}
async fn qualification(transport: &str, audio: &str) {
    let mut nodes = [
        Daemon::new("a"),
        Daemon::new("b"),
        Daemon::new("cdn"),
        Daemon::new("lb"),
    ];
    let mut active = None;
    let result = std::panic::AssertUnwindSafe(run(&mut nodes, transport, audio, &mut active))
        .catch_unwind()
        .await;
    let mut clean = true;
    if let Some(mut caller) = active {
        if caller.try_wait().unwrap().is_none() {
            caller.kill().await.unwrap();
        }
        caller.wait().await.unwrap();
    }
    for n in nodes.iter_mut().rev() {
        clean &= n.stop().await;
    }
    if result.is_err()
        && let Some(artifacts) = std::env::var_os("FLUSSONIX_SRT_RECOVERY_ARTIFACT_DIR")
    {
        let directory = std::path::PathBuf::from(artifacts)
            .join(format!("{transport}-{audio}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        // Owned synthetic captures/logs only; never copy configs or TLS keys.
        for (index, node) in nodes.iter().enumerate() {
            for file in std::fs::read_dir(node.directory.path()).unwrap().flatten() {
                if file
                    .path()
                    .extension()
                    .is_some_and(|e| e == "ts" || e == "log")
                {
                    std::fs::copy(
                        file.path(),
                        directory.join(format!("{index}-{}", file.file_name().to_string_lossy())),
                    )
                    .unwrap();
                }
            }
        }
        eprintln!("Owned SRT failure evidence: {}", directory.display());
    }
    assert!(
        nodes.iter().all(|n| n.unchanged()),
        "owned daemon configs unchanged"
    );
    assert!(
        clean,
        "all owned daemon and encoder processes must be reaped"
    );
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
#[tokio::test]
async fn encrypted_srt_reconnects_after_m4f_hevc_mp2_failover_and_blackout() {
    qualification("m4f", "mp2").await;
}
#[tokio::test]
async fn encrypted_srt_reconnects_after_m4f_hevc_mp3_failover_and_blackout() {
    qualification("m4f", "mp3").await;
}
#[tokio::test]
async fn encrypted_srt_reconnects_after_m4s_hevc_mp2_failover_and_blackout() {
    qualification("m4s", "mp2").await;
}
#[tokio::test]
async fn encrypted_srt_reconnects_after_m4s_hevc_mp3_failover_and_blackout() {
    qualification("m4s", "mp3").await;
}
