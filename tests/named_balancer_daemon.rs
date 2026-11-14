//! Isolated real-daemon source -> CDN -> named LB qualification over verified TLS.
use futures_util::{FutureExt, StreamExt};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::time::Duration;
#[path = "support/cluster_daemon.rs"]
mod daemon;
#[path = "support/tls.rs"]
mod tls_fixture;
use daemon::Daemon;

fn source(node: &Daemon) -> Value {
    json!({"api_url":node.url,"private_payload_url":node.url,
        "flussonix_tls_ca":node.cert.ca,"flussonix_media_tls_ca":node.cert.ca,
        "cluster_key":node.key,"flussonix_transport":"m4s"})
}

async fn run(nodes: &mut [Daemon; 3]) {
    nodes[0]
        .store()
        .put(
            "streams",
            "channel",
            json!({"static":false,
        "inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":"libx264","vb":800},
        "flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-viewer"))}),
        )
        .unwrap();
    nodes[0].start("named-source", "source").await;
    for index in [1, 2] {
        nodes[index]
            .store()
            .put("sources", "origin", source(&nodes[0]))
            .unwrap();
    }
    nodes[1].start("named-cdn", "cdn").await;
    nodes[2]
        .store()
        .put(
            "peers",
            "cdn",
            json!({"api_url":nodes[1].url,
        "public_payload_url":nodes[1].url,"private_payload_url":nodes[1].url,
        "flussonix_tls_ca":nodes[1].cert.ca,"cluster_key":nodes[1].key}),
        )
        .unwrap();
    nodes[2]
        .store()
        .put(
            "balancers",
            "lb0",
            json!({"mode":"bitrate",
        "servers":[{"name":"cdn"}]}),
        )
        .unwrap();
    nodes[2].start("named-lb", "lb").await;
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::none());
    for node in nodes.iter() {
        builder = builder.add_root_certificate(
            reqwest::Certificate::from_pem(&std::fs::read(&node.cert.ca).unwrap()).unwrap(),
        );
    }
    let client = builder.build().unwrap();
    assert_eq!(
        client
            .get(format!(
                "{}/lb0/channel/index.m3u8?token=wrong",
                nodes[2].url
            ))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert!(nodes.iter().all(|node| node.owned_encoders().is_empty()));
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let n = nodes[1].node(&client).await;
            if n["cpu"].as_f64().is_some_and(|v| v < 0.9)
                && n["ram"].as_f64().is_some_and(|v| v < 0.95)
                && n["uplink"].as_f64().is_some_and(|v| v < 0.8)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("measured CDN capacity");
    let response = client
        .get(format!(
            "{}/lb0/channel/index.m3u8?token=owned-viewer&x=%2B",
            nodes[2].url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 302);
    let location = response.headers()["location"].to_str().unwrap();
    let destination = url::Url::parse(location).unwrap();
    assert_eq!(destination.scheme(), "https");
    assert_eq!(destination.path(), "/channel/index.m3u8");
    assert_eq!(
        destination.query_pairs().find(|(k, _)| k == "x").unwrap().1,
        "+"
    );
    assert!(
        destination
            .query_pairs()
            .any(|(k, _)| k == "flussonix_ticket")
    );
    assert_eq!(
        destination.host_str(),
        url::Url::parse(&nodes[1].url).unwrap().host_str()
    );
    let redeemed = client.get(location).send().await.unwrap();
    assert_eq!(redeemed.status(), 302);
    let canonical = nodes[1].url.clone() + redeemed.headers()["location"].to_str().unwrap();
    assert_eq!(client.get(location).send().await.unwrap().status(), 503);
    let (playlist, segment) = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let response = client.get(&canonical).send().await.unwrap();
            if response.status() == 200 {
                let body = response.text().await.unwrap();
                if let Some(segment) = body
                    .lines()
                    .find(|line| !line.is_empty() && !line.starts_with('#'))
                {
                    break (body.clone(), segment.to_owned());
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("named HLS segment");
    assert!(playlist.contains("#EXTM3U"));
    let segment = url::Url::parse(&canonical).unwrap().join(&segment).unwrap();
    let response = client.get(segment).send().await.unwrap();
    assert_eq!(response.status(), 200);
    let bytes = response.bytes().await.unwrap();
    assert!(bytes.len() > 188 * 50 && bytes[0] == 0x47);
    let ts = nodes[2].directory.path().join("hls-segment.ts");
    std::fs::write(&ts, &bytes).unwrap();
    let probe = tokio::process::Command::new("/usr/bin/ffprobe")
        .args(["-v", "error", "-show_streams", "-of", "json"])
        .arg(&ts)
        .output()
        .await
        .unwrap();
    assert!(
        probe.status.success(),
        "independent HLS segment probe: {}",
        String::from_utf8_lossy(&probe.stderr)
    );
    let tracks: Value = serde_json::from_slice(&probe.stdout).unwrap();
    assert!(
        tracks["streams"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["codec_name"] == "h264"))
    );
    assert!(nodes[1].playlist().is_some());
    assert_eq!(
        client
            .get(format!("{}/lb0/channel/mpegts?token=wrong", nodes[2].url))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let routed = client
        .get(format!(
            "{}/lb0/channel/mpegts?token=owned-viewer",
            nodes[2].url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(routed.status(), 302);
    let mut endpoint = routed.headers()["location"].to_str().unwrap().to_owned();
    assert!(endpoint.starts_with(&nodes[1].url));
    assert!(endpoint.contains("/channel/mpegts?"));
    let mut response = client.get(&endpoint).send().await.unwrap();
    if response.status() == 302 {
        endpoint = url::Url::parse(&nodes[1].url)
            .unwrap()
            .join(response.headers()["location"].to_str().unwrap())
            .unwrap()
            .to_string();
        response = client.get(&endpoint).send().await.unwrap();
    }
    assert_eq!(response.status(), 200);
    let mut stream = response.bytes_stream();
    let data = tokio::time::timeout(Duration::from_secs(12), async {
        let mut data = Vec::new();
        while data.len() < 188 * 300 {
            let chunk = stream.next().await.expect("MPEG-TS stream ended").unwrap();
            data.extend_from_slice(&chunk);
            assert!(data.len() < 2 * 1024 * 1024, "bounded MPEG-TS sample");
        }
        data
    })
    .await
    .expect("MPEG-TS media sample");
    assert_eq!(data[0], 0x47);
    let ts = nodes[2].directory.path().join("continuous.ts");
    std::fs::write(&ts, data).unwrap();
    let probe = tokio::process::Command::new("/usr/bin/ffprobe")
        .args(["-v", "error", "-show_streams", "-of", "json"])
        .arg(ts)
        .output()
        .await
        .unwrap();
    assert!(
        probe.status.success(),
        "independent MPEG-TS probe: {}",
        String::from_utf8_lossy(&probe.stderr)
    );
    let tracks: Value = serde_json::from_slice(&probe.stdout).unwrap();
    assert!(
        tracks["streams"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["codec_name"] == "h264"))
    );
    assert!(nodes.iter().all(Daemon::unchanged));
    assert!(
        nodes[2].owned_encoders().is_empty(),
        "LB must never start a media worker"
    );
}

#[tokio::test]
async fn secure_named_balancer_real_daemons_deliver_hls_and_mpegts_without_an_lb_worker() {
    let mut nodes = [
        Daemon::new("named-source"),
        Daemon::new("named-cdn"),
        Daemon::new("named-lb"),
    ];
    let result = std::panic::AssertUnwindSafe(run(&mut nodes))
        .catch_unwind()
        .await;
    let mut clean = true;
    for node in nodes.iter_mut().rev() {
        clean &= node.stop().await;
    }
    assert!(clean, "all owned daemons and encoders must stop");
    if let Err(panic) = result {
        for node in &nodes {
            eprintln!(
                "owned daemon log: {}",
                std::fs::read_to_string(node.directory.path().join("daemon.log"))
                    .unwrap_or_default()
            );
        }
        std::panic::resume_unwind(panic);
    }
}
