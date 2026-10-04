#[path = "support/dvb_fixture.rs"]
mod dvb;
use dvb::carrier as teletext;
use flussonix::server::{App, Options, router};
use futures_util::StreamExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
static SERIAL: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
struct Node {
    app: Arc<App>,
    url: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Node {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn node(dir: &std::path::Path, name: &str, role: &str) -> Node {
    let app = App::new(
        dir.join(format!("{name}.json")),
        dir.join(name),
        Options {
            node_name: name.into(),
            role: role.into(),
            uplink_interface: "process".into(),
            admin_password: "owned-admin".into(),
            peer_key: "owned-regional-peer".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let a = app.clone();
    let task = tokio::spawn(async move {
        use std::future::IntoFuture;
        let server = axum::serve(listener, router(a.clone())).into_future();
        let sampling = async {
            let mut tick = tokio::time::interval(Duration::from_millis(100));
            loop {
                tick.tick().await;
                a.sample_metrics();
            }
        };
        tokio::select! {r=server=>r.unwrap(),_=sampling=>{}}
    });
    Node { app, url, task }
}
struct Cdn {
    url: String,
    child: tokio::process::Child,
    client: reqwest::Client,
}
impl Drop for Cdn {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            if let Some(pid) = self.child.id() {
                let _ = std::process::Command::new("kill")
                    .args(["-TERM", "--", &format!("-{pid}")])
                    .status();
            }
        }
    }
}
impl Cdn {
    async fn start(dir: &std::path::Path) -> Self {
        let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = reserved.local_addr().unwrap();
        drop(reserved);
        let log = std::fs::File::create(dir.join("cdn-daemon.log")).unwrap();
        let child = tokio::process::Command::new(env!("CARGO_BIN_EXE_flussonix"))
            .args([
                "--listen",
                &address.to_string(),
                "--role",
                "cdn",
                "--node-name",
                "owned-regional-cdn",
                "--uplink-interface",
                "process",
            ])
            .arg("--config")
            .arg(dir.join("cdn.json"))
            .arg("--media-dir")
            .arg(dir.join("cdn"))
            .env("FLUSSONIX_ADMIN_USER", "admin")
            .env("FLUSSONIX_ADMIN_PASSWORD", "owned-admin")
            .env("FLUSSONIX_PEER_KEY", "owned-regional-peer")
            .stdout(std::process::Stdio::null())
            .stderr(log)
            .process_group(0)
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut node = Self {
            url: format!("http://{address}"),
            child,
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                assert!(
                    node.child.try_wait().unwrap().is_none(),
                    "owned CDN daemon exited"
                );
                if let Ok(response) = node.client.get(format!("{}/health", node.url)).send().await {
                    if response.status() == 200 {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await
            }
        })
        .await
        .unwrap();
        assert_eq!(node.node().await["name"], "owned-regional-cdn");
        node
    }
    async fn put_source(&self, value: Value) {
        let response = self
            .client
            .put(format!(
                "{}/streamer/api/v3/cluster/sources/origin",
                self.url
            ))
            .basic_auth("admin", Some("owned-admin"))
            .json(&value)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }
    async fn node(&self) -> Value {
        self.client
            .get(format!("{}/flussonix/api/v1/node", self.url))
            .header("X-Flussonix-Peer", "owned-regional-peer")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }
    async fn count(&self) -> usize {
        self.node().await["streams"].as_array().unwrap().len()
    }
    async fn stats(&self) -> Value {
        self.node().await["streams"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["name"] == "region/owned")
            .map(|s| s["stats"].clone())
            .unwrap_or(Value::Null)
    }
    async fn read(&self, file: &str) -> Result<bytes::Bytes, String> {
        let response = self
            .client
            .get(format!(
                "{}/region/owned/{file}?token=owned-regional-viewer",
                self.url
            ))
            .send()
            .await
            .map_err(|_| "CDN unavailable")?;
        if response.status() != 200 {
            return Err(format!("CDN {}", response.status()));
        }
        response.bytes().await.map_err(|_| "CDN body failed".into())
    }
    async fn stop(&mut self) {
        let pid = self.child.id().unwrap();
        assert!(
            std::process::Command::new("kill")
                .args(["-TERM", &pid.to_string()])
                .status()
                .unwrap()
                .success()
        );
        tokio::time::timeout(Duration::from_secs(7), self.child.wait())
            .await
            .unwrap()
            .unwrap();
    }
}
async fn run(cpu: bool, bitmap: bool) {
    let _serial = SERIAL.acquire().await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let input = if bitmap {
        dvb::transport()
    } else {
        teletext::transport()
    };
    let source = node(dir.path(), "source", "source").await;
    let mut cdn = Cdn::start(dir.path()).await;
    let lb = node(dir.path(), "lb", "lb").await;
    let captions = if bitmap {
        json!([{"dvb_page":1,"ocr_language":"eng","language":"en","name":"English"},{"dvb_page":2,"ocr_language":"deu","language":"de","name":"German"}])
    } else {
        json!([{"teletext_page":888,"language":"de","name":"German"},{"teletext_page":889,"language":"fr","name":"French"}])
    };
    let mut cfg = json!({"static":false,"inputs":[{"url":"publish://"}],"flussonix_hls_subtitles":"convert","flussonix_subtitle_tracks":"preserve","flussonix_hls_captions":captions,"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-regional-viewer"))});
    if cpu {
        cfg["transcoder"] = json!({"encoder":"libx264","vb":300});
    }
    source
        .app
        .config
        .put("streams", "region/owned", cfg.clone())
        .unwrap();
    let source_relationship = json!({"api_url":source.url,"private_payload_url":source.url,"flussonix_transport":"mpegts"});
    cdn.put_source(source_relationship.clone()).await;
    lb.app
        .config
        .put("sources", "origin", source_relationship)
        .unwrap();
    lb.app
        .config
        .put(
            "peers",
            "edge",
            json!({"api_url":cdn.url,"public_payload_url":cdn.url}),
        )
        .unwrap();
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();
    assert_eq!(
        client
            .get(format!("{}/region/owned/index.m3u8", lb.url))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(cdn.count().await, 0);
    assert_eq!(source.app.media.count().await, 0);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let n: Value = client
                .get(format!("{}/flussonix/api/v1/node", cdn.url))
                .header("X-Flussonix-Peer", "owned-regional-peer")
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if n["cpu"].as_f64().is_some_and(|v| v < 0.9)
                && n["ram"].as_f64().is_some_and(|v| v < 0.95)
                && n["uplink"].as_f64().is_some_and(|v| v < 0.8)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let response = client
        .get(format!(
            "{}/region/owned/index.m3u8?token=owned-regional-viewer",
            lb.url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 302);
    let location = response.headers()["location"].to_str().unwrap();
    assert!(location.starts_with(&cdn.url));
    assert!(!location.contains("owned-regional-peer"));
    let response = client.get(location).send().await.unwrap();
    assert_eq!(response.status(), 302);
    let canonical = format!(
        "{}{}",
        cdn.url,
        response.headers()["location"].to_str().unwrap()
    );
    let mut publication = source
        .app
        .media
        .publish_guarded("region/owned", &cfg, std::future::ready(true))
        .await
        .unwrap();
    let c = client.clone();
    let master_task = tokio::spawn(async move { c.get(canonical).send().await.unwrap() });
    let stop = CancellationToken::new();
    let captured_stop = stop.clone();
    let c = client.clone();
    let url = format!(
        "{}/region/owned/mpegts?token=owned-regional-viewer",
        cdn.url
    );
    let captured = tokio::spawn(async move {
        let response = c.get(url).send().await.unwrap();
        assert_eq!(response.status(), 200);
        let mut stream = response.bytes_stream();
        let mut bytes = Vec::new();
        loop {
            tokio::select! {biased;_=captured_stop.cancelled()=>break,chunk=stream.next()=>match chunk{Some(Ok(chunk))=>{bytes.extend(chunk);assert!(bytes.len()<4*1024*1024)},Some(Err(e))=>panic!("TS capture failed: {e}"),None=>break}}
        }
        bytes
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while publication.worker.viewers.load(Ordering::Relaxed) != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("one authorized private source subscription before feeding media");
    // These fixtures contain sixteen seconds of broadcast media. Feed a live
    // clock instead of bursting the entire programme into the bounded decoder.
    let chunks = input.chunks(188 * 32);
    let period = Duration::from_secs(16) / chunks.len() as u32;
    let mut clock = tokio::time::interval(period);
    clock.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    for chunk in chunks {
        clock.tick().await;
        publication
            .stdin
            .as_mut()
            .unwrap()
            .write_all(chunk)
            .await
            .unwrap();
    }
    let master = master_task.await.unwrap();
    assert_eq!(master.status(), 200);
    let master = master.text().await.unwrap();
    assert!(
        master.contains("TYPE=SUBTITLES"),
        "master: {master}; source: {}; CDN: {}",
        publication.worker.stats(),
        cdn.stats().await
    );
    assert!(!master.contains("owned-regional-peer"));
    let readiness = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let mut ready = true;
            for prefix in ["", "fmp4/"] {
                ready &= cdn
                    .read(&format!("{prefix}av.m3u8"))
                    .await
                    .is_ok_and(|b| String::from_utf8_lossy(&b).matches("#EXTINF:").count() >= 5);
            }
            if ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await;
    if let Err(error) = readiness {
        eprintln!(
            "source: {}, CDN: {}",
            publication.worker.stats(),
            cdn.stats().await
        );
        for prefix in ["", "fmp4/"] {
            eprintln!(
                "{prefix}playlist: {:?}",
                cdn.read(&format!("{prefix}av.m3u8"))
                    .await
                    .map(|b| String::from_utf8_lossy(&b).into_owned())
            );
        }
        panic!("CDN quiet-tail readiness failed: {error}");
    }
    let native = client
        .get(format!(
            "{}/region/owned/m4s?token=owned-regional-viewer",
            cdn.url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        native.status(),
        200,
        "generated native output must remain available through a TS source pull"
    );
    let mut stream = native.bytes_stream();
    let mut decoder = flussonix::m4s::Decoder::default();
    let mut events = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let bytes = stream.next().await.unwrap().unwrap();
            events.extend(decoder.push(&bytes).unwrap());
            if events
                .iter()
                .any(|e| matches!(e, flussonix::m4s::Event::Info { .. }))
                && events.iter().any(|e| {
                    matches!(
                        e,
                        flussonix::m4s::Event::Frame { .. } | flussonix::m4s::Event::Gop { .. }
                    )
                })
            {
                break;
            }
        }
    })
    .await
    .unwrap();
    drop(stream);
    let selectors = if bitmap {
        vec![
            ("dvb1", "EUROPE DVB", "GRÜSSE"),
            ("dvb2", "GRÜSSE", "EUROPE DVB"),
        ]
    } else {
        vec![
            ("ttx888", "GRÜSSE", "français"),
            ("ttx889", "français", "GRÜSSE"),
        ]
    };
    let mut protected = None;
    for prefix in ["", "fmp4/"] {
        for (id, expected, other) in &selectors {
            let path = format!("{prefix}{id}.m3u8");
            let url = format!(
                "{}/region/owned/{path}?token=owned-regional-viewer",
                cdn.url
            );
            let response = client.get(url).send().await.unwrap();
            assert_eq!(response.status(), 200);
            let list = response.text().await.unwrap();
            let mut words = String::new();
            let mut empty = false;
            for segment in list
                .lines()
                .filter(|s| !s.starts_with('#') && !s.is_empty())
            {
                let segment_url = format!("{}/region/owned/{prefix}{segment}", cdn.url);
                assert!(segment_url.contains("token=owned-regional-viewer"));
                let response = client.get(&segment_url).send().await.unwrap();
                assert_eq!(response.status(), 200);
                let vtt = response.text().await.unwrap();
                assert!(vtt.starts_with("WEBVTT\nX-TIMESTAMP-MAP="));
                empty |= !vtt.contains("-->");
                words += &vtt;
                protected = Some(segment_url);
            }
            assert!(
                words.contains(expected),
                "{id} {prefix}: {words}; source: {}, CDN: {}",
                publication.worker.stats(),
                cdn.stats().await
            );
            assert!(!words.contains(other), "service isolation: {words}");
            assert!(empty, "quiet tail must include empty WebVTT");
            fn seconds(value: &str) -> f64 {
                let mut parts = value.split(':');
                parts.next().unwrap().parse::<f64>().unwrap() * 3600.0
                    + parts.next().unwrap().parse::<f64>().unwrap() * 60.0
                    + parts.next().unwrap().parse::<f64>().unwrap()
            }
            let bounds: Vec<_> = words
                .lines()
                .filter_map(|line| line.split_once(" --> "))
                .map(|(start, end)| (seconds(start), seconds(end)))
                .collect();
            assert!(
                bounds
                    .iter()
                    .any(|(start, end)| (*start - 2.16).abs() < 0.04 && (*end - 3.0).abs() < 0.04),
                "source cue timing changed by more than one video frame: {words}"
            );
        }
    }
    stop.cancel();
    let bytes = captured.await.unwrap();
    let descriptors = teletext::original::descriptors(&bytes);
    let expected: Vec<&[u8]> = if bitmap {
        vec![dvb::ENG_DESC, dvb::DEU_DESC]
    } else {
        vec![
            teletext::TTX888_DESC,
            teletext::TTX889_DESC,
            teletext::original::DVB_DESC,
        ]
    };
    let original = teletext::original::descriptors(&input);
    for descriptor in expected {
        let (out_pid, _) = descriptors
            .iter()
            .find(|(_, d)| d.as_slice() == descriptor)
            .expect("regional descriptor retained through CDN");
        let (in_pid, _) = original
            .iter()
            .find(|(_, d)| d.as_slice() == descriptor)
            .unwrap();
        let payloads = teletext::original::pes_bodies(&bytes, *out_pid);
        assert!(!payloads.is_empty());
        for payload in payloads {
            assert!(
                teletext::original::pes_bodies(&input, *in_pid).contains(&payload),
                "original encoded PES changed"
            );
        }
    }
    assert_eq!(cdn.stats().await["input_protocol"], "tshttp");
    assert_eq!(cdn.count().await, 1);
    assert_eq!(source.app.media.count().await, 1);
    assert_eq!(publication.worker.viewers.load(Ordering::Relaxed), 1);
    let url = protected.unwrap();
    assert_eq!(
        client
            .get(url.split('?').next().unwrap())
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    source
        .app
        .config
        .put("streams", "region/owned", json!({"disabled":true}))
        .unwrap();
    source.app.reconcile().await;
    let denied=tokio::time::timeout(Duration::from_secs(12),async {loop {let response=client.get(&url).send().await.unwrap();if matches!(response.status().as_u16(),403|503){break response;}tokio::time::sleep(Duration::from_millis(50)).await;}}).await.expect("disabled origin must deny cached subtitle media within the existing source refresh interval");
    assert!(!denied.text().await.unwrap().starts_with("WEBVTT"));
    cdn.stop().await;
    for n in [&source, &lb] {
        n.app.media.stop_all().await;
    }
}
#[tokio::test]
async fn copy_source_relays_teletext_originals_and_converted_hls() {
    run(false, false).await;
}
#[tokio::test]
async fn cpu_source_relays_teletext_without_reencoding_at_cdn() {
    run(true, false).await;
}
#[tokio::test]
async fn copy_source_relays_dvb_originals_and_recognized_hls() {
    run(false, true).await;
}
#[tokio::test]
async fn cpu_source_relays_dvb_without_reencoding_at_cdn() {
    run(true, true).await;
}
