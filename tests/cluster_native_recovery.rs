//! Real supervisor + equivalent origins + verified native TLS + LB admission.
use futures_util::FutureExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    time::{Duration, Instant},
};
#[path = "support/cluster_daemon.rs"]
mod daemon;
#[path = "support/tls.rs"]
mod tls_fixture;
use daemon::Daemon;

const STREAM: &str = "region/owned";
fn stats(node: &Value) -> Value {
    node["streams"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == STREAM)
        .map(|s| s["stats"].clone())
        .unwrap_or(Value::Null)
}
fn source(node: &Daemon, transport: &str) -> Value {
    json!({"api_url":node.url,"private_payload_url":node.url,
        "flussonix_tls_ca":node.cert.ca,"flussonix_media_tls_ca":node.cert.ca,
        "cluster_key":node.key,"flussonix_transport":transport,"flussonix_source_group":"owned-replicas"})
}
fn process(pid: u32, encoder: &str) -> Value {
    let directory = std::path::PathBuf::from(format!("/proc/{pid}"));
    assert_eq!(
        std::fs::read_link(directory.join("exe")).unwrap(),
        std::fs::canonicalize("/usr/bin/ffmpeg").unwrap()
    );
    let args: Vec<String> = std::fs::read(directory.join("cmdline"))
        .unwrap()
        .split(|b| *b == 0)
        .filter(|s| !s.is_empty())
        .map(|s| String::from_utf8(s.to_vec()).unwrap())
        .collect();
    assert!(
        args.windows(2).any(|p| p == ["-c:v", encoder]),
        "expected {encoder}: {args:?}"
    );
    let maps = std::fs::read_to_string(directory.join("maps")).unwrap();
    assert!(!maps.contains("/opt/flussonic"));
    if encoder == "copy" {
        assert!(args.windows(2).any(|p| p == ["-c:a", "copy"]));
        assert!(!args.iter().any(|arg| arg == "-vaapi_device"));
    }
    let driver: Vec<_> = maps.lines().filter_map(|line| line.split_whitespace().last())
        .filter(|path| path.contains("iHD_drv_video.so") || path.contains("libigdgmm"))
        .collect::<std::collections::BTreeSet<_>>().into_iter().map(|path| {
            json!({"path":path,"sha256":format!("{:x}",Sha256::digest(std::fs::read(path).unwrap()))})
        }).collect();
    if encoder == "h264_vaapi" {
        for pair in [
            ["-vaapi_device", "/dev/dri/renderD128"],
            ["-vf", "format=nv12,hwupload"],
            ["-rc_mode", "CQP"],
            ["-qp", "24"],
            ["-low_power", "0"],
        ] {
            assert!(args.windows(2).any(|p| p == pair));
        }
        assert!(
            driver
                .iter()
                .any(|d| d["path"].as_str().unwrap().contains("iHD_drv_video.so"))
        );
    }
    json!({"pid":pid,"arguments":args,"driver":driver})
}
async fn decode(path: &Path) -> Value {
    let probe = tokio::time::timeout(
        Duration::from_secs(20),
        tokio::process::Command::new("/usr/bin/ffprobe")
            .kill_on_drop(true)
            .args(["-v", "error", "-show_streams", "-of", "json"])
            .arg(path)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(probe.status.success() && probe.stderr.is_empty());
    let metadata: Value = serde_json::from_slice(&probe.stdout).unwrap();
    let tracks = metadata["streams"].as_array().unwrap();
    assert_eq!(tracks.len(), 2);
    assert!(
        tracks
            .iter()
            .any(|s| s["codec_name"] == "h264" && s["width"] == 640 && s["height"] == 360)
    );
    assert!(
        tracks
            .iter()
            .any(|s| s["codec_name"] == "aac" && s["sample_rate"] == "48000")
    );
    let output = tokio::time::timeout(
        Duration::from_secs(20),
        tokio::process::Command::new("/usr/bin/ffmpeg")
            .kill_on_drop(true)
            .args([
                "-nostdin",
                "-v",
                "error",
                "-xerror",
                "-err_detect",
                "explode",
                "-i",
            ])
            .arg(path)
            .args(["-map", "0:v:0", "-map", "0:a:0", "-f", "framemd5", "-"])
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
    let mut counts = [0usize; 2];
    let mut hashes: [std::collections::HashSet<String>; 2] = Default::default();
    for line in String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
    {
        let fields: Vec<_> = line.split(',').map(str::trim).collect();
        let index: usize = fields[0].parse().unwrap();
        counts[index] += 1;
        hashes[index].insert(fields[5].to_owned());
    }
    assert!(
        counts[0] >= 50 && counts[1] >= 80,
        "decoded frames {counts:?}"
    );
    assert!(hashes[0].len() >= 5 && hashes[1].len() >= 2);
    json!({"video_frames":counts[0],"audio_frames":counts[1],"strict_decoder_errors":0})
}
async fn admit(client: &reqwest::Client, lb: &Daemon, cdn: &Daemon) -> String {
    let response = client
        .get(format!("{}/{STREAM}/index.m3u8?token=owned-viewer", lb.url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 302);
    let ticket = response.headers()["location"].to_str().unwrap().to_string();
    assert!(ticket.starts_with(&cdn.url));
    assert!(ticket.contains("flussonix_ticket="));
    assert!(!ticket.contains(&cdn.key));
    let redeemed = client.get(&ticket).send().await.unwrap();
    assert_eq!(redeemed.status(), 302);
    let canonical = cdn.url.clone() + redeemed.headers()["location"].to_str().unwrap();
    assert!(!canonical.contains("flussonix_ticket="));
    assert_eq!(client.get(ticket).send().await.unwrap().status(), 503);
    canonical
}
fn playlist_generation(list: &str) -> String {
    let files: Vec<_> = list
        .lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
        .map(|line| line.split('?').next().unwrap().rsplit('/').next().unwrap())
        .collect();
    let generation = files
        .first()
        .expect("playlist contains a completed segment")
        .rsplit_once('_')
        .expect("owned segment generation prefix")
        .0;
    assert!(
        files
            .iter()
            .all(|file| file.starts_with(&format!("{generation}_"))),
        "playlist mixes worker generations"
    );
    generation.to_owned()
}
async fn playback(
    client: &reqwest::Client,
    canonical: &str,
    cdn: &Daemon,
    label: &str,
    expected_generation: &str,
) -> Value {
    let response = client.get(canonical).send().await.unwrap();
    assert_eq!(response.status(), 200);
    let list = response.text().await.unwrap();
    assert_eq!(
        playlist_generation(&list),
        expected_generation,
        "HTTP playback must serve the observed worker generation"
    );
    if label == "before" {
        std::fs::write(cdn.directory.path().join("before-playlist.m3u8"), &list).unwrap();
    }
    let segment = list
        .lines()
        .rfind(|l| !l.starts_with('#') && !l.is_empty())
        .unwrap();
    let url = url::Url::parse(canonical).unwrap().join(segment).unwrap();
    let mut anonymous = url.clone();
    anonymous.set_query(None);
    assert_eq!(client.get(anonymous).send().await.unwrap().status(), 403);
    let data = client
        .get(url.clone())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let filename = url.path_segments().unwrap().next_back().unwrap();
    let produced = cdn
        .directory
        .path()
        .join("media")
        .join(format!("{:x}", Sha256::digest(STREAM.as_bytes())))
        .join(filename);
    assert_eq!(
        Sha256::digest(&data),
        Sha256::digest(std::fs::read(produced).unwrap()),
        "decoded HTTP body must match the observed generation's completed segment"
    );
    let path = cdn.directory.path().join(format!("{label}.ts"));
    std::fs::write(&path, &data).unwrap();
    let mut report = decode(&path).await;
    report["generation"] = json!(expected_generation);
    report["segment_sha256"] = json!(format!("{:x}", Sha256::digest(&data)));
    report
}
async fn qualification(transport: &str, encoder: &str) {
    let mut nodes = [
        Daemon::new("a"),
        Daemon::new("b"),
        Daemon::new("cdn"),
        Daemon::new("lb"),
    ];
    let outcome = std::panic::AssertUnwindSafe(run(&mut nodes, transport, encoder))
        .catch_unwind()
        .await;
    let mut clean = true;
    for node in nodes.iter_mut().rev() {
        clean &= node.stop().await;
    }
    for node in nodes.iter() {
        assert!(node.owned_encoders().is_empty());
    }
    match outcome {
        Ok(report) => {
            assert!(clean, "owned daemon/encoder shutdown");
            println!(
                "cluster recovery: encoder={encoder}, transport={transport}, resume_ms={}, strict_decoded_outputs=2",
                report["automatic_resume_ms"]
            );
            if let Ok(directory) = std::env::var("FLUSSONIX_CLUSTER_RECOVERY_EVIDENCE_DIR") {
                std::fs::create_dir_all(&directory).unwrap();
                std::fs::write(
                    Path::new(&directory).join(format!("{encoder}-{transport}.json")),
                    serde_json::to_vec_pretty(&report).unwrap(),
                )
                .unwrap();
            }
        }
        Err(panic) => {
            for node in nodes.iter() {
                eprintln!(
                    "daemon log: {}",
                    std::fs::read_to_string(node.directory.path().join("daemon.log"))
                        .unwrap_or_default()
                );
            }
            std::panic::resume_unwind(panic);
        }
    }
}

#[tokio::test]
async fn daemon_recovers_equivalent_native_tls_origin_without_viewer_requests() {
    qualification("m4s", "libx264").await;
}
#[tokio::test]
#[ignore = "requires independently installed Intel H.264 VAAPI driver and render device"]
async fn daemon_recovers_gpu_origins_over_verified_m4s_tls() {
    qualification("m4s", "h264_vaapi").await;
}
#[tokio::test]
#[ignore = "requires independently installed Intel H.264 VAAPI driver and render device"]
async fn daemon_recovers_gpu_origins_over_verified_m4f_tls() {
    qualification("m4f", "h264_vaapi").await;
}

async fn run(nodes: &mut [Daemon; 4], transport: &str, encoder: &str) -> Value {
    for (index, name) in [(0, "a"), (1, "b")] {
        let transcoder = if encoder == "h264_vaapi" {
            json!({"encoder":encoder,"qp":24,"acodec":"aac","ab":96})
        } else {
            json!({"encoder":encoder,"vb":1200,"acodec":"aac","ab":96})
        };
        nodes[index]
            .store()
            .put(
                "streams",
                STREAM,
                json!({"static":false,"inputs":[{"url":"testsrc://"}],
                "transcoder":transcoder,"flussonix_content_id":"owned-equivalent-content",
                "flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-viewer"))}),
            )
            .unwrap();
        nodes[index].start(name, "source").await;
    }
    for (index, name, role) in [(2, "cdn", "cdn"), (3, "lb", "lb")] {
        for (origin, key) in [(0, "a"), (1, "b")] {
            nodes[index]
                .store()
                .put("sources", key, source(&nodes[origin], transport))
                .unwrap();
        }
        if index == 3 {
            nodes[index].store().put("peers","cdn",json!({"api_url":nodes[2].url,"public_payload_url":nodes[2].url,"private_payload_url":nodes[2].url,"flussonix_tls_ca":nodes[2].cert.ca,"cluster_key":nodes[2].key})).unwrap();
        }
        nodes[index].start(name, role).await;
    }
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(18))
        .redirect(reqwest::redirect::Policy::none());
    for node in nodes.iter() {
        builder = builder.add_root_certificate(
            reqwest::Certificate::from_pem(&std::fs::read(&node.cert.ca).unwrap()).unwrap(),
        );
    }
    let client = builder.build().unwrap();
    // An independent client with no owned CA must reject these endpoints.
    assert!(
        reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .get(format!("{}/health", nodes[2].url))
            .send()
            .await
            .is_err()
    );
    for index in [2, 3] {
        assert_eq!(
            client
                .get(format!("{}/{STREAM}/index.m3u8", nodes[index].url))
                .send()
                .await
                .unwrap()
                .status(),
            403
        );
    }
    assert!(nodes.iter().all(|n| n.owned_encoders().is_empty()));
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let n = nodes[2].node(&client).await;
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
    .expect("eligible measured CDN capacity");
    let canonical = admit(&client, &nodes[3], &nodes[2]).await;
    let initial = client.get(&canonical).send().await.unwrap();
    assert_eq!(initial.status(), 200);
    drop(initial);
    let first = stats(&nodes[2].node(&client).await);
    assert_eq!(first["upstream_source"], "a");
    assert_eq!(first["input_protocol"], format!("{transport}s"));
    let first_pid = u32::try_from(first["pid"].as_u64().unwrap()).unwrap();
    let origin_a = stats(&nodes[0].node(&client).await);
    let origin_a_pid = u32::try_from(origin_a["pid"].as_u64().unwrap()).unwrap();
    let before_origin = process(origin_a_pid, encoder);
    let before_cdn = process(first_pid, "copy");
    assert_eq!(nodes[0].owned_encoders(), [origin_a_pid]);
    assert_eq!(nodes[2].owned_encoders(), [first_pid]);
    assert!(nodes[1].owned_encoders().is_empty());
    assert!(nodes[3].owned_encoders().is_empty());
    let before_playlist = nodes[2].playlist().unwrap();
    let old_generation = playlist_generation(&before_playlist);
    let before_decode = playback(&client, &canonical, &nodes[2], "before", &old_generation).await;
    let fault = Instant::now();
    assert!(
        nodes[0].stop().await,
        "origin shutdown must reap its encoder"
    );
    assert!(!Path::new(&format!("/proc/{origin_a_pid}")).exists());
    // Only read-only node GETs below. No playback request can trigger recovery.
    let (second, observed_playlist) = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let s = stats(&nodes[2].node(&client).await);
            let playlist = nodes[2].playlist().unwrap_or_default();
            if s["upstream_source"] == "b"
                && s["source_switches"] == 1
                && s["status"] == "running"
                && s["pid"] != first["pid"]
                && s["bytes_in"].as_u64().is_some_and(|v| v > 250_000)
                && playlist
                    .lines()
                    .any(|l| !l.starts_with('#') && !l.is_empty())
                && playlist_generation(&playlist) != old_generation
            {
                break (s, playlist);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("real supervisor switches and produces fresh media without viewer requests");
    assert_eq!(second["input_protocol"], format!("{transport}s"));
    let automatic_resume_ms = fault.elapsed().as_millis();
    let second_pid = u32::try_from(second["pid"].as_u64().unwrap()).unwrap();
    assert!(!Path::new(&format!("/proc/{first_pid}")).exists());
    let origin_b = stats(&nodes[1].node(&client).await);
    let origin_b_pid = u32::try_from(origin_b["pid"].as_u64().unwrap()).unwrap();
    let after_origin = process(origin_b_pid, encoder);
    let after_cdn = process(second_pid, "copy");
    assert_eq!(nodes[1].owned_encoders(), [origin_b_pid]);
    assert_eq!(nodes[2].owned_encoders(), [second_pid]);
    assert!(nodes[3].owned_encoders().is_empty());
    let new_generation = playlist_generation(&observed_playlist);
    let after_decode = playback(&client, &canonical, &nodes[2], "after", &new_generation).await;
    let new_canonical = admit(&client, &nodes[3], &nodes[2]).await;
    let concurrent =
        futures_util::future::join_all((0..6).map(|_| client.get(&new_canonical).send())).await;
    assert!(concurrent.into_iter().all(|r| r.unwrap().status() == 200));
    tokio::time::sleep(Duration::from_secs(11)).await;
    let sticky = stats(&nodes[2].node(&client).await);
    assert_eq!(sticky["pid"], second["pid"]);
    assert_eq!(sticky["upstream_source"], "b");
    assert_eq!(sticky["source_switches"], 1);
    for index in [2, 3] {
        assert_eq!(
            client
                .get(format!("{}/{STREAM}/index.m3u8", nodes[index].url))
                .send()
                .await
                .unwrap()
                .status(),
            403
        );
    }
    assert!(nodes.iter().all(Daemon::unchanged));
    json!({"transport":transport,"encoder":encoder,"automatic_resume_ms":automatic_resume_ms,
            "before_origin":before_origin,"after_origin":after_origin,"before_cdn":before_cdn,"after_cdn":after_cdn,
            "before_decode":before_decode,"after_decode":after_decode,"source_switches":1,"read_only_recovery_observation":true})
}
