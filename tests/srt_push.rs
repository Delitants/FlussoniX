use flussonix::config::ConfigStore;
use serde_json::json;
use std::{path::Path, sync::atomic::Ordering, time::Duration};
use tokio::process::Command;

static MEDIA_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn unused_udp_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn receiver(path: &Path, port: u16, secret: &str) -> tokio::process::Child {
    let mut cmd = Command::new("ffmpeg");
    let url = format!(
        "srt://127.0.0.1:{port}?mode=listener&listen_timeout=15000000&timeout=5000000&enforced_encryption=1"
    );
    cmd.args(["-passphrase", secret]);
    let log = std::fs::File::create(path.with_extension("log")).unwrap();
    let child = cmd
        .args([
            "-hide_banner",
            "-loglevel",
            "verbose",
            "-y",
            "-f",
            "mpegts",
            "-i",
            &url,
            "-map",
            "0:v?",
            "-map",
            "0:a?",
            "-t",
            "2",
            "-c",
            "copy",
            "-f",
            "mpegts",
        ])
        .arg(path)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::from(log))
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    child
}

async fn received(child: &mut tokio::process::Child, path: &Path, video: &str, audio: &str) {
    assert!(
        tokio::time::timeout(Duration::from_secs(20), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    let result = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "stream=codec_name",
            "-of",
            "json",
        ])
        .arg(path)
        .output()
        .await
        .unwrap();
    assert!(result.status.success());
    let tracks: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    let codecs: Vec<_> = tracks["streams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["codec_name"].as_str().unwrap())
        .collect();
    assert!(codecs.contains(&video), "missing {video}: {codecs:?}");
    assert!(codecs.contains(&audio), "missing {audio}: {codecs:?}");
    let decoded = Command::new("ffmpeg")
        .args(["-v", "error", "-xerror", "-threads", "2", "-i"])
        .arg(path)
        .args(["-map", "0:v:0", "-map", "0:a:0", "-f", "null", "-"])
        .output()
        .await
        .unwrap();
    assert!(
        decoded.status.success(),
        "{}",
        String::from_utf8_lossy(&decoded.stderr)
    );
}

async fn wait_push(w: &flussonix::media::Worker, index: usize, state: &str) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let s = w.stats()["flussonix_pushes"][index].clone();
            if s["status"] == state {
                return s;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("push never reached {state}: {}", w.stats()))
}

#[tokio::test]
async fn encrypted_srt_delivers_h264_hevc_with_aac_layer_ii_and_mp3() {
    let _lock = MEDIA_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let engine = flussonix::media::Engine::new(dir.path().join("media"), "ffmpeg");
    for (video, encoder) in [("h264", "libx264"), ("hevc", "libx265")] {
        for (audio, acodec, ab) in [
            ("aac", "aac", 96),
            ("mp2", "mp2a", 192),
            ("mp3", "mp3", 128),
        ] {
            let port = unused_udp_port();
            let path = dir.path().join(format!("{video}-{audio}.ts"));
            let mut rx = receiver(&path, port, "owned-secret-123").await;
            let cfg = json!({"inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":encoder,"acodec":acodec,"ab":ab},"pushes":[{"url":format!("srt://127.0.0.1:{port}"),"streamid":"#!::r=owned,m=publish,password=owned-sensitive-id","passphrase":"owned-secret-123","latency":200,"retry_timeout":1}]});
            let worker = engine.ensure("owned", &cfg).await.unwrap();
            let s = wait_push(&worker, 0, "sending").await;
            assert!(s["muxed_bytes"].as_u64().unwrap() > 0);
            assert!(!worker.stats().to_string().contains("owned-secret"));
            assert!(!worker.stats().to_string().contains("owned-sensitive-id"));
            received(&mut rx, &path, video, audio).await;
            engine.stop_all().await;
            assert!(!worker.alive.load(Ordering::Relaxed));
            assert_eq!(worker.stats()["flussonix_pushes"][0]["pid"], 0);
        }
    }
}

#[tokio::test]
async fn unavailable_destination_does_not_block_healthy_output_and_reconnects() {
    let _lock = MEDIA_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let port = unused_udp_port();
    let bad = unused_udp_port();
    let path = dir.path().join("first.ts");
    let mut rx = receiver(&path, port, "").await;
    let engine = flussonix::media::Engine::new(dir.path().join("media"), "ffmpeg");
    let cfg = json!({"inputs":[{"url":"testsrc://"}],"pushes":[{"url":format!("srt://127.0.0.1:{port}"),"retry_timeout":1},{"url":format!("srt://127.0.0.1:{bad}"),"connect_timeout":1,"retry_timeout":1,"passphrase":"owned-unreachable-secret"}]});
    let w = engine.ensure("owned", &cfg).await.unwrap();
    wait_push(&w, 1, "retrying").await;
    received(&mut rx, &path, "h264", "aac").await;
    assert!(w.alive.load(Ordering::Relaxed));
    assert_eq!(engine.count().await, 1);
    assert!(!engine.read("owned", "index.m3u8").await.unwrap().is_empty());
    let second = dir.path().join("second.ts");
    let mut rx = receiver(&second, port, "").await;
    received(&mut rx, &second, "h264", "aac").await;
    assert!(
        w.stats()["flussonix_pushes"][0]["attempts"]
            .as_u64()
            .unwrap()
            >= 2
    );
    let pids: Vec<u64> = w.stats()["flussonix_pushes"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|s| s["pid"].as_u64())
        .filter(|p| *p > 0)
        .collect();
    tokio::time::timeout(Duration::from_secs(3), engine.stop_all())
        .await
        .unwrap();
    for pid in pids {
        assert!(!Path::new(&format!("/proc/{pid}")).exists());
    }
    assert_eq!(w.stats()["flussonix_pushes"][0]["status"], "stopped");
}

#[tokio::test]
async fn wrong_passphrase_fails_closed_and_replacement_reaps_old_pushes() {
    let _lock = MEDIA_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let port = unused_udp_port();
    let path = dir.path().join("wrong.ts");
    let mut rx = receiver(&path, port, "owned-right-secret").await;
    let engine = flussonix::media::Engine::new(dir.path().join("media"), "ffmpeg");
    let cfg = json!({"inputs":[{"url":"testsrc://"}],"pushes":[{"url":format!("srt://127.0.0.1:{port}"),"passphrase":"owned-wrong-secret","connect_timeout":1,"retry_timeout":1}]});
    let old = engine.ensure("owned", &cfg).await.unwrap();
    wait_push(&old, 0, "retrying").await;
    assert!(old.alive.load(Ordering::Relaxed));
    assert!(std::fs::metadata(&path).is_err());
    let mut replacement = cfg.clone();
    replacement["pushes"][0]["disabled"] = json!(true);
    let new = engine.ensure("owned", &replacement).await.unwrap();
    assert!(!std::sync::Arc::ptr_eq(&old, &new));
    assert!(!old.alive.load(Ordering::Relaxed));
    assert_eq!(old.stats()["flussonix_pushes"][0]["pid"], 0);
    assert_eq!(new.stats()["flussonix_pushes"][0]["status"], "disabled");
    assert_eq!(new.stats()["flussonix_pushes"][0]["attempts"], 0);
    engine.stop_all().await;
    let _ = rx.kill().await;
    let _ = rx.wait().await;
}

#[tokio::test]
async fn configured_push_starts_on_demand_but_disabled_and_publication_wait() {
    let _lock = MEDIA_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let store = ConfigStore::open(dir.path().join("config.json")).unwrap();
    let cfg = json!({"static":false,"inputs":[{"url":"testsrc://"}],"pushes":[{"url":format!("srt://127.0.0.1:{}",unused_udp_port()),"connect_timeout":1}]});
    store.put("streams", "owned", cfg.clone()).unwrap();
    let mut disabled = cfg.clone();
    disabled["pushes"][0]["disabled"] = json!(true);
    store.put("streams", "disabled", disabled).unwrap();
    store
        .put(
            "streams",
            "publication",
            json!({"static":false,"inputs":[{"url":"publish://"}],"pushes":cfg["pushes"]}),
        )
        .unwrap();
    let app = flussonix::server::App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        flussonix::server::Options {
            admin_password: "owned-admin-password".into(),
            peer_key: "owned-peer-secret".into(),
            ..Default::default()
        },
    )
    .unwrap();
    app.reconcile().await;
    assert_eq!(app.media.count().await, 1);
    assert_ne!(app.media.stats("owned").await["status"], "waiting");
    assert_eq!(app.media.stats("disabled").await["status"], "waiting");
    assert_eq!(app.media.stats("publication").await["status"], "waiting");
    app.media.stop_all().await;
}

#[cfg(unix)]
#[tokio::test]
async fn stalled_push_is_reaped_while_the_shared_worker_keeps_running() {
    use std::os::unix::fs::PermissionsExt;
    let _lock = MEDIA_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let wrapper = dir.path().join("owned-ffmpeg-wrapper");
    std::fs::write(&wrapper,"#!/usr/bin/python3\nimport os,sys,time\nif '-progress' in sys.argv: time.sleep(30)\nelse: os.execv('/usr/bin/ffmpeg',['ffmpeg']+sys.argv[1:])\n").unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    let engine = flussonix::media::Engine::new(dir.path().join("media"), wrapper.to_str().unwrap());
    let cfg = json!({"inputs":[{"url":"testsrc://"}],"pushes":[{"url":"srt://127.0.0.1:19999","retry_timeout":5}]});
    let w = engine.ensure("owned", &cfg).await.unwrap();
    let pid = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(pid) = w.stats()["flussonix_pushes"][0]["pid"]
                .as_u64()
                .filter(|p| *p > 0)
            {
                break pid;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let s = wait_push(&w, 0, "retrying").await;
    assert_eq!(s["last_error"], "push_stalled");
    assert!(!Path::new(&format!("/proc/{pid}")).exists());
    assert!(w.alive.load(Ordering::Relaxed));
    assert!(w.bytes.load(Ordering::Relaxed) > 0);
    let _ = wait_push(&w, 0, "connecting").await;
    let started = std::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(2), engine.stop_all())
        .await
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(w.stats()["flussonix_pushes"][0]["pid"], 0);
}

#[test]
fn destinations_inherit_persist_and_explicit_empty_disables_them() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("config.json");
    let store = ConfigStore::open(&path).unwrap();
    let push = json!({"url":"srt://127.0.0.1:19992","streamid":"#!::r=owned,m=publish","passphrase":"owned-secret-123","latency":250,"connect_timeout":2});
    store
        .put(
            "templates",
            "owned",
            json!({"static":false,"inputs":[{"url":"testsrc://"}],"pushes":[push]}),
        )
        .unwrap();
    store
        .put("streams", "one", json!({"template":"owned"}))
        .unwrap();
    assert_eq!(store.effective("one").unwrap()["pushes"][0], push);
    drop(store);
    let store = ConfigStore::open(&path).unwrap();
    assert_eq!(store.effective("one").unwrap()["pushes"][0], push);
    store.put("streams", "one", json!({"pushes":[]})).unwrap();
    assert_eq!(store.effective("one").unwrap()["pushes"], json!([]));
    store.put("streams", "one", json!({"pushes":null})).unwrap();
    assert_eq!(store.effective("one").unwrap()["pushes"][0], push);
}

#[test]
fn raw_and_encoded_stream_ids_are_accepted_without_accepting_hidden_options() {
    let d = tempfile::tempdir().unwrap();
    let store = ConfigStore::open(d.path().join("config.json")).unwrap();
    for url in [
        "srt://127.0.0.1:19992?streamid=#!::r=owned,m=publish&latency=250",
        "srt://[::1]:19992?streamid=%23!%3A%3Ar%3Downed%2Cm%3Dpublish",
        "srt://receiver.example:9000?mode=caller&passphrase=owned-secret-123",
    ] {
        store
            .put(
                "streams",
                "owned",
                json!({"inputs":[{"url":"testsrc://"}],"pushes":[{"url":url}]}),
            )
            .unwrap();
    }
    let saved = store.snapshot();
    for url in [
        "srt://127.0.0.1:19992?streamid=#!::r=owned&unknown=owned-secret-123",
        "srt://127.0.0.1:19992?passphrase=owned-secret-123&passphrase=second-secret",
        "srt://127.0.0.1:19992?mode=listener",
        "srt://127.0.0.1:19992?streamid=%FF",
        "srt://127.0.0.1:19992?streamid=%ZZ",
        "srt://127.0.0.1:19992?pbkeylen=0",
        "srt://127.0.0.1:19992/path",
        "srt://user:owned-secret-123@receiver:9000",
        "hlss://receiver:9000/index.m3u8",
    ] {
        let err = store
            .put("streams", "owned", json!({"pushes":[{"url":url}]}))
            .unwrap_err();
        assert!(!err.contains("owned-secret-123"));
        assert_eq!(store.snapshot(), saved);
    }
}

#[test]
fn invalid_push_profiles_are_rejected_atomically_without_secrets() {
    let d = tempfile::tempdir().unwrap();
    let store = ConfigStore::open(d.path().join("config.json")).unwrap();
    store
        .put("streams", "owned", json!({"inputs":[{"url":"testsrc://"}]}))
        .unwrap();
    let saved = store.snapshot();
    for patch in [
        json!({"pushes":{}}),
        json!({"pushes":[{"url":"srt://receiver:9000","latency":0}]}),
        json!({"pushes":[{"url":"srt://receiver:9000","connect_timeout":0}]}),
        json!({"pushes":[{"url":"srt://receiver:9000","retry_timeout":301}]}),
        json!({"pushes":[{"url":"srt://receiver:9000","passphrase":"short"}]}),
        json!({"pushes":[{"url":"srt://receiver:9000","enforcedencryption":false}]}),
        json!({"pushes":[{"url":"srt://receiver:9000?latency=100","latency":200}]}),
        json!({"pushes":[{"url":"srt://receiver:9000","disabled":"true"}]}),
        json!({"pushes":[{"url":"srt://receiver:9000","streamid":"x".repeat(513)}]}),
        json!({"pushes":vec![json!({"url":"srt://receiver:9000"});5]}),
    ] {
        assert!(store.put("streams", "owned", patch).is_err());
        assert_eq!(store.snapshot(), saved);
    }
    for patch in [
        json!({"pushes":[{"url":"srt://receiver:9000","latency":10000,"connect_timeout":30,"retry_timeout":300,"passphrase":"x".repeat(79)}]}),
        json!({"pushes":[{"url":"srt://receiver:9000","disabled":true}]}),
        json!({"pushes":[]}),
    ] {
        store.put("streams", "owned", patch).unwrap();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn a_long_connection_timeout_is_not_cut_short_by_the_output_watchdog() {
    use std::os::unix::fs::PermissionsExt;
    let _lock = MEDIA_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let wrapper = dir.path().join("owned-pending-handshake");
    std::fs::write(&wrapper,"#!/usr/bin/python3\nimport os,sys,time\nif '-progress' in sys.argv:\n while True:\n  os.read(0,16384)\nelse: os.execv('/usr/bin/ffmpeg',['ffmpeg']+sys.argv[1:])\n").unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    let engine = flussonix::media::Engine::new(dir.path().join("media"), wrapper.to_str().unwrap());
    let cfg = json!({"inputs":[{"url":"testsrc://"}],"pushes":[{"url":"srt://127.0.0.1:19999","connect_timeout":20}]});
    let w = engine.ensure("owned", &cfg).await.unwrap();
    tokio::time::sleep(Duration::from_millis(11000)).await;
    let s = w.stats()["flussonix_pushes"][0].clone();
    engine.stop_all().await;
    assert_eq!(s["status"], "connecting");
    assert_eq!(s["attempts"], 1);
}

#[path = "support/dvb_fixture.rs"]
mod subtitle_fixture;

#[tokio::test]
async fn srt_keeps_or_filters_original_dvb_tracks_from_http_publication() {
    use tokio::io::AsyncWriteExt;
    let _lock = MEDIA_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let engine = flussonix::media::Engine::new(dir.path().join("media"), "ffmpeg");
    let transport = subtitle_fixture::transport();
    for keep in [true, false] {
        let port = unused_udp_port();
        let path = dir
            .path()
            .join(if keep { "kept.ts" } else { "filtered.ts" });
        let mut rx = Command::new("ffmpeg")
            .args([
                "-v",
                "error",
                "-y",
                "-f",
                "mpegts",
                "-i",
                &format!(
                    "srt://127.0.0.1:{port}?mode=listener&listen_timeout=10000000&timeout=5000000"
                ),
                "-map",
                "0",
                "-c",
                "copy",
                "-max_interleave_delta",
                "100000",
                "-t",
                "6",
                "-f",
                "mpegts",
            ])
            .arg(&path)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let cfg = json!({"inputs":[{"url":"publish://"}],"flussonix_subtitle_tracks":if keep {"preserve"}else{"drop"},"pushes":[{"url":format!("srt://127.0.0.1:{port}")}]});
        let mut publisher = engine
            .publish_guarded("owned", &cfg, std::future::ready(true))
            .await
            .unwrap();
        publisher
            .stdin
            .as_mut()
            .unwrap()
            .write_all(&transport)
            .await
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(15), rx.wait()).await;
        engine.stop_all().await;
        assert!(
            outcome.unwrap().unwrap().success(),
            "{}",
            publisher.worker.stats()
        );
        let bytes = std::fs::read(&path).unwrap();
        let descriptors = subtitle_fixture::carrier::original::descriptors(&bytes);
        let dvb: Vec<_> = descriptors
            .iter()
            .filter(|(_, desc)| desc.contains(&0x59))
            .collect();
        if keep {
            assert_eq!(dvb.len(), 2);
            for (pid, _) in dvb {
                assert!(!subtitle_fixture::carrier::original::pes_bodies(&bytes, *pid).is_empty());
            }
        } else {
            assert!(dvb.is_empty());
        }
    }
}

#[tokio::test]
async fn receiver_observes_exact_stream_ids_and_punctuation_secrets() {
    let _lock = MEDIA_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let engine = flussonix::media::Engine::new(dir.path().join("media"), "ffmpeg");
    for query in [false, true] {
        let id = if query {
            "é".repeat(256)
        } else {
            "#!::r=owned,m=publish,password=id+% &".into()
        };
        let port = unused_udp_port();
        let path = dir
            .path()
            .join(if query { "query-id.ts" } else { "field-id.ts" });
        let secret = "owned+secret&% =?123";
        let mut rx = receiver(&path, port, secret).await;
        let push = if query {
            json!({"url":format!("srt://127.0.0.1:{port}?streamid={}&passphrase=owned%2Bsecret%26%25+%3D%3F123", "%C3%A9".repeat(256))})
        } else {
            json!({"url":format!("srt://127.0.0.1:{port}"),"streamid":id,"passphrase":secret})
        };
        let w = engine
            .ensure(
                "owned",
                &json!({"inputs":[{"url":"testsrc://"}],"pushes":[push]}),
            )
            .await
            .unwrap();
        received(&mut rx, &path, "h264", "aac").await;
        engine.stop_all().await;
        let log = std::fs::read(path.with_extension("log")).unwrap();
        assert!(
            String::from_utf8_lossy(&log)
                .contains(&format!("accept streamid [{id}], length {}", id.len())),
            "receiver handshake did not preserve the exact Stream ID"
        );
        assert!(!w.stats().to_string().contains(secret));
    }
}
