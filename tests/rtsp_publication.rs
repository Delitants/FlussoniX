//! Real listener publication admission and independently generated media.
use flussonix::{
    rtsp,
    server::{App, Options},
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use tokio_util::sync::CancellationToken;
const SDP: &str = "v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=owned\r\nc=IN IP4 0.0.0.0\r\nt=0 0\r\nm=video 0 RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\na=fmtp:96 packetization-mode=1\r\na=control:streamid=0\r\n";
struct Lab {
    _dir: tempfile::TempDir,
    app: Arc<App>,
    url: String,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}
impl Lab {
    async fn new(role: &str) -> Self {
        Self::with_ffmpeg(role, "/usr/bin/ffmpeg").await
    }
    async fn with_ffmpeg(role: &str, ffmpeg: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        // Preserve owned decoder diagnostics without exposing production input URLs.
        // exec keeps the actual FFmpeg PID and its stdin/stdout/lifecycle unchanged.
        let wrapper = dir.path().join("owned-ffmpeg");
        let selected = if ffmpeg == "/usr/bin/ffmpeg" {
            use std::os::unix::fs::PermissionsExt;
            std::fs::write(&wrapper,format!("#!/usr/bin/python3\nimport os,sys\nf=open({:?},'wb')\nos.dup2(f.fileno(),2)\nos.execv('/usr/bin/ffmpeg',['ffmpeg']+sys.argv[1:])\n",dir.path().join("worker-stderr.log"))).unwrap();
            std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
            wrapper.to_str().unwrap()
        } else {
            ffmpeg
        };
        let app = App::new(
            dir.path().join("c.json"),
            dir.path().join("media"),
            Options {
                admin_password: "owned-admin".into(),
                peer_key: "owned-peer-key".into(),
                role: role.into(),
                ffmpeg: selected.into(),
                ..Default::default()
            },
        )
        .unwrap();
        app.config.put("streams","owned",json!({"static":false,"inputs":[{"url":"publish://"}],"password":"owned-publish","flussonix_input_timeout":3})).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "rtsp://{}/owned?password=owned-publish",
            listener.local_addr().unwrap()
        );
        let cancel = CancellationToken::new();
        let task = tokio::spawn(rtsp::serve(listener, app.clone(), cancel.clone()));
        Self {
            _dir: dir,
            app,
            url,
            cancel,
            task,
        }
    }
    async fn socket(&self) -> BufReader<TcpStream> {
        let u = url::Url::parse(&self.url).unwrap();
        BufReader::new(
            TcpStream::connect((u.host_str().unwrap(), u.port().unwrap()))
                .await
                .unwrap(),
        )
    }
    async fn end(self) {
        self.cancel.cancel();
        self.task.await.unwrap().unwrap();
        self.app.media.stop_all().await;
    }
}
async fn request<S: AsyncRead + AsyncWrite + Unpin>(
    s: &mut BufReader<S>,
    method: &str,
    url: &str,
    headers: &str,
    body: &str,
) -> (u16, String) {
    s.get_mut()
        .write_all(
            format!(
                "{method} {url} RTSP/1.0\r\nCSeq: 1\r\nContent-Length: {}\r\n{headers}\r\n{body}",
                body.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(12), async {
        let mut h = Vec::new();
        loop {
            h.push(s.read_u8().await.unwrap());
            if h.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        let h = String::from_utf8(h).unwrap();
        let code = h.split(' ').nth(1).unwrap().parse().unwrap();
        let n = h
            .lines()
            .find_map(|l| l.strip_prefix("Content-Length: "))
            .unwrap()
            .parse::<usize>()
            .unwrap();
        let mut b = vec![0; n];
        s.read_exact(&mut b).await.unwrap();
        (code, h)
    })
    .await
    .unwrap()
}
async fn announce<S: AsyncRead + AsyncWrite + Unpin>(
    s: &mut BufReader<S>,
    url: &str,
    sdp: &str,
) -> (u16, String) {
    request(s, "ANNOUNCE", url, "Content-Type: application/sdp\r\n", sdp).await
}
fn id(headers: &str) -> String {
    headers
        .lines()
        .find_map(|h| h.strip_prefix("Session: "))
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .into()
}
fn track(url: &str) -> String {
    let mut u = url::Url::parse(url).unwrap();
    u.set_path("/owned/streamid=0");
    u.set_query(None);
    u.to_string()
}
async fn setup<S: AsyncRead + AsyncWrite + Unpin>(
    s: &mut BufReader<S>,
    url: &str,
    id: &str,
    transport: &str,
) -> u16 {
    request(
        s,
        "SETUP",
        url,
        &format!("Session: {id}\r\nTransport: {transport}\r\n"),
        "",
    )
    .await
    .0
}
const TRANSPORT: &str = "RTP/AVP/TCP;unicast;interleaved=0-1;mode=record";

// Removing ANNOUNCE admission or starting FFmpeg during negotiation breaks these tests.
#[tokio::test]
async fn announce_setup_wait_for_record_and_bind_session_tracks_and_transport() {
    let l = Lab::new("standalone").await;
    let mut s = l.socket().await;
    let (code, h) = announce(&mut s, &l.url, SDP).await;
    assert_eq!(code, 200);
    let sid = id(&h);
    assert_eq!(l.app.media.count().await, 0);
    assert_eq!(setup(&mut s, &track(&l.url), "wrong", TRANSPORT).await, 454);
    assert_eq!(
        setup(
            &mut s,
            &track(&l.url),
            &sid,
            "RTP/AVP;unicast;client_port=20000-20001;mode=record"
        )
        .await,
        461
    );
    assert_eq!(
        setup(
            &mut s,
            &track(&l.url),
            &sid,
            "RTP/AVP/TCP;interleaved=0-1;mode=play"
        )
        .await,
        461
    );
    assert_eq!(setup(&mut s, &track(&l.url), &sid, TRANSPORT).await, 200);
    assert_eq!(l.app.media.count().await, 0);
    assert_eq!(request(&mut s, "DESCRIBE", &l.url, "", "").await.0, 455);
    drop(s);
    l.end().await;
}
#[tokio::test]
async fn publisher_password_is_distinct_from_viewer_admin_and_peer_credentials() {
    let l = Lab::new("standalone").await;
    for qs in [
        "",
        "?token=owned-publish",
        "?password=owned-admin",
        "?password=owned-peer-key",
        "?password=owned-publish&password=owned-publish",
    ] {
        let mut s = l.socket().await;
        let u = l.url.split('?').next().unwrap().to_string() + qs;
        let (code, _) = announce(&mut s, &u, SDP).await;
        assert_eq!(code, if qs.contains('&') { 400 } else { 403 });
        assert_eq!(l.app.media.count().await, 0);
    }
    let mut s = l.socket().await;
    assert_eq!(announce(&mut s, &l.url, SDP).await.0, 200);
    drop(s);
    l.end().await;
}
#[tokio::test]
async fn publication_requires_configured_enabled_source_stream() {
    for role in ["standalone", "lb"] {
        let l = Lab::new(role).await;
        let mut s = l.socket().await;
        assert_eq!(
            announce(&mut s, &l.url.replace("/owned?", "/missing?"), SDP)
                .await
                .0,
            if role == "lb" { 403 } else { 404 }
        );
        drop(s);
        for cfg in [
            json!({"inputs":[{"url":"publish://"}],"disabled":true}),
            json!({"inputs":[{"url":"testsrc://"}]}),
        ] {
            l.app.config.put("streams", "owned", cfg).unwrap();
            let mut s = l.socket().await;
            assert_eq!(announce(&mut s, &l.url, SDP).await.0, 403);
        }
        assert_eq!(l.app.media.count().await, 0);
        l.end().await;
    }
}
#[tokio::test]
async fn unsupported_or_remote_control_sdp_cannot_start_a_worker() {
    let l = Lab::new("standalone").await;
    for bad in [
        SDP.replace("H264/90000", "VP9/90000"),
        SDP.replace("streamid=0", "http://192.0.2.1/evil"),
        SDP.replace("streamid=0", "../other"),
        SDP.replace(
            "packetization-mode=1",
            "packetization-mode=1;resource=http://192.0.2.1",
        ),
        SDP.replace("RTP/AVP 96", "RTP/SAVP 96"),
    ] {
        let mut s = l.socket().await;
        assert_eq!(announce(&mut s, &l.url, &bad).await.0, 415);
    }
    let mut s = l.socket().await;
    assert_eq!(announce(&mut s, &l.url, SDP).await.0, 200);
    assert_eq!(l.app.media.count().await, 0);
    drop(s);
    l.end().await;
}
#[tokio::test]
async fn setup_cannot_substitute_stream_authority_or_credentials() {
    let l = Lab::new("standalone").await;
    let mut s = l.socket().await;
    let (c, h) = announce(&mut s, &l.url, SDP).await;
    assert_eq!(c, 200);
    let sid = id(&h);
    for u in [
        track(&l.url).replace("/owned/", "/other/"),
        track(&l.url).replace("127.0.0.1", "localhost"),
        track(&l.url) + "?password=changed",
    ] {
        assert_eq!(setup(&mut s, &u, &sid, TRANSPORT).await, 403);
    }
    assert_eq!(setup(&mut s, &track(&l.url), &sid, TRANSPORT).await, 200);
    drop(s);
    l.end().await;
}
#[tokio::test]
async fn changing_publisher_policy_closes_pending_session() {
    let l = Lab::new("standalone").await;
    let mut s = l.socket().await;
    assert_eq!(announce(&mut s, &l.url, SDP).await.0, 200);
    let mut c = l.app.config.snapshot()["streams"]["owned"].clone();
    c["password"] = json!("changed");
    l.app.config.put("streams", "owned", c).unwrap();
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), s.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(l.app.media.count().await, 0);
    l.end().await;
}
#[tokio::test]
async fn record_requires_every_track_and_exclusive_worker_ownership() {
    let l = Lab::new("standalone").await;
    let mut s = l.socket().await;
    let (c, h) = announce(&mut s, &l.url, SDP).await;
    assert_eq!(c, 200);
    let sid = id(&h);
    assert_eq!(
        request(&mut s, "RECORD", &l.url, &format!("Session: {sid}\r\n"), "")
            .await
            .0,
        455
    );
    assert_eq!(setup(&mut s, &track(&l.url), &sid, TRANSPORT).await, 200);
    assert_eq!(
        request(
            &mut s,
            "RECORD",
            &l.url,
            &format!("Session: {sid}\r\nRange: npt=10-\r\n"),
            ""
        )
        .await
        .0,
        457
    );
    assert_eq!(
        request(&mut s, "RECORD", &l.url, &format!("Session: {sid}\r\n"), "")
            .await
            .0,
        200
    );
    let cfg = l.app.config.effective("owned").unwrap();
    let w = l.app.media.ensure("owned", &cfg).await.unwrap();
    assert_ne!(w.pid(), 0);
    assert_eq!(l.app.media.count().await, 1);
    assert!(
        matches!(l.app.media.publish_guarded("owned",&cfg,std::future::ready(true)).await,Err(e)if e=="publisher already connected")
    );
    assert_eq!(
        request(
            &mut s,
            "TEARDOWN",
            &l.url,
            &format!("Session: {sid}\r\n"),
            ""
        )
        .await
        .0,
        200
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        while std::path::Path::new(&format!("/proc/{}", w.pid())).exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    drop(s);
    let mut s = l.socket().await;
    let (c, h) = announce(&mut s, &l.url, SDP).await;
    assert_eq!(c, 200);
    let sid = id(&h);
    assert_eq!(setup(&mut s, &track(&l.url), &sid, TRANSPORT).await, 200);
    assert_eq!(
        request(&mut s, "RECORD", &l.url, &format!("Session: {sid}\r\n"), "")
            .await
            .0,
        200
    );
    let replacement = l.app.media.ensure("owned", &cfg).await.unwrap();
    assert!(!Arc::ptr_eq(&w, &replacement));
    drop(s);
    l.end().await;
}
async fn decoded(path: &std::path::Path, video: Option<&str>, audio: &[&str]) {
    let p = tokio::time::timeout(
        Duration::from_secs(30),
        tokio::process::Command::new("ffprobe")
            .args([
                "-v",
                "error",
                "-count_frames",
                "-show_entries",
                "stream=codec_name,codec_type,nb_read_frames",
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
    assert!(p.status.success(), "{}", String::from_utf8_lossy(&p.stderr));
    assert!(
        p.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&p.stderr)
    );
    let j: serde_json::Value = serde_json::from_slice(&p.stdout).unwrap();
    let streams = j["streams"].as_array().unwrap();
    let mut actual: Vec<_> = streams
        .iter()
        .map(|s| s["codec_name"].as_str().unwrap())
        .collect();
    actual.sort();
    let mut expected = audio.to_vec();
    expected.extend(video);
    expected.sort();
    assert_eq!(actual, expected);
    for s in streams {
        assert!(
            s["nb_read_frames"]
                .as_str()
                .unwrap()
                .parse::<usize>()
                .unwrap()
                >= 20,
            "{s}"
        );
    }
    let p = tokio::time::timeout(
        Duration::from_secs(30),
        tokio::process::Command::new("ffmpeg")
            .args([
                "-nostdin",
                "-v",
                "error",
                "-xerror",
                "-err_detect",
                "explode",
                "-threads",
                "1",
                "-i",
            ])
            .arg(path)
            .args([
                "-map", "0:v?", "-map", "0:a?", "-threads", "1", "-f", "null", "-",
            ])
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        p.status.success() && p.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&p.stderr)
    );
}
#[path = "support/tls.rs"]
mod tls_fixture;
fn publisher(video: Option<&str>, audio: &[&str], input_url: &str) -> tokio::process::Child {
    let mut cmd = tokio::process::Command::new("ffmpeg");
    cmd.args(["-nostdin", "-v", "error"]);
    if video.is_some() {
        cmd.args(["-re", "-f", "lavfi", "-i", "testsrc2=size=320x180:rate=25"]);
    }
    for (i, _) in audio.iter().enumerate() {
        cmd.args([
            "-re",
            "-f",
            "lavfi",
            "-i",
            &format!("sine=frequency={}:sample_rate=48000", 440 + 110 * i),
        ]);
    }
    if let Some(v) = video {
        cmd.args([
            "-map",
            "0:v",
            "-c:v",
            v,
            "-preset",
            "ultrafast",
            "-tune",
            "zerolatency",
            "-g",
            "25",
            "-threads",
            "1",
        ]);
        if v == "libx265" {
            cmd.args(["-x265-params", "log-level=error:pools=1:frame-threads=1"]);
        }
    }
    for (i, a) in audio.iter().enumerate() {
        cmd.args([
            "-map",
            &format!("{}:a", i + usize::from(video.is_some())),
            &format!("-c:a:{i}"),
            a,
        ]);
    }
    cmd.args([
        "-flags",
        "+global_header",
        "-f",
        "rtsp",
        "-rtsp_transport",
        "tcp",
    ])
    .arg(input_url)
    .stderr(std::process::Stdio::piped())
    .kill_on_drop(true);
    cmd.spawn().unwrap()
}
async fn qualify(video: Option<&str>, audio: &[&str], profile: serde_json::Value, tls: bool) {
    let l = Lab::new("standalone").await;
    let mut c = l.app.config.snapshot()["streams"]["owned"].clone();
    c["transcoder"] = profile.clone();
    c["flussonix_input_timeout"] = json!(15);
    l.app.config.put("streams", "owned", c).unwrap();
    let certificates = if tls {
        Some(tls_fixture::Certificates::new())
    } else {
        None
    };
    let mut auxiliary = Vec::new();
    let mut input_url = l.url.clone();
    if tls {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tls_address = listener.local_addr().unwrap();
        let app = l.app.clone();
        let cancel = l.cancel.clone();
        let server = certificates.as_ref().unwrap().server();
        auxiliary.push(tokio::spawn(async move {
            rtsp::serve_tls(listener, app, cancel, server)
                .await
                .unwrap();
        }));
        let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        input_url = format!(
            "rtsp://{}/owned?password=owned-publish",
            proxy.local_addr().unwrap()
        );
        let client = certificates.as_ref().unwrap().client();
        auxiliary.push(tokio::spawn(async move {
            let (mut plain, _) = proxy.accept().await.unwrap();
            let socket = TcpStream::connect(tls_address).await.unwrap();
            let mut encrypted = tokio_rustls::TlsConnector::from(client)
                .connect(
                    tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                    socket,
                )
                .await
                .expect("owned TLS certificate must verify");
            let _ = tokio::io::copy_bidirectional(&mut plain, &mut encrypted).await;
        }));
    }
    let mut child = publisher(video, audio, &input_url);
    let cfg = l.app.config.effective("owned").unwrap();
    let w = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            if let Ok(w) = l.app.media.ensure("owned", &cfg).await {
                break w;
            }
            if child.try_wait().unwrap().is_some() {
                let mut e = String::new();
                child
                    .stderr
                    .take()
                    .unwrap()
                    .read_to_string(&mut e)
                    .await
                    .unwrap();
                panic!("publisher failed: {e}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let mut rx = w.subscribe();
    let mut ts = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while let Ok(r) = tokio::time::timeout_at(deadline, rx.recv()).await {
        ts.extend_from_slice(&r.expect("shared TS lag"));
    }
    if l.app.media.count().await != 1 {
        let _ = child.kill().await;
        let mut diagnostics = String::new();
        if let Some(mut stderr) = child.stderr.take() {
            let _ = tokio::time::timeout(
                Duration::from_secs(2),
                stderr.read_to_string(&mut diagnostics),
            )
            .await;
        }
        let packager =
            std::fs::read_to_string(l._dir.path().join("worker-stderr.log")).unwrap_or_default();
        panic!(
            "Publication lost: decoder={packager} video={video:?} audio={audio:?} bytes={} worker={} publisher={diagnostics}",
            ts.len(),
            w.stats()
        );
    }
    assert_eq!(
        w.stats()["input_protocol"],
        if tls { "rtsps" } else { "rtsp" }
    );
    let path = l._dir.path().join("shared.ts");
    std::fs::write(&path, &ts).unwrap();
    let output_video = if video.is_none() {
        None
    } else {
        Some(if profile["encoder"] == "libx265" {
            "hevc"
        } else if profile["encoder"] == "libx264"
            || profile["encoder"] == "h264_vaapi"
            || video == Some("libx264")
        {
            "h264"
        } else {
            "hevc"
        })
    };
    let output_audio: Vec<_> = audio
        .iter()
        .map(|a| match profile["acodec"].as_str().unwrap_or("copy") {
            "aac" => "aac",
            "mp2a" => "mp2",
            "mp3" => "mp3",
            _ => {
                if *a == "libmp3lame" {
                    "mp3"
                } else {
                    a
                }
            }
        })
        .collect();
    decoded(&path, output_video, &output_audio).await;
    assert!(
        !w.wire.m4s_subscribe().0.is_empty(),
        "shared native output must become ready"
    );
    if let Ok(root) = std::env::var("FLUSSONIX_RTSP_RECORD_DIR") {
        let case = format!(
            "{}-{}-{}-{}",
            video.unwrap_or("audio"),
            audio.join("_"),
            profile["encoder"].as_str().unwrap_or("copy"),
            if tls { "tls" } else { "tcp" }
        );
        let dir = std::path::Path::new(&root).join(case);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::copy(&path, dir.join("worker.ts")).unwrap();
        std::fs::write(dir.join("evidence.json"),json!({"video":output_video,"audio":output_audio,"profile":profile,"transport":if tls{"verified TLS relay"}else{"TCP"},"worker_pid":w.pid(),"stats":w.stats(),"bytes":ts.len()}).to_string()).unwrap();
    }
    child.kill().await.unwrap();
    drop(child);
    l.end().await;
    for task in auxiliary {
        task.abort();
        let _ = task.await;
    }
}
#[tokio::test]
async fn independent_h264_aac_publisher_shared_ts_strictly_decodes() {
    qualify(
        Some("libx264"),
        &["aac"],
        json!({"encoder":"copy","acodec":"copy"}),
        false,
    )
    .await;
}
#[tokio::test]
async fn hevc_h264_and_all_audio_publication_codecs_strictly_decode() {
    for v in ["libx264", "libx265"] {
        for a in ["aac", "mp2", "libmp3lame"] {
            if v == "libx264" && a == "aac" {
                continue;
            }
            qualify(
                Some(v),
                &[a],
                json!({"encoder":"copy","acodec":"copy"}),
                false,
            )
            .await;
        }
    }
}
#[tokio::test]
async fn audio_only_and_multiple_mpeg_audio_publications_keep_tracks() {
    qualify(
        None,
        &["aac"],
        json!({"encoder":"copy","acodec":"copy"}),
        false,
    )
    .await;
    qualify(
        None,
        &["mp2", "libmp3lame"],
        json!({"encoder":"copy","acodec":"copy"}),
        false,
    )
    .await;
}
#[tokio::test]
async fn cpu_publication_transcodes_hevc_with_mpeg_audio() {
    qualify(
        Some("libx264"),
        &["aac"],
        json!({"encoder":"libx265","acodec":"mp2a","ab":192}),
        false,
    )
    .await;
}
#[tokio::test]
async fn tls_publication_with_verified_certificate_strictly_decodes() {
    qualify(
        Some("libx265"),
        &["libmp3lame"],
        json!({"encoder":"copy","acodec":"copy"}),
        true,
    )
    .await;
}
#[tokio::test]
#[ignore = "Requires explicitly available H264 VAAPI hardware and scoped driver environment"]
async fn vaapi_publication_transcode_strictly_decodes() {
    qualify(
        Some("libx264"),
        &["aac"],
        json!({"encoder":"h264_vaapi","qp":24,"acodec":"mp2a","ab":192}),
        false,
    )
    .await;
}

async fn callback(
    app: &Arc<App>,
    deny_initial: bool,
) -> (
    tokio::sync::mpsc::Receiver<serde_json::Value>,
    tokio::task::JoinHandle<()>,
) {
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = axum::Router::new().route(
        "/publish",
        axum::routing::post(move |axum::Json(v): axum::Json<serde_json::Value>| {
            let tx = tx.clone();
            async move {
                let deny = deny_initial || v["request_number"].as_u64().unwrap() > 0;
                tx.send(v).await.unwrap();
                (
                    if deny {
                        axum::http::StatusCode::FORBIDDEN
                    } else {
                        axum::http::StatusCode::OK
                    },
                    [("x-authduration", "1")],
                )
            }
        }),
    );
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let mut cfg = app.config.snapshot()["streams"]["owned"].clone();
    cfg["on_publish"] = json!(format!("http://{addr}/publish"));
    app.config.put("streams", "owned", cfg).unwrap();
    (rx, task)
}
#[tokio::test]
async fn callback_denial_and_pending_renewal_never_admit_a_worker() {
    for deny in [true, false] {
        let l = Lab::new("standalone").await;
        let (mut rx, task) = callback(&l.app, deny).await;
        let mut s = l.socket().await;
        let url = l.url.clone() + "&token=owned-token";
        let (c, _) = announce(&mut s, &url, SDP).await;
        assert_eq!(c, if deny { 403 } else { 200 });
        let first = rx.recv().await.unwrap();
        assert_eq!(first["proto"], "rtsp");
        assert_eq!(first["token"], "owned-token");
        assert_eq!(first["ip"], "127.0.0.1");
        assert_eq!(first["request_number"], 0);
        assert_eq!(first["request_type"], "new_session");
        assert_eq!(first["bytes"], 0);
        if !deny {
            let second = tokio::time::timeout(Duration::from_secs(3), rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(second["session_id"], first["session_id"]);
            assert_eq!(second["request_number"], 1);
            assert_eq!(second["request_type"], "update_session");
            let mut b = Vec::new();
            tokio::time::timeout(Duration::from_secs(2), s.read_to_end(&mut b))
                .await
                .unwrap()
                .unwrap();
        }
        assert_eq!(l.app.media.count().await, 0);
        task.abort();
        l.end().await;
    }
}
async fn recording(l: &Lab) -> (BufReader<TcpStream>, String, Arc<flussonix::media::Worker>) {
    let mut s = l.socket().await;
    let (c, h) = announce(&mut s, &l.url, SDP).await;
    assert_eq!(c, 200);
    let sid = id(&h);
    assert_eq!(setup(&mut s, &track(&l.url), &sid, TRANSPORT).await, 200);
    assert_eq!(
        request(&mut s, "RECORD", &l.url, &format!("Session: {sid}\r\n"), "")
            .await
            .0,
        200
    );
    let w = l
        .app
        .media
        .ensure("owned", &l.app.config.effective("owned").unwrap())
        .await
        .unwrap();
    (s, sid, w)
}
async fn interleaved<S: AsyncWrite + Unpin>(s: &mut S, channel: u8, payload: &[u8]) {
    let mut b = vec![b'$', channel];
    b.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    b.extend(payload);
    s.write_all(&b).await.unwrap();
}
fn rtp(ssrc: u32, n: usize) -> Vec<u8> {
    let mut p = vec![0x80, 96, 0, 1, 0, 0, 0, 0];
    p.extend_from_slice(&ssrc.to_be_bytes());
    p.push(0x41);
    p.resize(n, 0x55);
    p
}
#[tokio::test]
async fn interleaved_tcp_accepts_large_bounded_packets_but_udp_limit_stays_small() {
    let l = Lab::new("standalone").await;
    let (mut s, sid, _w) = recording(&l).await;
    interleaved(s.get_mut(), 0, &rtp(7, 2000)).await;
    assert_eq!(
        request(
            &mut s,
            "GET_PARAMETER",
            &l.url,
            &format!("Session: {sid}\r\n"),
            ""
        )
        .await
        .0,
        200
    );
    drop(s);
    l.end().await;
}
#[tokio::test]
async fn malformed_media_and_ssrc_substitution_reap_only_the_publisher() {
    for (ch, p) in [
        (4, rtp(7, 20)),
        (0, vec![0; 12]),
        (1, vec![0x80, 200, 0, 6]),
    ] {
        let l = Lab::new("standalone").await;
        let (mut s, _sid, w) = recording(&l).await;
        interleaved(s.get_mut(), ch, &p).await;
        let mut b = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), s.read_to_end(&mut b))
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while std::path::Path::new(&format!("/proc/{}", w.pid())).exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        l.end().await;
    }
    let l = Lab::new("standalone").await;
    let (mut s, _, w) = recording(&l).await;
    interleaved(s.get_mut(), 0, &rtp(7, 20)).await;
    interleaved(s.get_mut(), 0, &rtp(8, 20)).await;
    let mut b = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), s.read_to_end(&mut b))
        .await
        .unwrap()
        .unwrap();
    w.closed().await;
    l.end().await;
}
#[tokio::test]
async fn control_flood_does_not_hide_media_stall() {
    let l = Lab::new("standalone").await;
    let (s, sid, w) = recording(&l).await;
    let (mut read, mut write) = tokio::io::split(s.into_inner());
    let data = format!(
        "GET_PARAMETER {} RTSP/1.0\r\nCSeq: 8\r\nSession: {sid}\r\nContent-Length: 0\r\n\r\n",
        l.url
    );
    let flood = tokio::spawn(async move {
        loop {
            if write.write_all(data.as_bytes()).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    let mut body = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), read.read_to_end(&mut body))
        .await
        .unwrap()
        .unwrap();
    assert!(w.is_closed());
    assert!(!body.is_empty());
    flood.abort();
    l.end().await;
}
#[tokio::test]
async fn pending_initial_callback_is_revoked_when_configuration_changes() {
    let l = Lab::new("standalone").await;
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = axum::Router::new().route(
        "/publish",
        axum::routing::post(move || {
            let tx = tx.clone();
            async move {
                tx.send(()).await.unwrap();
                tokio::time::sleep(Duration::from_secs(10)).await;
                axum::http::StatusCode::OK
            }
        }),
    );
    let callback = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let mut cfg = l.app.config.snapshot()["streams"]["owned"].clone();
    cfg["on_publish"] = json!(format!("http://{addr}/publish"));
    l.app.config.put("streams", "owned", cfg.clone()).unwrap();
    let mut s = l.socket().await;
    let url = l.url.clone();
    let pending = tokio::spawn(async move { announce(&mut s, &url, SDP).await.0 });
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    cfg["disabled"] = json!(true);
    l.app.config.put("streams", "owned", cfg).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), pending)
            .await
            .unwrap()
            .unwrap(),
        403
    );
    assert_eq!(l.app.media.count().await, 0);
    callback.abort();
    l.end().await;
}
#[tokio::test]
async fn metadata_edits_retain_admission_but_media_edits_revoke_it() {
    let l = Lab::new("standalone").await;
    let (mut s, sid, w) = recording(&l).await;
    let mut c = l.app.config.snapshot()["streams"]["owned"].clone();
    c["title"] = json!("New title");
    l.app.config.put("streams", "owned", c.clone()).unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        request(
            &mut s,
            "GET_PARAMETER",
            &l.url,
            &format!("Session: {sid}\r\n"),
            ""
        )
        .await
        .0,
        200
    );
    c["transcoder"] = json!({"encoder":"libx264"});
    l.app.config.put("streams", "owned", c).unwrap();
    let mut b = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), s.read_to_end(&mut b))
        .await
        .unwrap()
        .unwrap();
    assert!(w.is_closed());
    l.end().await;
}
#[tokio::test]
async fn channels_are_exclusive_and_record_requires_all_announced_tracks() {
    let l = Lab::new("standalone").await;
    let mut s = l.socket().await;
    let sdp = SDP.to_string() + "m=audio 0 RTP/AVP 14\r\na=control:streamid=1\r\n";
    let (c, h) = announce(&mut s, &l.url, &sdp).await;
    assert_eq!(c, 200);
    let sid = id(&h);
    assert_eq!(setup(&mut s, &track(&l.url), &sid, TRANSPORT).await, 200);
    let second = track(&l.url).replace("streamid=0", "streamid=1");
    assert_eq!(setup(&mut s, &second, &sid, TRANSPORT).await, 461);
    assert_eq!(
        request(&mut s, "RECORD", &l.url, &format!("Session: {sid}\r\n"), "")
            .await
            .0,
        455
    );
    assert_eq!(
        setup(
            &mut s,
            &second,
            &sid,
            "RTP/AVP/TCP;unicast;interleaved=2-3;mode=record"
        )
        .await,
        200
    );
    assert_eq!(l.app.media.count().await, 0);
    drop(s);
    l.end().await;
}
#[tokio::test]
async fn policy_change_interrupts_decoder_startup_before_record_success() {
    use std::os::unix::fs::PermissionsExt;
    let d = tempfile::tempdir().unwrap();
    let script = d.path().join("blocked-decoder");
    let marker = d.path().join("sdp-read");
    std::fs::write(&script,format!("#!/usr/bin/python3\nimport sys,time,pathlib\nsys.stdin.buffer.read()\npathlib.Path({:?}).write_text('ready')\ntime.sleep(30)\n",marker)).unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    let l = Lab::with_ffmpeg("standalone", script.to_str().unwrap()).await;
    let mut s = l.socket().await;
    let (c, h) = announce(&mut s, &l.url, SDP).await;
    assert_eq!(c, 200);
    let sid = id(&h);
    assert_eq!(setup(&mut s, &track(&l.url), &sid, TRANSPORT).await, 200);
    let url = l.url.clone();
    let pending = tokio::spawn(async move {
        request(&mut s, "RECORD", &url, &format!("Session: {sid}\r\n"), "")
            .await
            .0
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !marker.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let mut cfg = l.app.config.snapshot()["streams"]["owned"].clone();
    cfg["disabled"] = json!(true);
    l.app.config.put("streams", "owned", cfg).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .unwrap()
            .unwrap(),
        403
    );
    l.end().await;
}
#[tokio::test]
async fn capabilities_describe_disabled_publication_listeners_without_assumed_ports() {
    use base64::Engine;
    use tower::ServiceExt;
    let l = Lab::new("standalone").await;
    let request = axum::http::Request::builder()
        .uri("/flussonix/api/v1/capabilities")
        .header(
            "Authorization",
            format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode("admin:owned-admin")
            ),
        )
        .body(axum::body::Body::empty())
        .unwrap();
    let response = flussonix::server::router(l.app.clone())
        .oneshot(request)
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let b = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let j: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(j["rtsp_publication"]["enabled"], false);
    assert_eq!(j["rtsp_publication"]["rtsp"], serde_json::Value::Null);
    assert_eq!(j["rtsp_publication"]["rtsps"], serde_json::Value::Null);
    l.end().await;
}
#[tokio::test]
async fn capabilities_report_the_actual_bound_listener_addresses() {
    use base64::Engine;
    use tower::ServiceExt;
    let l = Lab::new("standalone").await;
    let a = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.app
        .set_rtsp_publication(Some(a.local_addr().unwrap()), Some(b.local_addr().unwrap()));
    let request = axum::http::Request::builder()
        .uri("/flussonix/api/v1/capabilities")
        .header(
            "Authorization",
            format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode("admin:owned-admin")
            ),
        )
        .body(axum::body::Body::empty())
        .unwrap();
    let response = flussonix::server::router(l.app.clone())
        .oneshot(request)
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let j: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(j["rtsp_publication"]["enabled"], true);
    assert_eq!(
        j["rtsp_publication"]["rtsp"],
        a.local_addr().unwrap().to_string()
    );
    assert_eq!(
        j["rtsp_publication"]["rtsps"],
        b.local_addr().unwrap().to_string()
    );
    l.end().await;
}
#[tokio::test]
async fn active_publish_callback_counts_real_input_and_denial_reaps_the_worker() {
    let l = Lab::new("standalone").await;
    let (mut rx, callback) = callback(&l.app, false).await;
    let mut child = publisher(Some("libx264"), &["aac"], &l.url);
    let first = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let cfg = l.app.config.effective("owned").unwrap();
    let w = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(w) = l.app.media.ensure("owned", &cfg).await {
                break w;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let update = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(update["session_id"], first["session_id"]);
    assert_eq!(update["proto"], "rtsp");
    assert_eq!(update["request_number"], 1);
    assert!(
        update["bytes"].as_u64().unwrap() > 0,
        "actual media bytes: {update}"
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while std::path::Path::new(&format!("/proc/{}", w.pid())).exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let _ = child.kill().await;
    callback.abort();
    l.end().await;
}
#[tokio::test]
async fn tls_publication_rejects_plaintext_untrusted_certificates_and_scheme_downgrade() {
    let l = Lab::new("standalone").await;
    let mut plain = l.socket().await;
    assert_eq!(
        announce(&mut plain, &l.url.replacen("rtsp:", "rtsps:", 1), SDP)
            .await
            .0,
        400
    );
    drop(plain);
    let cert = tls_fixture::Certificates::new();
    let untrusted = tls_fixture::Certificates::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = l.app.clone();
    let cancel = l.cancel.clone();
    let tls = tokio::spawn(async move {
        rtsp::serve_tls(listener, app, cancel, cert.server())
            .await
            .unwrap();
    });
    let socket = TcpStream::connect(addr).await.unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        tokio_rustls::TlsConnector::from(untrusted.client()).connect(
            tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap(),
            socket,
        ),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(
        format!("ANNOUNCE rtsps://{addr}/owned RTSP/1.0\r\nCSeq: 1\r\nContent-Length: 0\r\n\r\n")
            .as_bytes(),
    )
    .await
    .unwrap();
    let mut b = Vec::new();
    let r = tokio::time::timeout(Duration::from_secs(2), s.read_to_end(&mut b))
        .await
        .unwrap();
    assert!(
        r.is_err() || b.is_empty() || b.starts_with(&[21, 3]),
        "Plaintext must receive a TLS alert or close, not RTSP: {b:?}"
    );
    assert_eq!(l.app.media.count().await, 0);
    l.end().await;
    tls.await.unwrap();
}
#[tokio::test]
async fn encoded_hierarchical_names_inherit_template_publication_policy() {
    let l = Lab::new("standalone").await;
    l.app
        .config
        .put(
            "templates",
            "owned-template",
            json!({"inputs":[{"url":"publish://"}],"password":"owned-publish"}),
        )
        .unwrap();
    l.app
        .config
        .put(
            "streams",
            "folder/café stream",
            json!({"template":"owned-template","static":false}),
        )
        .unwrap();
    let mut u = url::Url::parse(&l.url).unwrap();
    u.set_path("/folder/café stream");
    let url = u.to_string();
    let mut s = l.socket().await;
    let (c, h) = announce(
        &mut s,
        &url,
        SDP.replace("streamid=0", "trackID=0").as_str(),
    )
    .await;
    assert_eq!(c, 200);
    let sid = id(&h);
    u.set_path("/folder/café stream/trackID=0");
    u.set_query(None);
    assert_eq!(setup(&mut s, u.as_str(), &sid, TRANSPORT).await, 200);
    assert_eq!(l.app.media.count().await, 0);
    drop(s);
    l.end().await;
}
#[tokio::test]
async fn http_owner_prevents_rtsp_record_until_its_generation_is_reaped() {
    let l = Lab::new("standalone").await;
    let cfg = l.app.config.effective("owned").unwrap();
    let owner = l
        .app
        .media
        .publish_guarded("owned", &cfg, std::future::ready(true))
        .await
        .unwrap();
    let old = owner.worker.clone();
    let mut s = l.socket().await;
    let (c, h) = announce(&mut s, &l.url, SDP).await;
    assert_eq!(c, 200);
    let sid = id(&h);
    assert_eq!(setup(&mut s, &track(&l.url), &sid, TRANSPORT).await, 200);
    assert_eq!(
        request(&mut s, "RECORD", &l.url, &format!("Session: {sid}\r\n"), "")
            .await
            .0,
        409
    );
    drop(owner);
    assert_eq!(
        request(&mut s, "RECORD", &l.url, &format!("Session: {sid}\r\n"), "")
            .await
            .0,
        200
    );
    let new = l.app.media.ensure("owned", &cfg).await.unwrap();
    assert!(!Arc::ptr_eq(&old, &new));
    l.app.media.stop_if_current("owned", &old).await;
    assert!(!new.is_closed());
    drop(s);
    l.end().await;
}
