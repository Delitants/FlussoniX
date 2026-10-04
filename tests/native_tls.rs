use bytes::Bytes;
use flussonix::{
    http_tls, m4f, m4s,
    media::Engine,
    server::{App, Options},
    wire,
};
use futures_util::StreamExt;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
#[path = "support/native_text.rs"]
mod text_fixture;
#[path = "support/tls.rs"]
mod tls_fixture;
use tls_fixture::Certificates;
struct Source {
    cert: Certificates,
    url: String,
    gets: Arc<AtomicUsize>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Source {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
fn payload(protocol: &str) -> (Bytes, HashMap<String, Bytes>) {
    let (mut tracks, mut frames) = text_fixture::fixture(false);
    tracks.push(m4s::Track {
        id: 2,
        codec: "m2a".into(),
        config: vec![],
    });
    for n in 0..600 {
        frames.push(m4f::Frame {
            track_id: 2,
            dts: 90000 + n * 2160,
            pts_offset: 0,
            key: true,
            body: include_bytes!("fixtures/codecs/mp2.bin").to_vec(),
        });
    }
    frames.sort_by_key(|f| f.dts);
    if protocol == "m4ss" {
        let mut data = wire::encode_info(&tracks).unwrap();
        for frame in &frames {
            data.extend(
                wire::encode_frame(
                    tracks.iter().find(|t| t.id == frame.track_id).unwrap(),
                    frame,
                )
                .unwrap(),
            );
        }
        return (Bytes::from(data), HashMap::new());
    }
    let mut signals = String::new();
    let mut segments = HashMap::new();
    for n in 0..7u64 {
        let part: Vec<_> = frames
            .iter()
            .filter(|f| f.dts >= 90000 + n * 180000 && f.dts < 270000 + n * 180000)
            .cloned()
            .collect();
        let stamp = chrono::DateTime::from_timestamp(1700000000 + n as i64 * 2, 0)
            .unwrap()
            .format("%Y/%m/%d/%H/%M/%S")
            .to_string();
        signals += &format!("{n} {stamp}-2000\n");
        segments.insert(
            format!("/owned/{stamp}.m4f"),
            Bytes::from(m4f::pack(&tracks, &part, 180000).unwrap()),
        );
    }
    (Bytes::from(signals), segments)
}
#[derive(Clone)]
enum Redirect {
    Control(String),
    SameOrigin,
    Segment(String),
}
async fn source(protocol: &str, expired: bool, redirect: Option<Redirect>) -> Source {
    let cert = Certificates::new();
    if expired {
        cert.expire();
    }
    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let url = format!(
        "{protocol}://127.0.0.1:{}/owned?token=owned-source-token",
        address.port()
    );
    let acceptor = tokio_rustls::TlsAcceptor::from(cert.server());
    let gets = Arc::new(AtomicUsize::new(0));
    let count = gets.clone();
    let cancel = CancellationToken::new();
    let stop = cancel.clone();
    let (data, segments) = payload(protocol);
    let control = if protocol == "m4ss" {
        "/owned/m4s"
    } else {
        "/owned/m4f"
    };
    let task = tokio::spawn(async move {
        let mut children = JoinSet::new();
        loop {
            tokio::select! {biased;_=stop.cancelled()=>break,r=listener.accept()=>{
            let Ok((socket,_))=r else{break};let acceptor=acceptor.clone();let stop=stop.clone();let count=count.clone();let data=data.clone();let segments=segments.clone();let redirect=redirect.clone();
            children.spawn(async move{
            let accepted=tokio::select!{_=stop.cancelled()=>return,r=acceptor.accept(socket)=>r};let Ok(mut tls)=accepted else{return};
            let mut head=vec![];loop{let b=tokio::select!{_=stop.cancelled()=>return,r=tls.read_u8()=>r};let Ok(b)=b else{return};head.push(b);if head.ends_with(b"\r\n\r\n"){break}
            if head.len()>8192{return}}
            count.fetch_add(1,Ordering::SeqCst);let head=String::from_utf8(head).unwrap();let target=head.split_whitespace().nth(1).unwrap();assert!(target.ends_with("?token=owned-source-token"),"source query credential lost");let path=target.split('?').next().unwrap();
            let location=match &redirect {
                Some(Redirect::Control(location)) if path==control => Some(location.clone()),
                Some(Redirect::SameOrigin) if path==control => Some("/owned/reload?token=owned-source-token".to_string()),
                Some(Redirect::Segment(location)) if path!=control => Some(location.clone()),
                _=>None,
            };
            if let Some(location)=location{let _=tls.write_all(format!("HTTP/1.1 302 Found\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await;return;}
            if path==control||path=="/owned/reload"{
            let header=format!("HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n",data.len());if tls.write_all(header.as_bytes()).await.is_err(){return}
            if tls.write_all(&data).await.is_err(){return}let _=tls.write_all(b"\r\n").await;let _=tls.flush().await;stop.cancelled().await;
            }else{
            let body=segments.get(path).expect("unexpected source path");let header=format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len());let _=tls.write_all(header.as_bytes()).await;let _=tls.write_all(body).await;
            }
            });
            },_=children.join_next(),if !children.is_empty()=>{}}
        }
        children.abort_all();
        while children.join_next().await.is_some() {}
    });
    Source {
        cert,
        url,
        gets,
        cancel,
        task,
    }
}
async fn ready(engine: &Engine, worker: &flussonix::media::Worker) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if engine.read("owned", "nt7.m3u8").await.is_ok()
                && engine.read("owned", "fmp4/nt7.m3u8").await.is_ok()
            {
                return;
            }
            assert!(!worker.is_closed(), "{}", worker.stats());
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap_or_else(|e| panic!("subtitle delivery {e}: {}", worker.stats()));
}
async fn secure_delivery(protocol: &str) {
    let mut source = source(protocol, false, None).await;
    let dir = tempfile::tempdir().unwrap();
    let cert = Certificates::new();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-peer-secret".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let cfg = json!({"static":false,"inputs":[{"url":source.url,"flussonix_tls_ca":source.cert.ca}],"flussonix_subtitle_tracks":"preserve","flussonix_hls_subtitles":"convert","flussonix_hls_captions":[{"native_track":7,"language":"en","name":"English"},{"native_track":4294967295u32,"language":"de","name":"Deutsch"}],"flussonix_token_sha256":format!("{:x}",Sha256::digest(b"owned-viewer"))});
    app.config.put("streams", "owned", cfg).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    app.set_http_delivery(None, Some(addr));
    let stop = CancellationToken::new();
    let task = tokio::spawn(http_tls::serve(
        listener,
        cert.server(),
        app.clone(),
        stop.clone(),
    ));
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .add_root_certificate(
            reqwest::Certificate::from_pem(&std::fs::read(&cert.ca).unwrap()).unwrap(),
        )
        .build()
        .unwrap();
    let base = format!("https://{addr}/owned");
    assert_eq!(
        client
            .get(format!("{base}/index.m3u8"))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(source.gets.load(Ordering::SeqCst), 0);
    assert_eq!(app.media.count().await, 0);
    let response = client
        .get(format!("{base}/index.m3u8?token=owned-viewer"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let worker = app
        .media
        .ensure("owned", &app.config.effective("owned").unwrap())
        .await
        .unwrap();
    ready(&app.media, &worker).await;
    for prefix in ["", "fmp4/"] {
        let master = client
            .get(format!("{base}/{prefix}index.m3u8?token=owned-viewer"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(master.contains("TYPE=SUBTITLES"));
        assert!(!master.contains("owned-source-token"));
        let response = client
            .get(format!("{base}/{prefix}nt7.m3u8?token=owned-viewer"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let list = response.text().await.unwrap();
        let mut text = String::new();
        let mut cached = None;
        for file in list
            .lines()
            .filter(|l| !l.starts_with('#') && !l.is_empty())
        {
            assert!(file.contains("token=owned-viewer"));
            let target = format!("{base}/{prefix}{file}");
            let r = client.get(&target).send().await.unwrap();
            assert_eq!(r.status(), 200);
            text += &r.text().await.unwrap();
            cached = Some(target);
        }
        assert!(text.contains("AMERICA &lt;HELLO&gt;"));
        assert!(text.contains("00:00:01.920 --> 00:00:02.920"));
        assert!(!text.contains("EUROPE"));
        let target = cached.unwrap();
        assert_eq!(
            client
                .get(target.replace("token=owned-viewer", "token=wrong"))
                .send()
                .await
                .unwrap()
                .status(),
            403
        );
    }
    if protocol == "m4fs" {
        // A quiet late-join GOP legitimately has no text track. Verify the
        // retained cue/clear segment byte-for-byte over the secure native route.
        let (_, segments) = payload(protocol);
        let stamp = chrono::DateTime::from_timestamp(1700000002, 0)
            .unwrap()
            .format("%Y/%m/%d/%H/%M/%S")
            .to_string();
        let path = format!("/owned/{stamp}.m4f");
        let url = format!("https://{addr}{path}?token=owned-viewer");
        let response = client.get(&url).send().await.unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.bytes().await.unwrap(), segments[&path]);
        assert_eq!(
            client
                .get(url.replace("token=owned-viewer", "token=wrong"))
                .send()
                .await
                .unwrap()
                .status(),
            403
        );
    }
    let response = client
        .get(format!("{base}/m4s?token=owned-viewer"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let mut stream = response.bytes_stream();
    let mut decoder = m4s::Decoder::default();
    let mut text = false;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let body = stream.next().await.unwrap().unwrap();
            for e in decoder.push(&body).unwrap() {
                if let m4s::Event::Info { tracks, .. } | m4s::Event::Gop { tracks, .. } = e {
                    text = (protocol == "m4fs" || tracks.iter().any(|t| t.codec == "subtitle"))
                        && tracks.iter().any(|t| t.codec == "hevc")
                        && tracks.iter().any(|t| t.codec == "m2a");
                }
            }
            if text {
                break;
            }
        }
    })
    .await
    .unwrap();
    drop(stream);
    app.config
        .put("streams", "owned", json!({"disabled":true}))
        .unwrap();
    assert_eq!(
        client
            .get(format!("{base}/nt7.m3u8?token=owned-viewer"))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    app.media.stop_all().await;
    stop.cancel();
    task.await.unwrap().unwrap();
    source.cancel.cancel();
    assert!(source.gets.load(Ordering::SeqCst) >= 1);
    (&mut source.task).await.unwrap();
}
#[tokio::test]
async fn native_private_ca_m4ss_delivers_subtitles_and_originals_over_https() {
    secure_delivery("m4ss").await;
}
#[tokio::test]
async fn native_private_ca_m4fs_delivers_subtitles_and_originals_over_https() {
    secure_delivery("m4fs").await;
}
#[tokio::test]
async fn native_tls_rejects_untrusted_wrong_identity_and_expired_before_application_data() {
    for case in 0..4 {
        let source = source("m4ss", case == 2, None).await;
        let wrong_ca = Certificates::new();
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "ffmpeg");
        let mut input = json!({"url":source.url});
        if case != 0 {
            input["flussonix_tls_ca"] = json!(source.cert.ca);
        }
        if case == 1 {
            input["url"] = json!(source.url.replace("127.0.0.1", "127.0.0.2"));
        }
        if case == 3 {
            input["flussonix_tls_ca"] = json!(wrong_ca.ca);
        }
        let worker = engine
            .ensure("owned", &json!({"inputs":[input]}))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(6), async {
            while !worker.is_closed() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(worker.pid(), 0);
        assert_eq!(source.gets.load(Ordering::SeqCst), 0);
        engine.stop_all().await;
    }
}
#[tokio::test]
async fn native_secure_redirect_never_reaches_plaintext_or_foreign_origin() {
    for (protocol, segment, scheme) in [
        ("m4ss", false, "http"),
        ("m4ss", false, "https"),
        ("m4fs", false, "http"),
        ("m4fs", false, "https"),
        ("m4fs", true, "http"),
        ("m4fs", true, "https"),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let gets = Arc::new(AtomicUsize::new(0));
        let count = gets.clone();
        let addr = listener.local_addr().unwrap();
        let foreign = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                count.fetch_add(1, Ordering::SeqCst);
                drop(socket);
            }
        });
        let source = source(
            protocol,
            false,
            Some(if segment {
                Redirect::Segment(format!(
                    "{scheme}://{addr}/segment?token=owned-source-token"
                ))
            } else {
                Redirect::Control(format!(
                    "{scheme}://{addr}/owned/m4s?token=owned-source-token"
                ))
            }),
        )
        .await;
        let d = tempfile::tempdir().unwrap();
        let engine = Engine::new(d.path(), "ffmpeg");
        let worker = engine
            .ensure(
                "owned",
                &json!({"inputs":[{"url":source.url,"flussonix_tls_ca":source.cert.ca}]}),
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(6), async {
            while !worker.is_closed() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(worker.pid(), 0);
        assert_eq!(
            source.gets.load(Ordering::SeqCst),
            if segment { 2 } else { 1 }
        );
        assert_eq!(gets.load(Ordering::SeqCst), 0);
        engine.stop_all().await;
        foreign.abort();
    }
}

#[tokio::test]
async fn native_same_origin_https_redirect_is_allowed_except_for_peer_credentials() {
    for peer in [false, true] {
        let mut source = source("m4ss", false, Some(Redirect::SameOrigin)).await;
        let d = tempfile::tempdir().unwrap();
        let engine = Engine::new(d.path(), "ffmpeg");
        let worker = engine
            .ensure(
                "owned",
                &json!({
                    "inputs":[{"url":source.url,"flussonix_tls_ca":source.cert.ca}],
                    "flussonix_peer_key":if peer { Some("owned-peer") } else { None },
                    "flussonix_hls_subtitles":"convert",
                    "flussonix_hls_captions":[{"native_track":7,"language":"en","name":"English"}]
                }),
            )
            .await
            .unwrap();
        if peer {
            tokio::time::timeout(Duration::from_secs(6), async {
                while !worker.is_closed() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
            assert_eq!(source.gets.load(Ordering::SeqCst), 1);
        } else {
            ready(&engine, &worker).await;
            assert_eq!(source.gets.load(Ordering::SeqCst), 2);
        }
        engine.stop_all().await;
        source.cancel.cancel();
        (&mut source.task).await.unwrap();
    }
}
