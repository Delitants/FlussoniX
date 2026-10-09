//! Opt-in Intel VAAPI publishing qualification, using real sockets and decoders.
use super::*;
#[path = "http_gpu_native.rs"]
mod native;
#[path = "http_gpu_upstream.rs"]
mod upstream;
use std::{collections::HashSet, path::Path, sync::Arc};

fn destination(receiver: &Receiver) -> Value {
    let mut value = json!({"url":receiver.url.replace("://", "://gpu:owned-publishing-only@")+"/gpu/mpegts?token=owned-gpu","retry_timeout":1});
    if let Some(cert) = &receiver.cert {
        value["flussonix_tls_ca"] = json!(cert.ca);
    }
    value
}

fn encoder_arguments(worker: &flussonix::media::Worker, encoder: &str) -> Vec<String> {
    encoder_arguments_pid(worker.pid(), encoder)
}

fn encoder_arguments_pid(pid: u32, encoder: &str) -> Vec<String> {
    let process = std::path::PathBuf::from(format!("/proc/{pid}"));
    assert_eq!(
        std::fs::read_link(process.join("exe")).unwrap(),
        std::fs::canonicalize("/usr/bin/ffmpeg").unwrap()
    );
    let maps = std::fs::read_to_string(process.join("maps")).unwrap();
    assert!(!maps.contains("/opt/flussonic"));
    let args: Vec<String> = std::fs::read(process.join("cmdline"))
        .unwrap()
        .split(|b| *b == 0)
        .filter(|s| !s.is_empty())
        .map(|s| String::from_utf8(s.to_vec()).unwrap())
        .collect();
    assert!(args.windows(2).any(|a| a == ["-c:v", encoder]));
    if encoder == "h264_vaapi" {
        for pair in [
            ["-vaapi_device", "/dev/dri/renderD128"],
            ["-vf", "format=nv12,hwupload"],
            ["-rc_mode", "CQP"],
            ["-qp", "24"],
            ["-low_power", "0"],
        ] {
            assert!(args.windows(2).any(|a| a == pair));
        }
        assert!(
            maps.contains("iHD_drv_video.so"),
            "delivering encoder must actually load the independent Intel driver"
        );
    }
    args
}

async fn closed(receiver: &Receiver) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while receiver.capture.active.load(Ordering::SeqCst) != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("stopping/replacing worker closes each native publishing socket");
}

async fn decoded(path: &Path, audio: &str) -> Value {
    decoded_with_video(path, "h264", audio).await
}

async fn decoded_with_video(path: &Path, video: &str, audio: &str) -> Value {
    let probe = tokio::process::Command::new("ffprobe")
        .kill_on_drop(true)
        .args(["-v", "error", "-show_streams", "-of", "json"])
        .arg(path)
        .output();
    let probe = tokio::time::timeout(Duration::from_secs(20), probe)
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
            .any(|s| s["codec_name"] == video && s["width"] == 640 && s["height"] == 360)
    );
    assert!(
        tracks
            .iter()
            .any(|s| s["codec_name"] == audio && s["sample_rate"] == "48000")
    );
    let p = tokio::process::Command::new("ffmpeg")
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
        .output();
    let output = tokio::time::timeout(Duration::from_secs(20), p)
        .await
        .expect("independent full decode is bounded")
        .unwrap();
    assert!(
        output.status.success() && output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8(output.stdout).unwrap();
    let mut counts = [0usize; 2];
    let mut hashes: [HashSet<String>; 2] = Default::default();
    for line in text
        .lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
    {
        let fields: Vec<_> = line.split(',').map(str::trim).collect();
        assert_eq!(fields.len(), 6);
        let index: usize = fields[0].parse().unwrap();
        assert!(index < 2);
        counts[index] += 1;
        hashes[index].insert(fields[5].to_owned());
    }
    assert!(
        counts[0] >= 50 && counts[1] >= 80,
        "decoded frames: {counts:?}"
    );
    assert!(hashes[0].len() >= 5 && hashes[1].len() >= 2);
    json!({"video":video,"audio":audio,"video_frames":counts[0],"audio_frames":counts[1],"strict_decoder_errors":0})
}

async fn generation(
    engine: &flussonix::media::Engine,
    receivers: &[Receiver; 2],
    cfg: &Value,
    encoder: &str,
) -> (Arc<flussonix::media::Worker>, Vec<String>) {
    let worker = engine.ensure("owned-gpu", cfg).await.unwrap();
    for receiver in receivers {
        wait_capture(&receiver.capture).await;
        assert_eq!(receiver.capture.requests.load(Ordering::SeqCst), 1);
        assert_eq!(
            receiver.capture.paths.lock().unwrap().as_slice(),
            ["/gpu/mpegts?token=owned-gpu"]
        );
        assert_eq!(
            receiver.capture.headers.lock().unwrap().as_slice(),
            [format!(
                "Basic {}",
                STANDARD.encode("gpu:owned-publishing-only")
            )]
        );
    }
    assert_eq!(engine.count().await, 1);
    assert!(Arc::ptr_eq(
        &worker,
        &engine.ensure("owned-gpu", cfg).await.unwrap()
    ));
    tokio::time::timeout(Duration::from_secs(2), async {
        while !worker.stats()["flussonix_pushes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|s| s["body_bytes"].as_u64().unwrap() > 250000)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let args = encoder_arguments(&worker, encoder);
    for item in worker.stats()["flussonix_pushes"].as_array().unwrap() {
        assert_eq!(item["status"], "sending");
        assert_eq!(item["pid"], 0);
        assert!(item["body_bytes"].as_u64().unwrap() > 250000);
        assert!(!item.to_string().contains("owned-publishing-only"));
        assert!(!item.to_string().contains("owned-gpu"));
    }
    assert!(engine.http_push_egress.load(Ordering::Relaxed) > 500000);
    assert_eq!(engine.rtsp_push_egress.load(Ordering::Relaxed), 0);
    // This finite realtime fixture needs temporal coverage in addition to bytes:
    // CQP output may exceed the byte threshold in under two seconds.
    tokio::time::sleep(Duration::from_secs(3)).await;
    (worker, args)
}

async fn evidence(receivers: &[Receiver; 2], directory: &Path, audio: &str) -> Value {
    let mut reports = vec![];
    for (index, receiver) in receivers.iter().enumerate() {
        closed(receiver).await;
        let bytes = receiver.capture.data.lock().unwrap().clone();
        let file = directory.join(format!("output-{index}.ts"));
        std::fs::write(&file, &bytes[..bytes.len() / 188 * 188]).unwrap();
        let mut report = decoded(&file, audio).await;
        report["transport"] = json!(if index == 0 { "http" } else { "verified_https" });
        reports.push(report);
    }
    json!(reports)
}

fn retain(name: &str, source: &Path, report: &Value) {
    eprintln!("owned HTTP GPU qualification {name}: {report}");
    if let Some(root) = std::env::var_os("FLUSSONIX_HTTP_GPU_EVIDENCE_DIR") {
        let target = std::path::PathBuf::from(root).join(name);
        std::fs::create_dir_all(&target).unwrap();
        for index in 0..2 {
            std::fs::copy(
                source.join(format!("output-{index}.ts")),
                target.join(format!("output-{index}.ts")),
            )
            .unwrap();
        }
        std::fs::write(
            target.join("report.json"),
            serde_json::to_vec_pretty(report).unwrap(),
        )
        .unwrap();
    }
}

#[tokio::test]
#[ignore = "requires owned Intel VAAPI H.264 renderD128 and independent iHD driver environment"]
async fn intel_vaapi_h264_publishes_aac_mp2_mp3_over_http_and_verified_https() {
    for (audio, acodec, ab) in [
        ("aac", "aac", 96),
        ("mp2", "mp2a", 192),
        ("mp3", "mp3", 128),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut receivers = [Receiver::new(false).await, Receiver::new(true).await];
        let engine = flussonix::media::Engine::new(dir.path().join("media"), "/usr/bin/ffmpeg");
        let outcome = std::panic::AssertUnwindSafe(async {
            let store = ConfigStore::open(dir.path().join("config.json")).unwrap();
            store.put("templates", "gpu", json!({"transcoder":{"encoder":"h264_vaapi","qp":24,"acodec":acodec,"ab":ab},"pushes":receivers.iter().map(destination).collect::<Vec<_>>()})).unwrap();
            store.put("streams", "owned-gpu", json!({"template":"gpu","static":false,"inputs":[{"url":"testsrc://"}]})).unwrap();
            let cfg = store.effective("owned-gpu").unwrap();
            let (worker, args) = generation(&engine, &receivers, &cfg, "h264_vaapi").await;
            engine.stop_all().await;
            assert!(worker.is_closed());
            assert!(!Path::new(&format!("/proc/{}", worker.pid())).exists());
            let count = engine.http_push_egress.load(Ordering::Relaxed);
            let outputs = evidence(&receivers, dir.path(), audio).await;
            assert_eq!(engine.http_push_egress.load(Ordering::Relaxed), count);
            retain(audio, dir.path(), &json!({"encoder":"h264_vaapi","arguments":args,"outputs":outputs,"shared_worker_count":1,"reaped":true}));
        }).catch_unwind().await;
        engine.stop_all().await;
        for receiver in &mut receivers {
            receiver.stop().await;
        }
        if let Err(p) = outcome {
            std::panic::resume_unwind(p);
        }
    }
}

#[tokio::test]
#[ignore = "requires owned Intel VAAPI H.264 renderD128 and independent iHD driver environment"]
async fn gpu_cpu_gpu_replacement_reaps_encoders_and_closes_http_uploads() {
    let dir = tempfile::tempdir().unwrap();
    let engine = flussonix::media::Engine::new(dir.path().join("media"), "/usr/bin/ffmpeg");
    type SavedGeneration = (
        Arc<flussonix::media::Worker>,
        [Receiver; 2],
        Vec<String>,
        &'static str,
    );
    let mut previous: Option<SavedGeneration> = None;
    let mut current_receivers = [Receiver::new(false).await, Receiver::new(true).await];
    let outcome = std::panic::AssertUnwindSafe(async {
        for (index, (encoder, audio, acodec, ab)) in [("h264_vaapi","aac","aac",96),("libx264","mp2","mp2a",192),("h264_vaapi","mp3","mp3",128)].into_iter().enumerate() {
            let cfg = json!({"static":false,"inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":encoder,"acodec":acodec,"ab":ab},"pushes":current_receivers.iter().map(destination).collect::<Vec<_>>()});
            let (worker, args) = generation(&engine, &current_receivers, &cfg, encoder).await;
            if let Some((old, mut receivers, old_args, old_audio)) = previous.take() {
                assert_ne!(old.pid(),worker.pid());
                assert!(old.is_closed());
                assert!(!Path::new(&format!("/proc/{}",old.pid())).exists());
                let stage = dir.path().join(format!("generation-{}",index-1));std::fs::create_dir(&stage).unwrap();
                let outputs = evidence(&receivers,&stage,old_audio).await;
                retain(&format!("replacement-{}",index-1),&stage,&json!({"arguments":old_args,"outputs":outputs,"old_encoder_reaped":true}));
                for receiver in &mut receivers {receiver.stop().await;}
            }
            let next = [Receiver::new(false).await, Receiver::new(true).await];
            previous = Some((worker,std::mem::replace(&mut current_receivers,next),args,audio));
        }
        engine.stop_all().await;
        let (old,mut receivers,args,audio)=previous.take().unwrap();
        assert!(old.is_closed());assert!(!Path::new(&format!("/proc/{}",old.pid())).exists());
        let stage=dir.path().join("generation-2");std::fs::create_dir(&stage).unwrap();
        let count=engine.http_push_egress.load(Ordering::Relaxed);let outputs=evidence(&receivers,&stage,audio).await;
        assert_eq!(engine.http_push_egress.load(Ordering::Relaxed),count);
        retain("replacement-2",&stage,&json!({"arguments":args,"outputs":outputs,"reaped_on_stop":true}));
        for receiver in &mut receivers {receiver.stop().await;}
        assert_eq!(engine.count().await,0);
    }).catch_unwind().await;
    engine.stop_all().await;
    for receiver in &mut current_receivers {
        receiver.stop().await;
    }
    if let Some((_, mut receivers, _, _)) = previous.take() {
        for receiver in &mut receivers {
            receiver.stop().await;
        }
    }
    if let Err(p) = outcome {
        std::panic::resume_unwind(p);
    }
}

#[tokio::test]
#[ignore = "requires Intel GeminiLake 8086:3185 with H.264 available and HEVC unavailable"]
async fn unavailable_hevc_preserves_running_h264_gpu_uploads_without_cpu_fallback() {
    let hardware = Path::new("/sys/class/drm/renderD128/device");
    assert_eq!(
        std::fs::read_to_string(hardware.join("vendor"))
            .unwrap()
            .trim(),
        "0x8086"
    );
    assert_eq!(
        std::fs::read_to_string(hardware.join("device"))
            .unwrap()
            .trim(),
        "0x3185"
    );
    let dir = tempfile::tempdir().unwrap();
    let engine = flussonix::media::Engine::new(dir.path().join("media"), "/usr/bin/ffmpeg");
    let mut receivers = [Receiver::new(false).await, Receiver::new(true).await];
    let outcome = std::panic::AssertUnwindSafe(async {
        let cfg = json!({"static":false,"inputs":[{"url":"testsrc://"}],"transcoder":{"encoder":"h264_vaapi","acodec":"aac","ab":96},"pushes":receivers.iter().map(destination).collect::<Vec<_>>()});
        let (worker,args) = generation(&engine,&receivers,&cfg,"h264_vaapi").await;
        let mut unavailable = cfg.clone();unavailable["transcoder"]["encoder"] = json!("hevc_vaapi");
        let failure = tokio::time::timeout(Duration::from_secs(7),engine.ensure("owned-gpu",&unavailable)).await.unwrap().err().expect("unavailable HEVC must reject rather than fallback or replace encoder");
        assert_eq!(failure,"VAAPI HEVC encoder is unavailable on this host");
        assert!(!worker.is_closed());
        assert!(Arc::ptr_eq(&worker,&engine.ensure("owned-gpu",&cfg).await.unwrap()));
        assert_eq!(engine.count().await,1);
        let before: Vec<usize> = receivers.iter().map(|r| r.capture.data.lock().unwrap().len()).collect();
        tokio::time::timeout(Duration::from_secs(3),async {
            while receivers.iter().enumerate().any(|(i,r)|r.capture.data.lock().unwrap().len()<=before[i]) {tokio::time::sleep(Duration::from_millis(10)).await;}
        }).await.expect("both existing uploads must continue after failed HEVC replacement");
        assert_eq!(encoder_arguments(&worker,"h264_vaapi"),args);
        for receiver in &receivers {assert_eq!(receiver.capture.requests.load(Ordering::SeqCst),1);}
        engine.stop_all().await;
        assert!(!Path::new(&format!("/proc/{}",worker.pid())).exists());
        let outputs=evidence(&receivers,dir.path(),"aac").await;
        retain("hevc-denial-preserves-h264",dir.path(),&json!({"arguments":args,"outputs":outputs,"hevc_rejected":true,"existing_h264_uploads_preserved":true,"software_fallback":false,"reaped_on_stop":true}));
    }).catch_unwind().await;
    engine.stop_all().await;
    for receiver in &mut receivers {
        receiver.stop().await;
    }
    if let Err(p) = outcome {
        std::panic::resume_unwind(p);
    }
}
