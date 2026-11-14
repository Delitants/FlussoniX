use bytes::Bytes;
use flussonix::{
    fxlink::{Listener, Provider, Request, Settings, Source},
    m4f::Frame,
    m4s::Track,
    media::Engine,
    wire,
};
use futures_util::{FutureExt, StreamExt, future::BoxFuture};
use serde_json::{Value, json};
use std::{net::SocketAddr, panic::AssertUnwindSafe, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
#[path = "support/tls.rs"]
mod tls_fixture;
fn fixture() -> (Vec<Track>, Vec<Frame>) {
    let tracks = vec![
        Track {
            id: 1,
            codec: "hevc".into(),
            config: include_bytes!("fixtures/codecs/hevc.hvcc").to_vec(),
        },
        Track {
            id: 2,
            codec: "m2a".into(),
            config: vec![],
        },
        Track {
            id: 3,
            codec: "mp3".into(),
            config: vec![],
        },
    ];
    let timing: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/codecs/hevc-timing.json")).unwrap();
    let mut samples = vec![];
    for cycle in 0..20u64 {
        for (i, t) in timing.iter().enumerate() {
            let d = t["dts"].as_i64().unwrap();
            let p = t["pts"].as_i64().unwrap();
            samples.push(Frame {
                track_id: 1,
                dts: 90000 + cycle * 43200 + ((d + 1024) * 90000 / 12800) as u64,
                pts_offset: (p - d) * 90000 / 12800,
                key: t["flags"].as_str().unwrap().contains('K'),
                body: std::fs::read(format!("tests/fixtures/codecs/hevc-{i:02}.bin")).unwrap(),
            });
        }
    }
    for i in 0..370 {
        for (id, step, body) in [
            (
                2,
                2160,
                include_bytes!("fixtures/codecs/mp2.bin").as_slice(),
            ),
            (
                3,
                2351,
                include_bytes!("fixtures/codecs/mp3.bin").as_slice(),
            ),
        ] {
            samples.push(Frame {
                track_id: id,
                dts: 90000 + i * step,
                pts_offset: 0,
                key: true,
                body: body.to_vec(),
            });
        }
    }
    samples.sort_by_key(|f| f.dts);
    (tracks, samples)
}
struct NativeSource;
impl Provider for NativeSource {
    fn subscribe(&self, request: Request, _peer: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
        Box::pin(async move {
            if request.peer_key != "owned-codec-peer" || request.token != "owned-codec-viewer" {
                return Err(403);
            }
            let (tracks, frames) = fixture();
            let mut records = vec![(0, Bytes::from(wire::encode_info(&tracks).unwrap()))];
            for f in frames {
                records.push((
                    f.dts.saturating_sub(90000),
                    Bytes::from(
                        wire::encode_frame(tracks.iter().find(|t| t.id == f.track_id).unwrap(), &f)
                            .unwrap(),
                    ),
                ));
            }
            let begin = tokio::time::Instant::now();
            let stream = futures_util::stream::iter(records)
                .then(move |(dts, record)| async move {
                    tokio::time::sleep_until(
                        begin + Duration::from_micros(dts * 1_000_000 / 90000),
                    )
                    .await;
                    Ok(record)
                })
                .chain(futures_util::stream::pending());
            Ok(Source {
                pending_records: None,
                records: Box::pin(stream),
                current: Arc::new(|| true),
                cancelled: CancellationToken::new(),
            })
        })
    }
}
#[tokio::test]
async fn hevc_m2a_and_mp3_fxlink_input_preserves_codec_delivery() {
    codec_case(false).await;
}
#[tokio::test]
async fn v6_hevc_m2a_and_mp3_keep_native_delivery_and_exercise_trimmed_records() {
    codec_case(true).await;
}
async fn codec_case(trimmed: bool) {
    let cert = tls_fixture::Certificates::new();
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let listener = Listener::bind_with_features(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        Settings::default(),
        flussonix::fxlink::Features {
            trimmed_records: trimmed,
            ..Default::default()
        },
    )
    .unwrap();
    let url = format!("fxlink://{}/owned", listener.address());
    let task = tokio::spawn(listener.serve(Arc::new(NativeSource), cancel.clone()));
    let engine = Engine::new_with_fxlink(
        dir.path().join("media"),
        "ffmpeg",
        flussonix::fxlink::Features {
            trimmed_records: trimmed,
            ..Default::default()
        },
    );
    let outcome=AssertUnwindSafe(async {
  let cfg=json!({"flussonix_peer_key":"owned-codec-peer","inputs":[{"url":url,"flussonix_tls_ca":cert.ca,"flussonix_fxlink_token":"owned-codec-viewer"}]});
  let worker=engine.ensure("owned",&cfg).await.unwrap();
  let manifest=tokio::time::timeout(Duration::from_secs(20),async {
   loop {if let Ok(m)=engine.read("owned","index.m3u8").await {break m;}
    assert!(!worker.is_closed(),"FXLink native codec worker closed: {}",engine.stats("owned").await);
    tokio::time::sleep(Duration::from_millis(50)).await;
   }
  }).await.expect("HEVC/MPEG audio native records must produce HLS");
  let manifest=String::from_utf8(manifest.to_vec()).unwrap();
  let segment=manifest.lines().find(|line|!line.is_empty()&&!line.starts_with('#')).unwrap();
  let bytes=engine.read("owned",segment).await.unwrap();
  let file=dir.path().join("owned-codecs.ts");tokio::fs::write(&file,bytes).await.unwrap();
  let probe=tokio::time::timeout(Duration::from_secs(10),tokio::process::Command::new("ffprobe").kill_on_drop(true).args(["-v","error","-show_streams","-of","json"]).arg(&file).output()).await.unwrap().unwrap();
  assert!(probe.status.success());let info:Value=serde_json::from_slice(&probe.stdout).unwrap();
  let codecs=info["streams"].as_array().unwrap().iter().map(|s|s["codec_name"].as_str().unwrap()).collect::<Vec<_>>();
  assert_eq!(codecs,vec!["hevc","mp2","mp3"]);
  let decode=tokio::time::timeout(Duration::from_secs(15),tokio::process::Command::new("ffmpeg").kill_on_drop(true).args(["-nostdin","-v","error","-xerror","-i"]).arg(&file).args(["-map","0","-t","1","-f","null","-"]).output()).await.unwrap().unwrap();
  assert!(decode.status.success()&&decode.stderr.is_empty(),"{}",String::from_utf8_lossy(&decode.stderr));
  let (boot,_)=worker.wire.m4s_subscribe();
  let mut decoder=flussonix::m4s::Decoder::default();
  let mut metadata=Vec::new();for record in boot {for event in decoder.push(&record).unwrap(){if let flussonix::m4s::Event::Info{tracks,..}=event{metadata=tracks;}}}
  assert_eq!(metadata,fixture().0,"native metadata must retain HEVC, m2a and MP3 configurations");
  let stats=engine.stats("owned").await;
  assert_eq!(stats["fxlink"]["protocol_version"],if trimmed {6}else{5});
  if trimmed {assert!(stats["fxlink"]["trimmed_shards"].as_u64().unwrap()>0);}

 }).catch_unwind().await;
    cancel.cancel();
    engine.stop_all().await;
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
