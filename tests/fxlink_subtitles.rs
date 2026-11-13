//! FXLink reuses native text policy and HLS conversion, with original samples intact.
use bytes::Bytes;
use flussonix::{
    fxlink::{Listener, Provider, Request, Settings, Source},
    m4f::Frame,
    m4s::{Decoder, Event},
    media::Engine,
    wire,
};
use futures_util::{FutureExt, StreamExt, future::BoxFuture};
use serde_json::json;
use std::{net::SocketAddr, panic::AssertUnwindSafe, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
#[path = "support/native_text.rs"]
mod text_fixture;
#[path = "support/tls.rs"]
mod tls_fixture;
struct NativeText;
impl Provider for NativeText {
    fn subscribe(&self, _: Request, _: SocketAddr) -> BoxFuture<'_, Result<Source, u16>> {
        Box::pin(async {
            let (tracks, frames) = text_fixture::fixture(false);
            let mut records = vec![(0, Bytes::from(wire::encode_info(&tracks).unwrap()))];
            for f in frames {
                records.push((
                    f.dts - 90000,
                    Bytes::from(
                        wire::encode_frame(tracks.iter().find(|t| t.id == f.track_id).unwrap(), &f)
                            .unwrap(),
                    ),
                ));
            }
            let start = tokio::time::Instant::now();
            let records = futures_util::stream::iter(records)
                .then(move |(dts, bytes)| async move {
                    tokio::time::sleep_until(
                        start + Duration::from_micros(dts * 1_000_000 / 90000),
                    )
                    .await;
                    Ok(bytes)
                })
                .chain(futures_util::stream::pending());
            Ok(Source {
                pending_records: None,
                records: Box::pin(records),
                current: Arc::new(|| true),
                cancelled: CancellationToken::new(),
            })
        })
    }
}
fn text_frames(decoder: &mut Decoder, bytes: &[u8], output: &mut Vec<Frame>) {
    for event in decoder.push(bytes).unwrap() {
        match event {
            Event::Frame {
                track_id,
                dts,
                pts_offset,
                key,
                body,
                ..
            } if track_id != 1 => output.push(Frame {
                track_id,
                dts,
                pts_offset,
                key,
                body,
            }),
            Event::Gop { frames, .. } => {
                output.extend(frames.into_iter().filter(|f| f.track_id != 1))
            }
            _ => {}
        }
    }
}
#[tokio::test]
async fn native_text_can_be_kept_converted_or_filtered_over_fxlink() {
    let dir = tempfile::tempdir().unwrap();
    let cert = tls_fixture::Certificates::new();
    let cancel = CancellationToken::new();
    let listener = Listener::bind(
        "127.0.0.1:0".parse().unwrap(),
        &cert.cert,
        &cert.key,
        Settings::default(),
    )
    .unwrap();
    let url = format!("fxlink://{}/text", listener.address());
    let task = tokio::spawn(listener.serve(Arc::new(NativeText), cancel.clone()));
    let engine = Engine::new(dir.path().join("media"), "ffmpeg");
    let outcome=AssertUnwindSafe(async {
  for mode in ["passthrough","convert","drop"]{
   let preserve=mode!="drop";
   let cfg=json!({"flussonix_peer_key":"owned-subtitle-peer","inputs":[{"url":url,"flussonix_tls_ca":cert.ca}],
    "flussonix_subtitle_tracks":if preserve{"preserve"}else{"drop"},"flussonix_hls_subtitles":mode,
    "flussonix_hls_captions":[{"native_track":7,"language":"en","name":"English"},{"native_track":u32::MAX,"language":"de","name":"Deutsch"}]});
   let worker=engine.ensure(mode,&cfg).await.unwrap();
   let(bootstrap,mut receiver)=worker.wire.m4s_subscribe();
   let observe=async {
    let mut decoder=Decoder::default();let mut frames=Vec::new();
    if !preserve{return frames;}
    for bytes in bootstrap{text_frames(&mut decoder,&bytes,&mut frames);}
    while frames.len()<4 {text_frames(&mut decoder,&receiver.recv().await.unwrap(),&mut frames);}
    frames
   };
   let ready=async{
    let av=if mode=="convert"{"av.m3u8"}else{"index.m3u8"};
    loop{
     if engine.read(mode,av).await.is_ok_and(|b|String::from_utf8_lossy(&b).matches("#EXTINF:").count()>=3){break;}
     assert!(!worker.is_closed(),"native FXLink text worker closed: {}",worker.stats());
     tokio::time::sleep(Duration::from_millis(30)).await;
    }
   };
   let(frames,())=tokio::time::timeout(Duration::from_secs(15),async{tokio::join!(observe,ready)}).await.expect("text samples and HLS must progress");
   if preserve {
    let expected=text_fixture::fixture(false).1.into_iter().filter(|f|f.track_id!=1).collect::<Vec<_>>();
    assert_eq!(frames.iter().map(|f|(f.track_id,f.dts,f.pts_offset,f.key,&f.body)).collect::<Vec<_>>(),expected.iter().map(|f|(f.track_id,f.dts,f.pts_offset,f.key,&f.body)).collect::<Vec<_>>(),"preserved native text bodies, timing and clears must remain exact");
   }
   let mut decoder=Decoder::default();let mut text_tracks=0;
   for bytes in worker.wire.m4s_subscribe().0 {for event in decoder.push(&bytes).unwrap(){if let Event::Info{tracks,..}=event{text_tracks=tracks.iter().filter(|t|t.codec=="subtitle").count();}}}
   assert_eq!(text_tracks,if preserve{2}else{0});
   assert_eq!(worker.stats()["native_subtitle_output"],if preserve{"Kept"}else{"Filtered"});
   for prefix in ["","fmp4/"]{
    let master=String::from_utf8(engine.read(mode,&format!("{prefix}index.m3u8")).await.unwrap().to_vec()).unwrap();
    assert_eq!(master.contains("TYPE=SUBTITLES"),mode=="convert");
    for(id,want,other)in[(7,"AMERICA &lt;HELLO&gt;","EUROPE"),(u32::MAX,"EUROPE GRÜSSE","AMERICA")]{
     let list=engine.read(mode,&format!("{prefix}nt{id}.m3u8")).await;
     if mode!="convert"{assert!(list.is_err());continue;}
     let list=String::from_utf8(list.unwrap().to_vec()).unwrap();let mut text=String::new();
     for segment in list.lines().filter(|line|line.ends_with(".vtt")){
      let bytes=engine.read(mode,&format!("{prefix}{segment}")).await.unwrap();
      text+=std::str::from_utf8(&bytes).unwrap();
     }
     assert!(text.contains(want)&&!text.contains(other),"{text}");
     assert!(text.contains("00:00:01.920 --> 00:00:02.920"),"native early clear must truncate the HLS cue: {text}");
    }
   }
   engine.stop_all().await;
  }
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
