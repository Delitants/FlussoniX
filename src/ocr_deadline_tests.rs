use super::*;
use crate::{
    captions::configuration,
    dvb::{Frame, Image},
};
use serde_json::json;
#[tokio::test]
async fn an_idle_worker_expires_an_inflight_interval_without_source_or_process_events() {
    let state=Arc::new(State::new(Decoder::new(configuration(&json!({"flussonix_hls_captions":[{"dvb_page":1,"ocr_language":"eng","language":"en","name":"English"}]})).unwrap()),"owned-deadline".into(),0));
    let mut changes = {
        let mut d = state.decoder.lock().unwrap();
        d.dvb_ocr.ingest(
            Frame {
                page: 1,
                pts: 90000,
                expires: 270000,
                image: Some(Image {
                    width: 2,
                    height: 2,
                    pixels: vec![[255, 255]; 4],
                }),
            },
            std::time::Instant::now(),
        );
        // A claimed interval remains pending until its owner finishes or its
        // absolute deadline expires. The second worker must not need a tick.
        let _claimed = d.dvb_ocr.take_job().unwrap();
        d.latest_pts = 450000;
        d.dvb_ocr.subscribe()
    };
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(
        state
            .clone()
            .ocr("/never-spawn-owned-deadline".into(), cancel.clone()),
    );
    tokio::time::timeout(Duration::from_secs(2), changes.changed())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.stats()["dvb_ocr"][0]["pending"], 0);
    assert_eq!(state.stats()["dvb_ocr"][0]["last_error"], "dvb_ocr_timeout");
    assert_eq!(state.decoder.lock().unwrap().publication_frontier(), 450000);
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(1), worker)
        .await
        .unwrap()
        .unwrap();
}
