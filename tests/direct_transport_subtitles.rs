#[path = "support/direct_subtitles.rs"]
mod regional;
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
#[tokio::test]
async fn rtp_carries_608_708_dvb_teletext_and_independent_hls_policies() {
    let _guard = SERIAL.lock().await;
    regional::run(false).await;
}
#[tokio::test]
async fn srtp_carries_608_708_dvb_teletext_and_independent_hls_policies() {
    let _guard = SERIAL.lock().await;
    regional::run(true).await;
}
