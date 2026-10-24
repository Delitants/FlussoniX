// Appended only to private startup_tests; uses its real listener/router/decode gate.
#[tokio::test]
async fn private_checkpoint_observes_full_route_expiry() {
    let recorder=Arc::new(fx_packet_trace::Recorder::new(131072));
    assert!(fx_packet_trace::install(recorder.clone()));
    let path=std::path::PathBuf::from(std::env::var_os("FX_CHECKPOINT_EXPIRY_OUTPUT").expect("owned checkpoint output"));
    let monitor=fx_packet_trace::CheckpointMonitor::start(recorder.clone(),path.clone()).unwrap();
    full_route_case(true,4).await;
    let trigger=recorder.trigger().expect("actual expired subscription did not trigger checkpoint");
    assert_eq!(trigger.subscription,0);assert_eq!(trigger.record,0);assert!([1,2].contains(&trigger.reason));
    let metadata=std::path::PathBuf::from(format!("{}.coverage.json",path.display()));
    tokio::time::timeout(Duration::from_secs(3),async {while !metadata.exists() {tokio::time::sleep(Duration::from_millis(5)).await;}}).await.unwrap();
    monitor.stop().unwrap();
    let before=std::fs::read(&path).unwrap();recorder.failure(99,99,1);assert_eq!(recorder.trigger().unwrap(),trigger);assert_eq!(std::fs::read(&path).unwrap(),before);
}
