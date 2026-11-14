use flussonix::config::ConfigStore;
use flussonix::peer_hls::PeerHls;
use serde_json::json;

// Break caught: operators cannot explicitly scope legacy plaintext peer-key links.
#[test]
fn insecure_peer_http_override_is_scoped_to_saved_relationships() {
    let directory = tempfile::tempdir().unwrap();
    let store = ConfigStore::open(directory.path().join("config.json")).unwrap();
    let source = store
        .put(
            "sources",
            "m4s://origin.example",
            json!({"flussonix_allow_insecure_peer_http":true}),
        )
        .unwrap();
    assert_eq!(source["flussonix_allow_insecure_peer_http"], true);
    let peer = store
        .put(
            "peers",
            "edge",
            json!({"api_url":"http://edge.example", "flussonix_allow_insecure_peer_http":true}),
        )
        .unwrap();
    assert_eq!(peer["flussonix_allow_insecure_peer_http"], true);
    let stream = store
        .put(
            "streams",
            "music",
            json!({
                "inputs":[{"url":"m4s://origin.example/music"}],
                "flussonix_peer_key":"owned-native-peer-key",
                "flussonix_allow_insecure_peer_http":true
            }),
        )
        .unwrap();
    assert_eq!(stream["flussonix_allow_insecure_peer_http"], true);
    assert!(
        store
            .validate(
                json!({"streams":[{"name":"broken", "flussonix_allow_insecure_peer_http":"yes"}]})
            )
            .is_err()
    );
}

// Break caught: HLS or MPEG-TS media fetches bypass management transport checks.
#[tokio::test]
async fn keyed_remote_hls_input_requires_secure_transport_by_default() {
    match PeerHls::start("http://remote.example/live.m3u8", "owned-native-peer-key").await {
        Err(error) => assert_eq!(error, "peer key requires HTTPS or literal loopback"),
        Ok(_) => panic!("remote HTTP input accepted a peer key"),
    }
}
