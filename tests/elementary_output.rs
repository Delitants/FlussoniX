use flussonix::media::Engine;
use serde_json::json;
use std::{
    net::UdpSocket,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::net::UdpSocket as AsyncUdp;
fn receivers() -> (u16, Vec<AsyncUdp>) {
    for _ in 0..64 {
        let a = UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = a.local_addr().unwrap().port();
        if port > 65520 {
            continue;
        }
        let mut sockets = vec![a];
        for p in port + 1..port + 16 {
            if let Ok(s) = UdpSocket::bind(("127.0.0.1", p)) {
                sockets.push(s);
            } else {
                break;
            }
        }
        if sockets.len() != 16 {
            continue;
        }
        return (
            port,
            sockets
                .into_iter()
                .map(|s| {
                    s.set_nonblocking(true).unwrap();
                    AsyncUdp::from_std(s).unwrap()
                })
                .collect(),
        );
    }
    panic!("no owned pairs")
}
#[tokio::test]
async fn elementary_output_uses_native_tracks_shared_worker_and_sends_rtcp() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::new(d.path(), "/usr/bin/ffmpeg");
    let (port, sockets) = receivers();
    let cfg = json!({"inputs":[{"url":"testsrc://"}],"flussonix_rtp_outputs":[{"url":format!("rtp://127.0.0.1:{port}"),"flussonix_rtp":{"profile":"elementary"}}]});
    let w = e.ensure("owned", &cfg).await.unwrap();
    assert!(Arc::ptr_eq(&w, &e.ensure("owned", &cfg).await.unwrap()));
    let mut b = [0; 1601];
    let (n, _) = tokio::time::timeout(Duration::from_secs(10), sockets[0].recv_from(&mut b))
        .await
        .unwrap()
        .unwrap();
    let payload = b[1] & 127;
    let snapshot = w.stats();
    if payload != 96 {
        e.stop_all().await;
        assert_eq!(payload, 96, "video must be native elementary RTP, not PT33");
    }
    assert_eq!(
        snapshot["flussonix_rtp_outputs"][0]["profile"],
        "elementary"
    );
    assert_eq!(snapshot["flussonix_rtp_outputs"][0]["sdp_ready"], true);
    assert!(e.direct_egress.load(Ordering::Relaxed) >= n as u64 + 28);
    let (n, _) = tokio::time::timeout(Duration::from_secs(3), sockets[2].recv_from(&mut b))
        .await
        .unwrap()
        .unwrap();
    assert!(n > 12);
    assert_eq!(b[1] & 127, 97);
    let (n, _) = tokio::time::timeout(Duration::from_secs(8), sockets[1].recv_from(&mut b))
        .await
        .unwrap()
        .unwrap();
    assert!(flussonix::direct_rtp::packet::valid_rtcp(&b[..n]));
    assert_eq!(b[1], 200);
    w.wire.rtp.configure(&[]);
    tokio::time::timeout(Duration::from_secs(2), async {
        while w.stats()["flussonix_rtp_outputs"][0]["status"] != "failed" {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(w.stats()["flussonix_rtp_outputs"][0]["sdp_ready"], false);
    e.stop_all().await;
    assert!(w.is_closed());
    assert!(!std::path::Path::new(&format!("/proc/{}", w.pid())).exists());
}
#[tokio::test]
async fn zero_media_epoch_has_a_nonzero_rtp_origin_and_preserves_audio_spacing() {
    use flussonix::{m4f::Frame, m4s::Track};
    use std::os::unix::fs::PermissionsExt;
    let d = tempfile::tempdir().unwrap();
    let idle = d.path().join("owned-idle");
    // Framing/lifecycle fixture only; independent media tests use real FFmpeg.
    std::fs::write(&idle, "#!/bin/sh\nexec sleep 30\n").unwrap();
    std::fs::set_permissions(&idle, std::fs::Permissions::from_mode(0o700)).unwrap();
    let e = Engine::new(d.path().join("media"), idle.to_str().unwrap());
    let (port, sockets) = receivers();
    let cfg = json!({"inputs":[{"url":"testsrc://"}],"flussonix_rtp_outputs":[{"url":format!("rtp://127.0.0.1:{port}"),"flussonix_rtp":{"profile":"elementary"}}]});
    let w = e.ensure("owned", &cfg).await.unwrap();
    w.wire.rtp.configure(&[Track {
        id: 7,
        codec: "aac".into(),
        config: vec![0x11, 0x90],
    }]);
    for dts in [0, 1920] {
        w.wire.rtp.frame(&Frame {
            track_id: 7,
            dts,
            pts_offset: 0,
            key: true,
            body: vec![42],
        });
    }
    let mut b = [0; 1601];
    let mut stamps = vec![];
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(2), sockets[0].recv_from(&mut b))
            .await
            .unwrap()
            .unwrap();
        stamps.push(u32::from_be_bytes(b[4..8].try_into().unwrap()));
    }
    e.stop_all().await;
    assert_ne!(
        stamps[0], 0,
        "A zero RTP origin resets independent decoder clock initialization"
    );
    assert_eq!(stamps[1].wrapping_sub(stamps[0]), 1024);
    assert!(!std::path::Path::new(&format!("/proc/{}", w.pid())).exists());
}
#[tokio::test]
async fn receiver_can_bind_after_reading_actual_sdp_without_restarting_worker() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::new(d.path(), "/usr/bin/ffmpeg");
    let (port, reserved) = receivers();
    drop(reserved);
    let cfg = json!({"inputs":[{"url":"testsrc://"}],"flussonix_rtp_outputs":[{"url":format!("rtp://127.0.0.1:{port}"),"flussonix_rtp":{"profile":"elementary"}}]});
    let w = e.ensure("owned", &cfg).await.unwrap();
    tokio::time::sleep(Duration::from_secs(8)).await;
    let sdp = e.rtp_sdp("owned", 0, &cfg).await;
    let status = w.stats();
    let receiver = AsyncUdp::bind(("127.0.0.1", port)).await.unwrap();
    let _rtcp = AsyncUdp::bind(("127.0.0.1", port + 1)).await.unwrap();
    let _audio = AsyncUdp::bind(("127.0.0.1", port + 2)).await.unwrap();
    let _audio_rtcp = AsyncUdp::bind(("127.0.0.1", port + 3)).await.unwrap();
    let mut b = [0; 1601];
    let media = tokio::time::timeout(Duration::from_secs(2), receiver.recv_from(&mut b)).await;
    e.stop_all().await;
    assert!(
        sdp.is_ok(),
        "Actual SDP must survive a not-yet-bound receiver: {status}"
    );
    assert!(media.is_ok(), "Late receiver did not get media: {status}");
    assert!(
        status["flussonix_rtp_outputs"][0]["unreachable_packets"]
            .as_u64()
            .unwrap()
            > 0
    );
}
#[tokio::test]
async fn four_destinations_receive_distinct_track_pairs_from_one_worker() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::new(d.path(), "/usr/bin/ffmpeg");
    let mut endpoints = vec![];
    let mut listeners = vec![];
    for _ in 0..4 {
        let (port, sockets) = receivers();
        endpoints.push(json!({"url":format!("rtp://127.0.0.1:{port}"),"flussonix_rtp":{"profile":"elementary"}}));
        listeners.push((port, sockets));
    }
    let cfg = json!({"inputs":[{"url":"testsrc://"}],"flussonix_rtp_outputs":endpoints});
    let w = e.ensure("owned", &cfg).await.unwrap();
    assert!(Arc::ptr_eq(&w, &e.ensure("owned", &cfg).await.unwrap()));
    let mut b = [0; 1601];
    for (i, (port, sockets)) in listeners.iter().enumerate() {
        for (lane, payload) in [(0, 96), (2, 97)] {
            tokio::time::timeout(Duration::from_secs(10), sockets[lane].recv_from(&mut b))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(b[1] & 127, payload);
        }
        let sdp = e.rtp_sdp("owned", i, &cfg).await.unwrap();
        assert!(sdp.contains(&format!("m=video {port} RTP/AVP 96")));
        assert!(sdp.contains(&format!("m=audio {} RTP/AVP 97", port + 2)));
    }
    assert_eq!(e.count().await, 1);
    e.stop_all().await;
    assert!(!std::path::Path::new(&format!("/proc/{}", w.pid())).exists());
}
#[tokio::test]
async fn paced_destination_queue_lag_fails_and_clears_its_description() {
    use flussonix::{m4f::Frame, m4s::Track};
    use std::os::unix::fs::PermissionsExt;
    let d = tempfile::tempdir().unwrap();
    let idle = d.path().join("owned-idle");
    std::fs::write(&idle, "#!/bin/sh\nexec sleep 30\n").unwrap();
    std::fs::set_permissions(&idle, std::fs::Permissions::from_mode(0o700)).unwrap();
    let e = Engine::new(d.path().join("media"), idle.to_str().unwrap());
    let (port, _listeners) = receivers();
    let cfg = json!({"inputs":[{"url":"testsrc://"}],"flussonix_rtp_outputs":[{"url":format!("rtp://127.0.0.1:{port}"),"max_mbps":1,"flussonix_rtp":{"profile":"elementary"}}]});
    let w = e.ensure("owned", &cfg).await.unwrap();
    w.wire.rtp.configure(&[Track {
        id: 7,
        codec: "aac".into(),
        config: vec![0x11, 0x90],
    }]);
    w.wire.rtp.frame(&Frame {
        track_id: 7,
        dts: 0,
        pts_offset: 0,
        key: true,
        body: vec![42],
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !w.stats()["flussonix_rtp_outputs"][0]["sdp_ready"]
            .as_bool()
            .unwrap()
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    for n in 1..10000 {
        w.wire.rtp.frame(&Frame {
            track_id: 7,
            dts: n * 1920,
            pts_offset: 0,
            key: true,
            body: vec![42],
        });
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while w.stats()["flussonix_rtp_outputs"][0]["status"] != "failed" {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let stats = w.stats();
    e.stop_all().await;
    assert!(
        stats["flussonix_rtp_outputs"][0]["last_error"]
            .as_str()
            .unwrap()
            .contains("lagged")
    );
    assert_eq!(stats["flussonix_rtp_outputs"][0]["sdp_ready"], false);
    assert!(!std::path::Path::new(&format!("/proc/{}", w.pid())).exists());
}

#[tokio::test]
async fn related_tracks_share_cname_and_a_stable_clock_despite_composition_and_send_delay() {
    use flussonix::{m4f::Frame, m4s::Track};
    use std::os::unix::fs::PermissionsExt;
    let d = tempfile::tempdir().unwrap();
    let idle = d.path().join("owned-idle");
    std::fs::write(&idle, "#!/bin/sh\nexec sleep 30\n").unwrap();
    std::fs::set_permissions(&idle, std::fs::Permissions::from_mode(0o700)).unwrap();
    let e = Engine::new(d.path().join("media"), idle.to_str().unwrap());
    let (port, sockets) = receivers();
    let cfg = json!({"inputs":[{"url":"testsrc://"}],"flussonix_rtp_outputs":[{"url":format!("rtp://127.0.0.1:{port}"),"flussonix_rtp":{"profile":"elementary"}}]});
    let w = e.ensure("owned", &cfg).await.unwrap();
    w.wire.rtp.configure(&[
        Track {
            id: 7,
            codec: "h264".into(),
            config: vec![
                1, 100, 0, 31, 255, 225, 0, 4, 103, 100, 0, 31, 1, 0, 2, 104, 0,
            ],
        },
        Track {
            id: 8,
            codec: "aac".into(),
            config: vec![0x11, 0x90],
        },
    ]);
    let frame = |id, dts, offset| Frame {
        track_id: id,
        dts,
        pts_offset: offset,
        key: true,
        body: if id == 7 {
            vec![0, 0, 0, 2, 0x65, 42]
        } else {
            vec![42]
        },
    };
    for id in [7, 8] {
        w.wire
            .rtp
            .frame(&frame(id, 0, if id == 7 { 9000 } else { 0 }));
    }
    let mut b = [0; 1601];
    let mut origins = vec![];
    for (i, offset) in [(0, 9000), (2, 0)] {
        tokio::time::timeout(Duration::from_secs(2), sockets[i].recv_from(&mut b))
            .await
            .unwrap()
            .unwrap();
        origins.push(u32::from_be_bytes(b[4..8].try_into().unwrap()).wrapping_sub(offset));
    }
    w.wire.rtp.frame(&frame(7, 1920, 18000));
    tokio::time::timeout(Duration::from_secs(2), sockets[0].recv_from(&mut b))
        .await
        .unwrap()
        .unwrap();
    let changed_pts = u32::from_be_bytes(b[4..8].try_into().unwrap()).wrapping_sub(origins[0]);
    tokio::time::sleep(Duration::from_millis(800)).await;
    w.wire.rtp.frame(&frame(8, 1920, 0));
    tokio::time::timeout(Duration::from_secs(2), sockets[2].recv_from(&mut b))
        .await
        .unwrap()
        .unwrap();
    let mut names = vec![];
    let mut mapped = vec![];
    let mut ssrcs = vec![];
    for (j, i) in [1, 3].into_iter().enumerate() {
        let (n, _) = tokio::time::timeout(Duration::from_secs(7), sockets[i].recv_from(&mut b))
            .await
            .unwrap()
            .unwrap();
        assert!(flussonix::direct_rtp::packet::valid_rtcp(&b[..n]));
        ssrcs.push(u32::from_be_bytes(b[4..8].try_into().unwrap()));
        names.push(b[38..38 + usize::from(b[37])].to_vec());
        let ntp = u32::from_be_bytes(b[8..12].try_into().unwrap()) as f64
            + u32::from_be_bytes(b[12..16].try_into().unwrap()) as f64 / 4294967296.0;
        let stamp = u32::from_be_bytes(b[16..20].try_into().unwrap());
        mapped.push(
            ntp - stamp.wrapping_sub(origins[j]) as f64 / if j == 0 { 90000.0 } else { 48000.0 },
        );
    }
    e.stop_all().await;
    assert_eq!(
        changed_pts, 19920,
        "Changing composition offsets remain in media timestamps"
    );
    assert_ne!(ssrcs[0], ssrcs[1]);
    assert!(
        names[0] == names[1] && (mapped[0] - mapped[1]).abs() < 0.002,
        "Related tracks need one CNAME and media epoch despite PTS/send delay: CNAMEs={names:?}, wall epochs={mapped:?}"
    );
    assert!(!std::path::Path::new(&format!("/proc/{}", w.pid())).exists());
}
