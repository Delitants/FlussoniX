use base64::{Engine as _, engine::general_purpose::STANDARD};
use flussonix::{
    direct_rtp::{
        config::{OutputTrackKey, TrackMedia},
        crypto::{self, Session},
        packet,
    },
    m4f::Frame,
    m4s::Track,
    media::Engine,
    rtp::{Description, MediaTrack},
};
use serde_json::json;
use std::{
    collections::HashSet,
    net::UdpSocket,
    os::unix::fs::PermissionsExt,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::net::UdpSocket as AsyncUdp;
fn receivers() -> (u16, Vec<AsyncUdp>) {
    for _ in 0..64 {
        let first = UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = first.local_addr().unwrap().port();
        if port > 65520 {
            continue;
        }
        let mut sockets = vec![first];
        for p in port + 1..port + 16 {
            if let Ok(s) = UdpSocket::bind(("127.0.0.1", p)) {
                sockets.push(s)
            } else {
                break;
            }
        }
        if sockets.len() == 16 {
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
    }
    panic!("no owned pairs")
}
fn key(d: &std::path::Path) -> std::path::PathBuf {
    let p = d.join("owned.key");
    std::fs::write(&p, STANDARD.encode([0x31; 30])).unwrap();
    std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
    p
}
fn destination(port: u16, key: &std::path::Path) -> serde_json::Value {
    json!({"url":format!("srtp://127.0.0.1:{port}"),"flussonix_rtp":{"profile":"elementary","key_file":key}})
}

#[test]
fn per_track_output_keys_follow_stable_track_identity_and_reject_ambiguous_sets() {
    let directory = tempfile::tempdir().unwrap();
    let a = directory.path().join("a.key");
    let b = directory.path().join("b.key");
    for (path, value) in [(&a, 0x41), (&b, 0x42)] {
        std::fs::write(path, STANDARD.encode([value; 30])).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let description = Description {
        generation: 1,
        tracks: vec![
            MediaTrack {
                id: 7,
                payload: 96,
                clock: 48000,
                ssrc: 10,
                fmtp: String::new(),
                encoding: "MPEG4-GENERIC/48000/2".into(),
                video: false,
            },
            MediaTrack {
                id: 9,
                payload: 14,
                clock: 90000,
                ssrc: 11,
                fmtp: String::new(),
                encoding: "MPA/90000".into(),
                video: false,
            },
        ],
    };
    let keys = vec![
        OutputTrackKey {
            track_id: 9,
            media: TrackMedia::Audio,
            encoding: "MPA/90000".into(),
            key_file: b.clone(),
        },
        OutputTrackKey {
            track_id: 7,
            media: TrackMedia::Audio,
            encoding: "MPEG4-GENERIC/48000/2".into(),
            key_file: a.clone(),
        },
    ];
    let mut contexts = crypto::send_track_sessions(&keys, &description, &[101, 202]).unwrap();
    let first = packet::packet(1, 0, 101, &[1, 2, 3]);
    let second = packet::packet(1, 0, 202, &[4, 5, 6]);
    for (index, plain, key) in [(0, &first, 0x41), (1, &second, 0x42)] {
        let mut encrypted = plain.clone();
        contexts[index].1.protect(&mut encrypted, false).unwrap();
        let mut matching = encrypted.clone();
        Session::new([key; 30], None)
            .unwrap()
            .unprotect(&mut matching, false)
            .unwrap();
        assert_eq!(&matching, plain);
        assert!(
            Session::new([if key == 0x41 { 0x42 } else { 0x41 }; 30], None)
                .unwrap()
                .unprotect(&mut encrypted, false)
                .is_err()
        );
    }
    assert!(crypto::send_track_sessions(&keys, &description, &[101]).is_err());
    assert!(crypto::send_track_sessions(&keys[..1], &description, &[101, 202]).is_err());
    let mut extra = keys.clone();
    extra.push(OutputTrackKey {
        track_id: 10,
        media: TrackMedia::Audio,
        encoding: "MPA/90000".into(),
        key_file: directory.path().join("extra.key"),
    });
    assert!(crypto::send_track_sessions(&extra, &description, &[101, 202]).is_err());
    let mut replaced = description.clone();
    replaced.tracks[1].id = 10;
    assert!(crypto::send_track_sessions(&keys, &replaced, &[101, 202]).is_err());
    replaced.tracks.pop();
    assert!(crypto::send_track_sessions(&keys, &replaced, &[101]).is_err());
    let mut wrong = keys.clone();
    wrong[0].track_id = 7;
    assert!(crypto::send_track_sessions(&wrong, &description, &[101, 202]).is_err());
    wrong = keys.clone();
    wrong[1].media = TrackMedia::Video;
    assert!(crypto::send_track_sessions(&wrong, &description, &[101, 202]).is_err());
    wrong = keys.clone();
    wrong[1].encoding = "H264/90000".into();
    assert!(crypto::send_track_sessions(&wrong, &description, &[101, 202]).is_err());
    wrong = keys.clone();
    wrong[0].key_file = a;
    assert!(crypto::send_track_sessions(&wrong, &description, &[101, 202]).is_err());
    std::fs::write(&b, STANDARD.encode([0x41; 30])).unwrap();
    assert!(crypto::send_track_sessions(&keys, &description, &[101, 202]).is_err());
}

#[tokio::test]
async fn live_per_track_destination_uses_mapped_keys_and_withdraws_changed_track_set() {
    let directory = tempfile::tempdir().unwrap();
    let a = directory.path().join("a.key");
    let b = directory.path().join("b.key");
    for (path, value) in [(&a, 0x41), (&b, 0x42)] {
        std::fs::write(path, STANDARD.encode([value; 30])).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let idle = directory.path().join("idle");
    std::fs::write(&idle, "#!/bin/sh\nexec sleep 30\n").unwrap();
    std::fs::set_permissions(&idle, std::fs::Permissions::from_mode(0o700)).unwrap();
    let engine = Engine::new(directory.path().join("media"), idle.to_str().unwrap());
    let (port, sockets) = receivers();
    let config = json!({"inputs":[{"url":"testsrc://"}],"flussonix_rtp_outputs":[{
        "url":format!("srtp://127.0.0.1:{port}"),
        "flussonix_rtp":{"profile":"elementary","track_keys":[
            {"track_id":9,"media":"audio","encoding":"MPEG4-GENERIC/48000/2","key_file":b},
            {"track_id":7,"media":"audio","encoding":"MPEG4-GENERIC/48000/2","key_file":a}
        ]}
    }]});
    let worker = engine.ensure("owned", &config).await.unwrap();
    let tracks = |second_id| {
        [
            Track {
                id: 7,
                codec: "aac".into(),
                config: vec![0x11, 0x90],
            },
            Track {
                id: second_id,
                codec: "aac".into(),
                config: vec![0x11, 0x90],
            },
        ]
    };
    worker.wire.rtp.configure(&tracks(9));
    for id in [7, 9] {
        worker.wire.rtp.frame(&Frame {
            track_id: id,
            dts: 0,
            pts_offset: 0,
            key: true,
            body: vec![42],
        });
    }
    let mut observed = Vec::new();
    for (socket, key) in [(0, 0x41), (2, 0x42)] {
        let (mut packet, _) = recv(&sockets[socket]).await;
        let id = u32::from_be_bytes(packet[8..12].try_into().unwrap());
        Session::new([key; 30], None)
            .unwrap()
            .unprotect(&mut packet, false)
            .unwrap();
        observed.push(id);
    }
    let sdp = engine.rtp_sdp("owned", 0, &config).await.unwrap();
    for id in observed {
        assert!(sdp.contains(&format!("a=ssrc:{id} cname:")));
    }
    worker.wire.rtp.configure(&tracks(10));
    worker.wire.rtp.frame(&Frame {
        track_id: 10,
        dts: 1920,
        pts_offset: 0,
        key: true,
        body: vec![43],
    });
    wait_status(&worker, "failed").await;
    assert_eq!(
        worker.stats()["flussonix_rtp_outputs"][0]["sdp_ready"],
        false
    );
    engine.stop_all().await;
    assert!(!std::path::Path::new(&format!("/proc/{}", worker.pid())).exists());
    for socket in &sockets {
        let mut stale = [0; 2049];
        while socket.try_recv(&mut stale).is_ok() {}
    }
    let restarted = engine.ensure("owned", &config).await.unwrap();
    restarted.wire.rtp.configure(&tracks(10));
    for id in [7, 10] {
        restarted.wire.rtp.frame(&Frame {
            track_id: id,
            dts: 0,
            pts_offset: 0,
            key: true,
            body: vec![44],
        });
    }
    wait_status(&restarted, "failed").await;
    assert_eq!(
        restarted.stats()["flussonix_rtp_outputs"][0]["sdp_ready"],
        false
    );
    let mut unwanted = [0; 2049];
    assert!(
        sockets
            .iter()
            .all(|socket| socket.try_recv(&mut unwanted).is_err())
    );
    engine.stop_all().await;
    // Distinct paths with identical key bytes must also fail before either
    // destination socket or SDP is published on a later worker generation.
    std::fs::write(&b, STANDARD.encode([0x41; 30])).unwrap();
    let duplicate = engine.ensure("owned", &config).await.unwrap();
    duplicate.wire.rtp.configure(&tracks(9));
    for id in [7, 9] {
        duplicate.wire.rtp.frame(&Frame {
            track_id: id,
            dts: 0,
            pts_offset: 0,
            key: true,
            body: vec![45],
        });
    }
    wait_status(&duplicate, "failed").await;
    assert_eq!(
        duplicate.stats()["flussonix_rtp_outputs"][0]["sdp_ready"],
        false
    );
    assert!(
        sockets
            .iter()
            .all(|socket| socket.try_recv(&mut unwanted).is_err())
    );
    engine.stop_all().await;
}
async fn recv(socket: &AsyncUdp) -> (Vec<u8>, std::net::SocketAddr) {
    let mut b = [0; 2049];
    let (n, a) = tokio::time::timeout(Duration::from_secs(10), socket.recv_from(&mut b))
        .await
        .unwrap()
        .unwrap();
    (b[..n].to_vec(), a)
}
async fn wait_status(w: &flussonix::media::Worker, status: &str) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while w.stats()["flussonix_rtp_outputs"][0]["status"] != status {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn secure_four_destinations_have_fresh_epochs_encrypted_feedback_and_key_free_sdp() {
    let d = tempfile::tempdir().unwrap();
    let key = key(d.path());
    let e = Engine::new(d.path().join("media"), "/usr/bin/ffmpeg");
    let listeners: Vec<_> = (0..4).map(|_| receivers()).collect();
    let outputs: Vec<_> = listeners
        .iter()
        .map(|(p, _)| destination(*p, &key))
        .collect();
    let cfg = json!({"inputs":[{"url":"testsrc://"}],"flussonix_rtp_outputs":outputs});
    let w = e.ensure("owned", &cfg).await.unwrap();
    assert!(Arc::ptr_eq(&w, &e.ensure("owned", &cfg).await.unwrap()));
    let mut identities = HashSet::new();
    let mut first_ssrc = 0;
    for (i, (port, sockets)) in listeners.iter().enumerate() {
        let mut contexts = vec![];
        let mut wire_ssrcs = vec![];
        for (lane, pt) in [(0, 96), (2, 97)] {
            let (mut body, _) = recv(&sockets[lane]).await;
            assert_eq!(u16::from_be_bytes(body[2..4].try_into().unwrap()), 0);
            assert_eq!(body[1] & 127, pt);
            let id = u32::from_be_bytes(body[8..12].try_into().unwrap());
            wire_ssrcs.push(id);
            assert!(identities.insert(id));
            if i == 0 && lane == 0 {
                first_ssrc = id;
            }
            let cipher = body.clone();
            let mut rx = Session::new([0x31; 30], None).unwrap();
            rx.unprotect(&mut body, false).unwrap();
            assert_eq!(cipher.len(), body.len() + 10);
            assert_ne!(&cipher[12..body.len()], &body[12..]);
            assert!(body.len() > 12 && body[0] >> 6 == 2);
            contexts.push(rx);
        }
        let sdp = e.rtp_sdp("owned", i, &cfg).await.unwrap();
        assert!(sdp.contains(&format!("m=video {port} RTP/SAVP 96")));
        assert!(sdp.contains(&format!("m=audio {} RTP/SAVP 97", port + 2)));
        assert_eq!(sdp.matches("a=ssrc:").count(), 2);
        for id in &wire_ssrcs {
            assert!(sdp.contains(&format!("a=ssrc:{id} cname:")));
        }
        assert!(!sdp.contains("RTP/AVP"));
        assert!(!sdp.contains("crypto:"));
        assert!(!sdp.contains("owned.key"));
        assert!(!sdp.contains(&STANDARD.encode([0x31; 30])));
        if i == 0 {
            let mut names = vec![];
            for (j, lane) in [1, 3].into_iter().enumerate() {
                let (mut sr, source) = recv(&sockets[lane]).await;
                assert!(!packet::valid_rtcp(&sr));
                contexts[j].unprotect(&mut sr, true).unwrap();
                assert!(packet::valid_rtcp(&sr));
                names.push(sr[38..38 + usize::from(sr[37])].to_vec());
                if j == 0 {
                    let rr = |ssrc: u32| {
                        let mut b = vec![0x80, 201, 0, 1];
                        b.extend(ssrc.to_be_bytes());
                        b
                    };
                    sockets[lane].send_to(&rr(77), source).await.unwrap();
                    let mut wrong = Session::new([0x32; 30], Some(77)).unwrap();
                    let mut b = rr(77);
                    wrong.protect(&mut b, true).unwrap();
                    sockets[lane].send_to(&b, source).await.unwrap();
                    let mut malformed = Session::new([0x31; 30], Some(78)).unwrap();
                    let mut b = rr(78);
                    b[3] = 9;
                    malformed.protect(&mut b, true).unwrap();
                    sockets[lane].send_to(&b, source).await.unwrap();
                    let mut good = Session::new([0x31; 30], Some(77)).unwrap();
                    let mut b = rr(77);
                    good.protect(&mut b, true).unwrap();
                    let mut tamper = b.clone();
                    *tamper.last_mut().unwrap() ^= 1;
                    sockets[lane].send_to(&tamper, source).await.unwrap();
                    sockets[lane].send_to(&b, source).await.unwrap();
                    sockets[lane].send_to(&b, source).await.unwrap();
                }
            }
            assert_eq!(names[0], names[1]);
            let cname = std::str::from_utf8(&names[0]).unwrap();
            for id in &wire_ssrcs {
                assert!(sdp.contains(&format!("a=ssrc:{id} cname:{cname}\r\n")));
            }
        }
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let stats = w.stats();
    let out = &stats["flussonix_rtp_outputs"][0];
    assert_eq!(out["rtcp_packets"], 1);
    assert!(out["auth_failures"].as_u64().unwrap() >= 4);
    assert!(out["invalid_packets"].as_u64().unwrap() >= 1);
    assert!(e.direct_egress.load(Ordering::Relaxed) > 0);
    assert_eq!(e.count().await, 1);
    w.wire.rtp.configure(&[]);
    wait_status(&w, "failed").await;
    assert_eq!(w.stats()["flussonix_rtp_outputs"][0]["sdp_ready"], false);
    e.stop_all().await;
    assert!(!std::path::Path::new(&format!("/proc/{}", w.pid())).exists());
    // The same key/new worker must get a new encryption identity, never the shared hub's epoch.
    for socket in &listeners[0].1 {
        let mut b = [0; 2049];
        while socket.try_recv(&mut b).is_ok() {}
    }
    let restarted = e.ensure("owned", &cfg).await.unwrap();
    let (body, _) = recv(&listeners[0].1[0]).await;
    assert_ne!(
        first_ssrc,
        u32::from_be_bytes(body[8..12].try_into().unwrap())
    );
    assert_eq!(&body[2..4], &[0, 0]);
    e.stop_all().await;
    assert!(restarted.is_closed());
}
#[tokio::test]
async fn secure_rekeys_transport_sequence_from_zero_even_when_hub_has_rolled_over() {
    let d = tempfile::tempdir().unwrap();
    let key = key(d.path());
    let idle = d.path().join("idle");
    std::fs::write(&idle, "#!/bin/sh\nexec sleep 30\n").unwrap();
    std::fs::set_permissions(&idle, std::fs::Permissions::from_mode(0o700)).unwrap();
    let e = Engine::new(d.path().join("media"), idle.to_str().unwrap());
    let (port, sockets) = receivers();
    let cfg =
        json!({"inputs":[{"url":"testsrc://"}],"flussonix_rtp_outputs":[destination(port,&key)]});
    let w = e.ensure("owned", &cfg).await.unwrap();
    w.wire.rtp.configure(&[Track {
        id: 7,
        codec: "aac".into(),
        config: vec![0x11, 0x90],
    }]);
    // Queue initial frames before the output task observes its snapshot: the shared hub crosses ROC.
    for n in 0..65538 {
        w.wire.rtp.frame(&Frame {
            track_id: 7,
            dts: n * 1920,
            pts_offset: 0,
            key: true,
            body: vec![42],
        });
    }
    let (mut body, _) = recv(&sockets[0]).await;
    assert_eq!(&body[2..4], &[0, 0]);
    let mut rx = Session::new([0x31; 30], None).unwrap();
    rx.unprotect(&mut body, false).unwrap();
    assert_eq!(body.last(), Some(&42));
    assert!(e.direct_egress.load(Ordering::Relaxed) >= body.len() as u64 + 10 + 28);
    e.stop_all().await;
    assert!(w.is_closed());
    assert!(!std::path::Path::new(&format!("/proc/{}", w.pid())).exists());
}
#[tokio::test]
async fn unsafe_secure_output_key_fails_without_sdp_or_plaintext() {
    let d = tempfile::tempdir().unwrap();
    let key = key(d.path());
    std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o644)).unwrap();
    let e = Engine::new(d.path().join("media"), "/usr/bin/ffmpeg");
    let (port, sockets) = receivers();
    let cfg =
        json!({"inputs":[{"url":"testsrc://"}],"flussonix_rtp_outputs":[destination(port,&key)]});
    let w = e.ensure("owned", &cfg).await.unwrap();
    wait_status(&w, "failed").await;
    assert_eq!(w.stats()["flussonix_rtp_outputs"][0]["sdp_ready"], false);
    let mut b = [0; 2049];
    assert!(sockets[0].try_recv(&mut b).is_err());
    assert_eq!(e.direct_egress.load(Ordering::Relaxed), 0);
    e.stop_all().await;
}

#[tokio::test]
async fn secure_receiver_can_bind_after_reading_actual_sdp_without_restarting_worker() {
    let d = tempfile::tempdir().unwrap();
    let key = key(d.path());
    let e = Engine::new(d.path(), "/usr/bin/ffmpeg");
    let (port, reserved) = receivers();
    drop(reserved);
    let cfg = json!({"inputs":[{"url":"testsrc://"}],"flussonix_rtp_outputs":[{"url":format!("srtp://127.0.0.1:{port}"),"flussonix_rtp":{"profile":"elementary","key_file":key}}]});
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
async fn secure_paced_destination_queue_lag_fails_and_clears_its_description() {
    use flussonix::{m4f::Frame, m4s::Track};
    use std::os::unix::fs::PermissionsExt;
    let d = tempfile::tempdir().unwrap();
    let key = key(d.path());
    let idle = d.path().join("owned-idle");
    std::fs::write(&idle, "#!/bin/sh\nexec sleep 30\n").unwrap();
    std::fs::set_permissions(&idle, std::fs::Permissions::from_mode(0o700)).unwrap();
    let e = Engine::new(d.path().join("media"), idle.to_str().unwrap());
    let (port, _listeners) = receivers();
    let cfg = json!({"inputs":[{"url":"testsrc://"}],"flussonix_rtp_outputs":[{"url":format!("srtp://127.0.0.1:{port}"),"max_mbps":1,"flussonix_rtp":{"profile":"elementary","key_file":key}}]});
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
async fn secure_native_output_crosses_sequence_rollover_and_late_destinations_get_new_epochs() {
    use flussonix::direct_rtp::{config, output::State};
    use tokio_util::sync::CancellationToken;
    let d = tempfile::tempdir().unwrap();
    let key = key(d.path());
    let idle = d.path().join("idle");
    std::fs::write(&idle, "#!/bin/sh\nexec sleep 360\n").unwrap();
    std::fs::set_permissions(&idle, std::fs::Permissions::from_mode(0o700)).unwrap();
    let e = Engine::new(d.path().join("media"), idle.to_str().unwrap());
    let w = e
        .ensure(
            "owned",
            &json!({"inputs":[{"url":"testsrc://"}],"flussonix_input_timeout":300}),
        )
        .await
        .unwrap();
    w.wire.rtp.configure(&[Track {
        id: 7,
        codec: "aac".into(),
        config: vec![0x11, 0x90],
    }]);
    let (port, sockets) = receivers();
    let definition = config::outputs(&json!({"flussonix_rtp_outputs":[destination(port,&key)]}))
        .unwrap()
        .remove(0);
    let state = State::new(definition, 0);
    let cancel = CancellationToken::new();
    let task = tokio::spawn(state.clone().run_worker(w.clone(), cancel.clone()));
    let mut rx = Session::new([0x31; 30], None).unwrap();
    let mut sender = 0;
    let mut first_stamp = 0;
    // One in-flight owned datagram avoids receiver loss/queue overflow. The
    // media epoch stays fixed, so this exercises crypto rollover, not a long movie.
    for n in 0..65538u32 {
        w.wire.rtp.frame(&Frame {
            track_id: 7,
            dts: 0,
            pts_offset: 0,
            key: true,
            body: vec![42],
        });
        let (mut body, _) = recv(&sockets[0]).await;
        assert_eq!(u16::from_be_bytes(body[2..4].try_into().unwrap()), n as u16);
        rx.unprotect(&mut body, false).unwrap();
        assert_eq!(body.last(), Some(&42));
        let id = u32::from_be_bytes(body[8..12].try_into().unwrap());
        let stamp = u32::from_be_bytes(body[4..8].try_into().unwrap());
        if n == 0 {
            sender = id;
            first_stamp = stamp;
        } else {
            assert_eq!(id, sender);
            assert_eq!(stamp, first_stamp);
        }
    }
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.stats()["sdp_ready"], false);
    // Adding a destination to the unchanged, post-rollover worker uses another
    // SSRC/ROC-zero epoch. No source/encoder restart is involved.
    let (port, late) = receivers();
    let definition = config::outputs(&json!({"flussonix_rtp_outputs":[destination(port,&key)]}))
        .unwrap()
        .remove(0);
    let fresh = State::new(definition, 1);
    let cancel = CancellationToken::new();
    let task = tokio::spawn(fresh.clone().run_worker(w.clone(), cancel.clone()));
    let (mut body, _) = recv(&late[0]).await;
    assert_eq!(&body[2..4], &[0, 0]);
    assert_ne!(sender, u32::from_be_bytes(body[8..12].try_into().unwrap()));
    Session::new([0x31; 30], None)
        .unwrap()
        .unprotect(&mut body, false)
        .unwrap();
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(e.count().await, 1);
    e.stop_all().await;
    assert!(w.is_closed());
}
