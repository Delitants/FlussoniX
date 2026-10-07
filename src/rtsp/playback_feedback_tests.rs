use super::*;
async fn feedback_fixture() -> (HashMap<u32, Sender>, Vec<tokio::net::UdpSocket>) {
    let ip = "127.0.0.1".parse().unwrap();
    let mut pool = None;
    for base in (28000..32000).step_by(16) {
        let range = format!("{base}-{}", base + 15).parse().unwrap();
        if let Ok(found) = udp::Pool::bind(ip, range).await {
            pool = Some(found);
            break;
        }
    }
    let pool = pool.expect("unused eight-track UDP pool");
    let mut senders = HashMap::new();
    let mut clients = vec![];
    for id in 0..8 {
        let client = tokio::net::UdpSocket::bind((ip, 0)).await.unwrap();
        let mut port = client.local_addr().unwrap().port();
        // Only RTCP is received in this fixture; its advertised peer port is odd.
        if port % 2 == 0 {
            drop(client);
            for base in (32000..32760).step_by(2) {
                if let Ok(bound) = tokio::net::UdpSocket::bind((ip, base + 1)).await {
                    port = base + 1;
                    clients.push(bound);
                    break;
                }
            }
        } else {
            clients.push(client);
        }
        assert_eq!(clients.len(), id as usize + 1);
        let ports = protocol::ClientPorts {
            rtp: port - 1,
            rtcp: port,
        };
        let lease = pool.lease(ip, ports).await.unwrap();
        senders.insert(
            id,
            Sender {
                delivery: Delivery::Udp { lease, ports },
                ssrc: id + 11,
                packets: 0,
                octets: 0,
            },
        );
    }
    (senders, clients)
}
fn receiver_report(ssrc: u32) -> Vec<u8> {
    let sender = 77u32;
    let mut report = vec![0x81, 201, 0, 7];
    report.extend(sender.to_be_bytes());
    report.extend(ssrc.to_be_bytes());
    report.extend([0; 20]);
    report.extend([0x81, 202, 0, 2]);
    report.extend(sender.to_be_bytes());
    report.extend([1, 1, b'x', 0]);
    report
}
#[tokio::test]
async fn udp_playback_receives_feedback_on_each_of_eight_tracks() {
    let (senders, clients) = feedback_fixture().await;
    let mut cursor = 0;
    for id in 0..8 {
        let sender = &senders[&id];
        let Delivery::Udp { lease, .. } = &sender.delivery else {
            unreachable!()
        };
        clients[id as usize]
            .send_to(
                &receiver_report(sender.ssrc),
                ("127.0.0.1", lease.server_ports().1),
            )
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(
                Duration::from_secs(1),
                receive_reports(&senders, &mut cursor)
            )
            .await
            .is_ok_and(|accepted| accepted),
            "feedback ignored for track {id}"
        );
    }
}
#[tokio::test]
async fn udp_playback_invalid_feedback_cannot_starve_another_track() {
    let (senders, clients) = feedback_fixture().await;
    let order: Vec<_> = senders.keys().copied().collect();
    for id in &order[..7] {
        let sender = &senders[id];
        let Delivery::Udp { lease, .. } = &sender.delivery else {
            unreachable!()
        };
        for _ in 0..16 {
            clients[*id as usize]
                .send_to(
                    &receiver_report(sender.ssrc + 100),
                    ("127.0.0.1", lease.server_ports().1),
                )
                .await
                .unwrap();
        }
    }
    let id = order[7];
    let sender = &senders[&id];
    let Delivery::Udp { lease, .. } = &sender.delivery else {
        unreachable!()
    };
    clients[id as usize]
        .send_to(
            &receiver_report(sender.ssrc),
            ("127.0.0.1", lease.server_ports().1),
        )
        .await
        .unwrap();
    let mut cursor = 0;
    let mut accepted = false;
    for _ in 0..8 {
        accepted |= tokio::time::timeout(
            Duration::from_secs(1),
            receive_reports(&senders, &mut cursor),
        )
        .await
        .unwrap();
        if accepted {
            break;
        }
    }
    assert!(
        accepted,
        "invalid queued feedback must yield to the eighth track"
    );
}
#[tokio::test]
async fn udp_playback_feedback_preserves_endpoint_identity_and_size_checks() {
    let (senders, clients) = feedback_fixture().await;
    let sender = &senders[&7];
    let Delivery::Udp { lease, .. } = &sender.delivery else {
        unreachable!()
    };
    let destination = ("127.0.0.1", lease.server_ports().1);
    let foreign = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    foreign
        .send_to(&receiver_report(sender.ssrc), destination)
        .await
        .unwrap();
    let mut cursor = 0;
    assert!(
        !tokio::time::timeout(
            Duration::from_secs(1),
            receive_reports(&senders, &mut cursor)
        )
        .await
        .unwrap()
    );
    for body in [
        receiver_report(sender.ssrc + 1),
        vec![0; 8193],
        vec![0x80, 201, 0, 0],
    ] {
        clients[7].send_to(&body, destination).await.unwrap();
        assert!(
            !tokio::time::timeout(
                Duration::from_secs(1),
                receive_reports(&senders, &mut cursor)
            )
            .await
            .unwrap()
        );
    }
    clients[7]
        .send_to(&receiver_report(sender.ssrc), destination)
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_secs(1),
            receive_reports(&senders, &mut cursor)
        )
        .await
        .unwrap()
    );
}
