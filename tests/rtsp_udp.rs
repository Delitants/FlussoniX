use flussonix::rtsp::{
    protocol::{ClientPorts, Offer},
    udp::{Pool, PortRange, valid_receiver_report},
};
use std::{net::IpAddr, time::Duration};
use tokio::net::UdpSocket;
fn ip() -> IpAddr {
    "127.0.0.1".parse().unwrap()
}
#[path = "support/udp.rs"]
mod udp_fixture;
use udp_fixture::reserved;

#[test]
fn udp_transport_is_explicit_unicast_and_rejects_ambiguous_or_foreign_targets() {
    for value in [
        "RTP/AVP;unicast;client_port=20000-20001",
        "RTP/AVP/UDP;unicast;client_port=20000-20001;mode=\"PLAY\"",
    ] {
        assert_eq!(
            Offer::parse(value),
            Ok(Offer::Udp(ClientPorts {
                rtp: 20000,
                rtcp: 20001
            }))
        );
    }
    for value in [
        "RTP/AVP;client_port=20000-20001",
        "RTP/AVP;multicast;client_port=20000-20001",
        "RTP/AVP;unicast;client_port=22-23",
        "RTP/AVP;unicast;client_port=20001-20002",
        "RTP/AVP;unicast;client_port=20000-20002",
        "RTP/AVP;unicast;client_port=65536-65537",
        "RTP/AVP;unicast;client_port=20000-20001;destination=127.0.0.2",
        "RTP/AVP;unicast;client_port=20000-20001;source=127.0.0.1",
        "RTP/AVP;unicast;client_port=20000-20001;client_port=20002-20003",
        "RTP/AVP;unicast;client_port=20000-20001;mode=RECORD",
        "RTP/AVP;unicast;client_port=20000-20001,RTP/AVP/TCP;interleaved=0-1",
        "RTP/AVP;unicast;client_port=20000-20001;rtcp-mux",
    ] {
        assert_eq!(Offer::parse(value), Err(461), "{value}");
    }
}
#[test]
fn udp_range_limits_prevent_privileged_odd_or_unbounded_reservations() {
    assert!("20000-20003".parse::<PortRange>().is_ok());
    assert!("65280-65535".parse::<PortRange>().is_ok());
    for value in [
        "0-1",
        "22-23",
        "20001-20002",
        "20000-20000",
        "20000-20002",
        "20000-20257",
        "65534-65536",
        "20000",
        "20000-19999",
        "a-b",
    ] {
        assert!(value.parse::<PortRange>().is_err(), "{value}");
    }
}
#[tokio::test]
async fn occupied_range_binding_rolls_back_earlier_sockets() {
    let (range, mut held) = reserved(4);
    let base = held[0].local_addr().unwrap().port();
    let occupied = held.pop().unwrap();
    drop(held);
    assert!(Pool::bind(ip(), range).await.is_err());
    let reopened = UdpSocket::bind((ip(), base)).await.unwrap();
    drop(reopened);
    drop(occupied);
    assert!(Pool::bind(ip(), range).await.is_ok());
}
#[tokio::test]
async fn pool_exhaustion_returns_sockets_and_limits_rtp_to_negotiated_peer() {
    let (range, held) = reserved(4);
    drop(held);
    let pool = Pool::bind(ip(), range).await.unwrap();
    let (_, clients) = reserved(2);
    let rtp_port = clients[0].local_addr().unwrap().port();
    let a = pool
        .lease(
            ip(),
            ClientPorts {
                rtp: rtp_port,
                rtcp: rtp_port + 1,
            },
        )
        .await
        .unwrap();
    let ports = a.server_ports();
    let b = pool
        .lease(
            ip(),
            ClientPorts {
                rtp: rtp_port,
                rtcp: rtp_port + 1,
            },
        )
        .await
        .unwrap();
    assert_ne!(ports, b.server_ports());
    assert!(
        pool.lease(
            ip(),
            ClientPorts {
                rtp: rtp_port,
                rtcp: rtp_port + 1
            }
        )
        .await
        .is_err()
    );
    a.send_rtp(b"owned-rtp").await.unwrap();
    clients[0]
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let mut buf = [0; 32];
    let (n, from) = clients[0].recv_from(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"owned-rtp");
    assert_eq!(from.port(), ports.0);
    drop(a);
    let c = pool
        .lease(
            ip(),
            ClientPorts {
                rtp: rtp_port,
                rtcp: rtp_port + 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(ports, c.server_ports());
    assert!(
        pool.lease(ip(), ClientPorts { rtp: 1, rtcp: 2 })
            .await
            .is_err()
    );
}
#[test]
fn receiver_reports_validate_lengths_padding_and_reported_sender_identity() {
    let ssrc = 0x11223344;
    let mut report = vec![0x81, 201, 0, 7, 0, 0, 0, 99, 0x11, 0x22, 0x33, 0x44];
    report.extend([0; 20]);
    report.extend([0x81, 202, 0, 2, 0, 0, 0, 99, 1, 1, b'x', 0]);
    assert!(valid_receiver_report(&report, ssrc));
    assert!(!valid_receiver_report(&report, ssrc + 1));
    let mut truncated = report.clone();
    truncated.pop();
    assert!(!valid_receiver_report(&truncated, ssrc));
    let mut wrong_count = report.clone();
    wrong_count[0] = 0x82;
    assert!(!valid_receiver_report(&wrong_count, ssrc));
    let mut wrong_version = report.clone();
    wrong_version[0] = 0x41;
    assert!(!valid_receiver_report(&wrong_version, ssrc));
    assert!(!valid_receiver_report(&[0x80, 200, 0, 0], ssrc));
    assert!(!valid_receiver_report(&vec![0; 8193], ssrc));
}

#[tokio::test]
async fn recycled_udp_pair_discards_old_reports_and_filters_other_endpoints() {
    let (range, held) = reserved(2);
    drop(held);
    let pool = Pool::bind(ip(), range).await.unwrap();
    let (_, clients) = reserved(2);
    let ports = ClientPorts {
        rtp: clients[0].local_addr().unwrap().port(),
        rtcp: clients[1].local_addr().unwrap().port(),
    };
    let old = pool.lease(ip(), ports).await.unwrap();
    let server = old.server_ports();
    clients[1].send_to(b"old-report", (ip(), server.1)).unwrap();
    drop(old);
    let lease = pool.lease(ip(), ports).await.unwrap();
    let mut buffer = [0; 8193];
    assert!(
        tokio::time::timeout(Duration::from_millis(20), lease.recv_rtcp(&mut buffer))
            .await
            .is_err()
    );
    let foreign = UdpSocket::bind((ip(), 0)).await.unwrap();
    foreign
        .send_to(b"foreign-report", (ip(), server.1))
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), lease.recv_rtcp(&mut buffer))
            .await
            .is_err()
    );
    clients[1]
        .send_to(b"current-report", (ip(), server.1))
        .unwrap();
    let n = tokio::time::timeout(Duration::from_secs(1), lease.recv_rtcp(&mut buffer))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(&buffer[..n], b"current-report");
}
#[test]
fn compound_reports_require_well_formed_sdes_and_reject_empty_or_bad_padding() {
    let rr = [0x80, 201, 0, 1, 0, 0, 0, 99];
    let mut report = rr.to_vec();
    report.extend([0x81, 202, 0, 2, 0, 0, 0, 99, 1, 1, b'x', 0]);
    assert!(valid_receiver_report(&report, 0));
    assert!(!valid_receiver_report(&rr, 0));
    report[17] = 8;
    assert!(!valid_receiver_report(&report, 0));
    let mut invalid = rr.to_vec();
    invalid.extend([0xa1, 202, 0, 2, 0, 0, 0, 99, 1, 1, b'x', 0]);
    assert!(!valid_receiver_report(&invalid, 0));
}

#[test]
fn pacing_obeys_decode_time_and_a_refilling_byte_budget() {
    use flussonix::rtsp::udp::Pacer;
    let now = tokio::time::Instant::now();
    let mut pace = Pacer::new(1.0).unwrap();
    assert_eq!(pace.ready_at(u64::MAX - 90000, 1200, now).unwrap(), now);
    pace.sent(32768, now);
    let due = pace.ready_at(u64::MAX - 90000, 1200, now).unwrap();
    assert!(due.duration_since(now) >= Duration::from_micros(9600));
    assert!(due.duration_since(now) <= Duration::from_micros(9601));
    assert_eq!(
        pace.ready_at(u64::MAX, 1200, now).unwrap(),
        now + Duration::from_secs(1)
    );
    let later = now + Duration::from_secs(2);
    assert_eq!(pace.ready_at(u64::MAX, 1200, later).unwrap(), later);
    assert!(pace.ready_at(u64::MAX, 32769, later).is_err());
    for rate in [0.0, -1.0, f64::NAN, f64::INFINITY, 10001.0] {
        assert!(Pacer::new(rate).is_err());
    }
}
