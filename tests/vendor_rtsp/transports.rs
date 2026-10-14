use super::*;
#[path = "../support/tls.rs"]
mod tls_fixture;
#[path = "../support/udp.rs"]
mod udp_fixture;

#[tokio::test]
async fn vendor_rtsps_delivers_rtp_inside_verified_tls() {
    let f = Fixture::configured("cdn", policy(), None, false).await;
    let cert = tls_fixture::Certificates::new();
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("rtsps://{}/owned", tcp.local_addr().unwrap());
    let cancel = CancellationToken::new();
    let task = tokio::spawn(rtsp::serve_tls(
        tcp,
        f.app.clone(),
        cancel.clone(),
        cert.server(),
    ));
    let u = url::Url::parse(&url).unwrap();
    let socket = TcpStream::connect(("127.0.0.1", u.port().unwrap()))
        .await
        .unwrap();
    let tls = tokio_rustls::TlsConnector::from(cert.client())
        .connect(
            tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap(),
            socket,
        )
        .await
        .unwrap();
    let mut viewer = BufReader::new(tls);
    play(&mut viewer, &url).await;
    let body = packet(&mut viewer).await;
    assert_eq!(body[1] & 0x7f, 14);
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 1);
    drop(viewer);
    cancel.cancel();
    task.await.unwrap().unwrap();
    f.stop().await;
}

#[tokio::test]
async fn vendor_rtsp_udp_uses_negotiated_pair_and_never_opens_second_private_pull() {
    let f = Fixture::configured("cdn", policy(), None, false).await;
    let (range, held) = udp_fixture::reserved(2);
    drop(held);
    let pool = rtsp::udp::Pool::bind("127.0.0.1".parse().unwrap(), range)
        .await
        .unwrap();
    let (_, mut held) = udp_fixture::reserved(2);
    let client = held.remove(0);
    client.set_nonblocking(true).unwrap();
    let client = tokio::net::UdpSocket::from_std(client).unwrap();
    let port = client.local_addr().unwrap().port();
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("rtsp://{}/owned", tcp.local_addr().unwrap());
    let cancel = CancellationToken::new();
    let task = tokio::spawn(rtsp::serve_with_udp(
        tcp,
        f.app.clone(),
        cancel.clone(),
        Some(pool),
        100.0,
    ));
    let mut viewer = connect(&url).await;
    let (code, _, body) = request(
        &mut viewer,
        "DESCRIBE",
        &format!("{url}?token=owned-viewer"),
        "",
    )
    .await;
    assert_eq!(code, 200);
    let sdp = String::from_utf8(body).unwrap();
    let id = sdp
        .lines()
        .find_map(|l| l.strip_prefix("a=control:trackID="))
        .unwrap();
    let (code, h, _) = request(
        &mut viewer,
        "SETUP",
        &format!("{url}/trackID={id}"),
        &format!(
            "Transport: RTP/AVP;unicast;client_port={port}-{}\r\n",
            port + 1
        ),
    )
    .await;
    assert_eq!(code, 200);
    let session = h
        .lines()
        .find_map(|l| l.strip_prefix("Session: "))
        .unwrap()
        .split(';')
        .next()
        .unwrap();
    assert_eq!(
        request(
            &mut viewer,
            "PLAY",
            &url,
            &format!("Session: {session}\r\n")
        )
        .await
        .0,
        200
    );
    let mut body = [0; 2048];
    let (n, from) = tokio::time::timeout(Duration::from_secs(3), client.recv_from(&mut body))
        .await
        .unwrap()
        .unwrap();
    assert!(n > 16);
    assert_eq!(body[0] >> 6, 2);
    assert_eq!(body[1] & 0x7f, 14);
    assert!(h.contains(&format!("server_port={}-", from.port())));
    assert_eq!(f.origins[0].private_calls.load(Ordering::SeqCst), 1);
    assert!(f.app.rtsp_udp_egress.load(Ordering::Relaxed) > 0);
    drop(viewer);
    cancel.cancel();
    task.await.unwrap().unwrap();
    f.stop().await;
}
