//! Actual QUIC connection checks for the optional delivery-history hooks.
use super::*;
fn transport() -> Arc<TransportConfig> {
    let mut bbr = congestion::BbrConfig::default();
    bbr.initial_window(12_000).delivery_rate_sampling(true);
    let mut transport = TransportConfig::default();
    transport.congestion_controller_factory(Arc::new(bbr));
    Arc::new(transport)
}
fn client() -> ClientConfig {
    let mut c = client_config();
    c.transport_config(transport());
    c
}
fn pair() -> Pair {
    let mut server = server_config();
    server.transport_config(transport());
    let mut pair = Pair::new(Default::default(), server);
    pair.latency = Duration::from_millis(100);
    pair
}

// Break caught: packet construction/explicit ACK processing omits the opaque
// history; unit rate tests alone cannot detect this missing connection wiring.
#[test]
fn real_packet_history_produces_a_model_and_delivers_exact_datagrams() {
    let mut p = pair();
    let (client, server) = p.connect_with(client());
    for index in 0..16u8 {
        p.client_datagrams(client)
            .send(Bytes::from(vec![index; 1000]), false)
            .unwrap();
        p.drive();
        assert_eq!(
            p.server_datagrams(server).recv().unwrap(),
            Bytes::from(vec![index; 1000])
        );
    }
    assert!(
        p.client_conn_mut(client)
            .congestion_state()
            .metrics()
            .pacing_rate
            .unwrap()
            > 0
    );
}

// Break caught: stateless Retry's inferred acknowledgement is treated as a
// measured delivery. The delayed Retry is longer than the initialRTT baseline.
#[test]
fn delayed_retry_cannot_create_a_delivery_rate_sample() {
    let mut p = pair();
    p.latency = Duration::from_millis(250);
    p.server.handle_incoming = Box::new(|_| IncomingConnectionBehavior::Wait);
    let client = p.begin_connect(client());
    p.drive_client();
    p.time += Duration::from_millis(250);
    p.drive_server();
    let incoming = p.server.waiting_incoming.pop().unwrap();
    p.server.retry(incoming);
    p.drive_server();
    p.time += Duration::from_millis(250);
    p.drive_client();
    assert_eq!(
        p.client_conn_mut(client)
            .congestion_state()
            .metrics()
            .pacing_rate,
        Some(0)
    );
    p.server.handle_incoming = Box::new(|_| IncomingConnectionBehavior::Accept);
    p.drive();
    let server = p.server.assert_accept();
    p.finish_connect(client, server);
}

// Break caught: existing sent packets and new-generation flights mix across a
// NAT rebinding, preventing exact delivery or a new explicit rate sample.
#[test]
fn rebound_connection_delivers_new_flight_after_outstanding_old_path_packets() {
    let mut p = pair();
    let (client, server) = p.connect_with(client());
    p.server_datagrams(server)
        .send(Bytes::from_static(b"old-path"), false)
        .unwrap();
    p.drive_server();
    p.client.addr.set_port(p.client.addr.port() + 1);
    p.client_conn_mut(client).ping();
    p.drive();
    assert_eq!(p.server_conn_mut(server).remote_address(), p.client.addr);
    p.server_datagrams(server)
        .send(Bytes::from_static(b"new-path"), false)
        .unwrap();
    p.drive();
    let mut received = Vec::new();
    while let Some(packet) = p.client_datagrams(client).recv() {
        received.push(packet);
    }
    assert!(received.contains(&Bytes::from_static(b"new-path")));
    assert!(
        p.server_conn_mut(server)
            .congestion_state()
            .metrics()
            .pacing_rate
            .unwrap()
            > 0
    );
}
