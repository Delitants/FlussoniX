#[path = "support/decoder_readiness.rs"]
mod readiness;
use std::net::UdpSocket;
#[test]
fn receiver_readiness_requires_its_actual_sockets_and_observes_without_binding() {
    let (first, second) = loop {
        let first = UdpSocket::bind("127.0.0.1:0").unwrap();
        let p = first.local_addr().unwrap().port();
        if p < 65535 {
            if let Ok(second) = UdpSocket::bind(("127.0.0.1", p + 1)) {
                break (first, second);
            }
        }
    };
    let p = first.local_addr().unwrap().port();
    let ports = [p, p + 1];
    let mut child = std::process::Command::new("/bin/sleep")
        .arg("5")
        .spawn()
        .unwrap();
    let foreign = readiness::ready(child.id(), &ports);
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(
        !foreign.unwrap(),
        "foreign port occupancy must not make the receiver ready"
    );
    assert!(readiness::ready(std::process::id(), &ports).unwrap());
    let wildcard = UdpSocket::bind("0.0.0.0:0").unwrap();
    assert!(
        readiness::ready(std::process::id(), &[wildcard.local_addr().unwrap().port()]).unwrap()
    );
    assert!(!readiness::ready(0, &ports).unwrap());
    assert!(!readiness::ready(std::process::id(), &[]).unwrap());
    drop(second);
    for _ in 0..32 {
        assert!(!readiness::ready(std::process::id(), &ports).unwrap());
        let receiver = UdpSocket::bind(("127.0.0.1", p + 1)).unwrap();
        assert!(readiness::ready(std::process::id(), &ports).unwrap());
        drop(receiver);
    }
}
