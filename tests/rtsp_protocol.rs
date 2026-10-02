use flussonix::rtsp::protocol::{Event, Transport, read_event};
use tokio::io::AsyncWriteExt;
#[tokio::test]
async fn split_control_body_and_interleaved_pipeline_are_framed_exactly() {
    let (mut tx, mut rx) = tokio::io::duplex(4096);
    let producer = tokio::spawn(async move {
        for b in b"GET_PARAMETER rtsp://example/live RTSP/1.0\r\nCSeq: 42\r\nContent-Length: 3\r\n\r\nabc$\x01\x00\x03xyzOPTIONS * RTSP/1.0\r\nCSeq: 43\r\n\r\n" {tx.write_all(&[*b]).await.unwrap();}
    });
    let Event::Request(r) = read_event(&mut rx).await.unwrap() else {
        panic!()
    };
    assert_eq!(r.cseq, 42);
    assert_eq!(r.body, b"abc");
    let Event::Interleaved(channel, b) = read_event(&mut rx).await.unwrap() else {
        panic!()
    };
    assert_eq!(channel, 1);
    assert_eq!(b, b"xyz");
    let Event::Request(r) = read_event(&mut rx).await.unwrap() else {
        panic!()
    };
    assert_eq!(r.cseq, 43);
    producer.await.unwrap();
}
#[tokio::test]
async fn ambiguous_oversized_and_malformed_control_are_rejected() {
    for request in [
        "OPTIONS * RTSP/1.0\r\nCSeq: 1\r\ncseq: 2\r\n\r\n",
        "OPTIONS * RTSP/1.0\r\nCSeq: 1\r\nContent-Length: 65537\r\n\r\n",
        "OPTIONS * RTSP/1.0\r\nCSeq: -1\r\n\r\n",
        "OPTIONS * RTSP/1.0\r\nCSeq: 1\r\n folded: value\r\n\r\n",
        "OPTIONS * RTSP/1.0\r\n\r\n",
    ] {
        let mut r = request.as_bytes();
        assert!(read_event(&mut r).await.is_err());
    }
    let data = format!(
        "OPTIONS * RTSP/1.0\r\nCSeq: 1\r\nX-Large: {}\r\n\r\n",
        "a".repeat(17000)
    );
    assert!(read_event(&mut data.as_bytes()).await.is_err());
    let data = b"$\x01\x20\x01";
    assert!(read_event(&mut &data[..]).await.is_err());
    let mut data = &b"OPTIONS * RTSP/2.0\r\nCSeq: 9\r\n\r\n"[..];
    assert_eq!(read_event(&mut data).await.unwrap_err().code, 505);
}
#[test]
fn only_explicit_unicast_tcp_playback_transport_is_accepted() {
    assert_eq!(
        Transport::parse("RTP/AVP/TCP;unicast;interleaved=2-3;mode=\"PLAY\""),
        Ok(Transport { rtp: 2, rtcp: 3 })
    );
    assert!(Transport::parse("RTP/AVP/TCP;interleaved=0-1").is_ok());
    for invalid in [
        "RTP/AVP;unicast;client_port=1000-1001",
        "RTP/AVP/TCP;multicast;interleaved=0-1",
        "RTP/AVP/TCP;interleaved=0-0",
        "RTP/AVP/TCP;interleaved=256-257",
        "RTP/AVP/TCP;interleaved=0-1;interleaved=2-3",
        "RTP/AVP/TCP;interleaved=0-1;mode=RECORD",
        "RTP/AVP/TCP;interleaved=0-1;destination=other",
        "RTP/AVP/TCP;interleaved=0-1,RTP/AVP/TCP;interleaved=2-3",
    ] {
        assert_eq!(Transport::parse(invalid), Err(461));
    }
}
