//! One charged, disposable feedback copy. No background task or pacing history.
use super::{Budget, Reservation, feedback::MAX_FRAME};
use bytes::Bytes;
use futures_util::FutureExt;

const COPY_BYTES: usize = 8 * 1024;
pub(super) struct Replica {
    frames: Vec<Bytes>,
    pub(super) due: u64,
    deadline: u64,
    last_record: u64,
    _guard: Reservation,
}
impl Replica {
    pub(super) fn new(
        frames: Vec<Bytes>,
        queued_at: u64,
        deadline: u64,
        last_record: u64,
        budget: &Budget,
    ) -> Option<Self> {
        if frames.is_empty()
            || frames.len() > 4
            || frames.iter().any(|frame| frame.len() > MAX_FRAME)
        {
            return None;
        }
        let guard = budget.reserve(COPY_BYTES).ok()?;
        Some(Self {
            frames,
            due: queued_at.saturating_add(20),
            deadline,
            last_record,
            _guard: guard,
        })
    }
    // Also called immediately before a nonempty new primary: the monotonic
    // clock may cross the old due time after the loop's initial due check.
    pub(super) fn take_attempt(
        pending: &mut Option<Self>,
        connection: &quinn::Connection,
        at: u64,
        rtt: u64,
        delivered: u64,
    ) -> usize {
        pending
            .take()
            .map_or(0, |copy| copy.attempt(connection, at, rtt, delivered))
    }
    fn useful(&self, at: u64, rtt: u64, delivered: u64) -> bool {
        at >= self.due
            && delivered <= self.last_record
            && at.saturating_add(rtt).saturating_add(5) < self.deadline
    }
    // Consuming self retires this attempt even if the first frame blocks or
    // only a prefix fits. A pending send is never awaited and cannot evict
    // other streams' queued originals/feedback, unlike send_datagram.
    pub(super) fn attempt(
        self,
        connection: &quinn::Connection,
        at: u64,
        rtt: u64,
        delivered: u64,
    ) -> usize {
        if !self.useful(at, rtt, delivered) {
            return 0;
        }
        let mut sent = 0;
        for frame in &self.frames {
            if !matches!(
                connection.send_datagram_wait(frame.clone()).now_or_never(),
                Some(Ok(()))
            ) {
                break;
            }
            sent += 1;
        }
        sent
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn copy_is_optional_charged_bounded_and_keeps_original_deadline() {
        let budget = Budget::new(COPY_BYTES);
        let frames = vec![Bytes::from_static(b"feedback")];
        let copy = Replica::new(frames.clone(), 100, 500, 9, &budget).unwrap();
        assert_eq!(budget.used(), COPY_BYTES);
        assert!(Replica::new(frames.clone(), 100, 500, 9, &budget).is_none());
        assert!(!copy.useful(119, 190, 0));
        assert!(copy.useful(120, 190, 0));
        assert!(copy.useful(304, 190, 9));
        assert!(!copy.useful(305, 190, 9));
        assert!(!copy.useful(120, 190, 10));
        assert!(!copy.useful(u64::MAX, 190, 0));
        drop(copy);
        assert_eq!(budget.used(), 0);
        assert!(Replica::new(vec![], 100, 500, 9, &budget).is_none());
        assert!(Replica::new(vec![frames[0].clone(); 5], 100, 500, 9, &budget).is_none());
        assert!(
            Replica::new(
                vec![Bytes::from(vec![0; MAX_FRAME + 1])],
                100,
                500,
                9,
                &budget
            )
            .is_none()
        );
        assert_eq!(budget.used(), 0);
    }
    #[tokio::test]
    async fn optional_copy_under_real_quic_pressure_cannot_evict_queued_frames() {
        use crate::fxlink::{control, transport};
        use std::{sync::Arc, time::Duration};
        let _serial = transport::test_lock().lock().await;
        let cert = crate::transport_tls_fixture::Certificates::new();
        let mut tls = (*cert.server()).clone();
        tls.alpn_protocols = control::protocols();
        let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
        let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
        config.transport_config(transport::config());
        let server = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
        let mut tls = (*cert.client()).clone();
        tls.alpn_protocols = control::protocols();
        let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls).unwrap();
        let mut config = quinn::ClientConfig::new(Arc::new(crypto));
        config.transport_config(transport::config());
        let mut client = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        client.set_default_client_config(config);
        let (sender, receiver) = tokio::join!(
            client
                .connect(server.local_addr().unwrap(), "localhost")
                .unwrap(),
            async { server.accept().await.unwrap().await }
        );
        let sender = sender.unwrap();
        let receiver = receiver.unwrap();
        // No yield in this fill: this current-thread runtime cannot flush the
        // connection driver until the bounded send queue reports Pending.
        let mut originals = Vec::new();
        for sequence in 0u8..64 {
            let frame = Bytes::from(vec![sequence; 1000]);
            match sender.send_datagram_wait(frame.clone()).now_or_never() {
                Some(Ok(())) => originals.push(frame),
                None => break,
                Some(Err(error)) => panic!("queue fixture: {error}"),
            }
        }
        assert!(!originals.is_empty() && originals.len() < 64);
        let space = sender.datagram_send_buffer_space();
        let budget = Budget::new(COPY_BYTES);
        let copy =
            Replica::new(vec![Bytes::from(vec![255; 1000]); 4], 0, 5000, 9, &budget).unwrap();
        assert_eq!(copy.attempt(&sender, 20, 0, 0), 0);
        assert_eq!(
            sender.datagram_send_buffer_space(),
            space,
            "copy evicted queued traffic"
        );
        assert_eq!(budget.used(), 0, "blocked copy retained its reservation");
        let received = tokio::time::timeout(Duration::from_secs(2), async {
            let mut frames = Vec::new();
            for _ in 0..originals.len() {
                frames.push(receiver.read_datagram().await.unwrap());
            }
            frames
        })
        .await
        .unwrap();
        assert_eq!(
            received, originals,
            "optional copy displaced primary frames"
        );
        // The loop checked at19ms, but the primary floor becomes due at20ms.
        // A remaining old copy must get its one attempt before replacement,
        // releasing its charge so a replacement fits this single-copy budget.
        let frame = Bytes::from_static(b"old-copy-at-clock-boundary");
        let mut pending = Some(Replica::new(vec![frame.clone()], 0, 5000, 9, &budget).unwrap());
        assert!(19 < pending.as_ref().unwrap().due);
        assert_eq!(
            Replica::take_attempt(&mut pending, &sender, 20, 0, 0),
            1,
            "primary overwrote an eligible old copy"
        );
        assert!(pending.is_none());
        assert_eq!(budget.used(), 0);
        assert_eq!(Replica::take_attempt(&mut pending, &sender, 20, 0, 0), 0);
        let replacement = Replica::new(vec![frame.clone()], 20, 5000, 9, &budget).unwrap();
        let received_copy = tokio::time::timeout(Duration::from_secs(2), receiver.read_datagram())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received_copy, frame);
        drop(replacement);
        assert_eq!(budget.used(), 0);
        client.close(0u32.into(), b"done");
        server.close(0u32.into(), b"done");
        client.wait_idle().await;
        server.wait_idle().await;
    }
}
