use std::collections::VecDeque;

struct Packets {
    values: VecDeque<Vec<u8>>,
    bytes: usize,
    max_packets: usize,
    max_bytes: usize,
}

impl Packets {
    fn new(max_packets: usize, max_bytes: usize) -> Self {
        Self {
            values: VecDeque::new(),
            bytes: 0,
            max_packets,
            max_bytes,
        }
    }

    fn push(&mut self, packet: Vec<u8>) -> u64 {
        self.bytes = self.bytes.saturating_add(packet.len());
        self.values.push_back(packet);
        let mut dropped = 0u64;
        while self.values.len() > self.max_packets || self.bytes > self.max_bytes {
            let oldest = self
                .values
                .pop_front()
                .expect("queue contains newest packet");
            self.bytes -= oldest.len();
            dropped = dropped.saturating_add(1);
        }
        dropped
    }

    fn pop(&mut self) -> Option<Vec<u8>> {
        let packet = self.values.pop_front()?;
        self.bytes -= packet.len();
        Some(packet)
    }
}

pub(crate) struct StartupQueue {
    rtp: Packets,
    rtcp: Packets,
}

impl StartupQueue {
    pub(crate) fn new(
        rtp_limit_packets: usize,
        rtp_limit_bytes: usize,
        rtcp_limit_packets: usize,
        rtcp_limit_bytes: usize,
    ) -> Self {
        Self {
            rtp: Packets::new(rtp_limit_packets, rtp_limit_bytes),
            rtcp: Packets::new(rtcp_limit_packets, rtcp_limit_bytes),
        }
    }

    pub(crate) fn push_rtp(&mut self, packet: Vec<u8>) -> u64 {
        self.rtp.push(packet)
    }

    pub(crate) fn push_rtcp(&mut self, packet: Vec<u8>) -> u64 {
        self.rtcp.push(packet)
    }

    pub(crate) fn pop_rtp(&mut self) -> Option<Vec<u8>> {
        self.rtp.pop()
    }

    pub(crate) fn pop_rtcp(&mut self) -> Option<Vec<u8>> {
        self.rtcp.pop()
    }

    pub(crate) fn has_rtp(&self) -> bool {
        !self.rtp.values.is_empty()
    }

    pub(crate) fn has_rtcp(&self) -> bool {
        !self.rtcp.values.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::StartupQueue;
    use crate::direct_rtp::stats::Stats;
    use std::sync::atomic::Ordering;

    #[test]
    fn oldest_rtp_is_evicted_at_packet_and_byte_limits() {
        let mut count = StartupQueue::new(1024, 2 * 1024 * 1024, 64, 128 * 1024);
        for n in 0..1024 {
            assert_eq!(count.push_rtp(vec![(n % 251) as u8]), 0);
        }
        assert_eq!(count.push_rtp(vec![255]), 1);
        assert!(count.has_rtp());
        assert_eq!(count.pop_rtp(), Some(vec![1]));
        for n in 2..1024 {
            assert_eq!(count.pop_rtp(), Some(vec![(n % 251) as u8]));
        }
        assert_eq!(count.pop_rtp(), Some(vec![255]));
        assert_eq!(count.pop_rtp(), None);
        assert!(!count.has_rtp());

        let mut bytes = StartupQueue::new(1024, 2 * 1024 * 1024, 64, 128 * 1024);
        for n in 0..512 {
            assert_eq!(bytes.push_rtp(vec![(n % 251) as u8; 4096]), 0);
        }
        assert_eq!(bytes.push_rtp(vec![255]), 1);
        assert_eq!(bytes.pop_rtp(), Some(vec![1; 4096]));
    }

    #[test]
    fn rtcp_has_an_independent_64_packet_128_kib_cap() {
        let mut queue = StartupQueue::new(1024, 2 * 1024 * 1024, 64, 128 * 1024);
        assert_eq!(queue.push_rtp(vec![7]), 0);
        for n in 0..64 {
            assert_eq!(queue.push_rtcp(vec![n as u8; 2048]), 0);
        }
        assert_eq!(queue.push_rtcp(vec![255]), 1);
        assert!(queue.has_rtcp());
        assert_eq!(queue.pop_rtcp(), Some(vec![1; 2048]));
        assert_eq!(queue.pop_rtp(), Some(vec![7]));
    }

    #[test]
    fn stats_snapshot_reports_startup_discards_separately_from_loss() {
        let stats = Stats::default();
        stats.startup_dropped_packets.store(2, Ordering::Relaxed);
        stats
            .startup_dropped_rtcp_packets
            .store(3, Ordering::Relaxed);
        stats.lost.store(9, Ordering::Relaxed);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot["startup_dropped_packets"], 2);
        assert_eq!(snapshot["startup_dropped_rtcp_packets"], 3);
        assert_eq!(snapshot["lost"], 9);
    }
}
