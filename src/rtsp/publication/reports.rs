//! Receiver statistics describe validated packets received by the native relay.
use crate::direct_rtp::packet::{self, Parsed, Reorder};
use std::time::{Duration, Instant};
pub(super) struct Reports {
    id: u32,
    started: Instant,
    sequence: Reorder,
    packets: u64,
    last_highest: Option<u32>,
    last_loss: u64,
    transit: Option<u32>,
    jitter: f64,
    sr: Option<(u32, Instant)>,
}
impl Reports {
    pub fn new() -> Self {
        Self {
            id: u32::from_be_bytes(uuid::Uuid::new_v4().as_bytes()[..4].try_into().unwrap()),
            started: Instant::now(),
            sequence: Reorder::new(Duration::from_millis(50)),
            packets: 0,
            last_highest: None,
            last_loss: 0,
            transit: None,
            jitter: 0.0,
            sr: None,
        }
    }
    pub fn packet(&mut self, packet: &Parsed<'_>, clock: u32) {
        let now = Instant::now();
        self.packets = self.packets.saturating_add(1);
        // Allow brief UDP reordering, retaining at most 64 empty sequence markers.
        // Media itself goes directly to the decoder and is never held here.
        self.sequence.push(packet.sequence, Vec::new(), now);
        let arrival = (now.duration_since(self.started).as_nanos() * u128::from(clock)
            / 1_000_000_000) as u32;
        let transit = arrival.wrapping_sub(packet.timestamp);
        if let Some(previous) = self.transit {
            let delta = (transit.wrapping_sub(previous) as i32).unsigned_abs();
            self.jitter += (f64::from(delta) - self.jitter) / 16.0;
        }
        self.transit = Some(transit);
    }
    pub fn sender_report(&mut self, body: &[u8], at: Instant) {
        if body[1] == 200 {
            self.sr = Some((u32::from_be_bytes(body[10..14].try_into().unwrap()), at));
        }
    }
    pub fn report(&mut self, source: u32) -> Vec<u8> {
        self.sequence.flush(Instant::now());
        let highest = self.sequence.highest();
        let expected = self.last_highest.map_or(
            self.packets
                .saturating_sub(self.sequence.duplicates)
                .saturating_add(self.sequence.lost),
            |previous| u64::from(highest.wrapping_sub(previous)),
        );
        let fraction = self
            .sequence
            .lost
            .saturating_sub(self.last_loss)
            .saturating_mul(256)
            .checked_div(expected)
            .unwrap_or(0)
            .min(255) as u8;
        self.last_highest = Some(highest);
        self.last_loss = self.sequence.lost;
        let (last_sr, delay_sr) = self.sr.map_or((0, 0), |(sr, at)| {
            (
                sr,
                (at.elapsed().as_micros() * 65536 / 1_000_000).min(u128::from(u32::MAX)) as u32,
            )
        });
        packet::receiver_report(
            if self.id == source {
                self.id ^ 1
            } else {
                self.id
            },
            source,
            &packet::Reception {
                highest,
                lost: self.sequence.lost,
                fraction,
                jitter: self.jitter.min(f64::from(u32::MAX)) as u32,
                last_sr,
                delay_sr,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reordered_and_duplicate_packets_are_not_reported_as_loss() {
        let mut reports = Reports::new();
        for sequence in [100, 102, 101, 102, 103] {
            reports.packet(
                &Parsed {
                    sequence,
                    ssrc: 7,
                    timestamp: 90000,
                    payload: &[],
                },
                90000,
            );
        }
        let report = reports.report(7);
        assert_eq!(
            &report[13..16],
            &[0, 0, 0],
            "reordered packet filled the gap"
        );
        assert_eq!(u32::from_be_bytes(report[16..20].try_into().unwrap()), 103);
    }
}
