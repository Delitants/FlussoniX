//! Bounded retry history. New missing shards bypass another shard's cooldown.
use super::{Budget, MAX_BLOCKS_PER_RECORD, Reservation};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Default)]
struct Credit {
    epoch: Option<u64>,
    original: usize,
    repaired: usize,
}

/// Repair credit belongs to the originals' creation cohort, not the request's
/// arrival window. A record's unchanged TTL spans at most two cohorts, so no
/// unbounded credit bank or per-packet allocation is necessary.
pub(super) struct Allowance {
    origin: u64,
    latency: u64,
    credit: [Credit; 2],
}
impl Allowance {
    pub(super) fn new(origin: u64, latency: u64) -> Self {
        assert!(latency > 0);
        Self {
            origin,
            latency,
            credit: [Credit::default(); 2],
        }
    }
    fn epoch(&self, created: u64, at: u64) -> Option<u64> {
        (created >= self.origin && created <= at && at - created < self.latency)
            .then(|| (created - self.origin) / self.latency)
    }
    pub(super) fn original(&mut self, created: u64, at: u64, bytes: usize) -> bool {
        let Some(epoch) = self.epoch(created, at) else {
            return false;
        };
        let credit = &mut self.credit[(epoch % 2) as usize];
        if credit.epoch != Some(epoch) {
            *credit = Credit {
                epoch: Some(epoch),
                ..Credit::default()
            };
        }
        let Some(total) = credit.original.checked_add(bytes) else {
            return false;
        };
        credit.original = total;
        true
    }
    pub(super) fn repair(&mut self, created: u64, at: u64, bytes: usize) -> bool {
        let Some(epoch) = self.epoch(created, at) else {
            return false;
        };
        let credit = &mut self.credit[(epoch % 2) as usize];
        if credit.epoch != Some(epoch) {
            return false;
        }
        let Some(total) = credit
            .repaired
            .checked_add(bytes)
            .filter(|total| *total <= credit.original / 4)
        else {
            return false;
        };
        // Reserve before the async enqueue; a failed send can only consume
        // allowance conservatively, and must never grant it a second time.
        credit.repaired = total;
        true
    }
    pub(super) fn counters(&self, created: u64, at: u64) -> (usize, usize) {
        let Some(epoch) = self.epoch(created, at) else {
            return (0, 0);
        };
        let credit = &self.credit[(epoch % 2) as usize];
        if credit.epoch == Some(epoch) {
            (credit.original, credit.repaired)
        } else {
            (0, 0)
        }
    }
}

pub(super) const POLL_MS: u64 = 20;

// Bound first-request reordering independently of per-shard retry history.
pub(super) fn reordering_grace(latency: u64, rtt: u64) -> u64 {
    let two_rounds = rtt.saturating_mul(2).saturating_add(5);
    if two_rounds < latency {
        // Preserve even a one-millisecond two-round margin when division
        // rounds its grace to zero.
        rtt.min((latency - two_rounds) / 2)
    } else if rtt.saturating_add(5).saturating_add(2 * POLL_MS) < latency {
        // No second round fits. Still let intact media follow its announcement
        // before spending scarce repair credit. A short wait avoids skipping
        // the next useful poll against an older reliable writer's deadline.
        // Keep two nominal polls of margin for tick phase and this wait; the
        // ordinary arrival/cache guards stay authoritative.
        rtt.min(5)
    } else {
        // Preserve the immediate opportunity when a polling delay cannot fit.
        0
    }
}

pub(super) fn after_reordering(at: u64, first: u64, grace: u64, wake: &mut Option<u64>) -> bool {
    let due = first.saturating_add(grace);
    if at >= due {
        return true;
    }
    *wake = Some(wake.map_or(due, |previous| previous.min(due)));
    false
}

struct Attempt {
    until: u64,
    shards: [u64; 4],
    _reservation: Reservation,
}
pub(super) struct PreparedBlock {
    block: u64,
    reservation: Option<Reservation>,
}
pub(super) struct Pacing {
    blocks: BTreeMap<u64, Attempt>,
    next_retry: Option<u64>,
    budget: Budget,
    limit: usize,
    _metadata: Reservation,
}
impl Pacing {
    pub(super) fn new(budget: Budget, limit: usize) -> Result<Self, String> {
        let metadata = budget.reserve(65536).map_err(|e| e.to_string())?;
        Ok(Self {
            blocks: BTreeMap::new(),
            next_retry: None,
            budget,
            limit,
            _metadata: metadata,
        })
    }
    pub(super) fn prune(&mut self, now: u64, delivered: u64) {
        let mut next_retry: Option<u64> = None;
        self.blocks.retain(|block, attempt| {
            let keep = *block / MAX_BLOCKS_PER_RECORD as u64 >= delivered && now < attempt.until;
            if keep {
                next_retry = Some(next_retry.map_or(attempt.until, |at| at.min(attempt.until)));
            }
            keep
        });
        self.next_retry = next_retry;
    }
    /// Conservative earliest retry; prune refreshes an extended or delivered
    /// entry during its existing scan. Reading this hint never scans history.
    pub(super) fn wake_at(&self) -> Option<u64> {
        self.next_retry
    }
    pub(super) fn eligible(&self, block: u64, shard: u16, now: u64) -> bool {
        self.blocks.get(&block).is_none_or(|attempt| {
            now >= attempt.until
                || attempt.shards[shard as usize / 64] & (1u64 << (shard % 64)) == 0
        })
    }
    pub(super) fn has_history(&self, block: u64) -> bool {
        self.blocks.contains_key(&block)
    }
    /// Selected IDs are unique. Reserve their uncommitted nodes before optional
    /// geometry considers any new block, including later known-loss selections.
    pub(super) fn spare_after(&self, selected: &[u64]) -> usize {
        self.limit.saturating_sub(self.blocks.len()).saturating_sub(
            selected
                .iter()
                .filter(|block| !self.blocks.contains_key(block))
                .count(),
        )
    }
    /// Receiver visits blocks in ascending order. Reserve history capacity
    /// during selection, before a rejected new block can consume the shard cap.
    /// This constant-size batch view preserves in-flight entries and cooldowns;
    /// only sent() commits and charges the selected history nodes.
    pub(super) fn batch_eligible(&self, now: u64) -> impl FnMut(u64, u16) -> bool + '_ {
        let mut remaining = self.limit.saturating_sub(self.blocks.len());
        let mut previous = None;
        let mut admitted = false;
        move |block, shard| {
            if !self.eligible(block, shard, now) {
                return false;
            }
            if previous != Some(block) {
                previous = Some(block);
                admitted = self.blocks.contains_key(&block);
                if !admitted && remaining > 0 {
                    remaining -= 1;
                    admitted = true;
                }
            }
            admitted
        }
    }
    pub(super) fn sent(
        &mut self,
        block: u64,
        shards: &[u16],
        now: u64,
        rtt: u64,
    ) -> Result<(), String> {
        let prepared = self.prepare_block(block)?;
        self.sent_prepared(prepared, shards, now, rtt)
    }
    pub(super) fn prepare_block(&self, block: u64) -> Result<PreparedBlock, String> {
        let reservation = if self.blocks.contains_key(&block) {
            None
        } else {
            if self.blocks.len() >= self.limit {
                return Err("FXLink repair history limit".into());
            }
            Some(self.budget.reserve(256).map_err(|e| e.to_string())?)
        };
        Ok(PreparedBlock { block, reservation })
    }
    pub(super) fn sent_prepared(
        &mut self,
        prepared: PreparedBlock,
        shards: &[u16],
        now: u64,
        rtt: u64,
    ) -> Result<(), String> {
        let block = prepared.block;
        if !self.blocks.contains_key(&block) {
            if self.blocks.len() >= self.limit {
                return Err("FXLink repair history limit".into());
            }
            // Charge node storage before allocation; the fixed metadata charge
            // also covers B-tree slack and an initially sparse root.
            let reservation = prepared
                .reservation
                .ok_or("FXLink prepared history expired")?;
            self.blocks.insert(
                block,
                Attempt {
                    until: 0,
                    shards: [0; 4],
                    _reservation: reservation,
                },
            );
        }
        let attempt = self.blocks.get_mut(&block).unwrap();
        if now >= attempt.until {
            attempt.shards = [0; 4];
        }
        for shard in shards {
            attempt.shards[*shard as usize / 64] |= 1u64 << (*shard % 64);
        }
        attempt.until = now.saturating_add(rtt.saturating_add(5).max(POLL_MS));
        self.next_retry = Some(
            self.next_retry
                .map_or(attempt.until, |at| at.min(attempt.until)),
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fxlink::{Receiver, Settings};

    // Break caught: an extended or delivered history entry leaves a due wake
    // permanently armed, or a later entry hides an earlier eligible retry.
    #[test]
    fn retry_wake_refreshes_after_extensions_and_delivery() {
        let budget = Budget::new(1 << 20);
        let mut pacing = Pacing::new(budget.clone(), 64).unwrap();
        assert_eq!(pacing.wake_at(), None);
        pacing.sent(0, &[0], 100, 191).unwrap();
        assert_eq!(pacing.wake_at(), Some(296));
        pacing
            .sent(MAX_BLOCKS_PER_RECORD as u64, &[0], 110, 50)
            .unwrap();
        assert_eq!(pacing.wake_at(), Some(165));
        pacing
            .sent(MAX_BLOCKS_PER_RECORD as u64, &[1], 120, 100)
            .unwrap();
        assert!(pacing.wake_at().is_some_and(|at| at <= 225));
        pacing.prune(165, 0);
        assert_eq!(
            pacing.wake_at(),
            Some(225),
            "stale wake did not advance after prune"
        );
        assert!(!pacing.eligible(MAX_BLOCKS_PER_RECORD as u64, 0, 224));
        pacing.prune(166, 1);
        assert_eq!(pacing.wake_at(), Some(225));
        pacing.prune(225, 1);
        assert_eq!(
            pacing.wake_at(),
            None,
            "expired history left a busy timer armed"
        );
        drop(pacing);
        assert_eq!(budget.used(), 0);
    }

    // Break caught: an RTT-eligible retry waits for an unrelated polling phase,
    // reaching the source after its cache expires despite a useful earlier slot.
    #[test]
    fn retry_wake_recovers_before_the_original_cache_expires() {
        use crate::fxlink::Encoder;
        use bytes::Bytes;
        fn record(id: u8) -> Bytes {
            let mut bytes = vec![id; 1000];
            bytes[..4].copy_from_slice(&996u32.to_be_bytes());
            Bytes::from(bytes)
        }
        let settings = Settings {
            latency_ms: 500,
            ..Settings::default()
        };
        let budget = Budget::new(1 << 20);
        let mut encoder = Encoder::new(settings.clone(), 0, 1, Budget::new(1 << 20)).unwrap();
        let encoded = (0..12)
            .map(|id| {
                encoder
                    .encode(record(id), if id == 0 { 0 } else { 43 })
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let mut receiver = Receiver::new(settings, 0, 1, budget.clone()).unwrap();
        let mut pacing = Pacing::new(budget.clone(), 64).unwrap();
        let mut allowance = Allowance::new(0, 500);
        for frame in &encoded {
            assert!(allowance.original(
                frame.created_ms,
                frame.created_ms,
                frame.datagrams[0].len()
            ));
        }
        receiver.announce(0, 1000, 138).unwrap();
        for frame in &encoded[1..] {
            assert!(
                receiver
                    .ingest(&frame.datagrams[0], 138)
                    .unwrap()
                    .is_empty()
            );
        }
        let first = receiver.repair_requests_if(195, 191, 5, |b, s| pacing.eligible(b, s, 195));
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].block_id, 0);
        assert_eq!(first[0].shard_indices, [0]);
        pacing.sent(0, &[0], 195, 191).unwrap();
        assert!(allowance.repair(0, 290, encoded[0].datagrams[0].len()));
        // Drop that reply. A poll at385 precedes eligibility391; the next
        // ordinary poll413 would arrive at the source508, after its TTL500.
        pacing.prune(385, 0);
        assert!(
            receiver
                .repair_requests_if(385, 191, 5, |b, s| pacing.eligible(b, s, 385))
                .is_empty()
        );
        let retry_at = pacing.wake_at().unwrap_or(413).min(413);
        pacing.prune(retry_at, 0);
        let retry =
            receiver.repair_requests_if(retry_at, 191, 5, |b, s| pacing.eligible(b, s, retry_at));
        assert_eq!(retry.len(), 1);
        pacing.sent(0, &[0], retry_at, 191).unwrap();
        let source_at = retry_at + 95;
        let response =
            source_at + 5 < 500 && allowance.repair(0, source_at, encoded[0].datagrams[0].len());
        let output = if response {
            receiver
                .ingest(&encoded[0].datagrams[0], source_at + 95)
                .unwrap()
        } else {
            Vec::new()
        };
        assert_eq!(
            output.len(),
            12,
            "retry reached source after the original cache TTL"
        );
        assert_eq!(retry_at, 391);
        assert_eq!(source_at, 486);
        assert_eq!(source_at + 95, 581);
        assert!(source_at + 95 < 638, "receiver deadline cannot be extended");
        assert_eq!(output, (0..12).map(record).collect::<Vec<_>>());
        assert_eq!(receiver.stats().expired_records, 0);
        drop(receiver);
        drop(pacing);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn repair_credit_survives_a_boundary_without_borrowing_another_cohorts_quota() {
        let mut allowance = Allowance::new(0, 1000);
        // Captured startup sequence: the next epoch has only 544 bytes left,
        // while the still-valid original cohort can afford the missing shard.
        assert!(allowance.original(600, 600, 611_456));
        assert!(allowance.repair(600, 900, 40_256));
        assert!(allowance.original(1050, 1050, 15_232));
        assert!(allowance.repair(1050, 1100, 3264));
        assert!(!allowance.repair(1050, 1100, 1088));
        assert!(allowance.repair(600, 1100, 1088));
        assert!(allowance.repair(600, 1100, 111_520));
        assert!(!allowance.repair(600, 1100, 1));
        assert_eq!(allowance.counters(600, 1100), (611_456, 152_864));
    }

    #[test]
    fn repair_credit_cannot_outlive_records_or_leak_into_reused_epochs() {
        for latency in [500, 1000, 1500] {
            let mut allowance = Allowance::new(100, latency);
            let created = 100 + latency - 1;
            assert!(allowance.original(created, created + latency / 2, 4352));
            assert!(allowance.repair(created, created + latency - 1, 1088));
            assert!(!allowance.repair(created, created + latency, 1));
            assert!(!allowance.original(created, created + latency, 4352));
            let next = 100 + 2 * latency;
            assert!(allowance.original(next, next, 4));
            assert!(!allowance.repair(next, next, 2));
            assert!(allowance.repair(next, next, 1));
            assert!(!allowance.repair(created, next, 1));
            assert!(!allowance.original(next + 1, next, 4));
        }
    }

    // Break caught: an announcement wins a scheduling turn over intact media.
    // A redundant repair then spends the sole allowance before real loss appears.
    #[test]
    fn metadata_race_cannot_spend_the_only_repair_on_an_intact_record() {
        use crate::fxlink::Encoder;
        use bytes::Bytes;
        fn record(id: u8) -> Bytes {
            let mut bytes = vec![id; 1000];
            bytes[..4].copy_from_slice(&996u32.to_be_bytes());
            Bytes::from(bytes)
        }
        let settings = Settings {
            latency_ms: 500,
            ..Settings::default()
        };
        let budget = Budget::new(1 << 20);
        let mut encoder = Encoder::new(settings.clone(), 0, 1, Budget::new(1 << 20)).unwrap();
        let encoded = (0..5)
            .map(|id| {
                encoder
                    .encode(record(id), if id == 0 { 500 } else { 660 })
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let mut receiver = Receiver::new(settings.clone(), 0, 1, budget.clone()).unwrap();
        let mut pacing = Pacing::new(budget.clone(), settings.max_blocks).unwrap();
        let mut allowance = Allowance::new(0, 500);
        assert!(allowance.original(500, 500, encoded[0].datagrams[0].len()));
        receiver.announce(0, 1000, 650).unwrap();
        let grace = reordering_grace(500, 300);
        let early = {
            let mut eligible = pacing.batch_eligible(650);
            receiver.repair_requests_if(650, 300, 5, |block, shard| {
                650 >= 650 + grace && eligible(block, shard)
            })
        };
        for request in &early {
            pacing
                .sent(request.block_id, &request.shard_indices, 650, 300)
                .unwrap();
        }
        // The intact original follows its announcement by one scheduling turn.
        let mut output = receiver.ingest(&encoded[0].datagrams[0], 651).unwrap();
        for frame in &encoded[1..] {
            assert!(allowance.original(660, 660, frame.datagrams[0].len()));
        }
        // Feedback takes 150 ms each way. Later originals now fund the stale
        // request, exactly as in the encrypted failure trace.
        let duplicate = early
            .iter()
            .any(|r| r.block_id == 0 && r.shard_indices.contains(&0))
            && allowance.repair(500, 800, encoded[0].datagrams[0].len());
        for frame in &encoded[2..] {
            assert!(
                receiver
                    .ingest(&frame.datagrams[0], 810)
                    .unwrap()
                    .is_empty()
            );
        }
        // A poll at age 19 ms followed by a 30 ms tick/poll phase must not
        // postpone this one-round repair beyond the older held writer deadline.
        let mut repairs = Vec::new();
        let mut queried_at = 0;
        for at in [829, 859] {
            pacing.prune(at, 1);
            let batch = {
                let mut eligible = pacing.batch_eligible(at);
                receiver.repair_requests_if(at, 300, 5, |block, shard| {
                    at >= 810 + grace && eligible(block, shard)
                })
            };
            if !batch.is_empty() {
                queried_at = at;
                for request in &batch {
                    pacing
                        .sent(request.block_id, &request.shard_indices, at, 300)
                        .unwrap();
                }
                repairs = batch;
                break;
            }
        }
        if duplicate {
            assert!(
                receiver
                    .ingest(&encoded[0].datagrams[0], 950)
                    .unwrap()
                    .is_empty()
            );
        }
        let repaired = repairs
            .iter()
            .any(|r| r.block_id == MAX_BLOCKS_PER_RECORD as u64 && r.shard_indices.contains(&0))
            && allowance.repair(660, queried_at + 150, encoded[1].datagrams[0].len());
        let arrived_at = queried_at + 300;
        if repaired && arrived_at < 651 + settings.latency_ms {
            output.extend(
                receiver
                    .ingest(&encoded[1].datagrams[0], arrived_at)
                    .unwrap(),
            );
        }
        assert_eq!(
            output.len(),
            5,
            "duplicate repair or skipped poll prevented real loss recovery"
        );
        assert_eq!(
            output,
            (0..5).map(record).collect::<Vec<_>>(),
            "duplicate repair or skipped poll prevented real loss recovery"
        );
        assert!(
            arrived_at < 651 + settings.latency_ms,
            "repair must precede the held writer's deadline"
        );
        assert_eq!(receiver.stats().late_shards, 0);
        assert_eq!(receiver.stats().expired_records, 0);
        drop(receiver);
        drop(pacing);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn single_round_reordering_wait_keeps_a_useful_poll_and_return_trip() {
        for (latency, rtt, expected) in [
            (500, 300, 5),
            (500, 455, 0),
            (501, 455, 5),
            (500, 454, 5),
            (500, 456, 0),
            // One millisecond of two-round margin still keeps its old zero
            // grace; rounding must not silently switch to the single-round case.
            (500, 247, 0),
            (502, 248, 0),
            (503, 248, 1),
            (500, 0, 0),
            (0, 0, 0),
            (500, u64::MAX, 0),
            (u64::MAX, u64::MAX, 0),
            (u64::MAX, u64::MAX / 2, 5),
        ] {
            assert_eq!(
                reordering_grace(latency, rtt),
                expected,
                "latency={latency}, rtt={rtt}"
            );
        }
        for latency in (500u64..=1500).step_by(50) {
            for rtt in 0u64..=750 {
                if rtt * 2 + 5 < latency {
                    continue;
                }
                let grace = reordering_grace(latency, rtt);
                assert!(grace <= rtt);
                if grace > 0 {
                    assert!(
                        grace <= 5 && grace + POLL_MS + rtt + 5 < latency,
                        "reordering wait used the final useful repair opportunity"
                    );
                }
            }
        }
    }

    #[test]
    fn reordering_grace_retains_a_conservative_two_round_trip_margin() {
        // Retain the conservative grace for announcement/control delay and
        // repair travel. The listener separately enforces its original cache TTL.
        for latency in (500u64..=1500).step_by(50) {
            for rtt in 0u64..=750 {
                let grace = reordering_grace(latency, rtt);
                assert!(grace <= rtt);
                let arrival_and_guard = rtt * 2 + 5;
                if arrival_and_guard < latency {
                    assert!(
                        arrival_and_guard + grace < latency,
                        "grace consumes repair scheduling margin: latency={latency}, rtt={rtt}, grace={grace}"
                    );
                }
                // The no-two-round case has a separate one-round/poll contract.
            }
        }
        assert_eq!(reordering_grace(500, 190), 57);
        assert_eq!(reordering_grace(1000, 190), 190);
        assert_eq!(reordering_grace(u64::MAX, u64::MAX), 0);
        assert_eq!(reordering_grace(0, 0), 0);
    }

    #[test]
    fn unsent_shards_bypass_cooldown_before_batch_cap_is_applied() {
        let budget = Budget::new(1024 * 1024);
        let mut pacing = Pacing::new(budget.clone(), 2048).unwrap();
        let mut receiver = Receiver::new(
            Settings {
                latency_ms: 500,
                max_repair_shards: 1,
                ..Settings::default()
            },
            0,
            1,
            budget,
        )
        .unwrap();
        receiver.announce(0, 4000, 0).unwrap();
        let first = receiver.repair_requests_if(10, 300, 5, |b, s| pacing.eligible(b, s, 10));
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].shard_indices, [0]);
        pacing.sent(0, &[0], 10, 300).unwrap();
        let next = receiver.repair_requests_if(30, 300, 5, |b, s| pacing.eligible(b, s, 30));
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].shard_indices, [1]);
        assert!(!pacing.eligible(0, 0, 314));
        assert!(pacing.eligible(0, 0, 315));
        assert!(
            receiver
                .repair_requests_if(315, 300, 5, |b, s| pacing.eligible(b, s, 315))
                .is_empty(),
            "retry cooldown must not extend original receive deadline"
        );
    }

    #[test]
    fn history_limit_and_pruning_preserve_budget_ownership() {
        let budget = Budget::new(1024 * 1024);
        let mut pacing = Pacing::new(budget.clone(), 1).unwrap();
        let base = budget.used();
        pacing.sent(0, &[0], 10, 300).unwrap();
        assert!(budget.used() > base);
        let charged = budget.used();
        assert!(pacing.sent(2048, &[0], 30, 300).is_err());
        assert_eq!(
            budget.used(),
            charged,
            "rejected history insertion leaked a charge"
        );
        pacing.prune(30, 1);
        assert_eq!(
            budget.used(),
            base,
            "completed record retained retry metadata"
        );
        pacing.sent(2048, &[127], 40, 300).unwrap();
        assert!(!pacing.eligible(2048, 127, 50));
        pacing.prune(345, 1);
        assert_eq!(budget.used(), base, "expired history retained charges");
        drop(pacing);
        assert_eq!(budget.used(), 0);
    }
}
