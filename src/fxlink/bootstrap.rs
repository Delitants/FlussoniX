//! Optional v8 planning layered over ordinary receiver repair selection.
use super::{
    Budget, MAX_BLOCKS_PER_RECORD, Receiver, RepairRequest, Reservation, Settings,
    feedback::DatagramRequest,
    repair::{Pacing, after_reordering, reordering_grace},
};

#[derive(Clone, Copy)]
pub(super) struct View<'a> {
    pub(super) receiver: &'a Receiver,
    pub(super) pacing: &'a Pacing,
    pub(super) settings: &'a Settings,
    pub(super) budget: &'a Budget,
    pub(super) at: u64,
    pub(super) rtt: u64,
    pub(super) deadline: &'a dyn Fn(u64) -> u64,
    /// Existing timing policy, including conservative inherited discard age.
    pub(super) geometry_known: &'a dyn Fn(u64) -> bool,
    /// An actual header/Control shape; inherited age alone is not a shape.
    pub(super) authoritative_shape: &'a dyn Fn(u64) -> bool,
    pub(super) pending_shard: &'a dyn Fn(u64, u16) -> bool,
    pub(super) pending_record: &'a dyn Fn(u64) -> bool,
}
pub(super) struct PlannedRepair {
    pub(super) request: DatagramRequest,
    pub(super) deadline: u64,
}
pub(super) fn reserve_batch(slot: &mut Slot, budget: &Budget) -> Result<Reservation, super::Error> {
    match budget.reserve(65536) {
        Ok(metadata) => Ok(metadata),
        Err(error) if slot.0.is_none() => Err(error),
        Err(_) => {
            slot.abandon();
            budget.reserve(65536)
        }
    }
}
pub(super) fn prepare_history(
    requests: &mut [PlannedRepair],
    slot: &mut Slot,
    pacing: &Pacing,
) -> Result<Vec<super::repair::PreparedBlock>, String> {
    let prepared: Result<Vec<_>, _> = requests
        .iter()
        .map(|entry| pacing.prepare_block(entry.request.block_id()))
        .collect();
    match prepared {
        Ok(prepared) => return Ok(prepared),
        Err(error) if slot.0.is_none() => return Err(error),
        Err(_) => {}
    }
    // All partial reservations have dropped. Optional bootstrap bookkeeping must
    // not take memory needed by the already selected ordinary baseline.
    slot.abandon();
    for entry in requests.iter_mut() {
        if let DatagramRequest::Bootstrap { block_id, .. } = entry.request {
            entry.request = DatagramRequest::Ordinary(RepairRequest {
                block_id,
                shard_indices: vec![0],
            });
        }
    }
    requests
        .iter()
        .map(|entry| pacing.prepare_block(entry.request.block_id()))
        .collect()
}
enum Phase {
    Pending {
        due: u64,
    },
    Flight {
        base: u64,
        quiet: u64,
        last: Option<u64>,
        header_seen: bool,
    },
}
struct Attempt {
    block: u64,
    deadline: u64,
    reply_limit: u16,
    phase: Phase,
    _charge: Reservation,
}
#[derive(Default)]
pub(super) struct Slot(Option<Attempt>);
impl Slot {
    fn pending(
        &mut self,
        block: u64,
        deadline: u64,
        due: u64,
        reply_limit: u16,
        budget: &Budget,
    ) -> bool {
        if !(2..=128).contains(&reply_limit) {
            return false;
        }
        if let Some(attempt) = &mut self.0 {
            if !matches!(attempt.phase, Phase::Pending { .. }) {
                return false;
            }
            if attempt.block == block {
                attempt.deadline = attempt.deadline.min(deadline);
                attempt.reply_limit = reply_limit;
                attempt.phase = Phase::Pending { due };
                return true;
            }
        }
        self.abandon();
        let charge = 128
            .max(std::mem::size_of::<Self>().saturating_add(budget.reservation_metadata_bytes()));
        let Ok(charge) = budget.reserve(charge) else {
            return false;
        };
        self.0 = Some(Attempt {
            block,
            deadline,
            reply_limit,
            phase: Phase::Pending { due },
            _charge: charge,
        });
        true
    }
    pub(super) fn admitted(
        &mut self,
        block: u64,
        reply_limit: u16,
        queued_at: u64,
        rtt: u64,
    ) -> Result<(), String> {
        let mut attempt = self
            .0
            .take()
            .ok_or("FXLink bootstrap reservation missing")?;
        let margin = rtt.saturating_add(5);
        if attempt.block != block
            || attempt.reply_limit != reply_limit
            || !matches!(attempt.phase,Phase::Pending{due} if queued_at>=due)
            || queued_at.saturating_add(margin) >= attempt.deadline
        {
            return Err("FXLink bootstrap admission expired or changed".into());
        }
        let base = queued_at.saturating_add(margin.max(20));
        let last = attempt
            .deadline
            .checked_sub(margin)
            .and_then(|v| v.checked_sub(1));
        let quiet = last
            .filter(|last| base <= *last)
            .map_or(base, |last| base.saturating_add(20).min(last));
        attempt.phase = Phase::Flight {
            base,
            quiet,
            last,
            header_seen: false,
        };
        self.0 = Some(attempt);
        Ok(())
    }
    pub(super) fn observe_header(&mut self, record: u64, at: u64) {
        let Some(attempt) = &mut self.0 else { return };
        if attempt.block / MAX_BLOCKS_PER_RECORD as u64 != record {
            return;
        }
        if at >= attempt.deadline
            || matches!(attempt.phase, Phase::Pending { .. })
            || matches!(attempt.phase,Phase::Flight{quiet,..} if at>=quiet)
        {
            self.abandon();
            return;
        }
        if let Phase::Flight {
            base,
            quiet,
            last,
            header_seen,
        } = &mut attempt.phase
        {
            if *header_seen {
                return;
            }
            *header_seen = true;
            if let Some(last) = last.filter(|last| *base <= *last) {
                *quiet = (*quiet).max(at.saturating_add(20).min(base.saturating_add(40)).min(last));
            }
        }
    }
    pub(super) fn prune(&mut self, at: u64, delivered: u64) {
        if self.0.as_ref().is_some_and(|attempt| {
            attempt.block / (MAX_BLOCKS_PER_RECORD as u64) < delivered
                || at >= attempt.deadline
                || matches!(attempt.phase,Phase::Flight{quiet,..} if at>=quiet)
        }) {
            self.abandon();
        }
    }
    pub(super) fn blocked(&self, block: u64, at: u64) -> bool {
        self.0.as_ref().is_some_and(|attempt| {
            attempt.block == block
                && at < attempt.deadline
                && matches!(attempt.phase,Phase::Flight{quiet,..} if at<quiet)
        })
    }
    pub(super) fn wake_at(&self) -> Option<u64> {
        self.0.as_ref().map(|attempt| {
            attempt.deadline.min(match attempt.phase {
                Phase::Pending { due } => due,
                Phase::Flight { quiet, .. } => quiet,
            })
        })
    }
    pub(super) fn abandon(&mut self) {
        self.0 = None;
    }
    pub(super) fn has_attempt(&self) -> bool {
        self.0.is_some()
    }
}

pub(super) fn baseline(
    view: &View<'_>,
    slot: &Slot,
    exclude: Option<u64>,
    wake: &mut Option<u64>,
) -> Vec<RepairRequest> {
    let mut eligible = view.pacing.batch_eligible(view.at);
    view.receiver.repair_requests_avoiding(
        view.at,
        view.rtt,
        5,
        |block, index| {
            if exclude == Some(block) || slot.blocked(block, view.at) {
                return false;
            }
            let record = block / MAX_BLOCKS_PER_RECORD as u64;
            let deadline = (view.deadline)(record);
            if view.at.saturating_add(view.rtt).saturating_add(5) >= deadline {
                return false;
            }
            let normal = reordering_grace(view.settings.latency_ms, view.rtt);
            let grace = if (view.geometry_known)(record) {
                normal
            } else {
                normal.min(20)
            };
            after_reordering(
                view.at,
                deadline.saturating_sub(view.settings.latency_ms),
                grace,
                wake,
            ) && eligible(block, index)
        },
        view.pending_shard,
    )
}
pub(super) fn ordinary(view: &View<'_>, requests: Vec<RepairRequest>) -> Vec<PlannedRepair> {
    requests
        .into_iter()
        .map(|request| PlannedRepair {
            deadline: (view.deadline)(request.block_id / MAX_BLOCKS_PER_RECORD as u64),
            request: DatagramRequest::Ordinary(request),
        })
        .collect()
}
pub(super) fn plan(
    view: View<'_>,
    slot: &mut Slot,
    first_wake: &mut Option<u64>,
) -> Vec<PlannedRepair> {
    slot.prune(view.at, 0);
    if view.receiver.is_retired() {
        slot.abandon();
        return Vec::new();
    }
    let requests = baseline(&view, slot, None, first_wake);
    plan_from_baseline(view, slot, first_wake, requests)
}
/// Reuse a prospective selection without spending another traversal/rebuild.
pub(super) fn plan_from_baseline(
    view: View<'_>,
    slot: &mut Slot,
    first_wake: &mut Option<u64>,
    requests: Vec<RepairRequest>,
) -> Vec<PlannedRepair> {
    if slot
        .0
        .as_ref()
        .is_some_and(|attempt| matches!(attempt.phase, Phase::Flight { .. }))
    {
        return ordinary(&view, requests);
    }
    let candidate = requests.iter().position(|request| {
        let record = request.block_id / MAX_BLOCKS_PER_RECORD as u64;
        request.shard_indices == [0]
            && view.receiver.unknown_middle(request.block_id)
            && !(view.geometry_known)(record)
            && !(view.pending_record)(record)
    });
    let Some(index) = candidate else {
        slot.abandon();
        return ordinary(&view, requests);
    };
    let block = requests[index].block_id;
    let used: usize = requests.iter().map(|r| r.shard_indices.len()).sum();
    let grant = view
        .settings
        .data_shards
        .min(1 + view.settings.max_repair_shards.saturating_sub(used));
    if grant < 2 {
        slot.abandon();
        return ordinary(&view, requests);
    }
    let mut deadline = (view.deadline)(block / MAX_BLOCKS_PER_RECORD as u64);
    if let Some(old) = &slot.0 {
        if old.block == block {
            deadline = deadline.min(old.deadline);
        }
    }
    let due = deadline
        .saturating_sub(view.settings.latency_ms)
        .saturating_add(reordering_grace(view.settings.latency_ms, view.rtt));
    if !slot.pending(block, deadline, due, grant as u16, view.budget) {
        return ordinary(&view, requests);
    }
    if view.at < due {
        *first_wake = Some(first_wake.map_or(due, |previous| previous.min(due)));
        return ordinary(&view, baseline(&view, slot, Some(block), first_wake));
    }
    requests
        .into_iter()
        .enumerate()
        .map(|(i, request)| {
            if i == index {
                PlannedRepair {
                    request: DatagramRequest::Bootstrap {
                        block_id: block,
                        reply_limit: grant as u16,
                    },
                    deadline,
                }
            } else {
                PlannedRepair {
                    deadline: (view.deadline)(request.block_id / MAX_BLOCKS_PER_RECORD as u64),
                    request: DatagramRequest::Ordinary(request),
                }
            }
        })
        .collect()
}

#[cfg(test)]
#[path = "bootstrap_tests.rs"]
mod tests;
