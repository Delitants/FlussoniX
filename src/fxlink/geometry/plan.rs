//! Exact-shape overlay on a prospective v8 selection, with no new gap or age.
use super::receive::Snapshot;
use crate::fxlink::{
    MAX_BLOCKS_PER_RECORD, RepairRequest,
    bootstrap::{self, PlannedRepair, Slot, View},
    feedback::DatagramRequest,
    repair::reordering_grace,
};
pub(crate) fn plan(
    view: View<'_>,
    hints: &Snapshot,
    proof: Option<u64>,
    slot: &mut Slot,
    wake: &mut Option<u64>,
) -> Vec<PlannedRepair> {
    slot.prune(view.at, 0);
    if slot.has_attempt() || view.receiver.is_retired() || proof.is_none() {
        return bootstrap::plan(view, slot, wake);
    }
    let mut baseline = bootstrap::baseline(&view, slot, None, wake);
    let shapes: Vec<(u64, usize)> = baseline
        .iter()
        .filter_map(|request| {
            let record = request.block_id / MAX_BLOCKS_PER_RECORD as u64;
            if record > proof.unwrap()
                || !view.receiver.missing_shape(record)
                || (view.authoritative_shape)(record)
                || (view.pending_record)(record)
            {
                return None;
            }
            hints
                .shape_at(record, view.at)
                .map(|length| (record, length))
        })
        .collect();
    if shapes.is_empty() {
        return bootstrap::plan_from_baseline(view, slot, wake, baseline);
    }
    let shape = |record| {
        shapes
            .iter()
            .find(|(id, _)| *id == record)
            .map(|(_, length)| *length)
    };
    let candidate = baseline.iter().position(|request| {
        request.shard_indices == [0]
            && view.receiver.unknown_middle(request.block_id)
            && !(view.geometry_known)(request.block_id / MAX_BLOCKS_PER_RECORD as u64)
            && !(view.pending_record)(request.block_id / MAX_BLOCKS_PER_RECORD as u64)
    });
    let mut selected = if let Some(index) = candidate {
        let record = baseline[index].block_id / MAX_BLOCKS_PER_RECORD as u64;
        if let Some(length) = shape(record) {
            let used: usize = baseline.iter().map(|r| r.shard_indices.len()).sum();
            let grant = view
                .settings
                .data_shards
                .min(1 + view.settings.max_repair_shards.saturating_sub(used));
            let due = (view.deadline)(record)
                .saturating_sub(view.settings.latency_ms)
                .saturating_add(reordering_grace(view.settings.latency_ms, view.rtt));
            if view.settings.fragment_count(length) > 1 && grant >= 2 && view.at < due {
                *wake = Some(wake.map_or(due, |previous| previous.min(due)));
                // Preserve v8's prospective withholding, without allocating a
                // guard for a shape that can become exact ordinary data at due.
                baseline = bootstrap::baseline(&view, slot, Some(baseline[index].block_id), wake);
            }
            bootstrap::ordinary(&view, baseline)
        } else {
            bootstrap::plan_from_baseline(view, slot, wake, baseline)
        }
    } else {
        bootstrap::ordinary(&view, baseline)
    };
    // Bound ordinary requests without replaying stateful history eligibility.
    view.receiver.bound_shape_requests(
        selected
            .iter_mut()
            .filter_map(|entry| match &mut entry.request {
                DatagramRequest::Ordinary(request) => Some(request),
                _ => None,
            }),
        shape,
    );
    selected.retain(|entry| entry.request.units() != 0);
    let mut left = view
        .settings
        .max_repair_shards
        .saturating_sub(selected.iter().map(|entry| entry.request.units()).sum());
    let ids: Vec<_> = selected
        .iter()
        .map(|entry| entry.request.block_id())
        .collect();
    let mut history = view.pacing.spare_after(&ids);
    let mut blocks = view.receiver.hint_extra_block_capacity();
    for (record, length) in &shapes {
        let base = record * MAX_BLOCKS_PER_RECORD as u64;
        if !selected
            .iter()
            .any(|entry| entry.request.block_id() == base)
        {
            continue;
        }
        let due = (view.deadline)(*record)
            .saturating_sub(view.settings.latency_ms)
            .saturating_add(reordering_grace(view.settings.latency_ms, view.rtt));
        if view.receiver.unknown_middle(base) && view.at < due {
            if view.settings.fragment_count(*length) > 1 {
                *wake = Some(wake.map_or(due, |previous| previous.min(due)));
            }
            continue;
        }
        for index in 0..view.settings.block_count(*length) {
            if left == 0 {
                break;
            }
            let block = base + index as u64;
            if index != 0 {
                if blocks == 0 {
                    break;
                }
                blocks -= 1;
            }
            let existing = selected
                .iter()
                .position(|entry| entry.request.block_id() == block);
            if existing.is_none() && !view.pacing.has_history(block) && history == 0 {
                continue;
            }
            let missing = (0..view.settings.block_data(*length, index))
                .map(|i| i as u16)
                .filter(|i| {
                    !existing.is_some_and(|position| match &selected[position].request {
                        DatagramRequest::Ordinary(request) => request.shard_indices.contains(i),
                        _ => true,
                    })
                })
                .filter(|i| {
                    !(view.pending_shard)(block, *i) && view.pacing.eligible(block, *i, view.at)
                })
                .take(left)
                .collect::<Vec<_>>();
            if missing.is_empty() {
                continue;
            }
            left -= missing.len();
            if let Some(position) = existing {
                if let DatagramRequest::Ordinary(request) = &mut selected[position].request {
                    request.shard_indices.extend(missing);
                }
            } else {
                if !view.pacing.has_history(block) {
                    history -= 1;
                }
                selected.push(PlannedRepair {
                    deadline: (view.deadline)(*record),
                    request: DatagramRequest::Ordinary(RepairRequest {
                        block_id: block,
                        shard_indices: missing,
                    }),
                });
            }
        }
    }
    // Every systematic selection wins before optional fixed-layout parity.
    // A one-fragment hint never asserts cached compact parity.
    for entry in &mut selected {
        if left == 0 {
            break;
        }
        let DatagramRequest::Ordinary(request) = &mut entry.request else {
            continue;
        };
        let record = request.block_id / MAX_BLOCKS_PER_RECORD as u64;
        let Some(length) = shape(record) else {
            continue;
        };
        let due = (view.deadline)(record)
            .saturating_sub(view.settings.latency_ms)
            .saturating_add(reordering_grace(view.settings.latency_ms, view.rtt));
        if view
            .receiver
            .unknown_middle(record * MAX_BLOCKS_PER_RECORD as u64)
            && view.at < due
        {
            continue;
        }
        if view.settings.fragment_count(length) <= 1 {
            continue;
        }
        let index = (request.block_id % MAX_BLOCKS_PER_RECORD as u64) as usize;
        let data = view.settings.block_data(length, index);
        let parity = data as u16;
        if data > 1
            && data * view.settings.parity_shards / view.settings.data_shards > 0
            && !request.shard_indices.contains(&parity)
            && !(view.pending_shard)(request.block_id, parity)
            && view.pacing.eligible(request.block_id, parity, view.at)
        {
            request.shard_indices.push(parity);
            left -= 1;
        }
    }
    selected.sort_unstable_by_key(|entry| entry.request.block_id());
    selected
}
