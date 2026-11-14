//! Detects GOP-sized transient allocations on an output with no RTP viewers.
use flussonix::{m4f::Frame, m4s::Track, rtp::Hub};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};
struct Allocations;
thread_local! { static LARGEST: Cell<Option<usize>> = const { Cell::new(None) }; }
fn allocated(size: usize) {
    let _ = LARGEST.try_with(|cell| {
        if let Some(previous) = cell.get() {
            cell.set(Some(previous.max(size)));
        }
    });
}
unsafe impl GlobalAlloc for Allocations {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        allocated(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        allocated(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        allocated(size);
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: Allocations = Allocations;
fn hub() -> Hub {
    let hub = Hub::new();
    hub.configure(&[Track {
        id: 7,
        codec: "h264".into(),
        config: vec![
            1, 100, 0, 31, 255, 225, 0, 4, 103, 100, 0, 31, 1, 0, 2, 104, 0,
        ],
    }]);
    hub
}
fn gop() -> Vec<Frame> {
    (0..128)
        .map(|i| {
            let mut body = 32768u32.to_be_bytes().to_vec();
            body.push(if i == 0 { 0x65 } else { 0x41 });
            body.extend(std::iter::repeat_n(i as u8, 32767));
            Frame {
                track_id: 7,
                dts: i * 3600,
                pts_offset: 0,
                key: i == 0,
                body,
            }
        })
        .collect()
}
fn measured(hub: &Hub, frames: &[Frame]) -> usize {
    LARGEST.set(Some(0));
    hub.frames(frames);
    LARGEST.replace(None).unwrap()
}
#[test]
fn unused_rtp_gop_does_not_allocate_a_second_contiguous_delivery_copy() {
    let hub = hub();
    let frames = gop();
    let largest = measured(&hub, &frames);
    let snapshot = hub.play_snapshot().unwrap();
    // Each 32768-byte NAL requires 28 FU-A packets, retained for a late join.
    assert_eq!(snapshot.packets.len(), 3584);
    assert_eq!(snapshot.decode_times[0], 0);
    assert_eq!(*snapshot.decode_times.last().unwrap(), 457200);
    assert!(
        largest < 512 * 1024,
        "unused RTP delivery allocated {largest} bytes at once"
    );
    drop(snapshot);
    let largest = measured(&hub, &frames);
    assert!(
        largest < 512 * 1024,
        "delivery copy returned after last RTP viewer left: {largest}"
    );
}

#[test]
fn oversized_rtp_producer_batch_is_rejected_with_or_without_receivers() {
    // 64 MiB of NAL data exceeds the producer budget after RTP envelopes.
    let one_gop = gop();
    let frames: Vec<_> = (0..16).flat_map(|_| one_gop.iter().cloned()).collect();
    for active in [false, true] {
        let hub = hub();
        let receiver = active.then(|| hub.subscribe().unwrap());
        hub.frames(&frames);
        assert!(hub.description().unwrap().is_err());
        assert!(hub.play_snapshot().is_err());
        drop(receiver);
    }
}

#[tokio::test]
async fn malformed_later_access_unit_invalidates_idle_and_active_profiles() {
    let mut frames = gop();
    frames[100].body = vec![0, 0, 255, 255, 0x65];
    for active in [false, true] {
        let hub = hub();
        let receiver = active.then(|| hub.subscribe().unwrap().2);
        hub.frames(&frames);
        assert!(hub.description().unwrap().is_err());
        assert!(hub.play_snapshot().is_err());
        if let Some(mut receiver) = receiver {
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(20), receiver.recv())
                    .await
                    .is_err(),
                "published part of an invalid GOP"
            );
        }
    }
}

#[test]
fn producer_budget_resets_between_independent_idle_gops() {
    let hub = hub();
    let frames = gop();
    for _ in 0..18 {
        hub.frames(&frames);
    }
    assert_eq!(hub.play_snapshot().unwrap().packets.len(), 3584);
}

#[tokio::test]
async fn concurrent_join_gets_complete_bootstrap_or_complete_live_gop() {
    use std::sync::{Arc, Barrier};
    let frames = Arc::new(gop());
    for _ in 0..8 {
        let hub = Arc::new(hub());
        let barrier = Arc::new(Barrier::new(2));
        let writer = {
            let hub = hub.clone();
            let barrier = barrier.clone();
            let frames = frames.clone();
            std::thread::spawn(move || {
                barrier.wait();
                hub.frames(&frames);
            })
        };
        barrier.wait();
        let (_, mut packets, mut receiver) = hub.subscribe().unwrap();
        writer.join().unwrap();
        assert!(
            packets.is_empty() || packets.len() == 3584,
            "partial bootstrap during publication"
        );
        while packets.len() < 3584 {
            packets.push(
                tokio::time::timeout(std::time::Duration::from_millis(500), receiver.recv())
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        for pair in packets.windows(2) {
            let before = u16::from_be_bytes(pair[0][6..8].try_into().unwrap());
            let after = u16::from_be_bytes(pair[1][6..8].try_into().unwrap());
            assert_eq!(after, before.wrapping_add(1));
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), receiver.recv())
                .await
                .is_err(),
            "duplicated snapshot packets in live output"
        );
    }
}
