use super::*;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};

// Count only this thread's explicitly measured synchronous hot path. Other
// unit tests and background tasks cannot contribute to the measurement.
struct Allocations;
thread_local! {static COUNT:Cell<Option<usize>>=const {Cell::new(None)};}
fn allocated() {
    let _ = COUNT.try_with(|c| {
        if let Some(n) = c.get() {
            c.set(Some(n + 1));
        }
    });
}
unsafe impl GlobalAlloc for Allocations {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        allocated();
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        allocated();
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        allocated();
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: Allocations = Allocations;

#[tokio::test]
async fn native_rtsp_packet_permission_check_allocates_no_codec_description() {
    let dir = tempfile::tempdir().unwrap();
    let engine = crate::media::Engine::new(dir.path(), "ffmpeg");
    let worker = engine
        .ensure("owned", &json!({"inputs":[{"url":"testsrc://"}]}))
        .await
        .unwrap();
    worker.wire.rtp.configure(&[crate::m4s::Track {
        id: 1,
        codec: "mp3".into(),
        config: vec![],
    }]);
    let description = worker.wire.rtp.description().unwrap().unwrap();
    let playback = Playback {
        worker,
        authority: Authority::Native(Grant::peer()),
        description,
        name: "owned".into(),
        attached: false,
    };
    COUNT.set(Some(0));
    let mut current = true;
    for _ in 0..100 {
        current &= std::hint::black_box(playback.current());
    }
    let allocations = COUNT.replace(None).unwrap();
    // Changing codec metadata must still invalidate this exact generation.
    playback.worker.wire.rtp.configure(&[crate::m4s::Track {
        id: 2,
        codec: "m2a".into(),
        config: vec![],
    }]);
    let retired = !playback.current();
    engine.stop_all().await;
    assert!(current);
    assert!(
        retired,
        "codec generation change escaped the packet permission fence"
    );
    assert_eq!(
        allocations, 0,
        "per-packet permission checks copied codec metadata"
    );
}
