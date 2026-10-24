//! Native GOP wrapping must not allocate several copies of an owned payload.
use bytes::Bytes;
use flussonix::m4s::{PackedGop, encode_gop};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};

#[derive(Clone, Copy, Debug, Default)]
struct Traffic {
    bytes: usize,
    largest: usize,
    payload_sized: usize,
    threshold: usize,
}
struct Allocations;
thread_local! { static TRAFFIC: Cell<Option<Traffic>> = const { Cell::new(None) }; }
fn allocated(size: usize) {
    let _ = TRAFFIC.try_with(|cell| {
        if let Some(mut current) = cell.get() {
            current.bytes += size;
            current.largest = current.largest.max(size);
            current.payload_sized += usize::from(size >= current.threshold);
            cell.set(Some(current));
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

fn gop(body: Bytes) -> PackedGop {
    PackedGop {
        utc: 0x01020304,
        dts_ms: 1.5,
        sequence: 0x11223344,
        duration_ms: 2000.0,
        body,
    }
}
fn measured(gop: &PackedGop) -> (Result<Bytes, String>, Traffic) {
    TRAFFIC.set(Some(Traffic {
        threshold: gop.body.len(),
        ..Default::default()
    }));
    let result = encode_gop(gop);
    let traffic = TRAFFIC.replace(None).unwrap();
    (result, traffic)
}

#[test]
fn native_gop_envelope_matches_literal_wire_contract() {
    let packet = encode_gop(&gop(Bytes::from_static(b"\xaa\xbb\xcc"))).unwrap();
    // 83-byte Fgop: 8-byte outer header, 64-byte goph, 11-byte body atom.
    let expected = b"\x00\x00\x00\x53\x00\x00\x00\x53Fgop\x00\x00\x00\x40goph\
        \x00\x00\x00\x0c utc\x01\x02\x03\x04\
        \x00\x00\x00\x10 dts\x3f\xf8\x00\x00\x00\x00\x00\x00\
        \x00\x00\x00\x0c num\x11\x22\x33\x44\
        \x00\x00\x00\x10 dur\x40\x9f\x40\x00\x00\x00\x00\x00\
        \x00\x00\x00\x0bbody\xaa\xbb\xcc";
    assert_eq!(packet.as_ref(), expected);
}

#[test]
fn native_gop_wrapping_allocates_only_one_payload_sized_buffer() {
    let input = gop(Bytes::from(vec![0xa5; 4 * 1024 * 1024]));
    let (packet, traffic) = measured(&input);
    let packet = packet.unwrap();
    assert_eq!(&packet[84..], input.body.as_ref());
    eprintln!("4MiB GOP wrapping allocation traffic: {traffic:?}");
    assert_eq!(traffic.payload_sized, 1, "duplicate full-GOP buffers");
    assert!(traffic.bytes <= input.body.len() + 4096, "{traffic:?}");
}

#[test]
fn native_gop_limit_includes_envelope_and_rejects_before_payload_copy() {
    // The transport's 16MiB bound excludes its 4-byte length prefix and
    // includes 80 bytes of Fgop/goph/field/body container overhead.
    let largest = gop(Bytes::from(vec![0x5a; 16 * 1024 * 1024 - 80]));
    let packet = encode_gop(&largest).unwrap();
    assert_eq!(packet.len(), 16 * 1024 * 1024 + 4);
    assert_eq!(&packet[84..], largest.body.as_ref());
    drop(packet);
    let oversized = gop(Bytes::from(vec![0x5a; 16 * 1024 * 1024 - 79]));
    let (result, traffic) = measured(&oversized);
    assert!(result.is_err());
    eprintln!("oversized GOP rejection allocation traffic: {traffic:?}");
    assert!(traffic.largest < 4096, "copied a rejected GOP: {traffic:?}");
}
