//! Private bounded diagnostics. No URL, key, token or payload is retained.
use std::{
    collections::VecDeque,
    fs::OpenOptions,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};
mod checkpoint;
mod coverage;
pub use checkpoint::{Monitor as CheckpointMonitor, Trigger};

pub const MAX_EVENTS: usize = 131072;
pub const EVENT_SIZE: usize = 96;
pub mod stage {
    pub const QUEUE: u16 = 1;
    pub const BEGIN: u16 = 2;
    pub const FRAME: u16 = 3;
    pub const PACKET: u16 = 4;
    pub const SEGMENT: u16 = 5;
    pub const END: u16 = 6;
    pub const PENDING: u16 = 7;
    pub const WOULD_BLOCK: u16 = 8;
    pub const ACCEPTED: u16 = 9;
    pub const SOCKET_ERROR: u16 = 10;
    pub const INGRESS: u16 = 11;
    pub const RX_FRAME: u16 = 12;
    pub const RX_DROP: u16 = 13;
    pub const DEQUEUE: u16 = 14;
    pub const ADMIT: u16 = 15;
    pub const DISPATCH: u16 = 16;
    pub const SHAPER_SEEN: u16 = 17;
    pub const SHAPER_DROP: u16 = 18;
    pub const SHAPER_SENT: u16 = 19;
    pub const SHAPER_ERROR: u16 = 20;
}
#[derive(Clone, Debug)]
pub struct Event {
    pub stage: u16,
    pub key: [u8; 32],
    pub len: u32,
    pub conn: [u8; 16],
    pub numbers: [u64; 3],
    pub seq: u64,
    pub ns: u64,
}
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub events: Vec<Event>,
    pub overwritten: u64,
    pub busy: u64,
}
struct State {
    events: VecDeque<Event>,
    overwritten: u64,
    next: u64,
}
pub struct Recorder {
    capacity: usize,
    state: Mutex<State>,
    busy: AtomicU64,
    closed: AtomicBool,
    start: Instant,
    unix_ns: u64,
    coverage: coverage::Coverage,
    active: AtomicU64,
    frozen: Mutex<Option<u64>>,
    first_failure: OnceLock<Trigger>,
    failure_claimed: AtomicBool,
}
impl Recorder {
    pub fn new(capacity: usize) -> Self {
        assert!((1..=MAX_EVENTS).contains(&capacity));
        Self {
            capacity,
            coverage: coverage::Coverage::new(),
            active: AtomicU64::new(0),
            frozen: Mutex::new(None),
            first_failure: OnceLock::new(),
            failure_claimed: AtomicBool::new(false),
            state: Mutex::new(State {
                events: VecDeque::with_capacity(capacity),
                overwritten: 0,
                next: 1,
            }),
            busy: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            start: Instant::now(),
            unix_ns: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
        }
    }
    fn enter(&self) -> Option<Active<'_>> {
        if self.closed.load(Ordering::SeqCst) {
            return None;
        }
        self.active.fetch_add(1, Ordering::SeqCst);
        let guard = Active(&self.active);
        if self.closed.load(Ordering::SeqCst) {
            return None;
        }
        Some(guard)
    }
    pub fn record(&self, stage: u16, conn: [u8; 16], numbers: [u64; 3], bytes: &[u8]) {
        self.record_timed(stage, conn, numbers, bytes, || {
            self.start.elapsed().as_nanos() as u64
        });
    }
    fn record_timed(
        &self,
        stage: u16,
        conn: [u8; 16],
        numbers: [u64; 3],
        bytes: &[u8],
        clock: impl FnOnce() -> u64,
    ) {
        let Some(_active) = self.enter() else {
            return;
        };
        let ns = clock();
        let bin = self.coverage.attempt(ns);
        let Ok(mut s) = self.state.try_lock() else {
            self.busy.fetch_add(1, Ordering::Relaxed);
            self.coverage.note(bin, 2);
            return;
        };
        let key = digest(bytes);
        let seq = s.next;
        s.next += 1;
        if s.events.len() == self.capacity {
            if let Some(old) = s.events.pop_front() {
                self.coverage.overwritten(old.ns);
            }
            s.overwritten += 1;
        }
        s.events.push_back(Event {
            stage,
            key,
            len: bytes.len().try_into().expect("diagnostic payload length"),
            conn,
            numbers,
            seq,
            ns,
        });
        self.coverage.note(bin, 1);
    }
    pub fn freeze(&self) -> u64 {
        let mut frozen = self.frozen.lock().unwrap();
        if let Some(ns) = *frozen {
            return ns;
        }
        let ns = self.start.elapsed().as_nanos() as u64;
        self.closed.store(true, Ordering::SeqCst);
        while self.active.load(Ordering::SeqCst) != 0 {
            std::thread::yield_now();
        }
        *frozen = Some(ns);
        ns
    }
    pub fn failure(&self, subscription: u64, record: u64, reason: u64) {
        let Some(_active) = self.enter() else {
            return;
        };
        if !(1..=3).contains(&reason) || self.failure_claimed.swap(true, Ordering::SeqCst) {
            return;
        }
        let at_ns = self.start.elapsed().as_nanos() as u64;
        let id = tag(&[self.unix_ns.to_le_bytes(), at_ns.to_le_bytes()].concat())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let _ = self.first_failure.set(Trigger {
            id,
            subscription,
            record,
            reason,
            at_ns,
            origin: "local".into(),
        });
    }
    pub fn trigger(&self) -> Option<Trigger> {
        self.first_failure.get().cloned()
    }
    pub fn request(&self, mut trigger: Trigger) {
        let Some(_active) = self.enter() else {
            return;
        };
        if self.failure_claimed.swap(true, Ordering::SeqCst) {
            return;
        }
        trigger.origin = "external".into();
        trigger.at_ns = self.start.elapsed().as_nanos() as u64;
        let _ = self.first_failure.set(trigger);
    }
    pub fn snapshot(&self) -> Snapshot {
        let s = self.state.lock().unwrap();
        Snapshot {
            events: s.events.iter().cloned().collect(),
            overwritten: s.overwritten,
            busy: self.busy.load(Ordering::Relaxed),
        }
    }
    pub fn close_and_dump(&self, path: &Path) -> io::Result<()> {
        let stop_ns = self.freeze();
        let s = self.state.lock().unwrap();
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = io::BufWriter::new(options.open(path)?);
        file.write_all(b"FXTRACE1")?;
        file.write_all(&(EVENT_SIZE as u32).to_le_bytes())?;
        file.write_all(&1u32.to_le_bytes())?;
        for n in [
            self.capacity as u64,
            s.events.len() as u64,
            s.overwritten,
            self.busy.load(Ordering::Relaxed),
            self.unix_ns,
            stop_ns,
        ] {
            file.write_all(&n.to_le_bytes())?;
        }
        for e in &s.events {
            file.write_all(&e.seq.to_le_bytes())?;
            file.write_all(&e.ns.to_le_bytes())?;
            file.write_all(&e.stage.to_le_bytes())?;
            file.write_all(&0u16.to_le_bytes())?;
            file.write_all(&e.len.to_le_bytes())?;
            file.write_all(&e.conn)?;
            file.write_all(&e.key)?;
            for n in e.numbers {
                file.write_all(&n.to_le_bytes())?;
            }
        }
        file.flush()?;
        file.get_ref().sync_data()
    }
}
struct Active<'a>(&'a AtomicU64);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
pub fn digest(bytes: &[u8]) -> [u8; 32] {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .try_into()
        .unwrap()
}
pub fn tag(bytes: &[u8]) -> [u8; 16] {
    digest(bytes)[..16].try_into().unwrap()
}
static GLOBAL: OnceLock<Arc<Recorder>> = OnceLock::new();
pub fn install(recorder: Arc<Recorder>) -> bool {
    GLOBAL.set(recorder).is_ok()
}
pub fn enabled() -> bool {
    GLOBAL.get().is_some()
}
pub fn emit(stage: u16, conn: [u8; 16], numbers: [u64; 3], bytes: &[u8]) {
    if let Some(r) = GLOBAL.get() {
        r.record(stage, conn, numbers, bytes);
    }
}
pub fn failure(subscription: u64, record: u64, reason: u64) {
    if let Some(r) = GLOBAL.get() {
        r.failure(subscription, record, reason);
    }
}
pub fn udp(stage: u16, conn: [u8; 16], bytes: &[u8], segment: Option<usize>) {
    if !enabled() {
        return;
    }
    for (index, data) in bytes
        .chunks(segment.unwrap_or(bytes.len().max(1)).max(1))
        .enumerate()
    {
        emit(
            stage,
            conn,
            [(index * segment.unwrap_or(bytes.len())) as u64, 0, 0],
            data,
        );
    }
}
pub struct Scope([u8; 16]);
pub fn batch(conn: [u8; 16]) -> Scope {
    emit(stage::BEGIN, conn, [0; 3], &[]);
    Scope(conn)
}
impl Drop for Scope {
    fn drop(&mut self) {
        emit(stage::END, self.0, [0; 3], &[]);
    }
}
pub struct ExportGuard(Option<(Arc<Recorder>, PathBuf, Option<checkpoint::Monitor>)>);
impl ExportGuard {
    pub fn from_env() -> Self {
        let Some(path) = std::env::var_os("FLUSSONIX_PACKET_TRACE") else {
            return Self(None);
        };
        let r = Arc::new(Recorder::new(65536));
        if !install(r.clone()) {
            return Self(None);
        }
        let monitor = std::env::var_os("FLUSSONIX_PACKET_CHECKPOINT").and_then(|p| {
            match checkpoint::Monitor::start(r.clone(), p.into()) {
                Ok(m) => Some(m),
                Err(_) => {
                    eprintln!("private checkpoint monitor failed");
                    None
                }
            }
        });
        Self(Some((r, path.into(), monitor)))
    }
}
impl Drop for ExportGuard {
    fn drop(&mut self) {
        if let Some((r, path, monitor)) = &mut self.0 {
            if let Some(m) = monitor.take() {
                if m.stop().is_err() {
                    eprintln!("private checkpoint export failed");
                }
            }
            if r.close_and_dump(path).is_err() {
                eprintln!("private packet trace export failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_hash_and_numeric_fields_without_payload() {
        let r = Recorder::new(2);
        r.record(3, [9; 16], [7, 8, 9], b"secret-token-and-media");
        let s = r.snapshot();
        assert_eq!(s.events.len(), 1);
        assert_eq!(s.events[0].len, 22);
        assert_eq!(s.events[0].numbers, [7, 8, 9]);
        assert_eq!(
            s.events[0].key,
            ring::digest::digest(&ring::digest::SHA256, b"secret-token-and-media").as_ref()
        );
    }
    #[test]
    fn bounded_tail_marks_overwrite() {
        let r = Recorder::new(2);
        for i in 0..4 {
            r.record(1, [0; 16], [i, 0, 0], &[i as u8]);
        }
        let s = r.snapshot();
        assert_eq!(s.events.len(), 2);
        assert_eq!(s.overwritten, 2);
        assert_eq!(
            s.events.iter().map(|e| e.numbers[0]).collect::<Vec<_>>(),
            [2, 3]
        );
    }
    #[test]
    fn contention_drops_without_blocking() {
        let r = Arc::new(Recorder::new(1));
        let lock = r.state.lock().unwrap();
        let c = r.clone();
        std::thread::spawn(move || c.record(1, [0; 16], [0; 3], b"x"))
            .join()
            .unwrap();
        drop(lock);
        assert_eq!(r.snapshot().busy, 1);
    }
    #[test]
    #[should_panic]
    fn hard_memory_cap() {
        let _ = Recorder::new(131073);
    }
}

#[cfg(test)]
mod window_tests;
