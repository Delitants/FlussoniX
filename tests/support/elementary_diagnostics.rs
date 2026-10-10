//! Opt-in passive evidence for owned elementary-media fixtures, never used by the server.
use flussonix::media::Worker;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::Path,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
// Case paths are reserved before the fixture starts collecting any evidence.
pub fn create_case_directory(base: &Path, name: &str) -> io::Result<std::path::PathBuf> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::create_dir_all(base)?;
    let path = base.join(name);
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&path)
        .map_err(|error| {
            if error.kind() == io::ErrorKind::AlreadyExists {
                io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "elementary evidence case already exists; choose a new empty artifact directory",
                )
            } else {
                error
            }
        })?;
    Ok(path)
}
const READ_LIMIT: u64 = 1024 * 1024;
const FILE_LIMIT: u64 = 8 * 1024 * 1024;
// This lock protects only two small values. Never hold it across observations or I/O.
#[derive(Clone)]
struct Activity(Arc<Mutex<(&'static str, Instant)>>);
impl Activity {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(("starting", Instant::now()))))
    }
    fn set(&self, operation: &'static str) {
        *self.0.lock().unwrap() = (operation, Instant::now());
    }
    fn snapshot(&self) -> (&'static str, u128) {
        let (operation, started) = *self.0.lock().unwrap();
        (operation, started.elapsed().as_millis())
    }
}
struct Target {
    pid: u32,
    start_ticks: Option<u64>,
}
struct State {
    activity: Activity,
    file: File,
    bytes: u64,
    limit: u64,
    capped: bool,
    retention_failed: bool,
    packet_taps: Vec<Value>,
    previous_write_ms: u128,
    start: Instant,
    stage: &'static str,
    targets: BTreeMap<&'static str, Target>,
    worker: Option<Weak<Worker>>,
    dropped: Arc<AtomicU64>,
    shutdown_timed_out: Arc<AtomicBool>,
}
impl State {
    fn record(&mut self, event: &str, emitted: Option<Instant>) {
        let terminal = matches!(event, "finished" | "unwinding");
        if self.capped && !terminal {
            return;
        }
        if terminal {
            self.activity.set("write_terminal");
            let stage: String = self
                .stage
                .chars()
                .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
                .take(64)
                .collect();
            let value = json!({"event":event,"stage":stage,"elapsed_ms":self.start.elapsed().as_millis(),
                "incomplete":self.capped || self.retention_failed || self.shutdown_timed_out.load(Ordering::Relaxed) || self.dropped.load(Ordering::Relaxed)>0,
                "shutdown_timed_out":self.shutdown_timed_out.load(Ordering::Relaxed),"dropped_commands":self.dropped.load(Ordering::Relaxed)});
            let mut bytes = serde_json::to_vec(&value).unwrap();
            bytes.push(b'\n');
            if self.bytes + bytes.len() as u64 <= self.limit {
                if self
                    .file
                    .write_all(&bytes)
                    .and_then(|_| self.file.flush())
                    .is_err()
                {
                    eprintln!("Owned media terminal evidence write failed");
                }
                self.bytes += bytes.len() as u64;
            }
            return;
        }
        let observation_start = Instant::now();
        self.activity.set("observe_processes");
        let mut value = json!({"elapsed_ms":self.start.elapsed().as_millis(),"event":event,"stage":self.stage,"processes":observations(&self.targets)});
        value["packet_taps"] = json!(self.packet_taps);
        if let Some(worker) = self.worker.as_ref().and_then(Weak::upgrade) {
            self.activity.set("observe_worker");
            value["worker"] = worker.stats();
        }
        self.activity.set("observe_host_pressure");
        value["host_io_pressure"] = bounded(Path::new("/proc/pressure/io"))
            .ok()
            .map_or(Value::Null, Value::String);
        value["event_queue_delay_ms"] = emitted
            .map(|t| t.elapsed().as_millis())
            .map_or(Value::Null, |n| json!(n));
        value["shutdown_timed_out"] = json!(self.shutdown_timed_out.load(Ordering::Relaxed));
        value["dropped_commands"] = json!(self.dropped.load(Ordering::Relaxed));
        value["observation_ms"] = json!(observation_start.elapsed().as_millis());
        value["previous_evidence_write_ms"] = json!(self.previous_write_ms);
        self.activity.set("serialize_observation");
        let Ok(mut bytes) = serde_json::to_vec(&value) else {
            return;
        };
        bytes.push(b'\n');
        if self.bytes + bytes.len() as u64 > self.limit - 512 {
            self.capped = true;
            bytes = b"{\"event\":\"evidence_capped\",\"observation\":\"incomplete\"}\n".to_vec();
        }
        self.activity.set("write_observation");
        let write_start = Instant::now();
        if self
            .file
            .write_all(&bytes)
            .and_then(|_| self.file.flush())
            .is_err()
        {
            eprintln!("Owned media boundary evidence write failed");
            self.capped = true;
        }
        self.previous_write_ms = write_start.elapsed().as_millis();
        self.bytes += bytes.len() as u64;
    }
}
enum Event {
    Process(&'static str, Target),
    Worker(Target, Weak<Worker>),
    Stage(&'static str),
    Captures(Vec<DatagramCapture>),
    PacketTap(Value, Option<DatagramCapture>),
}
pub struct Evidence {
    activity: Activity,
    completion: mpsc::Receiver<()>,
    shutdown_timed_out: Arc<AtomicBool>,
    sender: Option<mpsc::SyncSender<(Instant, Event)>>,
    thread: Option<JoinHandle<()>>,
    final_stage: Arc<Mutex<&'static str>>,
    unwinding: Arc<AtomicBool>,
    dropped: Arc<AtomicU64>,
}
impl Evidence {
    pub fn start(source: &Path, artifact: &Path) -> io::Result<Self> {
        Self::start_limited(source, artifact, FILE_LIMIT)
    }
    pub fn start_limited(source: &Path, artifact: &Path, limit: u64) -> io::Result<Self> {
        if !(1024..=FILE_LIMIT).contains(&limit) {
            return Err(io::Error::other("invalid evidence limit"));
        }
        std::fs::create_dir_all(artifact)?;
        std::fs::set_permissions(artifact, std::fs::Permissions::from_mode(0o700))?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(artifact.join("boundary-evidence.jsonl"))?;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        let dropped = Arc::new(AtomicU64::new(0));
        let shutdown_timed_out = Arc::new(AtomicBool::new(false));
        let activity = Activity::new();
        let mut state = State {
            activity: activity.clone(),
            file,
            bytes: 0,
            limit,
            capped: false,
            retention_failed: false,
            packet_taps: Vec::new(),
            previous_write_ms: 0,
            start: Instant::now(),
            stage: "fixture_start",
            targets: BTreeMap::new(),
            worker: None,
            dropped: dropped.clone(),
            shutdown_timed_out: shutdown_timed_out.clone(),
        };
        // This initial write happens before any media process or socket starts.
        state.record("started", None);
        let (sender, receiver) = mpsc::sync_channel(32);
        let final_stage = Arc::new(Mutex::new("fixture_start"));
        let terminal_stage = final_stage.clone();
        let unwinding = Arc::new(AtomicBool::new(false));
        let terminal_unwind = unwinding.clone();
        let source = source.to_path_buf();
        let artifact = artifact.to_path_buf();
        let (finished, completion) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let deadline = state.start + Duration::from_secs(60);
            let mut next_sample = Instant::now() + Duration::from_millis(100);
            let mut sampling = true;
            loop {
                state.activity.set("wait_for_event");
                let event = if sampling {
                    receiver.recv_timeout(next_sample.saturating_duration_since(Instant::now()))
                } else {
                    receiver
                        .recv()
                        .map_err(|_| mpsc::RecvTimeoutError::Disconnected)
                };
                match event {
                    Ok((emitted, event)) => {
                        state.activity.set("handle_event");
                        let label = match event {
                            Event::Process(role, target) => {
                                state.targets.insert(role, target);
                                "process_registered"
                            }
                            Event::Worker(target, worker) => {
                                state.targets.insert("worker", target);
                                state.worker = Some(worker);
                                "process_registered"
                            }
                            Event::Captures(captures) => {
                                for capture in captures {
                                    state.activity.set("save_datagram_capture");
                                    if capture.save().is_err() {
                                        state.retention_failed = true;
                                        eprintln!("Owned datagram capture retention failed");
                                    }
                                }
                                "captures_saved"
                            }
                            Event::PacketTap(summary, capture) => {
                                state.retention_failed |=
                                    summary["incomplete"].as_bool() != Some(false);
                                if let Some(capture) = capture {
                                    state.activity.set("save_packet_tap");
                                    if capture.save().is_err() {
                                        state.retention_failed = true;
                                        eprintln!("Owned packet tap retention failed");
                                    }
                                }
                                state.packet_taps.push(summary);
                                "packet_tap_saved"
                            }
                            Event::Stage(stage) => {
                                state.stage = stage;
                                "stage"
                            }
                        };
                        state.record(label, Some(emitted));
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        state.retention_failed |= preserve(&source, &artifact, &state.activity);
                        state.stage = *terminal_stage.lock().unwrap();
                        state.record(
                            if terminal_unwind.load(Ordering::Relaxed) {
                                "unwinding"
                            } else {
                                "finished"
                            },
                            None,
                        );
                        state.activity.set("finished");
                        let _ = finished.send(());
                        return;
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
                let now = Instant::now();
                if sampling && now >= deadline {
                    sampling = false;
                    state.record("sampling_deadline", None);
                } else if sampling && now >= next_sample {
                    state.record("sample", None);
                    next_sample = Instant::now() + Duration::from_millis(100);
                }
            }
        });
        Ok(Self {
            activity,
            completion,
            shutdown_timed_out,
            sender: Some(sender),
            thread: Some(thread),
            final_stage,
            unwinding,
            dropped,
        })
    }
    fn notify(&self, event: Event) {
        if self
            .sender
            .as_ref()
            .unwrap()
            .try_send((Instant::now(), event))
            .is_err()
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn watch_process(&self, role: &'static str, pid: u32) {
        let start_ticks = identity(pid).ok().map(|p| p.0);
        self.notify(Event::Process(role, Target { pid, start_ticks }));
    }
    #[allow(
        dead_code,
        reason = "collector contract tests do not start a media worker"
    )]
    pub fn watch_worker(&self, worker: &Arc<Worker>) {
        let pid = worker.pid();
        let start_ticks = identity(pid).ok().map(|p| p.0);
        self.notify(Event::Worker(
            Target { pid, start_ticks },
            Arc::downgrade(worker),
        ));
    }
    #[allow(
        dead_code,
        reason = "packet taps are optional Linux fixture diagnostics"
    )]
    pub fn save_packet_tap(&self, summary: Value, capture: Option<DatagramCapture>) {
        self.notify(Event::PacketTap(summary, capture));
    }
    pub fn save_captures(&self, captures: Vec<DatagramCapture>) {
        self.notify(Event::Captures(captures));
    }
    pub fn stage(&self, stage: &'static str) {
        // Never hold this tiny metadata lock during proc reads or file writes.
        *self.final_stage.lock().unwrap() = stage;
        self.notify(Event::Stage(stage));
    }
}
impl Drop for Evidence {
    fn drop(&mut self) {
        self.unwinding
            .store(std::thread::panicking(), Ordering::Relaxed);
        self.sender.take();
        let failure = match self.completion.recv_timeout(Duration::from_secs(2)) {
            Ok(()) => self.thread.take().unwrap().join().is_err(),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let _ = self.thread.take().unwrap().join();
                true
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.shutdown_timed_out.store(true, Ordering::Relaxed);
                // Blocking filesystem I/O cannot safely be interrupted. Detach this
                // test-only observer; it owns no media child and no strong worker reference.
                self.thread.take();
                true
            }
        };
        if failure {
            let stage = *self.final_stage.lock().unwrap();
            let (operation, elapsed) = self.activity.snapshot();
            let message = format!(
                "Owned media evidence incomplete: observer did not finish; stage={stage}; operation={operation}; operation_elapsed_ms={elapsed}; shutdown_timed_out={}",
                self.shutdown_timed_out.load(Ordering::Relaxed)
            );
            eprintln!("{message}");
            if !std::thread::panicking() {
                panic!("{message}");
            }
        }
    }
}
fn bounded(path: &Path) -> io::Result<String> {
    let mut data = Vec::new();
    File::open(path)?
        .take(READ_LIMIT + 1)
        .read_to_end(&mut data)?;
    if data.len() as u64 > READ_LIMIT {
        return Err(io::Error::other("observation exceeds limit"));
    }
    String::from_utf8(data).map_err(io::Error::other)
}
pub(super) fn identity(pid: u32) -> io::Result<(u64, String)> {
    let stat = bounded(Path::new(&format!("/proc/{pid}/stat")))?;
    let (_, rest) = stat
        .rsplit_once(')')
        .ok_or_else(|| io::Error::other("invalid process stat"))?;
    let fields: Vec<_> = rest.split_ascii_whitespace().collect();
    let start = fields
        .get(19)
        .ok_or_else(|| io::Error::other("missing process identity"))?
        .parse()
        .map_err(io::Error::other)?;
    let state = fields
        .first()
        .ok_or_else(|| io::Error::other("missing process state"))?
        .to_string();
    Ok((start, state))
}
fn observations(targets: &BTreeMap<&'static str, Target>) -> Vec<Value> {
    targets
        .iter()
        .map(|(role, target)| {
            let mut value = json!({"role":role,"pid":target.pid,"start_ticks":target.start_ticks});
            match observe(target) {
                Ok((state, rows, races)) => {
                    value["observation"] = json!(if races == 0 { "available" } else { "partial" });
                    value["process_state"] = json!(state);
                    value["descriptor_races"] = json!(races);
                    value["udp"] = json!(rows);
                }
                Err(_) => {
                    value["observation"] = json!("unavailable");
                }
            }
            value
        })
        .collect()
}
fn observe(target: &Target) -> io::Result<(String, Vec<Value>, usize)> {
    let expected = target
        .start_ticks
        .ok_or_else(|| io::Error::other("process identity unavailable"))?;
    let (start, state) = identity(target.pid)?;
    if start != expected {
        return Err(io::Error::other("process identity changed"));
    }
    let mut inodes = std::collections::HashSet::new();
    let mut races = 0;
    for (n, entry) in std::fs::read_dir(format!("/proc/{}/fd", target.pid))?.enumerate() {
        if n >= 4096 {
            return Err(io::Error::other("descriptor limit exceeded"));
        }
        let link = match std::fs::read_link(entry?.path()) {
            Ok(link) => link,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                races += 1;
                continue;
            }
            Err(e) => return Err(e),
        };
        if let Some(inode) = link
            .to_str()
            .and_then(|s| s.strip_prefix("socket:["))
            .and_then(|s| s.strip_suffix(']'))
            .and_then(|s| s.parse::<u64>().ok())
        {
            inodes.insert(inode);
        }
    }
    let table = bounded(Path::new(&format!("/proc/{}/net/udp", target.pid)))?;
    let mut rows = Vec::new();
    for line in table.lines().skip(1) {
        let fields: Vec<_> = line.split_ascii_whitespace().collect();
        let Some(inode) = fields.get(9).and_then(|s| s.parse::<u64>().ok()) else {
            continue;
        };
        if !inodes.contains(&inode) {
            continue;
        }
        if rows.len() >= 256 {
            return Err(io::Error::other("socket limit exceeded"));
        }
        let invalid = || io::Error::other("invalid owned UDP row");
        let (local_ip, local_port) = fields
            .get(1)
            .and_then(|s| s.split_once(':'))
            .ok_or_else(invalid)?;
        let (remote_ip, remote_port) = fields
            .get(2)
            .and_then(|s| s.split_once(':'))
            .ok_or_else(invalid)?;
        let (tx, rx) = fields
            .get(4)
            .and_then(|s| s.split_once(':'))
            .ok_or_else(invalid)?;
        let port = |s| u16::from_str_radix(s, 16).map_err(|_| invalid());
        let queue = |s| u64::from_str_radix(s, 16).map_err(|_| invalid());
        let drops = fields
            .get(12)
            .ok_or_else(invalid)?
            .parse::<u64>()
            .map_err(|_| invalid())?;
        rows.push(
            json!({"inode":inode,"local_ip_hex":local_ip,"local_port":port(local_port)?,
            "remote_ip_hex":remote_ip,"remote_port":port(remote_port)?,
            "tx_queue_bytes":queue(tx)?,"rx_queue_bytes":queue(rx)?,"drops":drops}),
        );
    }
    if identity(target.pid)?.0 != expected {
        return Err(io::Error::other("process identity changed"));
    }
    Ok((state, rows, races))
}
fn preserve(source: &Path, artifact: &Path, activity: &Activity) -> bool {
    let mut failed = false;
    // The allowlist excludes inline keys, wrapper arguments and any host configuration.
    for (name, operation) in [
        ("input.sdp", "retain_input.sdp"),
        ("output.sdp", "retain_output.sdp"),
        ("receiver.sdp", "retain_receiver.sdp"),
        ("sender.log", "retain_sender.log"),
        ("receiver.log", "retain_receiver.log"),
        ("worker.log", "retain_worker.log"),
        ("received.ts", "retain_received.ts"),
        ("worker.ts", "retain_worker.ts"),
    ] {
        activity.set(operation);
        let from = source.join(name);
        if !from.exists() {
            continue;
        }
        let save = || -> io::Result<()> {
            let limit = if name.ends_with(".ts") {
                64 * READ_LIMIT
            } else {
                READ_LIMIT
            };
            let input = File::open(from)?;
            if input.metadata()?.len() > limit {
                return Err(io::Error::other("artifact exceeds retention limit"));
            }
            let sanitized = if name.ends_with(".sdp") {
                let mut text = String::new();
                (&input).take(limit).read_to_string(&mut text)?;
                let mut sanitized = Vec::new();
                for line in text.lines().filter(|line| !line.starts_with("a=crypto:")) {
                    writeln!(sanitized, "{line}\r")?;
                }
                if sanitized.len() as u64 > limit {
                    return Err(io::Error::other(
                        "sanitized artifact exceeds retention limit",
                    ));
                }
                Some(sanitized)
            } else {
                None
            };
            let mut output = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .mode(0o600)
                .open(artifact.join(name))?;
            output.set_permissions(std::fs::Permissions::from_mode(0o600))?;
            if let Some(sanitized) = sanitized {
                output.write_all(&sanitized)?;
            } else {
                io::copy(&mut input.take(limit), &mut output)?;
            }
            Ok(())
        };
        if save().is_err() {
            failed = true;
            eprintln!("Owned media artifact retention failed: {name}");
        }
    }
    failed
}
// Owned datagrams are buffered during delivery and saved only after media shutdown.
pub struct DatagramCapture {
    path: std::path::PathBuf,
    bytes: Vec<u8>,
}
impl DatagramCapture {
    pub fn new(path: std::path::PathBuf) -> Self {
        Self {
            path,
            bytes: Vec::new(),
        }
    }
    pub fn append(&mut self, packet: &[u8]) -> io::Result<()> {
        let size = self
            .bytes
            .len()
            .checked_add(4)
            .and_then(|n| n.checked_add(packet.len()));
        if size.is_none_or(|n| n > 4 * 1024 * 1024) {
            return Err(io::Error::other("owned datagram capture exceeds limit"));
        }
        self.bytes
            .extend_from_slice(&(packet.len() as u32).to_be_bytes());
        self.bytes.extend_from_slice(packet);
        Ok(())
    }
    pub fn save(self) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(self.path)?;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        file.write_all(&self.bytes)
    }
}
