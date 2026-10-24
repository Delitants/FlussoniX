//! First failure metadata and immutable, private checkpoint export.
use super::*;
use serde_json::{json, Value};
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Trigger {
    pub id: String,
    pub subscription: u64,
    pub record: u64,
    pub reason: u64,
    pub at_ns: u64,
    pub origin: String,
}
impl Trigger {
    pub fn value(&self) -> Value {
        json!({"id":self.id,"subscription":self.subscription,"record":self.record,"reason":self.reason,"at_ns":self.at_ns,"origin":self.origin})
    }
    pub fn parse(v: &Value) -> Option<Self> {
        let m = v.as_object()?;
        if m.len() != 6 {
            return None;
        }
        let id = v["id"].as_str()?;
        let origin = v["origin"].as_str()?;
        let reason = v["reason"].as_u64()?;
        if id.len() != 32
            || !id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || !["local", "external"].contains(&origin)
            || !(1..=3).contains(&reason)
        {
            return None;
        }
        Some(Self {
            id: id.into(),
            subscription: v["subscription"].as_u64()?,
            record: v["record"].as_u64()?,
            reason,
            at_ns: v["at_ns"].as_u64()?,
            origin: origin.into(),
        })
    }
}
fn suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut p = path.as_os_str().to_os_string();
    p.push(suffix);
    p.into()
}
pub fn write_private(path: &Path, value: &Value) -> io::Result<()> {
    let temporary = suffix(path, ".writing");
    let mut o = OpenOptions::new();
    o.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        o.mode(0o600);
    }
    let file = o.open(&temporary)?;
    let result = (|| {
        let mut f = io::BufWriter::new(file);
        serde_json::to_writer(&mut f, value)?;
        f.write_all(b"\n")?;
        f.flush()?;
        f.get_ref().sync_data()?;
        std::fs::hard_link(&temporary, path)
    })();
    let cleanup = std::fs::remove_file(&temporary);
    result.and(cleanup)
}
fn request(path: &Path) -> io::Result<Option<Trigger>> {
    use std::io::Read;
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut data = Vec::new();
    file.take(4097).read_to_end(&mut data)?;
    if data.len() > 4096 || !data.ends_with(b"\n") {
        return Err(io::Error::other("invalid checkpoint request framing"));
    }
    let value: Value = serde_json::from_slice(&data)?;
    Trigger::parse(&value)
        .map(Some)
        .ok_or_else(|| io::Error::other("invalid checkpoint request"))
}
pub struct Monitor {
    recorder: Arc<Recorder>,
    path: PathBuf,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<io::Result<()>>>,
}
impl Monitor {
    pub fn start(recorder: Arc<Recorder>, path: PathBuf) -> io::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let done = stop.clone();
        let held = recorder.clone();
        let owned_path = path.clone();
        let thread = std::thread::Builder::new()
            .name("private-checkpoint".into())
            .spawn(move || loop {
                if recorder.trigger().is_none() {
                    if let Some(trigger) = request(&suffix(&path, ".request"))? {
                        recorder.request(trigger);
                    }
                }
                if let Some(trigger) = recorder.trigger() {
                    write_private(&suffix(&path, ".trigger"), &trigger.value())?;
                    let until = Instant::now() + std::time::Duration::from_millis(200);
                    while !done.load(Ordering::Acquire) && Instant::now() < until {
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    return recorder.checkpoint(&path);
                }
                if may_stop(&done, &recorder) {
                    return Ok(());
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            })?;
        Ok(Self {
            recorder: held,
            path: owned_path,
            stop,
            thread: Some(thread),
        })
    }
    fn finish(&mut self) -> io::Result<()> {
        if self.thread.is_none() {
            return Ok(());
        }
        let pending = if self.recorder.trigger().is_none() {
            match request(&suffix(&self.path, ".request")) {
                Ok(Some(t)) => {
                    self.recorder.request(t);
                    Ok(())
                }
                Ok(None) => Ok(()),
                Err(e) => Err(e),
            }
        } else {
            Ok(())
        };
        // Quiesce admitted failure producers before allowing the watcher to stop.
        self.recorder.freeze();
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| io::Error::other("private checkpoint worker panicked"))??;
        }
        pending
    }
    pub fn stop(mut self) -> io::Result<()> {
        self.finish()
    }
}
impl Drop for Monitor {
    fn drop(&mut self) {
        if self.finish().is_err() {
            eprintln!("private checkpoint export failed");
        }
    }
}

impl Recorder {
    pub fn checkpoint(&self, path: &Path) -> io::Result<()> {
        self.freeze();
        let trigger = self
            .trigger()
            .ok_or_else(|| io::Error::other("checkpoint has no failure identity"))?;
        self.close_and_dump(path)?;
        let bytes = std::fs::read(path)?;
        let sha: String = digest(&bytes).iter().map(|b| format!("{b:02x}")).collect();
        let metadata = json!({"version":1,"bucket_ns":coverage::BUCKET_NS,"bucket_count":coverage::BUCKET_COUNT,"bins":self.coverage.snapshot(),"out_of_range":self.coverage.out_of_range.load(Ordering::Relaxed),"stop_ns":self.freeze(),"start_unix_ns":self.unix_ns,"trigger":trigger.value(),"trace_sha256":sha});
        let mut sidecar = path.as_os_str().to_os_string();
        sidecar.push(".coverage.json");
        write_private(Path::new(&sidecar), &metadata)
    }
}

pub(super) fn may_stop(done: &AtomicBool, recorder: &Recorder) -> bool {
    // Acquire the published stop first: finish has quiesced all admitted
    // producers before releasing it. Recheck after that synchronization.
    done.load(Ordering::Acquire) && recorder.trigger().is_none()
}
