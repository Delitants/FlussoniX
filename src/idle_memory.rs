//! Best-effort unused-heap release after a fenced, sustained media-idle period.
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};
use tokio_util::sync::CancellationToken;

struct Policy {
    grace: Duration,
    cooldown: Duration,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            grace: Duration::from_secs(5),
            cooldown: Duration::from_secs(300),
        }
    }
}
#[derive(Default)]
struct State {
    active: usize,
    generation: u64,
    exhausted: bool,
    idle_since: Option<Instant>,
    pending: Option<u64>,
    released_generation: Option<u64>,
    last_release: Option<Instant>,
    runs: u64,
    last_duration_ms: Option<f64>,
    last_released_memory: Option<bool>,
}
impl State {
    fn eligible(&self, now: Instant, policy: &Policy) -> bool {
        self.active == 0
            && self.generation != 0
            && !self.exhausted
            && self.released_generation != Some(self.generation)
            && self
                .idle_since
                .is_some_and(|since| now.saturating_duration_since(since) >= policy.grace)
            && self
                .last_release
                .is_none_or(|last| now.saturating_duration_since(last) >= policy.cooldown)
    }
}
struct Inner {
    gate: Arc<RwLock<()>>,
    state: Mutex<State>,
    policy: Policy,
    running: AtomicBool,
}
#[derive(Clone)]
pub(crate) struct Controller {
    inner: Arc<Inner>,
}
impl Default for Controller {
    fn default() -> Self {
        // malloc_trim affects the process. All engines, including embedded
        // native/vendor engines, therefore share one activity fence and owner.
        static PROCESS: std::sync::OnceLock<Controller> = std::sync::OnceLock::new();
        PROCESS.get_or_init(|| Self::new(Policy::default())).clone()
    }
}
/// Counts preparation before awaiting the reader gate. Keep this guard through
/// all work and destruction it protects; nested readers never queue a writer.
pub(crate) struct Activity {
    inner: Arc<Inner>,
    _read: Option<OwnedRwLockReadGuard<()>>,
}
struct Ticket {
    inner: Arc<Inner>,
    generation: u64,
    cancel: CancellationToken,
}
struct Run {
    inner: Arc<Inner>,
    cancel: CancellationToken,
}
impl Controller {
    fn new(policy: Policy) -> Self {
        Self {
            inner: Arc::new(Inner {
                gate: Arc::new(RwLock::new(())),
                state: Mutex::new(State::default()),
                policy,
                running: AtomicBool::new(false),
            }),
        }
    }
    pub(crate) async fn activity(&self) -> Activity {
        {
            let mut state = self.inner.state.lock().unwrap();
            state.active += 1;
            if let Some(next) = state.generation.checked_add(1) {
                state.generation = next;
            } else {
                state.exhausted = true;
            }
            state.idle_since = None;
        }
        let mut activity = Activity {
            inner: self.inner.clone(),
            _read: None,
        };
        activity._read = Some(self.inner.gate.clone().read_owned().await);
        activity
    }
    fn reserve(&self, now: Instant, cancel: CancellationToken) -> Option<Ticket> {
        let mut state = self.inner.state.lock().unwrap();
        if cancel.is_cancelled()
            || state.pending.is_some()
            || !state.eligible(now, &self.inner.policy)
        {
            return None;
        }
        let generation = state.generation;
        state.pending = Some(generation);
        Some(Ticket {
            inner: self.inner.clone(),
            generation,
            cancel,
        })
    }
    pub(crate) fn snapshot(&self) -> Value {
        let supported = platform::supported();
        let now = Instant::now();
        // Copy scalar observations before allocating JSON. Admission registration
        // must never wait on allocation or diagnostics under this state lock.
        let (active, idle_since, runs, last_duration_ms, last_released_memory) = {
            let state = self.inner.state.lock().unwrap();
            (
                state.active,
                state.idle_since,
                state.runs,
                state.last_duration_ms,
                state.last_released_memory,
            )
        };
        json!({
            "supported": supported,
            "enabled": self.inner.running.load(Ordering::Acquire),
            "active_operations": active,
            "idle_grace_ms": self.inner.policy.grace.as_millis(),
            "cooldown_ms": self.inner.policy.cooldown.as_millis(),
            "idle_for_ms": idle_since.map(|since| now.saturating_duration_since(since).as_millis()),
            "runs": runs,
            "last_duration_ms": last_duration_ms,
            "last_released_memory": last_released_memory,
        })
    }
    pub(crate) async fn run(&self, cancel: CancellationToken) {
        if !platform::supported() || self.inner.running.swap(true, Ordering::AcqRel) {
            return;
        }
        let local = cancel.child_token();
        let _run = Run {
            inner: self.inner.clone(),
            cancel: local.clone(),
        };
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {biased;
                _ = local.cancelled() => break,
                _ = interval.tick() => {
                    if let Some(ticket) = self.reserve(Instant::now(), local.clone()) {
                        // Never acquire a writer before entering the blocking pool:
                        // queued housekeeping must not delay an incoming start.
                        if tokio::task::spawn_blocking(move || ticket.execute(Instant::now(), platform::release)).await.is_err() {
                            tracing::warn!("Idle heap cleanup task failed");
                        }
                    }
                }
            }
        }
    }
}
impl Ticket {
    fn execute(self, now: Instant, release: impl FnOnce() -> bool) -> bool {
        let Ok(writer) = self.inner.gate.try_write() else {
            return false;
        };
        {
            let mut state = self.inner.state.lock().unwrap();
            if self.cancel.is_cancelled()
                || state.pending != Some(self.generation)
                || state.generation != self.generation
                || !state.eligible(now, &self.inner.policy)
            {
                return false;
            }
            // Consume the attempt before the call, including zero-page releases
            // and unwinding. Activity may register now, but cannot prepare media
            // until the writer is dropped after this call completes.
            state.released_generation = Some(self.generation);
            state.last_release = Some(now);
            state.runs = state.runs.saturating_add(1);
        }
        let started = Instant::now();
        let released = release();
        let elapsed = started.elapsed();
        {
            let mut state = self.inner.state.lock().unwrap();
            state.last_release = Some(now + elapsed);
            state.last_duration_ms = Some(elapsed.as_secs_f64() * 1000.0);
            state.last_released_memory = Some(released);
        }
        drop(writer);
        tracing::debug!(
            released,
            duration_ms = elapsed.as_secs_f64() * 1000.0,
            "Idle heap cleanup completed"
        );
        true
    }
}
impl Drop for Activity {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        state.active -= 1;
        if state.active == 0 {
            state.idle_since = Some(Instant::now());
        }
    }
}
impl Drop for Ticket {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        if state.pending == Some(self.generation) {
            state.pending = None;
        }
    }
}
impl Drop for Run {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.inner.running.store(false, Ordering::Release);
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
mod platform {
    use std::{ffi::CStr, sync::OnceLock};
    fn origin(name: &CStr) -> Option<*mut libc::c_void> {
        // SAFETY: NUL-terminated symbol name and the process-global loader scope.
        let address = unsafe { libc::dlsym(libc::RTLD_DEFAULT, name.as_ptr()) };
        if address.is_null() {
            return None;
        }
        let mut info = std::mem::MaybeUninit::<libc::Dl_info>::uninit();
        // SAFETY: dladdr writes Dl_info only when it returns nonzero.
        if unsafe { libc::dladdr(address, info.as_mut_ptr()) } == 0 {
            return None;
        }
        // SAFETY: The successful dladdr call initialized this value.
        let base = unsafe { info.assume_init() }.dli_fbase;
        (!base.is_null()).then_some(base)
    }
    pub(super) fn supported() -> bool {
        static SUPPORTED: OnceLock<bool> = OnceLock::new();
        *SUPPORTED.get_or_init(|| {
            // The server uses Rust's System allocator. Conservatively disable
            // this glibc-specific action if allocator entry points are interposed
            // by another shared object, or symbol ownership cannot be established.
            let Some(glibc) = origin(c"gnu_get_libc_version") else {
                return false;
            };
            [
                c"malloc",
                c"calloc",
                c"realloc",
                c"free",
                c"posix_memalign",
                c"malloc_trim",
            ]
            .into_iter()
            .all(|name| origin(name) == Some(glibc))
        })
    }
    pub(super) fn release() -> bool {
        // SAFETY: GNU malloc_trim releases only unused allocator pages and is
        // thread-safe. Media activity is additionally fenced by the caller.
        // https://man7.org/linux/man-pages/man3/malloc_trim.3.html
        unsafe { libc::malloc_trim(0) != 0 }
    }
}
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
mod platform {
    pub(super) fn supported() -> bool {
        false
    }
    pub(super) fn release() -> bool {
        false
    }
}

#[cfg(test)]
mod lifecycle_tests;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;
