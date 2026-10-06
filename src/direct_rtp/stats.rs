use serde_json::{Value, json};
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};
#[derive(Default)]
pub struct Stats {
    pub profile: Mutex<&'static str>,
    pub packets: AtomicU64,
    pub bytes: AtomicU64,
    pub invalid: AtomicU64,
    pub foreign: AtomicU64,
    pub auth_failed: AtomicU64,
    pub duplicates: AtomicU64,
    pub lost: AtomicU64,
    pub rtcp: AtomicU64,
    pub unreachable: AtomicU64,
    pub status: Mutex<&'static str>,
    pub error: Mutex<Option<&'static str>>,
}
impl Stats {
    pub fn snapshot(&self) -> Value {
        let get = |n: &AtomicU64| n.load(Ordering::Relaxed);
        let profile = *self.profile.lock().unwrap();
        json!({"profile":if profile.is_empty(){"MP2T"}else{profile},"packets":get(&self.packets),"bytes":get(&self.bytes),"invalid_packets":get(&self.invalid),"foreign_packets":get(&self.foreign),"auth_failures":get(&self.auth_failed),"duplicates":get(&self.duplicates),"lost":get(&self.lost),"rtcp_packets":get(&self.rtcp),"unreachable_packets":get(&self.unreachable),"status":*self.status.lock().unwrap(),"last_error":*self.error.lock().unwrap()})
    }
}
