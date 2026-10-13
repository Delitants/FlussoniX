//! One bounded owner; pending renewal futures are never detached from this loop.
use super::*;
use crate::source_directory::modern_coordinator::SourceHandle;
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::{collections::HashMap, future::Future, pin::Pin};
type Job = Pin<Box<dyn Future<Output = usize> + Send>>;
struct Attempt {
    // Keep identity allocated while its pointer is used as a scheduling key.
    _handle: SourceHandle,
    when: Instant,
}
struct Source {
    handle: SourceHandle,
    deadline: Instant,
}
impl SharedHlsSessions {
    fn sources(&self) -> HashMap<usize, Source> {
        self.prune(&mut self.state.lock().unwrap());
        let mut sources: HashMap<usize, Source> = HashMap::new();
        for (key, handle, deadline) in self.registry.renewal_sources().unwrap_or_default() {
            sources
                .entry(key)
                .and_modify(|s| s.deadline = s.deadline.min(deadline))
                .or_insert(Source { handle, deadline });
        }
        sources
    }
    pub(super) async fn maintain(&self, cancel: CancellationToken) {
        let mut tick = tokio::time::interval(self.options.maintenance_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut jobs = FuturesUnordered::<Job>::new();
        let mut active = HashMap::<usize, CancellationToken>::new();
        let mut attempted = HashMap::<usize, Attempt>::new();
        loop {
            tokio::select! {biased;
                _=cancel.cancelled()=>break,
                Some(key)=jobs.next(),if !jobs.is_empty()=>{active.remove(&key);},
                _=tick.tick()=>{},
                _=self.changed.notified()=>{},
            }
            if cancel.is_cancelled() {
                break;
            }
            let sources = self.sources();
            attempted.retain(|key, _| sources.contains_key(key));
            for (key, token) in &active {
                if !sources.contains_key(key) {
                    token.cancel();
                }
            }
            let now = Instant::now();
            let mut due = sources
                .into_iter()
                .filter(|(key, source)| {
                    !active.contains_key(key)
                        && now + self.options.renew_margin >= source.deadline
                        && attempted.get(key).is_none_or(|last| {
                            now.duration_since(last.when) >= self.options.retry_delay
                        })
                })
                .collect::<Vec<_>>();
            // Visit unattempted sources before retries, then earliest deadlines.
            due.sort_by_key(|(key, source)| {
                (
                    attempted.get(key).map(|attempt| attempt.when),
                    source.deadline,
                    *key,
                )
            });
            for (key, source) in due.into_iter().take(8usize.saturating_sub(active.len())) {
                let token = CancellationToken::new();
                active.insert(key, token.clone());
                attempted.insert(
                    key,
                    Attempt {
                        _handle: source.handle.clone(),
                        when: now,
                    },
                );
                let registry = self.registry.clone();
                let lifetime = self.options.lease_lifetime;
                jobs.push(Box::pin(async move{tokio::select!{biased;_=token.cancelled()=>{},_=registry.renew(&source.handle,lifetime)=>{}}key}));
            }
        }
        // Dropping all futures cancels direct HTTP I/O before RunGuard clears demand.
        drop(jobs);
    }
}
