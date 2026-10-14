//! Four owned catalog jobs; complete inventories only, no I/O under publication locks.
use super::*;
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::{future::Future, pin::Pin};
impl Runtime {
    async fn poll_static(&self, app: &App, key: &str, revision: u64) {
        let result = tokio::time::timeout(
            Duration::from_secs(4),
            self.replica_inventory(app, key, revision, true),
        )
        .await;
        let rows = match result {
            Ok(Ok((_, Ok(rows)))) => rows,
            _ => {
                app.config.at_revision(revision, |_| {
                    if let Some(c) = self.activation.lock().unwrap().catalogs.get_mut(key) {
                        c.error = Some("unavailable");
                    }
                });
                return;
            }
        };
        let names = rows
            .names()
            .filter_map(|name| {
                let candidate = rows.candidate(name).ok()?;
                self.owner
                    .publish(&candidate, |v, _| {
                        v.selected_static.then(|| name.to_owned())
                    })
                    .ok()
                    .flatten()
            })
            .collect::<BTreeSet<_>>();
        // The whole inventory, including zero matches, must still be current.
        let _ = self.owner.at_inventory_current(&rows, || {
            let mut state = self.activation.lock().unwrap();
            if state.revision != revision {
                return;
            }
            let used = state
                .catalogs
                .iter()
                .filter(|(k, _)| k.as_str() != key)
                .map(|(_, c)| c.names.len())
                .sum::<usize>();
            let Some(c) = state.catalogs.get_mut(key) else {
                return;
            };
            if used.saturating_add(names.len()) > 10000 {
                c.names.clear();
                c.expires = None;
                c.error = Some("capacity");
                return;
            }
            c.names = names;
            c.expires = Some(rows.expires_at());
            c.error = None;
        });
    }
    pub(in crate::vendor_playback) async fn poll_static_catalogs(
        &self,
        app: &App,
        cancel: CancellationToken,
    ) {
        type Job<'a> = Pin<Box<dyn Future<Output = String> + Send + 'a>>;
        let mut jobs = FuturesUnordered::<Job<'_>>::new();
        let mut active = HashMap::<String, (u64, CancellationToken)>::new();
        let mut tick = tokio::time::interval(Duration::from_millis(250));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {biased;_=cancel.cancelled()=>break,Some(key)=jobs.next(),if !jobs.is_empty()=>{active.remove(&key);},_=tick.tick()=>{}}
            if cancel.is_cancelled() {
                break;
            }
            let (root, revision) = app.config.read(|r| (r.clone(), app.config.revision()));
            let keys = root["sources"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|s| {
                    app.options.role != "lb"
                        && s["flussonix_source_kind"] == "flussonic"
                        && !s["only"].as_array().is_some_and(Vec::is_empty)
                })
                .filter_map(|s| crate::config::item_key("sources", s).map(str::to_owned))
                .collect::<BTreeSet<_>>();
            let due = app
                .config
                .at_revision(revision, |_| {
                    let mut state = self.activation.lock().unwrap();
                    if state.revision != revision {
                        state.catalogs.clear();
                        state.demands.clear();
                        state.revision = revision;
                        state.cursor = None;
                    }
                    state.catalogs.retain(|k, _| keys.contains(k));
                    for key in &keys {
                        state.catalogs.entry(key.clone()).or_insert(Catalog {
                            names: BTreeSet::new(),
                            expires: None,
                            next_poll: Instant::now(),
                            error: None,
                        });
                    }
                    for (key, (rev, token)) in &active {
                        if *rev != revision || !keys.contains(key) {
                            token.cancel();
                        }
                    }
                    let due = state
                        .catalogs
                        .iter()
                        .filter(|(k, c)| !active.contains_key(*k) && c.next_poll <= Instant::now())
                        .map(|(k, c)| (k.clone(), c.next_poll))
                        .collect::<Vec<_>>();
                    let mut due = due;
                    due.sort_by_key(|(key, t)| (*t, key.clone()));
                    due.into_iter()
                        .take(4usize.saturating_sub(active.len()))
                        .map(|(key, _)| {
                            state.catalogs.get_mut(&key).unwrap().next_poll =
                                Instant::now() + Duration::from_secs(10);
                            key
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            for key in due {
                let token = cancel.child_token();
                active.insert(key.clone(), (revision, token.clone()));
                jobs.push(Box::pin(async move{tokio::select!{biased;_=token.cancelled()=>{},_=self.poll_static(app,&key,revision)=>{}}key}));
            }
        }
        drop(jobs);
    }
}
