//! Sticky source selection and publication fencing. Lives beneath server to keep authority private.
use super::*;
use crate::source_directory::{LookupFailure, query};
use futures_util::{StreamExt, stream};
enum OriginState {
    Ready,
    Unavailable,
    Denied,
    Unresolved,
}
#[derive(Clone)]
pub(super) struct Ticket {
    id: uuid::Uuid,
    done: tokio::sync::watch::Sender<bool>,
}
struct Lookup {
    old: Option<Mirror>,
    ticket: Ticket,
    deadline: tokio::time::Instant,
}
impl Drop for Lookup {
    fn drop(&mut self) {
        self.ticket.done.send_replace(true);
    }
}
impl App {
    async fn begin_lookup(&self, name: &str, root: &Value, revision: u64) -> Option<Lookup> {
        let mut tickets = self.source_lookups.lock().await;
        tickets.retain(|_, t| !*t.done.borrow());
        if tickets.len() >= 10000 && !tickets.contains_key(name) {
            return None;
        }
        let mut mirrors = self.mirrors.lock().await;
        mirrors.retain(|_, m| m.known || m.when.elapsed() < Duration::from_secs(1));
        self.config.at_revision(revision, |_| {
            let old = mirrors
                .get(name)
                .filter(|m| {
                    root["sources"]
                        .as_array()
                        .is_some_and(|s| s.contains(&m.source))
                })
                .cloned();
            let ticket = Ticket {
                id: uuid::Uuid::new_v4(),
                done: tokio::sync::watch::channel(false).0,
            };
            tickets.insert(name.into(), ticket.clone());
            Lookup {
                old,
                ticket,
                deadline: tokio::time::Instant::now() + Duration::from_secs(3),
            }
        })
    }
    async fn latest_lookup(&self, name: &str, lookup: &Lookup, revision: u64) -> Option<Resolved> {
        loop {
            let tickets = self.source_lookups.lock().await;
            let Some(ticket) = tickets.get(name).cloned() else {
                let mirrors = self.mirrors.lock().await;
                return self.config.at_revision(revision, |root| {
                    mirrors
                        .get(name)
                        .filter(|m| {
                            root["sources"]
                                .as_array()
                                .is_some_and(|s| s.contains(&m.source))
                        })
                        .and_then(|m| self.publish_resolved(name, m, root, revision))
                })?;
            };
            drop(tickets);
            let mut done = ticket.done.subscribe();
            tokio::time::timeout_at(lookup.deadline, done.wait_for(|complete| *complete))
                .await
                .ok()?
                .ok()?;
            let tickets = self.source_lookups.lock().await;
            if tickets
                .get(name)
                .is_none_or(|latest| latest.id != ticket.id)
            {
                continue;
            }
            let mirrors = self.mirrors.lock().await;
            return self.config.at_revision(revision, |root| {
                mirrors
                    .get(name)
                    .filter(|m| {
                        root["sources"]
                            .as_array()
                            .is_some_and(|s| s.contains(&m.source))
                    })
                    .and_then(|m| self.publish_resolved(name, m, root, revision))
            })?;
        }
    }

    async fn query_source(&self, source: &Value, name: &str) -> Result<Value, LookupFailure> {
        tokio::time::timeout(Duration::from_millis(750), async {
            let _permit = self
                .source_queries
                .acquire()
                .await
                .map_err(|_| LookupFailure::Unavailable)?;
            let client = self
                .cluster_client(source)
                .map_err(|_| LookupFailure::Unavailable)?;
            query(&client, source, name, &self.options.peer_key).await
        })
        .await
        .unwrap_or(Err(LookupFailure::Unavailable))
    }
    pub(super) async fn stream_stats(&self, name: &str) -> Value {
        let mut stats = self.media.stats(name).await;
        if let Some(m) = self
            .mirrors
            .lock()
            .await
            .get(name)
            .filter(|m| m.known && self.config.effective(name).is_none())
        {
            stats["upstream_source"] = m.source["hostname"].clone();
            stats["source_group"] = m.source["flussonix_source_group"].clone();
            stats["source_switches"] = json!(m.switches);
            stats["source_available"] = json!(m.available);
        }
        stats
    }
    pub(super) async fn refresh_sources(&self) {
        let workers = self
            .media
            .workers()
            .await
            .into_iter()
            .map(|(n, _)| n)
            .collect::<std::collections::HashSet<_>>();
        let sessions = self
            .playback_auth
            .snapshots()
            .into_iter()
            .filter_map(|s| s["name"].as_str().map(str::to_owned))
            .collect::<std::collections::HashSet<_>>();
        let mirrors = self.mirrors.lock().await.clone();
        let mut due = Vec::new();
        for (name, m) in mirrors {
            let stats = self.media.stats(&name).await;
            let failed = stats["status"] == "retrying" && stats["retry_in_ms"] == 0;
            if failed
                || m.when.elapsed() >= Duration::from_secs(10)
                    && (workers.contains(&name) || sessions.contains(&name))
            {
                due.push((m.when, name));
            }
        }
        due.sort_by_key(|(when, _)| *when);
        stream::iter(due.into_iter().take(64).map(|(_, name)| async move {
            self.resolve(&name).await;
        }))
        .buffer_unordered(16)
        .collect::<Vec<_>>()
        .await;
    }
    fn normalize_origin(
        &self,
        source: &Value,
        name: &str,
        mut c: Value,
        root: &Value,
    ) -> Result<Value, ()> {
        if c["name"].as_str() != Some(name) || c.get("disabled").is_some_and(|v| !v.is_boolean()) {
            return Err(());
        }
        if let Some(id) = c.get("flussonix_content_id") {
            crate::config::valid_identity(id).map_err(|_| ())?;
        }
        Policy::from_config(&c, root).map_err(|_| ())?;
        let api = source["api_url"].as_str().ok_or(())?;
        let private = source["private_payload_url"].as_str().unwrap_or(api);
        let transport = source["flussonix_transport"].as_str().unwrap_or("hls");
        let input = crate::cluster::source_input_url(private, name, transport).map_err(|_| ())?;
        c["inputs"] = json!([{"url":input}]);
        if url::Url::parse(private).is_ok_and(|u| u.scheme() == "https") {
            if let Some(ca) = source
                .get("flussonix_media_tls_ca")
                .or_else(|| source.get("flussonix_tls_ca"))
            {
                c["inputs"][0]["flussonix_tls_ca"] = ca.clone();
            }
        }
        c.as_object_mut().ok_or(())?.remove("transcoder");
        // Output destinations belong to local configuration, never source discovery.
        c.as_object_mut().ok_or(())?.remove("pushes");
        c["static"] = json!(false);
        c["flussonix_peer_key"] = json!(
            source["cluster_key"]
                .as_str()
                .unwrap_or(&self.options.peer_key)
        );
        Ok(c)
    }
    fn publish_resolved(
        &self,
        name: &str,
        m: &Mirror,
        root: &Value,
        revision: u64,
    ) -> Option<Resolved> {
        let policy = if m.available && m.config["disabled"] != true {
            Policy::from_config(&m.config, root).ok()
        } else {
            None
        };
        self.playback_auth
            .publish(name, policy)
            .map(|policy| Resolved {
                config: m.config.clone(),
                policy,
                revision,
            })
    }
    async fn install_origin(
        &self,
        name: &str,
        lookup: &Lookup,
        source: &Value,
        config: Value,
        state: OriginState,
        revision: u64,
    ) -> Option<Resolved> {
        let tickets = self.source_lookups.lock().await;
        if tickets
            .get(name)
            .is_none_or(|current| current.id != lookup.ticket.id)
        {
            drop(tickets);
            return self.latest_lookup(name, lookup, revision).await;
        }
        let old = lookup.old.as_ref();
        let available = matches!(state, OriginState::Ready);
        let known = !matches!(state, OriginState::Unresolved);
        let mut mirrors = self.mirrors.lock().await;
        self.config.at_revision(revision, |root| {
            if crate::config::effective(root, name).is_some() {
                return None;
            }
            let current = mirrors.get(name);
            if current.map(|m| m.serial) != old.map(|m| m.serial) {
                return current
                    .filter(|m| {
                        root["sources"]
                            .as_array()
                            .is_some_and(|s| s.contains(&m.source))
                    })
                    .and_then(|m| self.publish_resolved(name, m, root, revision));
            }
            if !root["sources"]
                .as_array()
                .is_some_and(|s| s.contains(source))
                || mirrors.len() >= 10000 && current.is_none()
            {
                return None;
            }
            let switches = old.map_or(0, |m| {
                m.switches.saturating_add(u64::from(
                    m.known && known && m.source["hostname"] != source["hostname"],
                ))
            });
            let mirror = Mirror {
                when: Instant::now(),
                source: source.clone(),
                config,
                available,
                denied: matches!(state, OriginState::Denied),
                known,
                serial: old.map_or(Some(1), |m| m.serial.checked_add(1))?,
                switches,
            };
            let resolved = self.publish_resolved(name, &mirror, root, revision);
            mirrors.insert(name.into(), mirror);
            resolved
        })?
    }
    pub(super) async fn resolve(&self, name: &str) -> Option<Resolved> {
        let revision = self.config.revision();
        if let Some(c) = self.config.effective(name) {
            return self.resolved(name, c, revision);
        }
        let root = self.config.snapshot();
        let sources = root["sources"].as_array()?;
        let old = self
            .mirrors
            .lock()
            .await
            .get(name)
            .filter(|m| sources.contains(&m.source))
            .cloned();
        let stats = self.media.stats(name).await;
        let failed = stats["status"] == "retrying" && stats["retry_in_ms"] == 0;
        if let Some(m) = old.as_ref() {
            if m.available && m.when.elapsed() < Duration::from_secs(10) && !failed {
                // Stats acquisition may yield while a newer lookup publishes a
                // denial or switches origins. Publish the current mirror under
                // its lock, never the snapshot taken before that await.
                let mirrors = self.mirrors.lock().await;
                return self.config.at_revision(revision, |root| {
                    mirrors
                        .get(name)
                        .filter(|current| {
                            root["sources"]
                                .as_array()
                                .is_some_and(|s| s.contains(&current.source))
                        })
                        .and_then(|current| self.publish_resolved(name, current, root, revision))
                })?;
            }
            if !m.available && m.when.elapsed() < Duration::from_secs(1) {
                return None;
            }
        }
        let lookup = self.begin_lookup(name, &root, revision).await?;
        let old = lookup.old.clone();
        let deadline = lookup.deadline;
        let mut authority = None;
        let mut current = None;
        if let Some(m) = old.as_ref().filter(|m| m.known) {
            let result = if m.source["drain"] == true {
                Err(LookupFailure::Unavailable)
            } else {
                self.query_source(&m.source, name).await
            };
            match result {
                Ok(c) => match self.normalize_origin(&m.source, name, c, &root) {
                    Ok(c) if c["disabled"] != true => {
                        if !failed {
                            return self
                                .install_origin(
                                    name,
                                    &lookup,
                                    &m.source,
                                    c,
                                    OriginState::Ready,
                                    revision,
                                )
                                .await;
                        }
                        authority = Some(c.clone());
                        current = Some(c);
                    }
                    Ok(c) => {
                        return self
                            .install_origin(
                                name,
                                &lookup,
                                &m.source,
                                c,
                                OriginState::Denied,
                                revision,
                            )
                            .await;
                    }
                    Err(_) => {
                        return self
                            .install_origin(
                                name,
                                &lookup,
                                &m.source,
                                m.config.clone(),
                                OriginState::Denied,
                                revision,
                            )
                            .await;
                    }
                },
                Err(LookupFailure::Absent | LookupFailure::Invalid) => {
                    return self
                        .install_origin(
                            name,
                            &lookup,
                            &m.source,
                            m.config.clone(),
                            OriginState::Denied,
                            revision,
                        )
                        .await;
                }
                Err(LookupFailure::Unavailable) if m.denied => {
                    return self
                        .install_origin(
                            name,
                            &lookup,
                            &m.source,
                            m.config.clone(),
                            OriginState::Denied,
                            revision,
                        )
                        .await;
                }
                Err(LookupFailure::Unavailable) => authority = Some(m.config.clone()),
            }
        }
        let candidates: Vec<_> = if let Some(m) = old.as_ref().filter(|m| m.known) {
            let group = m.source["flussonix_source_group"].as_str();
            let id = m.config["flussonix_content_id"].as_str();
            if group.is_some()
                && id.is_some()
                && authority
                    .as_ref()
                    .is_some_and(|a| a["flussonix_content_id"] == m.config["flussonix_content_id"])
            {
                let start = sources.iter().position(|s| s == &m.source).unwrap_or(0) + 1;
                sources
                    .iter()
                    .cycle()
                    .skip(start)
                    .take(sources.len())
                    .filter(|s| {
                        *s != &m.source
                            && s["drain"] != true
                            && s["flussonix_source_group"].as_str() == group
                    })
                    .cloned()
                    .collect()
            } else {
                Vec::new()
            }
        } else {
            sources
                .iter()
                .filter(|s| s["drain"] != true)
                .cloned()
                .collect()
        };
        let mut lookups = stream::iter(candidates.into_iter().map(|source: Value| async move {
            let result = self.query_source(&source, name).await;
            (source, result)
        }))
        .buffered(4);
        while let Ok(Some((source, result))) =
            tokio::time::timeout_at(deadline, lookups.next()).await
        {
            let Ok(raw) = result else { continue };
            let Ok(c) = self.normalize_origin(&source, name, raw, &root) else {
                continue;
            };
            if let Some(expected) = authority.as_ref() {
                if c["disabled"] == true
                    || c["flussonix_content_id"] != expected["flussonix_content_id"]
                    || Policy::from_config(&c, &root).ok()
                        != Policy::from_config(expected, &root).ok()
                {
                    continue;
                }
            }
            let available = c["disabled"] != true;
            return self
                .install_origin(
                    name,
                    &lookup,
                    &source,
                    c,
                    if available {
                        OriginState::Ready
                    } else {
                        OriginState::Denied
                    },
                    revision,
                )
                .await;
        }
        if let Some(m) = old.as_ref().filter(|m| m.known) {
            let available = current.is_some();
            return self
                .install_origin(
                    name,
                    &lookup,
                    &m.source,
                    current.unwrap_or_else(|| m.config.clone()),
                    if available {
                        OriginState::Ready
                    } else {
                        OriginState::Unavailable
                    },
                    revision,
                )
                .await;
        }
        // An unresolved tombstone fences a late cold lookup after newer publication.
        if let Some(source) = sources.first() {
            return self
                .install_origin(
                    name,
                    &lookup,
                    source,
                    json!({"name":name,"disabled":true}),
                    OriginState::Unresolved,
                    revision,
                )
                .await;
        }
        self.config.at_revision(revision, |_| {
            self.playback_auth.publish(name, None);
        });
        None
    }
}

#[cfg(test)]
mod cache_tests {
    #[test]
    fn discovered_origin_pushes_are_never_activated_on_a_cdn() {
        let d = tempfile::tempdir().unwrap();
        let app = super::App::new(
            d.path().join("config.json"),
            d.path().join("media"),
            super::Options {
                admin_password: "owned-admin".into(),
                peer_key: "owned-peer-secret".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let raw = serde_json::json!({"name":"owned","pushes":[{"url":"srt://receiver:9000","passphrase":"owned-upstream-secret"}]});
        let normalized = app
            .normalize_origin(
                &serde_json::json!({"api_url":"http://127.0.0.1:19990"}),
                "owned",
                raw,
                &app.config.snapshot(),
            )
            .unwrap();
        assert!(normalized.get("pushes").is_none());
        assert!(!crate::srt_push::enabled(&normalized));
    }
    use super::*;
    #[tokio::test]
    async fn completed_probe_capacity_is_reclaimed_for_new_stream_lookup() {
        let d = tempfile::tempdir().unwrap();
        let app = App::new(
            d.path().join("config.json"),
            d.path().join("media"),
            Options {
                admin_password: "owned-cache-admin".into(),
                peer_key: "owned-cache-peer-key".into(),
                ..Default::default()
            },
        )
        .unwrap();
        app.config
            .put("sources", "a", json!({"api_url":"http://127.0.0.1:19996"}))
            .unwrap();
        let root = app.config.snapshot();
        let source = root["sources"][0].clone();
        let mut tickets = app.source_lookups.lock().await;
        let mut mirrors = app.mirrors.lock().await;
        for n in 0..10000 {
            let name = format!("missing-{n}");
            tickets.insert(
                name.clone(),
                Ticket {
                    id: uuid::Uuid::new_v4(),
                    done: tokio::sync::watch::channel(true).0,
                },
            );
            mirrors.insert(
                name.clone(),
                Mirror {
                    when: Instant::now() - Duration::from_secs(2),
                    config: json!({"name":name,"disabled":true}),
                    source: source.clone(),
                    available: false,
                    denied: false,
                    known: false,
                    serial: 1,
                    switches: 0,
                },
            );
        }
        drop(mirrors);
        drop(tickets);
        assert!(
            app.begin_lookup("real-stream", &root, app.config.revision())
                .await
                .is_some(),
            "failed probes permanently exhausted discovery"
        );
        assert!(app.source_lookups.lock().await.len() < 10000);
        assert!(app.mirrors.lock().await.len() < 10000);
    }
}
