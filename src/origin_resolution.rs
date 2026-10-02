//! Sticky source selection and publication fencing. Lives beneath server to keep authority private.
use super::*;
use crate::source_directory::{LookupFailure, query};
use futures_util::{StreamExt, stream};
enum OriginState {
    Ready,
    Unavailable,
    Unresolved,
}
impl App {
    async fn query_source(&self, source: &Value, name: &str) -> Result<Value, LookupFailure> {
        tokio::time::timeout(Duration::from_millis(750), async {
            let _permit = self
                .source_queries
                .acquire()
                .await
                .map_err(|_| LookupFailure::Unavailable)?;
            query(&self.client, source, name, &self.options.peer_key).await
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
        c.as_object_mut().ok_or(())?.remove("transcoder");
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
        old: Option<&Mirror>,
        source: &Value,
        config: Value,
        state: OriginState,
        revision: u64,
    ) -> Option<Resolved> {
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
                return self.resolved(name, m.config.clone(), revision);
            }
            if !m.available && m.when.elapsed() < Duration::from_secs(1) {
                return None;
            }
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
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
                                    old.as_ref(),
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
                                old.as_ref(),
                                &m.source,
                                c,
                                OriginState::Unavailable,
                                revision,
                            )
                            .await;
                    }
                    Err(_) => {
                        return self
                            .install_origin(
                                name,
                                old.as_ref(),
                                &m.source,
                                m.config.clone(),
                                OriginState::Unavailable,
                                revision,
                            )
                            .await;
                    }
                },
                Err(LookupFailure::Absent | LookupFailure::Invalid) => {
                    return self
                        .install_origin(
                            name,
                            old.as_ref(),
                            &m.source,
                            m.config.clone(),
                            OriginState::Unavailable,
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
                sources
                    .iter()
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
                    old.as_ref(),
                    &source,
                    c,
                    if available {
                        OriginState::Ready
                    } else {
                        OriginState::Unavailable
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
                    old.as_ref(),
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
                    old.as_ref(),
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
