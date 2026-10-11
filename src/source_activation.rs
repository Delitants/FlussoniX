//! Bounded explicit native source static demand, separate from viewer demand.
use super::*;
use futures_util::{StreamExt, stream};
use std::collections::{BTreeMap, BTreeSet};
#[derive(Default)]
pub(super) struct State {
    entries: BTreeMap<String, Entry>,
    cursor: Option<String>,
}
struct Entry {
    source: Value,
    attempted: Option<Instant>,
    confirmed: Option<Instant>,
    names: BTreeSet<String>,
    error: Option<&'static str>,
}
impl State {
    fn expire(&mut self) {
        for entry in self.entries.values_mut() {
            if entry
                .confirmed
                .is_some_and(|when| when.elapsed() >= Duration::from_secs(30))
            {
                entry.names.clear();
                entry.confirmed = None;
            }
        }
    }
    fn current<'a>(&'a self, root: &'a Value) -> impl Iterator<Item = (&'a String, &'a Entry)> {
        self.entries.iter().filter(move |(_, e)| {
            root["sources"]
                .as_array()
                .is_some_and(|s| s.contains(&e.source))
        })
    }
    fn wanted(&self, root: &Value) -> BTreeSet<String> {
        self.current(root)
            .filter(|(_, e)| {
                e.confirmed
                    .is_some_and(|when| when.elapsed() < Duration::from_secs(30))
            })
            .flat_map(|(_, e)| e.names.iter().cloned())
            .collect()
    }
}
impl App {
    pub(super) async fn source_is_warm(&self, name: &str) -> bool {
        if self.options.role == "lb" {
            return false;
        }
        let state = self.source_activation.lock().await;
        self.config.read(|root| {
            state.current(root).any(|(_, e)| {
                e.confirmed
                    .is_some_and(|when| when.elapsed() < Duration::from_secs(30))
                    && e.names.contains(name)
            })
        })
    }
    pub(super) async fn source_activation_status(&self) -> Value {
        let state = self.source_activation.lock().await;
        self.config.read(|root| json!({"wanted_streams":state.wanted(root).len(),"sources":state.current(root).map(|(key,e)| json!({"source":key,"last_error":e.error,"inventory_streams":e.names.len(),"lease_valid":e.confirmed.is_some_and(|when| when.elapsed()<Duration::from_secs(30))})).collect::<Vec<_>>()}))
    }
    pub(super) async fn reclaim_source_activation_metadata(&self) {
        let wanted = {
            let state = self.source_activation.lock().await;
            self.config.read(|root| state.wanted(root))
        };
        let workers = self
            .media
            .workers()
            .await
            .into_iter()
            .map(|(name, _)| name)
            .collect::<BTreeSet<_>>();
        // Match resolver order and exclude in-flight lookups. Request consumers
        // claim the final mirror under this lock before publishing any snapshot.
        let tickets = self.source_lookups.lock().await;
        let mut mirrors = self.mirrors.lock().await;
        mirrors.retain(|name, mirror| {
            !mirror.activation_only
                || wanted.contains(name)
                || workers.contains(name)
                || tickets
                    .get(name)
                    .is_some_and(|ticket| !*ticket.done.borrow())
                || !self.playback_auth.forget_unobserved_activation(name)
        });
    }
    pub(super) async fn refresh_source_activation(&self) {
        if self.options.role == "lb" {
            return;
        }
        // All reconciliation entry points share one bounded activation pass.
        let Ok(_pass) = self.source_activation_refresh.try_lock() else {
            return;
        };
        let (root, revision) = self
            .config
            .read(|root| (root.clone(), self.config.revision()));
        let sources = root["sources"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|s| s["only"].as_array().is_some_and(|a| !a.is_empty()))
            .cloned()
            .collect::<Vec<_>>();
        let due = {
            let mut state = self.source_activation.lock().await;
            self.config
                .at_revision(revision, |_| {
                    state.expire();
                    state.entries.retain(|_, e| sources.contains(&e.source));
                    for source in sources {
                        let key = crate::config::item_key("sources", &source)
                            .unwrap()
                            .to_owned();
                        state.entries.entry(key).or_insert(Entry {
                            source,
                            attempted: None,
                            confirmed: None,
                            names: BTreeSet::new(),
                            error: None,
                        });
                    }
                    state
                        .entries
                        .values_mut()
                        .filter(|e| {
                            e.attempted
                                .is_none_or(|when| when.elapsed() >= Duration::from_secs(10))
                        })
                        .map(|e| {
                            e.attempted = Some(Instant::now());
                            e.source.clone()
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };
        stream::iter(due.into_iter().map(|source| async move {
            let result = tokio::time::timeout(Duration::from_millis(750), async {
                let _permit = self
                    .source_queries
                    .acquire()
                    .await
                    .map_err(|_| crate::source_directory::LookupFailure::Unavailable)?;
                let client = self
                    .cluster_client(&source)
                    .map_err(|_| crate::source_directory::LookupFailure::Unavailable)?;
                crate::source_directory::inventory(&client, &source, &self.options.peer_key).await
            })
            .await
            .unwrap_or(Err(crate::source_directory::LookupFailure::Unavailable));
            let mut state = self.source_activation.lock().await;
            self.config.at_revision(revision, |root| {
                if !root["sources"].as_array().unwrap().contains(&source) {
                    return;
                }
                let key = crate::config::item_key("sources", &source).unwrap();
                state.expire();
                let used = state
                    .entries
                    .iter()
                    .filter(|(k, _)| k.as_str() != key)
                    .map(|(_, e)| e.names.len())
                    .sum::<usize>();
                let Some(entry) = state.entries.get_mut(key).filter(|e| e.source == source) else {
                    return;
                };
                match result {
                    Ok(names) if used.saturating_add(names.len()) <= 10000 => {
                        entry.names = names.into_iter().collect();
                        entry.confirmed = Some(Instant::now());
                        entry.error = None;
                    }
                    Ok(_) => {
                        entry.names.clear();
                        entry.confirmed = None;
                        entry.error = Some("capacity");
                    }
                    Err(crate::source_directory::LookupFailure::Invalid) => {
                        entry.error = Some("invalid")
                    }
                    Err(_) => entry.error = Some("unavailable"),
                }
            });
        }))
        .buffer_unordered(16)
        .collect::<Vec<_>>()
        .await;
        self.reclaim_source_activation_metadata().await;
        let names = {
            let mut state = self.source_activation.lock().await;
            self.config
                .at_revision(revision, |root| {
                    let wanted = state.wanted(root);
                    let names = wanted
                        .iter()
                        .filter(|n| state.cursor.as_ref().is_none_or(|c| *n > c))
                        .chain(
                            wanted
                                .iter()
                                .filter(|n| state.cursor.as_ref().is_some_and(|c| *n <= c)),
                        )
                        .take(64)
                        .cloned()
                        .collect::<Vec<_>>();
                    state.cursor = names.last().cloned();
                    names
                })
                .unwrap_or_default()
        };
        stream::iter(names.into_iter().map(|name| async move {
            if self.config.effective(&name).is_some() {
                return;
            }
            let Some(resolved) = self.resolve_for_activation(&name).await else {
                return;
            };
            let revision = resolved.revision;
            let signature = crate::media::media_signature(&resolved.config);
            let check = async {
                self.source_is_warm(&name).await
                    && self.config.revision() == revision
                    && self.recovery_current(&name, &signature, None).await
                    && self.config.revision() == revision
            };
            if let Ok(worker) = self
                .media
                .ensure_guarded(&name, &resolved.config, false, check)
                .await
            {
                self.finish_recovery(&name, &signature, None, &worker).await;
            }
        }))
        .buffer_unordered(8)
        .collect::<Vec<_>>()
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Break caught: stale source snapshots or expired leases retain static demand.
    #[test]
    fn expired_inventory_and_changed_source_cannot_keep_names_warm() {
        let source = json!({"hostname":"owned","only":["news"]});
        let mut state = State::default();
        state.entries.insert(
            "owned".into(),
            Entry {
                source: source.clone(),
                attempted: None,
                confirmed: Some(Instant::now()),
                names: BTreeSet::from(["news".into()]),
                error: None,
            },
        );
        let root = json!({"sources":[source]});
        assert_eq!(state.wanted(&root), BTreeSet::from(["news".into()]));
        assert!(
            state
                .wanted(&json!({"sources":[{"hostname":"owned","only":[]}]}))
                .is_empty()
        );
        state.entries.get_mut("owned").unwrap().confirmed =
            Some(Instant::now() - Duration::from_secs(31));
        assert!(state.wanted(&root).is_empty());
    }
    // Break caught: expired failed inventory permanently reserves selection
    // capacity and prevents another valid source from activating its stream.
    #[tokio::test]
    async fn expired_inventory_releases_capacity_for_another_source() {
        let dir = tempfile::tempdir().unwrap();
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            Options {
                admin_password: "owned-admin".into(),
                peer_key: "owned-peer-secret".into(),
                uplink_interface: "process".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let old = app
            .config
            .put(
                "sources",
                "old",
                json!({"api_url":"http://127.0.0.1:9","only":["old/*"]}),
            )
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let routes = axum::Router::new().route(
            "/flussonix/api/v1/source-streams",
            axum::routing::get(|| async {
                axum::Json(json!({"streams":[{"name":"new/news","static":true,"disabled":false}]}))
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, routes).await.unwrap() });
        app.config
            .put("sources", "new", json!({"api_url":url,"only":["new/*"]}))
            .unwrap();
        app.source_activation.lock().await.entries.insert(
            "old".into(),
            Entry {
                source: old,
                attempted: None,
                confirmed: Some(Instant::now() - Duration::from_secs(31)),
                names: (0..10000).map(|n| format!("old/{n}")).collect(),
                error: None,
            },
        );
        app.refresh_source_activation().await;
        let status = app.source_activation_status().await;
        server.abort();
        let _ = server.await;
        app.media.stop_all().await;
        assert_eq!(
            status["wanted_streams"], 1,
            "expired selection retained capacity: {status}"
        );
        assert!(
            status["sources"]
                .as_array()
                .unwrap()
                .iter()
                .all(|s| s["last_error"] != "capacity")
        );
    }

    // Break caught: historical eager discoveries fill both route and authority caches.
    #[tokio::test]
    async fn warm_name_churn_reclaims_unconsumed_metadata_and_preserves_demand_authority() {
        let dir = tempfile::tempdir().unwrap();
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            Options {
                admin_password: "owned-admin".into(),
                peer_key: "owned-cache-peer".into(),
                role: "cdn".into(),
                ffmpeg: "/owned-cache-no-ffmpeg".into(),
                uplink_interface: "process".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let selected = Arc::new(std::sync::Mutex::new("old/0/0".to_owned()));
        let picked = selected.clone();
        let routes=axum::Router::new()
          .route("/flussonix/api/v1/source-streams",axum::routing::get(move || {let picked=picked.clone();async move {
              axum::Json(json!({"streams":[{"name":picked.lock().unwrap().clone(),"static":true,"disabled":false}]}))
          }}))
          .route("/flussonix/api/v1/stream/{*name}",axum::routing::get(|axum::extract::Path(name):axum::extract::Path<String>|async move {
              axum::Json(json!({"name":name,"static":true,"disabled":false,"flussonix_content_id":"owned-cache"}))
          }));
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(l, routes).await.unwrap() });
        app.config
            .put(
                "sources",
                "owned",
                json!({"api_url":url,"flussonix_transport":"m4s","only":["old/*","warm/*"]}),
            )
            .unwrap();
        app.refresh_source_activation().await;
        app.media.stop_all().await;
        let seed = app.mirrors.lock().await["old/0/0"].clone();
        let viewer = app.resolve("viewer").await.unwrap();
        let AuthOutcome::Allowed(grant) = app
            .playback_auth
            .authorize(
                viewer.policy,
                ViewerRequest {
                    name: "viewer".into(),
                    proto: "hls".into(),
                    ip: "owned".into(),
                    ..Default::default()
                },
            )
            .await
        else {
            panic!("owned viewer denied")
        };
        app.resolve("denied").await.unwrap();
        {
            let mut mirrors = app.mirrors.lock().await;
            let denied = mirrors.get_mut("denied").unwrap();
            denied.denied = true;
            denied.available = false;
        }
        app.playback_auth.publish("denied", None);
        let mut observations = Vec::new();
        for round in 0..2 {
            let root = app.config.snapshot();
            {
                let mut mirrors = app.mirrors.lock().await;
                mirrors.retain(|name, _| name == "viewer" || name == "denied");
                for n in 0..9998 {
                    let name = format!("old/{round}/{n}");
                    let mut mirror = seed.clone();
                    mirror.config["name"] = json!(name);
                    mirror.when = Instant::now() - Duration::from_secs(31);
                    // Include background-only denied metadata in the churn budget.
                    if n % 2 == 0 {
                        mirror.denied = true;
                        mirror.available = false;
                    }
                    app.playback_auth.publish(
                        &name,
                        Some(Policy::from_config(&mirror.config, &root).unwrap()),
                    );
                    if mirror.denied {
                        app.playback_auth.publish(&name, None);
                    }
                    mirrors.insert(name, mirror);
                }
            }
            *selected.lock().unwrap() = format!("warm/{round}");
            {
                let mut state = app.source_activation.lock().await;
                let e = state.entries.get_mut("owned").unwrap();
                e.attempted = None;
                e.confirmed = Some(Instant::now() - Duration::from_secs(31));
            }
            app.refresh_source_activation().await;
            let warm = app
                .mirrors
                .lock()
                .await
                .contains_key(&format!("warm/{round}"));
            let ondemand = app.resolve(&format!("on-demand/{round}")).await.is_some();
            let mirrors = app.mirrors.lock().await;
            observations.push((
                warm,
                ondemand,
                mirrors.contains_key("viewer"),
                mirrors.get("denied").is_some_and(|m| m.denied),
            ));
            drop(mirrors);
            app.media.stop_all().await;
        }
        server.abort();
        let _ = server.await;
        app.media.stop_all().await;
        assert!(
            observations
                .iter()
                .all(|row| *row == (true, true, true, true)),
            "historical activation metadata blocked current discovery or lost demand authority: {observations:?}"
        );
        assert!(
            !grant.is_cancelled(),
            "cache reclamation revoked an admitted viewer"
        );
    }
    // Break caught: source edits discard eager route ownership before releasing authority.
    #[tokio::test]
    async fn source_edit_and_removal_reclaim_unconsumed_activation_authority() {
        let dir = tempfile::tempdir().unwrap();
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            Options {
                admin_password: "owned-admin".into(),
                peer_key: "owned-cache-peer".into(),
                role: "cdn".into(),
                ffmpeg: "/owned-cache-no-ffmpeg".into(),
                uplink_interface: "process".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let selected = Arc::new(std::sync::Mutex::new("old/0/0".to_owned()));
        let picked = selected.clone();
        let routes=axum::Router::new()
          .route("/flussonix/api/v1/source-streams",axum::routing::get(move || {let picked=picked.clone();async move {
              axum::Json(json!({"streams":[{"name":picked.lock().unwrap().clone(),"static":true,"disabled":false}]}))
          }}))
          .route("/flussonix/api/v1/stream/{*name}",axum::routing::get(|axum::extract::Path(name):axum::extract::Path<String>|async move {
              axum::Json(json!({"name":name,"static":true,"disabled":false,"flussonix_content_id":"owned-cache"}))
          }));
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(l, routes).await.unwrap() });
        app.config
            .put(
                "sources",
                "owned",
                json!({"api_url":url,"flussonix_transport":"m4s","only":["old/*","warm/*"]}),
            )
            .unwrap();
        app.refresh_source_activation().await;
        app.media.stop_all().await;
        let seed = app.mirrors.lock().await["old/0/0"].clone();
        app.config
            .put("streams", "local-viewer", json!({"static":false}))
            .unwrap();
        let viewer = app.resolve("local-viewer").await.unwrap();
        let AuthOutcome::Allowed(grant) = app
            .playback_auth
            .authorize(
                viewer.policy,
                ViewerRequest {
                    name: "local-viewer".into(),
                    proto: "hls".into(),
                    ip: "owned".into(),
                    ..Default::default()
                },
            )
            .await
        else {
            panic!("owned local viewer denied")
        };
        let denied = app.resolve("claimed-denied").await.unwrap();
        let denied_policy = Policy::from_config(&denied.config, &app.config.snapshot()).unwrap();
        app.playback_auth.publish("claimed-denied", None);
        let mut observations = Vec::new();
        for round in 0..3 {
            let root = app.config.snapshot();
            {
                let mut mirrors = app.mirrors.lock().await;
                mirrors.retain(|_, mirror| !mirror.activation_only);
                for n in 0..9997 {
                    let name = format!("old/{round}/{n}");
                    let mut mirror = seed.clone();
                    mirror.source = root["sources"][0].clone();
                    mirror.config["name"] = json!(name);
                    app.playback_auth.publish(
                        &name,
                        Some(Policy::from_config(&mirror.config, &root).unwrap()),
                    );
                    if n % 2 == 0 {
                        mirror.denied = true;
                        mirror.available = false;
                        app.playback_auth.publish(&name, None);
                    }
                    mirrors.insert(name, mirror);
                }
            }
            // Exercise the same invalidation called by source PUT and DELETE,
            // including removal before recreation in the middle round.
            if round == 1 {
                app.config.delete("sources", "owned").unwrap();
                app.invalidate_sessions().await;
            }
            app.config.put("sources", "owned", json!({"api_url":url,"flussonix_transport":"m4s","only":["old/*","warm/*",format!("marker{round}")]})).unwrap();
            app.invalidate_sessions().await;
            let retained_denial = app
                .playback_auth
                .publish("claimed-denied", Some(denied_policy.clone()))
                .is_some();
            let stale_denied = matches!(
                app.playback_auth
                    .authorize(
                        denied.policy.clone(),
                        ViewerRequest {
                            name: "claimed-denied".into(),
                            proto: "hls".into(),
                            ip: "owned".into(),
                            ..Default::default()
                        }
                    )
                    .await,
                AuthOutcome::Denied
            );
            app.playback_auth.publish("claimed-denied", None);
            *selected.lock().unwrap() = format!("warm/{round}");
            app.refresh_source_activation().await;
            let warm = app
                .resolve_for_activation(&format!("warm/{round}"))
                .await
                .is_some();
            let ondemand = app.resolve(&format!("on-demand/{round}")).await.is_some();
            app.config
                .put(
                    "streams",
                    &format!("local-{round}"),
                    json!({"static":false}),
                )
                .unwrap();
            let local = app.resolve(&format!("local-{round}")).await.is_some();
            observations.push((warm, ondemand, local, retained_denial, stale_denied));
            app.media.stop_all().await;
        }
        server.abort();
        let _ = server.await;
        app.media.stop_all().await;
        assert!(
            observations
                .iter()
                .all(|row| *row == (true, true, true, true, true)),
            "source invalidation leaked unused authority or reset claimed denial: {observations:?}"
        );
        assert!(
            !grant.is_cancelled(),
            "source churn revoked an unrelated local viewer"
        );
    }
}
