//! SRT TS playback uses viewer policy/source fences without RTP codec limits.
use super::*;
pub(crate) struct Playback {
    pub worker: Arc<Worker>,
    pub grant: Grant,
    pub name: String,
    attached: bool,
}
impl Playback {
    pub fn attach(&mut self) {
        if !self.attached {
            self.worker.viewers.fetch_add(1, Ordering::Relaxed);
            self.worker.touch();
            self.attached = true;
        }
    }
}
impl Drop for Playback {
    fn drop(&mut self) {
        if self.attached {
            self.worker.viewers.fetch_sub(1, Ordering::Relaxed);
            self.worker.touch();
        }
    }
}
impl App {
    pub(crate) async fn ts_admit(&self, viewer: ViewerRequest) -> Result<Playback, u16> {
        if self.options.role == "lb" || self.options.drain {
            return Err(503);
        }
        let name = viewer.name.clone();
        let resolved = self.resolve(&name).await.ok_or(404u16)?;
        let grant = match self.playback_auth.authorize(resolved.policy, viewer).await {
            AuthOutcome::Allowed(g) => g,
            _ => return Err(403),
        };
        if self.config.revision() != resolved.revision {
            self.resolve(&name).await.ok_or(404u16)?;
        }
        let (cfg, _) = self.media_config(&name).await.ok_or(404u16)?;
        let signature = crate::media::media_signature(&cfg);
        let check = async {
            let current = self.media_config(&name).await.is_some_and(|(c, _)| {
                c["disabled"] != true && crate::media::media_signature(&c) == signature
            });
            current && !grant.is_cancelled()
        };
        let worker = self
            .media
            .ensure_guarded(&name, &cfg, true, check)
            .await
            .map_err(|_| 503u16)?;
        // A viewer losing authorization owns only its grant/reference, never
        // another viewer's shared worker. Stop only a genuinely stale route.
        if grant.is_cancelled() {
            return Err(403);
        }
        let current = self.media_config(&name).await.is_some_and(|(c, _)| {
            c["disabled"] != true && crate::media::media_signature(&c) == worker.signature()
        });
        if grant.is_cancelled() {
            return Err(403);
        }
        if worker.is_closed() || !current {
            self.media.stop_if_current(&name, &worker).await;
            return Err(503);
        }
        Ok(Playback {
            worker,
            grant,
            name,
            attached: false,
        })
    }
    pub(crate) async fn ts_current(&self, playback: &Playback) -> bool {
        let current = self
            .media_config(&playback.name)
            .await
            .is_some_and(|(c, _)| {
                c["disabled"] != true
                    && crate::media::media_signature(&c) == playback.worker.signature()
            });
        current && !playback.grant.is_cancelled() && !playback.worker.is_closed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::atomic::AtomicUsize, task::Poll};
    use tokio_util::sync::CancellationToken;
    fn request(token: &str) -> ViewerRequest {
        ViewerRequest {
            name: "owned".into(),
            proto: "srt".into(),
            ip: "127.0.0.1".into(),
            token: token.into(),
            ..Default::default()
        }
    }
    fn app(dir: &std::path::Path) -> Arc<App> {
        App::new(
            dir.join("config.json"),
            dir.join("media"),
            Options {
                admin_password: "owned-race-admin".into(),
                peer_key: "owned-race-peer".into(),
                ..Default::default()
            },
        )
        .unwrap()
    }
    fn revoke(app: &App, token: &str) {
        use sha2::{Digest, Sha256};
        let hash = format!("{:x}", Sha256::digest(token.as_bytes()));
        let id = app
            .playback_auth
            .snapshots()
            .into_iter()
            .find(|s| s["user_id"] == hash)
            .unwrap()["id"]
            .as_str()
            .unwrap()
            .to_owned();
        assert!(app.playback_auth.revoke(&id));
    }
    #[tokio::test]
    async fn revocation_during_write_validation_emits_no_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let app = app(dir.path());
        app.config
            .put(
                "streams",
                "owned",
                json!({"static":false,"inputs":[{"url":"testsrc://"}]}),
            )
            .unwrap();
        let playback = app.ts_admit(request("viewer-a")).await.unwrap();
        let mut revision = app.config.revision();
        app.config
            .put("templates", "unused", json!({"static":false}))
            .unwrap();
        let guard = app.mirrors.lock().await;
        let writes = AtomicUsize::new(0);
        let cancel = CancellationToken::new();
        let mut sending = Box::pin(crate::srt_playback::send_with(
            b"owned media",
            &app,
            &playback,
            &cancel,
            &mut revision,
            |_| {
                writes.fetch_add(1, Ordering::Relaxed);
                Ok(true)
            },
        ));
        assert!(matches!(
            futures_util::poll!(sending.as_mut()),
            Poll::Pending
        ));
        revoke(&app, "viewer-a");
        drop(guard);
        let denied = sending.await.is_err();
        app.media.stop_all().await;
        assert!(denied, "revoked viewer was allowed to write");
        assert_eq!(writes.load(Ordering::Relaxed), 0);
    }
    #[tokio::test]
    async fn equivalent_policy_source_switch_during_send_wait_emits_no_old_media() {
        let dir = tempfile::tempdir().unwrap();
        let app = app(dir.path());
        app.config
            .put("sources", "origin", json!({"api_url":"http://127.0.0.1:1"}))
            .unwrap();
        let source = app.config.snapshot()["sources"][0].clone();
        let cfg = json!({"name":"owned","static":false,"inputs":[{"url":"testsrc://"}]});
        app.mirrors.lock().await.insert(
            "owned".into(),
            Mirror {
                when: Instant::now(),
                config: cfg.clone(),
                source,
                available: true,
                denied: false,
                known: true,
                serial: 1,
                switches: 0,
            },
        );
        let policy = Policy::from_config(&cfg, &app.config.snapshot()).unwrap();
        let snapshot = app.playback_auth.publish("owned", Some(policy)).unwrap();
        let AuthOutcome::Allowed(grant) = app
            .playback_auth
            .authorize(snapshot, request("viewer-a"))
            .await
        else {
            panic!("owned policy denied")
        };
        let worker = app.media.ensure("owned", &cfg).await.unwrap();
        let playback = Playback {
            worker,
            grant,
            name: "owned".into(),
            attached: false,
        };
        let original = app.config.revision();
        let mut revision = original;
        let writes = AtomicUsize::new(0);
        let cancel = CancellationToken::new();
        let mut sending = Box::pin(crate::srt_playback::send_with(
            b"old origin media",
            &app,
            &playback,
            &cancel,
            &mut revision,
            |_| Ok(writes.fetch_add(1, Ordering::Relaxed) > 0),
        ));
        assert!(matches!(
            futures_util::poll!(sending.as_mut()),
            Poll::Pending
        ));
        assert_eq!(writes.load(Ordering::Relaxed), 1);
        app.mirrors.lock().await.get_mut("owned").unwrap().config["transcoder"] =
            json!({"encoder":"libx264","vb":900});
        assert_eq!(app.config.revision(), original);
        assert!(!playback.grant.is_cancelled());
        let denied = sending.await.is_err();
        app.media.stop_all().await;
        assert!(denied, "old source retried after equivalent-policy switch");
        assert_eq!(writes.load(Ordering::Relaxed), 1);
        assert_eq!(app.srt_egress.load(Ordering::Relaxed), 0);
    }
    #[tokio::test]
    async fn revoked_pending_admission_preserves_existing_viewer_worker_and_delivery() {
        let dir = tempfile::tempdir().unwrap();
        let app = app(dir.path());
        let cfg = json!({"static":false,"inputs":[{"url":"testsrc://"}]});
        app.config.put("streams", "owned", cfg.clone()).unwrap();
        let mut first = app.ts_admit(request("viewer-a")).await.unwrap();
        first.attach();
        let pid = first.worker.pid();
        let mut receiver = first.worker.subscribe();
        let startups = app.media.hold_startups_for_test().await;
        let mut admitting = Box::pin(app.ts_admit(request("viewer-b")));
        assert!(matches!(
            futures_util::poll!(admitting.as_mut()),
            Poll::Pending
        ));
        let mirrors = app.mirrors.lock().await;
        drop(startups);
        assert!(matches!(
            futures_util::poll!(admitting.as_mut()),
            Poll::Pending
        ));
        revoke(&app, "viewer-b");
        drop(mirrors);
        let denied = admitting.await.is_err();
        let alive = !first.worker.is_closed();
        let same = app.media.count().await == 1 && first.worker.pid() == pid;
        let delivered = if alive {
            tokio::time::timeout(Duration::from_secs(3), receiver.recv())
                .await
                .is_ok_and(|r| r.is_ok_and(|d| !d.is_empty()))
        } else {
            false
        };
        app.media.stop_all().await;
        assert!(denied);
        assert!(
            alive && same && delivered,
            "revoked admission interrupted the existing viewer"
        );
    }
}
