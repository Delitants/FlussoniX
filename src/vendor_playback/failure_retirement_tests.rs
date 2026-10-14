//! Real managed private failures across retired configuration generations.
use super::*;
use crate::playback_auth::ViewerRequest;
use axum::{
    Router, body::Body, extract::OriginalUri, http::HeaderMap, response::Response, routing::get,
};
use serde_json::json;
use std::sync::atomic::AtomicUsize;
pub(in crate::vendor_playback) struct Origin {
    pub(in crate::vendor_playback) key: String,
    task: tokio::task::JoinHandle<()>,
    pub(in crate::vendor_playback) calls: Arc<AtomicUsize>,
}
impl Drop for Origin {
    fn drop(&mut self) {
        self.task.abort();
    }
}
pub(in crate::vendor_playback) async fn origin(id: &'static str) -> Origin {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let key = format!("m4s://{}", listener.local_addr().unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let private = calls.clone();
    let router=Router::new().fallback(get(move |OriginalUri(uri):OriginalUri,headers:HeaderMap| {
        let calls=private.clone();async move {
            let digest=|s:String|ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY,s.as_bytes()).as_ref().iter().map(|b|format!("{b:02x}")).collect::<String>();
            if uri.path().ends_with("/m4s") {
                let name=uri.path().strip_prefix('/').unwrap().strip_suffix("/m4s").unwrap();
                let time=headers["cluster-auth-time"].to_str().unwrap();
                assert_eq!(headers["cluster-auth-key"],digest(format!("{time}:owned-retirement-key:{name}")));
                assert!(!headers.contains_key("authorization"));
                calls.fetch_add(1,Ordering::SeqCst);
                return Response::builder().header("cluster-auth","yes").header("connection","close").body(Body::empty()).unwrap();
            }
            let auth=headers["authorization"].to_str().unwrap().strip_prefix("Cluster ").unwrap();
            let (time,hash)=auth.split_once(':').unwrap();assert_eq!(hash,digest(format!("{time}:owned-retirement-key")));
            let value=if uri.path().ends_with("/config") {json!({"stats":{"id":id}})}else{
                let rows=(0..=256).map(|n|json!({"name":format!("owned{n}"),"named_by":"config","static":true,"groups":[]})).collect::<Vec<_>>();
                json!({"server_id":id,"streams":rows})
            };
            Response::builder().header("connection","close").body(Body::from(value.to_string())).unwrap()
        }
    }));
    Origin {
        key,
        task: tokio::spawn(async move { axum::serve(listener, router).await.unwrap() }),
        calls,
    }
}
#[tokio::test]
async fn retired_private_notices_do_not_discard_a_new_streams_backup_signal() {
    let a = origin("owned-retired-a").await;
    let b = origin("owned-retired-b").await;
    let dir = tempfile::tempdir().unwrap();
    let app = App::new(
        dir.path().join("config.json"),
        dir.path().join("media"),
        crate::server::Options {
            admin_password: "owned-admin".into(),
            peer_key: "owned-native-peer".into(),
            ..Default::default()
        },
    )
    .unwrap();
    for key in [&a.key, &b.key] {
        app.config.put("sources",key,json!({"flussonix_source_kind":"flussonic","cluster_key":"owned-retirement-key","on_play":"true","flussonix_source_group":"replicas","flussonix_content_id":"music"})).unwrap();
    }
    let owner =
        Arc::new(ModernSourceCoordinator::new(app.config.clone(), "owned-notices").unwrap());
    let runtime = Runtime::new(owner);
    // Run real shared-session ownership, but leave recovery scheduling paused.
    // Configuration retirement therefore races no synthetic queue consumer.
    let cancel = CancellationToken::new();
    let _cleanup = cancel.clone().drop_guard();
    let (sessions, c) = (runtime.sessions.clone(), cancel.clone());
    let driver = tokio::spawn(async move { sessions.run(c).await });
    while !runtime.running() {
        tokio::task::yield_now().await;
    }

    let mut retired_anchor = None;
    let mut old_attempt = None;
    for n in 0usize..=257 {
        // The previous failed name is retired before the owner can consume it.
        app.config
            .put(
                "templates",
                "unrelated",
                json!({"comment":format!("revision{n}")}),
            )
            .unwrap();
        let name = format!("owned{}", n.saturating_sub(1));
        let revision = app.config.revision();
        let candidate = runtime
            .candidate(&app, &a.key, revision, &name)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "candidate {n}: {e:?}, private calls {}",
                    a.calls.load(Ordering::SeqCst)
                )
            });
        let replicas::Candidate::Replica(group, _) = &candidate else {
            panic!("missing group")
        };
        let group = group.clone();
        let outcome = runtime
            .admit_selection(
                candidate,
                &app.playback_auth,
                ViewerRequest {
                    name: name.clone(),
                    proto: "hls".into(),
                    ip: "127.0.0.1".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let crate::source_directory::modern_coordinator::AdmissionOutcome::Allowed(admission) =
            outcome
        else {
            panic!("denied owned viewer")
        };
        let attempt = runtime
            .register_anchor(&name, group.clone(), &admission, &json!({}))
            .unwrap();
        if n == 256 {
            retired_anchor = Some((group.clone(), admission.anchor_copy()));
        }
        if n == 257 {
            let (old_group, old_anchor) = retired_anchor.take().unwrap();
            assert!(
                runtime
                    .register_anchor("owned255", old_group, &old_anchor, &json!({}))
                    .is_err(),
                "retired foreground registration replaced current recovery entries"
            );
        }

        if n == 1 {
            drop(old_attempt.take());
            assert!(
                runtime.recovery.lock().unwrap().entries[&name].pending > 0,
                "retired request completion removed current foreground work"
            );
        }
        let mut attachment = runtime
            .inputs
            .attach_configured(admission, &json!({}))
            .unwrap();
        assert!(attachment.input.take().unwrap().run().await.is_err());
        if n == 0 {
            old_attempt = Some(attempt);
        } else {
            drop(attempt);
        }
        if n == 257 {
            runtime.process_failure(&name, &group);
            assert!(
                runtime.recovery_mode(&name, &group) == replicas::Mode::Rotate,
                "retired names exhausted the notice queue and discarded the current private failure"
            );
            tokio::time::sleep(Duration::from_millis(550)).await;
            let backup = runtime
                .choose_group_locked(&app, &group, &name, replicas::Mode::Rotate)
                .await
                .unwrap();
            assert_eq!(
                backup.source_key(),
                b.key,
                "new stream did not select its healthy catalog backup"
            );
        }
        drop(attachment);
    }
    assert_eq!(a.calls.load(Ordering::SeqCst), 258);
    assert_eq!(b.calls.load(Ordering::SeqCst), 0);
    runtime.recovery.lock().unwrap().entries.clear();
    runtime.inputs.shutdown();
    cancel.cancel();
    assert!(driver.await.unwrap().is_ok());
    assert_eq!(app.playback_auth.live_grants(), 0);
}
