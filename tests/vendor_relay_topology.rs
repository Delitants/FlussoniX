//! Explicit local paths and all-hop freshness. No vendor service or vendor code.
#[path = "support/vendor_media_ack.rs"]
mod vendor_fixture;
use axum::{
    Router,
    body::Body,
    extract::OriginalUri,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::get,
};
use flussonix::{
    config::ConfigStore,
    source_directory::modern_coordinator::{
        CoordinatedInventory, CoordinationFailure, ModernSourceCoordinator, RelayCandidate,
        RelayFailure, RelayHop, RelayRoute, SourceHandle,
    },
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::oneshot;
const NAME: &str = "Music %2F 日本 HD";
struct Hold {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<StatusCode>,
}
struct Server {
    key: String,
    expected: String,
    id: Arc<Mutex<String>>,
    rows: Arc<Mutex<Value>>,
    paths: Arc<Mutex<Vec<String>>>,
    hold: Arc<Mutex<Option<Hold>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn start(index: usize, terminal: bool) -> Self {
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let key = format!("m4s://{}", tcp.local_addr().unwrap());
        let expected = format!("node-{index}");
        let id = Arc::new(Mutex::new(expected.clone()));
        let rows = Arc::new(Mutex::new(
            json!([{"name":NAME,"named_by":if terminal {"config"} else {"remote"},"groups":[],"static":true}]),
        ));
        let paths = Arc::new(Mutex::new(vec![]));
        let hold: Arc<Mutex<Option<Hold>>> = Arc::new(Mutex::new(None));
        let (ids, rs, ps, hs) = (id.clone(), rows.clone(), paths.clone(), hold.clone());
        let router=Router::new().fallback(get(move |OriginalUri(uri):OriginalUri,headers:HeaderMap| {
            let (ids,rs,ps,hs)=(ids.clone(),rs.clone(),ps.clone(),hs.clone());
            async move {
                ps.lock().unwrap().push(uri.path().into());
                if uri.path().ends_with("/m4s") {
                    assert!(headers.contains_key("cluster-auth-key"));assert!(!headers.contains_key("authorization"));
                    let track=flussonix::m4s::Track{id:1,codec:"mp3".into(),config:vec![]};
                    let frame=flussonix::m4f::Frame{track_id:1,dts:90000,pts_offset:0,key:true,body:include_bytes!("fixtures/codecs/mp3.bin").to_vec()};
                    let mut bytes=flussonix::wire::encode_info(std::slice::from_ref(&track)).unwrap();bytes.extend(flussonix::wire::encode_frame(&track,&frame).unwrap());
                    return Response::new(Body::from(bytes));
                }
                assert!(headers.contains_key("authorization"));assert!(!headers.contains_key("cluster-auth-key"));
                if uri.path().ends_with("/config") {
                    let held=hs.lock().unwrap().take();
                    if let Some(held)=held { let _=held.entered.send(());let status=held.release.await.unwrap_or(StatusCode::SERVICE_UNAVAILABLE);if status!=StatusCode::OK {return Response::builder().status(status).body(Body::empty()).unwrap();} }
                    Response::new(Body::from(json!({"stats":{"id":ids.lock().unwrap().clone()}}).to_string()))
                } else {
                    assert!(uri.path().ends_with("/streamer/api/v3/streams"));
                    Response::new(Body::from(json!({"server_id":ids.lock().unwrap().clone(),"streams":rs.lock().unwrap().clone()}).to_string()))
                }
            }
        }));
        let task = tokio::spawn(async move {
            axum::serve(tcp, vendor_fixture::acknowledge_media(router))
                .await
                .unwrap()
        });
        Self {
            key,
            expected,
            id,
            rows,
            paths,
            hold,
            task,
        }
    }
    fn source(&self) -> Value {
        json!({"url":self.key,"cluster_key":"owned-relay-key","only":[NAME]})
    }
    fn hold_probe(&self) -> (oneshot::Receiver<()>, oneshot::Sender<StatusCode>) {
        let (entered, rx) = oneshot::channel();
        let (tx, release) = oneshot::channel();
        assert!(
            self.hold
                .lock()
                .unwrap()
                .replace(Hold { entered, release })
                .is_none()
        );
        (rx, tx)
    }
    fn media_count(&self) -> usize {
        self.paths
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p.ends_with("/m4s"))
            .count()
    }
}
struct Lab {
    _dir: tempfile::TempDir,
    config: Arc<ConfigStore>,
    c: Arc<ModernSourceCoordinator>,
    servers: Vec<Server>,
    handles: Vec<SourceHandle>,
}
impl Lab {
    async fn new(n: usize) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let config = Arc::new(ConfigStore::open(dir.path().join("config.json")).unwrap());
        let mut servers = vec![];
        for i in 0..n {
            let s = Server::start(i, i + 1 == n).await;
            config.put("sources", &s.key, s.source()).unwrap();
            servers.push(s);
        }
        let c = Arc::new(ModernSourceCoordinator::new(config.clone(), "local-relay").unwrap());
        let handles = servers
            .iter()
            .map(|s| install(&c, s, Some((&json!({"on_play":"true"}), &json!({})))))
            .collect();
        Self {
            _dir: dir,
            config,
            c,
            servers,
            handles,
        }
    }
    fn route(&self) -> RelayRoute {
        self.route_names(&vec![NAME; self.servers.len()])
    }
    fn route_names(&self, names: &[&str]) -> RelayRoute {
        RelayRoute::new(
            self.servers
                .iter()
                .zip(names)
                .map(|(s, n)| RelayHop::new(&s.key, &s.expected, n).unwrap())
                .collect(),
        )
        .unwrap()
    }
    fn enable(&self) {
        self.c.replace_relay_routes(vec![self.route()]).unwrap();
    }
    async fn inventories(&self) -> Vec<CoordinatedInventory> {
        let mut v = vec![];
        for h in &self.handles {
            v.push(self.c.inventory(h, Duration::from_secs(20)).await.unwrap());
        }
        v
    }
    fn candidate(&self, rows: &[CoordinatedInventory]) -> RelayCandidate {
        self.c
            .relay_candidate(NAME, &rows.iter().collect::<Vec<_>>())
            .unwrap()
    }
    fn no_media(&self) {
        assert!(self.servers.iter().all(|s| s.media_count() == 0));
    }
}
fn install(
    c: &ModernSourceCoordinator,
    s: &Server,
    policy: Option<(&Value, &Value)>,
) -> SourceHandle {
    let mut p = c.capture(&s.key).unwrap();
    if let Some((source, groups)) = policy {
        p = p.with_policy(source, groups).unwrap();
    }
    c.install(p.prepare(Duration::from_secs(3)).unwrap())
        .unwrap()
}
fn publish(c: &ModernSourceCoordinator, r: &RelayCandidate) -> Result<(), RelayFailure> {
    c.publish_relay(r, |view, _| {
        assert_eq!(view.name, NAME);
        assert!(view.policy.url.is_none());
    })
}
fn stale(c: &ModernSourceCoordinator, r: &RelayCandidate) {
    let entered = std::sync::atomic::AtomicBool::new(false);
    assert!(
        c.publish_relay(r, |_, _| entered
            .store(true, std::sync::atomic::Ordering::SeqCst))
            .is_err()
    );
    assert!(!entered.load(std::sync::atomic::Ordering::SeqCst));
}
async fn entered(rx: oneshot::Receiver<()>) {
    tokio::time::timeout(Duration::from_secs(2), rx)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn explicit_path_uses_only_entry_media_and_minimum_deadline() {
    let l = Lab::new(3).await;
    l.enable();
    let rows = l.inventories().await;
    let r = l.candidate(&rows);
    assert_eq!(r.hop_count(), 3);
    assert_eq!(r.origin_id().as_str(), "node-2");
    assert_eq!(
        r.expires_at(),
        rows.iter().map(|r| r.expires_at()).min().unwrap()
    );
    let input =
        l.c.publish_relay(&r, |view, input| {
            assert_eq!(view.source, l.servers[0].key);
            assert_eq!(view.name, NAME);
            assert!(view.selected_static);
            input
        })
        .unwrap();
    let (tx, rx) = oneshot::channel();
    let mut bytes = vec![];
    let _ = tokio::time::timeout(
        Duration::from_secs(2),
        input.pull_ready(&mut bytes, None, tx),
    )
    .await
    .unwrap();
    assert_eq!(rx.await.unwrap()[0].codec, "mp3");
    assert!(!bytes.is_empty());
    assert_eq!(l.servers[0].media_count(), 1);
    assert!(l.servers[1..].iter().all(|s| s.media_count() == 0));
    assert!(
        l.servers[0]
            .paths
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .contains("%252F")
    );
}
#[tokio::test]
async fn eight_hops_and_explicit_literal_name_mapping_are_supported() {
    let l = Lab::new(8).await;
    let names = [
        NAME,
        "origin/One",
        "%2f",
        "name with spaces",
        "日本",
        "Case",
        "case",
        "last",
    ];
    for (s, name) in l.servers.iter().zip(names) {
        s.rows.lock().unwrap()[0]["name"] = json!(name);
    }
    l.c.replace_relay_routes(vec![l.route_names(&names)])
        .unwrap();
    let rows = l.inventories().await;
    let r = l.candidate(&rows);
    assert_eq!(r.hop_count(), 8);
    publish(&l.c, &r).unwrap();
    l.no_media();
}
#[test]
fn path_constructor_rejects_short_long_duplicate_sources_and_semantic_id_cycles() {
    let hop = |key: &str, id: &str| RelayHop::new(key, id, NAME).unwrap();
    assert!(matches!(
        RelayRoute::new(vec![]),
        Err(RelayFailure::Invalid)
    ));
    assert!(matches!(
        RelayRoute::new(vec![hop("a", "a")]),
        Err(RelayFailure::Invalid)
    ));
    assert!(matches!(
        RelayRoute::new(
            (0..9)
                .map(|i| hop(&format!("s{i}"), &format!("i{i}")))
                .collect()
        ),
        Err(RelayFailure::Invalid)
    ));
    assert!(matches!(
        RelayRoute::new(vec![hop("same", "one"), hop("same", "two")]),
        Err(RelayFailure::Loop)
    ));
    assert!(matches!(
        RelayRoute::new(vec![hop("one", "same"), hop("two", "same")]),
        Err(RelayFailure::Loop)
    ));
    assert!(matches!(
        RelayRoute::new(vec![
            hop("one", "11111111-2222-3333-4444-555555555555"),
            hop("two", "{11111111-2222-3333-4444-555555555555}\r\n")
        ]),
        Err(RelayFailure::Loop)
    ));
}
#[test]
fn hop_fields_are_bounded_and_custom_identity_remains_exact() {
    for source in ["", " ", "bad\nkey", &"x".repeat(4097)] {
        assert!(RelayHop::new(source, "id", NAME).is_err());
    }
    for id in [
        "",
        " ",
        "00000000-0000-0000-0000-000000000000",
        &"x".repeat(257),
    ] {
        assert!(RelayHop::new("source", id, NAME).is_err());
    }
    for name in ["", &"x".repeat(257)] {
        assert!(RelayHop::new("source", "id", name).is_err());
    }
    assert!(
        RelayRoute::new(vec![
            RelayHop::new("one", "Custom", NAME).unwrap(),
            RelayHop::new("two", "Custom\r\n", NAME).unwrap()
        ])
        .is_ok()
    );
}
#[tokio::test]
async fn local_primary_and_explicit_aliases_cannot_enter_paths() {
    let l = Lab::new(2).await;
    l.c.replace_aliases(&["local-alias"]).unwrap();
    for id in ["local-relay", "local-alias"] {
        let r = RelayRoute::new(vec![
            RelayHop::new(&l.servers[0].key, id, NAME).unwrap(),
            RelayHop::new(&l.servers[1].key, "other", NAME).unwrap(),
        ])
        .unwrap();
        assert_eq!(l.c.replace_relay_routes(vec![r]), Err(RelayFailure::Loop));
    }
    l.no_media();
}
#[tokio::test]
async fn path_count_duplicate_entry_names_and_invalid_edits_are_atomic() {
    let l = Lab::new(2).await;
    l.enable();
    let rows = l.inventories().await;
    let r = l.candidate(&rows);
    assert_eq!(
        l.c.replace_relay_routes(vec![l.route(), l.route()]),
        Err(RelayFailure::Invalid)
    );
    publish(&l.c, &r).unwrap();
    let routes = |count: usize| {
        (0..count)
            .map(|i| l.route_names(&[&format!("entry{i}"), NAME]))
            .collect()
    };
    assert_eq!(
        l.c.replace_relay_routes(routes(65)),
        Err(RelayFailure::Invalid)
    );
    publish(&l.c, &r).unwrap();
    l.c.replace_relay_routes(routes(64)).unwrap();
    stale(&l.c, &r);
}
#[tokio::test]
async fn no_implicit_path_or_name_normalization() {
    let l = Lab::new(2).await;
    let rows = l.inventories().await;
    assert!(matches!(
        l.c.relay_candidate(NAME, &rows.iter().collect::<Vec<_>>()),
        Err(RelayFailure::Missing)
    ));
    l.enable();
    assert!(matches!(
        l.c.relay_candidate(
            &NAME.replace("%2F", "%2f"),
            &rows.iter().collect::<Vec<_>>()
        ),
        Err(RelayFailure::Missing)
    ));
    l.no_media();
}
#[tokio::test]
async fn every_hop_identity_is_pinned_and_cannot_follow_a_changed_claim() {
    for i in 0..3 {
        let l = Lab::new(3).await;
        l.enable();
        *l.servers[i].id.lock().unwrap() = "changed-identity".into();
        let rows = l.inventories().await;
        assert!(matches!(
            l.c.relay_candidate(NAME, &rows.iter().collect::<Vec<_>>()),
            Err(RelayFailure::Identity)
        ));
        l.no_media();
    }
}
#[tokio::test]
async fn observed_uuid_alias_matches_pinned_uuid() {
    let l = Lab::new(2).await;
    let id = "11111111-2222-3333-4444-555555555555";
    *l.servers[1].id.lock().unwrap() = format!("{{{id}}}\r\n");
    let route = RelayRoute::new(vec![
        RelayHop::new(&l.servers[0].key, "node-0", NAME).unwrap(),
        RelayHop::new(&l.servers[1].key, id, NAME).unwrap(),
    ])
    .unwrap();
    l.c.replace_relay_routes(vec![route]).unwrap();
    let rows = l.inventories().await;
    publish(&l.c, &l.candidate(&rows)).unwrap();
}

#[tokio::test]
async fn custom_identity_is_exact_and_local_uuid_aliases_are_semantic() {
    let l = Lab::new(2).await;
    l.enable();
    *l.servers[1].id.lock().unwrap() = "node-1\r\n".into();
    let rows = l.inventories().await;
    assert!(matches!(
        l.c.relay_candidate(NAME, &rows.iter().collect::<Vec<_>>()),
        Err(RelayFailure::Identity)
    ));
    let uuid = "11111111-2222-3333-4444-555555555555";
    l.c.replace_aliases(&[uuid]).unwrap();
    let route = RelayRoute::new(vec![
        RelayHop::new(&l.servers[0].key, "node-0", NAME).unwrap(),
        RelayHop::new(&l.servers[1].key, &format!("{{{uuid}}}\r\n"), NAME).unwrap(),
    ])
    .unwrap();
    assert_eq!(
        l.c.replace_relay_routes(vec![route]),
        Err(RelayFailure::Loop)
    );
    l.no_media();
}
#[tokio::test]
async fn inventories_cannot_be_omitted_reordered_or_substituted() {
    let l = Lab::new(3).await;
    l.enable();
    let rows = l.inventories().await;
    assert!(l.c.relay_candidate(NAME, &[&rows[0], &rows[2]]).is_err());
    assert!(
        l.c.relay_candidate(NAME, &[&rows[1], &rows[0], &rows[2]])
            .is_err()
    );
    assert!(
        l.c.relay_candidate(NAME, &[&rows[0], &rows[0], &rows[2]])
            .is_err()
    );
    l.no_media();
}
#[tokio::test]
async fn intermediate_and_terminal_roles_are_mandatory() {
    for (i, role) in [
        (0, json!("config")),
        (0, json!("user")),
        (0, json!("external")),
        (1, json!("remote")),
        (1, json!("external")),
    ] {
        let l = Lab::new(2).await;
        l.enable();
        l.servers[i].rows.lock().unwrap()[0]["named_by"] = role;
        let rows = l.inventories().await;
        assert!(
            l.c.relay_candidate(NAME, &rows.iter().collect::<Vec<_>>())
                .is_err()
        );
        l.no_media();
    }
    let l = Lab::new(2).await;
    l.enable();
    l.servers[0].rows.lock().unwrap()[0]
        .as_object_mut()
        .unwrap()
        .remove("named_by");
    let rows = l.inventories().await;
    assert!(
        l.c.relay_candidate(NAME, &rows.iter().collect::<Vec<_>>())
            .is_err()
    );
}
#[tokio::test]
async fn every_hop_requires_enabled_explicit_memberships_and_existing_stream() {
    for i in 0..3 {
        for mode in 0..3 {
            let l = Lab::new(3).await;
            l.enable();
            {
                let mut row = l.servers[i].rows.lock().unwrap();
                match mode {
                    0 => {
                        row[0]["disabled"] = json!(true);
                    }
                    1 => {
                        row[0].as_object_mut().unwrap().remove("groups");
                    }
                    _ => {
                        *row = json!([]);
                    }
                }
            }
            let rows = l.inventories().await;
            assert!(
                l.c.relay_candidate(NAME, &rows.iter().collect::<Vec<_>>())
                    .is_err()
            );
            l.no_media();
        }
    }
}
#[tokio::test]
async fn intermediate_filters_and_unresolved_local_policy_are_not_bypassed() {
    for mode in 0..2 {
        let mut l = Lab::new(3).await;
        l.enable();
        if mode == 0 {
            l.config
                .put("sources", &l.servers[1].key, json!({"except":[NAME]}))
                .unwrap();
            l.handles = l
                .servers
                .iter()
                .map(|s| install(&l.c, s, Some((&json!({"on_play":"true"}), &json!({})))))
                .collect();
        } else {
            l.handles[1] = install(&l.c, &l.servers[1], None);
        }
        let rows = l.inventories().await;
        assert!(
            l.c.relay_candidate(NAME, &rows.iter().collect::<Vec<_>>())
                .is_err()
        );
        l.no_media();
    }
}
#[tokio::test]
async fn conflicting_intermediate_group_policy_fails_closed() {
    let mut l = Lab::new(3).await;
    l.enable();
    l.servers[1].rows.lock().unwrap()[0]["groups"] = json!(["a", "b"]);
    l.handles[1] = install(
        &l.c,
        &l.servers[1],
        Some((
            &json!({"on_play":"true"}),
            &json!({"a":{"on_play":"https://one.invalid"},"b":{"on_play":"https://two.invalid"}}),
        )),
    );
    let rows = l.inventories().await;
    assert!(
        l.c.relay_candidate(NAME, &rows.iter().collect::<Vec<_>>())
            .is_err()
    );
    l.no_media();
}
#[tokio::test]
async fn ordinary_candidates_still_reject_relay_and_route_edits_do_not_revoke_direct_origin() {
    let l = Lab::new(2).await;
    l.enable();
    let rows = l.inventories().await;
    assert!(rows[0].candidate(NAME).is_err());
    let direct = rows[1].candidate(NAME).unwrap();
    let r = l.candidate(&rows);
    l.c.replace_relay_routes(vec![l.route()]).unwrap();
    stale(&l.c, &r);
    l.c.publish(&direct, |_, _| ()).unwrap();
    let r = l.candidate(&rows);
    publish(&l.c, &r).unwrap();
    l.c.replace_relay_routes(vec![]).unwrap();
    stale(&l.c, &r);
    l.c.publish(&direct, |_, _| ()).unwrap();
}

#[tokio::test]
async fn on_demand_entry_and_user_origin_preserve_selection_classification() {
    let l = Lab::new(2).await;
    l.enable();
    l.servers[0].rows.lock().unwrap()[0]["static"] = json!(false);
    l.servers[1].rows.lock().unwrap()[0]["named_by"] = json!("user");
    let rows = l.inventories().await;
    let candidate = l.candidate(&rows);
    l.c.publish_relay(&candidate, |view, _| {
        assert_eq!(
            view.kind,
            flussonix::source_directory::flussonic::CandidateKind::OnDemand
        );
        assert!(!view.selected_static);
    })
    .unwrap();
    l.no_media();
}
#[tokio::test]
async fn unrelated_global_edit_fences_the_whole_path() {
    let l = Lab::new(3).await;
    l.enable();
    let rows = l.inventories().await;
    let r = l.candidate(&rows);
    l.config.put("templates", "unrelated", json!({})).unwrap();
    stale(&l.c, &r);
    l.no_media();
}
#[tokio::test]
async fn replacing_any_source_at_same_config_revision_fences_the_path() {
    for i in 0..3 {
        let l = Lab::new(3).await;
        l.enable();
        let rows = l.inventories().await;
        let r = l.candidate(&rows);
        install(
            &l.c,
            &l.servers[i],
            Some((&json!({"on_play":"true"}), &json!({}))),
        );
        stale(&l.c, &r);
        l.no_media();
    }
}
#[tokio::test]
async fn alias_edit_and_restoration_never_revive_an_old_path() {
    let l = Lab::new(3).await;
    l.enable();
    let rows = l.inventories().await;
    let r = l.candidate(&rows);
    l.c.replace_aliases(&["new-alias"]).unwrap();
    stale(&l.c, &r);
    l.c.replace_aliases(&[]).unwrap();
    stale(&l.c, &r);
    l.no_media();
}
#[tokio::test]
async fn every_hop_expiry_prevents_publication_without_media_io() {
    for short in 0..3 {
        let l = Lab::new(3).await;
        l.enable();
        let mut rows = vec![];
        for (i, h) in l.handles.iter().enumerate() {
            rows.push(
                l.c.inventory(
                    h,
                    if i == short {
                        Duration::from_millis(150)
                    } else {
                        Duration::from_secs(20)
                    },
                )
                .await
                .unwrap(),
            );
        }
        let r = l.candidate(&rows);
        publish(&l.c, &r).unwrap();
        tokio::time::sleep_until((r.expires_at() + Duration::from_millis(5)).into()).await;
        assert_eq!(
            publish(&l.c, &r),
            Err(RelayFailure::Coordination(CoordinationFailure::Lease(
                flussonix::source_directory::flussonic_config::LeaseFailure::Expired
            )))
        );
        l.no_media();
    }
}
#[tokio::test]
async fn every_hop_newest_refresh_supersedes_old_path_even_when_rows_are_equal() {
    for i in 0..3 {
        let l = Lab::new(3).await;
        l.enable();
        let rows = l.inventories().await;
        let r = l.candidate(&rows);
        l.c.inventory(&l.handles[i], Duration::from_secs(20))
            .await
            .unwrap();
        stale(&l.c, &r);
        l.no_media();
    }
}
#[tokio::test]
async fn held_failed_or_cancelled_non_entry_refresh_cannot_restore_old_path() {
    for cancel in [false, true] {
        let l = Lab::new(3).await;
        l.enable();
        let rows = l.inventories().await;
        let r = l.candidate(&rows);
        let (rx, tx) = l.servers[1].hold_probe();
        let c = l.c.clone();
        let h = l.handles[1].clone();
        let task = tokio::spawn(async move { c.inventory(&h, Duration::from_secs(20)).await });
        entered(rx).await;
        stale(&l.c, &r);
        if cancel {
            task.abort();
            match task.await {
                Err(error) => assert!(error.is_cancelled()),
                Ok(_) => panic!("cancelled refresh returned"),
            };
            drop(tx);
        } else {
            tx.send(StatusCode::SERVICE_UNAVAILABLE).unwrap();
            assert!(task.await.unwrap().is_err());
        }
        stale(&l.c, &r);
        l.no_media();
    }
}
#[tokio::test]
async fn cross_coordinator_inventory_and_candidate_cannot_substitute() {
    let l = Lab::new(2).await;
    l.enable();
    let other = ModernSourceCoordinator::new(l.config.clone(), "other-local").unwrap();
    other.replace_relay_routes(vec![l.route()]).unwrap();
    let mut rows = vec![];
    for s in &l.servers {
        let h = install(&other, s, Some((&json!({"on_play":"true"}), &json!({}))));
        rows.push(other.inventory(&h, Duration::from_secs(20)).await.unwrap());
    }
    assert!(
        l.c.relay_candidate(NAME, &rows.iter().collect::<Vec<_>>())
            .is_err()
    );
    let r = other
        .relay_candidate(NAME, &rows.iter().collect::<Vec<_>>())
        .unwrap();
    stale(&l.c, &r);
    l.no_media();
}
#[tokio::test]
async fn source_remove_readd_requires_fresh_handles_and_inventory() {
    let mut l = Lab::new(3).await;
    l.enable();
    let rows = l.inventories().await;
    let r = l.candidate(&rows);
    l.config.delete("sources", &l.servers[1].key).unwrap();
    l.config
        .put("sources", &l.servers[1].key, l.servers[1].source())
        .unwrap();
    stale(&l.c, &r);
    l.handles = l
        .servers
        .iter()
        .map(|s| install(&l.c, s, Some((&json!({"on_play":"true"}), &json!({})))))
        .collect();
    let rows = l.inventories().await;
    publish(&l.c, &l.candidate(&rows)).unwrap();
    stale(&l.c, &r);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn path_and_configuration_edits_and_non_entry_refresh_wait_for_publication() {
    let l = Lab::new(3).await;
    l.enable();
    let rows = l.inventories().await;
    let r = Arc::new(l.candidate(&rows));
    let (in_tx, in_rx) = std::sync::mpsc::channel();
    let (out_tx, out_rx) = std::sync::mpsc::channel();
    let c = l.c.clone();
    let candidate = r.clone();
    let p = std::thread::spawn(move || {
        c.publish_relay(&candidate, |_, _| {
            in_tx.send(()).unwrap();
            out_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        })
    });
    in_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let (attempt_tx, attempt_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let c = l.c.clone();
    let route = l.route();
    let a = attempt_tx.clone();
    let d = done_tx.clone();
    let edit = std::thread::spawn(move || {
        a.send(()).unwrap();
        c.replace_relay_routes(vec![route]).unwrap();
        d.send(()).unwrap();
    });
    let config = l.config.clone();
    let a = attempt_tx.clone();
    let d = done_tx.clone();
    let cfg = std::thread::spawn(move || {
        a.send(()).unwrap();
        config.put("templates", "held", json!({})).unwrap();
        d.send(()).unwrap();
    });
    let c = l.c.clone();
    let h = l.handles[1].clone();
    let a = attempt_tx.clone();
    let d = done_tx.clone();
    let refresh = tokio::spawn(async move {
        a.send(()).unwrap();
        let result = c.inventory(&h, Duration::from_secs(20)).await;
        d.send(()).unwrap();
        result
    });
    for _ in 0..3 {
        attempt_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    }
    assert!(done_rx.recv_timeout(Duration::from_millis(80)).is_err());
    out_tx.send(()).unwrap();
    p.join().unwrap().unwrap();
    edit.join().unwrap();
    cfg.join().unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(3), refresh)
        .await
        .unwrap()
        .unwrap();
    for _ in 0..3 {
        done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    }
    stale(&l.c, &r);
    l.no_media();
}
