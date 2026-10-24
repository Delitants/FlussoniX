use flussonix::{
    iptv::store::{IptvDocument, IptvStore},
    server::{App, Options},
};
use serde_json::{Value, json};
use std::{fs, path::Path, time::SystemTime};

fn replace(store: &IptvStore, value: Value) -> Result<(), String> {
    let document: IptvDocument = serde_json::from_value(value).unwrap();
    store.transact(|current| {
        *current = document;
        Ok(())
    })?;
    Ok(())
}

fn fixture() -> Value {
    json!({
        "version": 1,
        "enabled": true,
        "packages": [{"name":"sports","channels":["news", "staged/offline"]}],
        "subscribers": [{"name":"alice","token":"private-alice-token","packages":["sports"],"max_sessions":2}]
    })
}

fn open(path: &Path) -> std::sync::Arc<IptvStore> {
    IptvStore::open(path.to_path_buf()).unwrap()
}

#[test]
fn absent_file_starts_disabled_and_denies_playback() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("iptv.json");
    let store = open(&path);
    assert!(!store.snapshot().enabled());
    assert_eq!(store.revision(), 0);
    assert!(
        store
            .snapshot()
            .authorize("anything", "news", SystemTime::now())
            .is_none()
    );
    assert!(!path.exists());
}

#[test]
fn committed_document_survives_restart_and_indexes_entitlements() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("iptv.json");
    let store = open(&path);
    replace(&store, fixture()).unwrap();
    assert_eq!(store.revision(), 1);
    let grant = store
        .snapshot()
        .authorize("private-alice-token", "news", SystemTime::now())
        .unwrap();
    assert_eq!(grant.subscriber, "alice");
    assert_eq!(grant.max_sessions, Some(2));
    assert_eq!(grant.revision, 1);
    assert!(
        store
            .snapshot()
            .authorize("wrong", "news", SystemTime::now())
            .is_none()
    );
    assert!(
        store
            .snapshot()
            .authorize("private-alice-token", "foreign", SystemTime::now())
            .is_none()
    );
    drop(store);
    let reloaded = open(&path);
    assert!(reloaded.snapshot().enabled());
    assert!(
        reloaded
            .snapshot()
            .authorize("private-alice-token", "news", SystemTime::now())
            .is_some()
    );
    assert!(
        fs::read_to_string(&path)
            .unwrap()
            .contains("staged/offline")
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&fs::read(path).unwrap()).unwrap()["version"],
        1
    );
}

#[test]
fn invalid_edits_never_change_active_snapshot_or_saved_document() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("iptv.json");
    let store = open(&path);
    replace(&store, fixture()).unwrap();
    let before = fs::read(&path).unwrap();
    let duplicate = json!({"version":1,"enabled":true,"packages":[{"name":"sports","channels":["news"]}],"subscribers":[
        {"name":"alice","token":"shared","packages":["sports"]},
        {"name":"bob","token":"shared","packages":["sports"]}
    ]});
    assert!(replace(&store, duplicate).is_err());
    let missing_package = json!({"version":1,"enabled":true,"packages":[],"subscribers":[
        {"name":"alice","token":"new-token","packages":["unknown"]}
    ]});
    assert!(replace(&store, missing_package).is_err());
    assert_eq!(store.revision(), 1);
    assert_eq!(fs::read(path).unwrap(), before);
    assert!(
        store
            .snapshot()
            .authorize("private-alice-token", "news", SystemTime::now())
            .is_some()
    );
}

#[test]
fn names_and_tokens_are_bounded_before_save() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("iptv.json");
    let store = open(&path);
    for invalid in [
        json!({"version":1,"enabled":true,"packages":[{"name":"a".repeat(257),"channels":[]}],"subscribers":[]}),
        json!({"version":1,"enabled":true,"packages":[{"name":"bad/../name","channels":[]}],"subscribers":[]}),
        json!({"version":1,"enabled":true,"packages":[{"name":"sports","channels":["x".repeat(257)]}],"subscribers":[]}),
        json!({"version":1,"enabled":true,"packages":[],"subscribers":[{"name":"alice","token":"t".repeat(4097),"packages":[]}]}),
        json!({"version":1,"enabled":true,"packages":[],"subscribers":[{"name":"alice","token":"a\nb","packages":[]}]}),
    ] {
        assert!(replace(&store, invalid).is_err());
        assert_eq!(store.revision(), 0);
    }
    assert!(!path.exists());
}

#[test]
fn collection_limits_reject_oversized_documents() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir.path().join("iptv.json"));
    let packages: Vec<_> = (0..1025)
        .map(|n| json!({"name":format!("p{n}"),"channels":[]}))
        .collect();
    assert!(
        replace(
            &store,
            json!({"version":1,"enabled":true,"packages":packages,"subscribers":[]})
        )
        .is_err()
    );
    let channels: Vec<_> = (0..4097).map(|n| format!("c{n}")).collect();
    assert!(replace(&store, json!({"version":1,"enabled":true,"packages":[{"name":"p","channels":channels}],"subscribers":[]})).is_err());
    let packages: Vec<_> = (0..65)
        .map(|n| json!({"name":format!("p{n}"),"channels":[]}))
        .collect();
    let memberships: Vec<_> = (0..65).map(|n| format!("p{n}")).collect();
    assert!(replace(&store, json!({"version":1,"enabled":true,"packages":packages,"subscribers":[{"name":"alice","token":"a","packages":memberships}]})).is_err());
    let subscribers: Vec<_> = (0..100001)
        .map(|n| json!({"name":format!("s{n}"),"token":format!("t{n}"),"packages":[]}))
        .collect();
    assert!(
        replace(
            &store,
            json!({"version":1,"enabled":true,"packages":[],"subscribers":subscribers})
        )
        .is_err()
    );
    assert_eq!(store.revision(), 0);
}

#[test]
fn oversized_corrupt_or_unsafe_existing_files_refuse_startup() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("iptv.json");
    fs::write(&path, b"{").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    }
    assert!(IptvStore::open(path.clone()).is_err());
    fs::remove_file(&path).unwrap();
    let file = fs::File::create(&path).unwrap();
    file.set_len(64 * 1024 * 1024 + 1).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    }
    assert!(IptvStore::open(path.clone()).is_err());
    fs::remove_file(&path).unwrap();
    let source = dir.path().join("real.json");
    fs::write(&source, serde_json::to_vec(&fixture()).unwrap()).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&source, fs::Permissions::from_mode(0o600)).unwrap();
        std::os::unix::fs::symlink(&source, &path).unwrap();
        assert!(IptvStore::open(path.clone()).is_err());
        fs::remove_file(&path).unwrap();
        fs::set_permissions(&source, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(IptvStore::open(source).is_err());
    }
}

#[cfg(unix)]
#[test]
fn nonregular_iptv_fifo_is_rejected_without_blocking_startup() {
    use std::{ffi::CString, os::unix::ffi::OsStrExt, sync::mpsc, time::Duration};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("iptv.json");
    let cpath = CString::new(path.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(cpath.as_ptr(), 0o600) }, 0);
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(IptvStore::open(path).is_err());
    });
    assert_eq!(rx.recv_timeout(Duration::from_millis(500)), Ok(true));
}

#[test]
fn failed_disk_write_keeps_previous_enabled_state_and_grants() {
    let dir = tempfile::tempdir().unwrap();
    let parent = dir.path().join("data");
    fs::create_dir(&parent).unwrap();
    let path = parent.join("iptv.json");
    let store = open(&path);
    replace(&store, fixture()).unwrap();
    fs::rename(&parent, dir.path().join("moved")).unwrap();
    fs::write(&parent, b"blocks directory creation").unwrap();
    assert!(
        store
            .transact(|doc| {
                doc.enabled = false;
                Ok(())
            })
            .is_err()
    );
    assert_eq!(store.revision(), 1);
    assert!(store.snapshot().enabled());
    assert!(
        store
            .snapshot()
            .authorize("private-alice-token", "news", SystemTime::now())
            .is_some()
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&fs::read(dir.path().join("moved/iptv.json")).unwrap())
            .unwrap()["enabled"],
        true
    );
}

#[cfg(unix)]
#[test]
fn saved_file_is_owner_only() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("iptv.json");
    let store = open(&path);
    replace(&store, fixture()).unwrap();
    assert_eq!(
        fs::metadata(path).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

#[test]
fn app_loads_separate_iptv_file_without_exporting_tokens_to_main_config() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.json");
    let custom = dir.path().join("private/subscribers.json");
    fs::create_dir(custom.parent().unwrap()).unwrap();
    let store = open(&custom);
    replace(&store, fixture()).unwrap();
    drop(store);
    let app = App::new(
        &config,
        dir.path().join("media"),
        Options {
            admin_password: "local-admin-secret".into(),
            peer_key: "local-peer-secret".into(),
            iptv_data: Some(custom),
            ..Options::default()
        },
    )
    .unwrap();
    assert!(app.iptv.snapshot().enabled());
    assert!(
        app.iptv
            .snapshot()
            .authorize("private-alice-token", "news", SystemTime::now())
            .is_some()
    );
    assert!(
        !app.config
            .snapshot()
            .to_string()
            .contains("private-alice-token")
    );
}

#[test]
fn app_defaults_iptv_data_to_config_sibling() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("server.json");
    let sibling = dir.path().join("iptv.json");
    replace(&open(&sibling), fixture()).unwrap();
    let app = App::new(
        &config,
        dir.path().join("media"),
        Options {
            admin_password: "local-admin-secret".into(),
            peer_key: "local-peer-secret".into(),
            ..Options::default()
        },
    )
    .unwrap();
    assert!(
        app.iptv
            .snapshot()
            .authorize("private-alice-token", "news", SystemTime::now())
            .is_some()
    );
}
