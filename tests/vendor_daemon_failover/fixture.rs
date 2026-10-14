use axum::{
    Router,
    body::Body,
    extract::OriginalUri,
    http::{HeaderMap, Request, StatusCode},
    response::Response,
    routing::get,
};
use flussonix::{
    m4f::Frame,
    m4s::Track,
    server::{App, Options, router},
    wire,
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
type Hold = Arc<
    Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
>;
pub(super) struct Origin {
    pub hold: Hold,
    pub stall_metadata: Arc<AtomicBool>,
    pub key: String,
    pub api: Arc<Mutex<StatusCode>>,
    pub media: Arc<Mutex<StatusCode>>,
    pub rows: Arc<Mutex<Value>>,
    pub epoch: Arc<Mutex<CancellationToken>>,
    pub private_calls: Arc<AtomicUsize>,
    pub management_calls: Arc<AtomicUsize>,
    pub secret: String,
    task: tokio::task::JoinHandle<()>,
}
fn digest(value: &str) -> String {
    ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, value.as_bytes())
        .as_ref()
        .iter()
        .map(|x| format!("{x:02x}"))
        .collect()
}
impl Origin {
    pub async fn start(id: &'static str) -> Self {
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let key = format!("m4s://{}", tcp.local_addr().unwrap());
        let secret = format!("owned-{id}-private-key");
        let api = Arc::new(Mutex::new(StatusCode::OK));
        let media = Arc::new(Mutex::new(StatusCode::OK));
        let rows = Arc::new(Mutex::new(
            json!([{"name":"owned","named_by":"config","static":true,"groups":[]}]),
        ));
        let epoch = Arc::new(Mutex::new(CancellationToken::new()));
        let private_calls = Arc::new(AtomicUsize::new(0));
        let management_calls = Arc::new(AtomicUsize::new(0));
        let hold: Hold = Arc::new(Mutex::new(None));
        let held = hold.clone();
        let stall_metadata = Arc::new(AtomicBool::new(false));
        let stalled = stall_metadata.clone();
        let (a, m, r, e, p, c, k) = (
            api.clone(),
            media.clone(),
            rows.clone(),
            epoch.clone(),
            private_calls.clone(),
            management_calls.clone(),
            secret.clone(),
        );
        let service=Router::new().fallback(get(move|OriginalUri(uri):OriginalUri,headers:HeaderMap|{
   let held=held.clone();let stalled=stalled.clone();
   let (a,m,r,e,p,c,k)=(a.clone(),m.clone(),r.clone(),e.clone(),p.clone(),c.clone(),k.clone());async move{
    assert!(!headers.contains_key("x-flussonix-peer"));
    if let Some(name)=uri.path().strip_prefix('/').and_then(|p|p.strip_suffix("/m4s")) {
     p.fetch_add(1,Ordering::SeqCst);assert!(uri.query().is_none());assert!(!headers.contains_key("authorization"));
     let time=headers["cluster-auth-time"].to_str().unwrap();assert_eq!(headers["cluster-auth-key"],digest(&format!("{time}:{k}:{name}")));
     let status=*m.lock().unwrap();if status!=StatusCode::OK {let mut out=Response::new(Body::empty());*out.status_mut()=status;return out;}
     if stalled.load(Ordering::SeqCst) {
         let mut out=Response::new(Body::from_stream(futures_util::stream::pending::<Result<bytes::Bytes,std::io::Error>>()));
         out.headers_mut().insert("cluster-auth","yes".parse().unwrap());return out;
     }
     let stop=e.lock().unwrap().clone();let track=Track{id:1,codec:"mp3".into(),config:vec![]};
     let stream=futures_util::stream::unfold((0u64,track,stop),|(i,track,stop)|async move{
      if stop.is_cancelled(){return None;}
      let bytes=if i==0 {wire::encode_info(std::slice::from_ref(&track)).unwrap()} else {
       tokio::select!{biased;_=stop.cancelled()=>return None,_=tokio::time::sleep(Duration::from_millis(3))=>{}}
       wire::encode_frame(&track,&Frame{track_id:1,dts:90_000+(i-1)*2351,pts_offset:0,key:true,body:include_bytes!("../fixtures/codecs/mp3.bin").to_vec()}).unwrap()
      };Some((Ok::<_,std::io::Error>(bytes::Bytes::from(bytes)),(i+1,track,stop)))
     });let mut out=Response::new(Body::from_stream(stream));out.headers_mut().insert("cluster-auth","yes".parse().unwrap());return out;
    }
    c.fetch_add(1,Ordering::SeqCst);assert!(!headers.contains_key("cluster-auth-key"));
    if uri.path().ends_with("/config") {
        let pause=held.lock().unwrap().take();
        if let Some((started,release))=pause {let _=started.send(());let _=release.await;}
    }
    let auth=headers["authorization"].to_str().unwrap().strip_prefix("Cluster ").unwrap();let (time,hash)=auth.split_once(':').unwrap();assert_eq!(hash,digest(&format!("{time}:{k}")));
    let status=*a.lock().unwrap();if status!=StatusCode::OK {let mut out=Response::new(Body::empty());*out.status_mut()=status;return out;}
    let value=if uri.path().ends_with("/config"){json!({"stats":{"id":id}})}else{assert_eq!(uri.path(),"/streamer/api/v3/streams");json!({"server_id":id,"streams":r.lock().unwrap().clone()})};Response::new(Body::from(value.to_string()))
   }
  }));
        let task = tokio::spawn(async move { axum::serve(tcp, service).await.unwrap() });
        Self {
            hold,
            stall_metadata,
            key,
            api,
            media,
            rows,
            epoch,
            private_calls,
            management_calls,
            secret,
            task,
        }
    }
    pub fn close_media(&self) {
        self.epoch.lock().unwrap().cancel();
        *self.epoch.lock().unwrap() = CancellationToken::new();
    }
    pub async fn stop(mut self) {
        self.task.abort();
        let _ = (&mut self.task).await;
    }
}
impl Drop for Origin {
    fn drop(&mut self) {
        self.task.abort();
    }
}
pub(super) struct Fixture {
    pub app: Arc<App>,
    pub origins: Vec<Origin>,
    cancel: CancellationToken,
    owner: Option<tokio::task::JoinHandle<Result<(), String>>>,
    _dir: tempfile::TempDir,
}
impl Fixture {
    pub async fn new() -> Self {
        Self::with_role("standalone").await
    }
    pub async fn with_role(role: &str) -> Self {
        Self::configured(role, json!({"only":[]}), None, true).await
    }
    pub async fn configured(role: &str, patch: Value, rows: Option<Value>, grouped: bool) -> Self {
        let origins = vec![
            Origin::start("owned-origin-a").await,
            Origin::start("owned-origin-b").await,
        ];
        let dir = tempfile::tempdir().unwrap();
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            Options {
                role: role.into(),
                uplink_interface: "process".into(),
                admin_password: "owned-admin".into(),
                peer_key: "owned-native-peer".into(),
                ..Default::default()
            },
        )
        .unwrap();
        if patch["flussonix_output_template"] == "owned-aac" {
            app.config.put("templates","owned-aac",json!({"transcoder":{"encoder":"copy","acodec":"aac","ab":96},"flussonix_hls_subtitles":"drop"})).unwrap();
        }
        for origin in origins.iter().take(if grouped { 2 } else { 1 }) {
            if let Some(rows) = &rows {
                *origin.rows.lock().unwrap() = rows.clone();
            }
            let mut source = json!({"flussonix_source_kind":"flussonic","cluster_key":origin.secret,"on_play":"true"});
            if grouped {
                source["flussonix_source_group"] = json!("music-origins");
                source["flussonix_content_id"] = json!("explicit-music-v1");
            }
            for (key, value) in patch.as_object().unwrap() {
                source[key] = value.clone();
            }
            app.config.put("sources", &origin.key, source).unwrap();
        }
        let cancel = CancellationToken::new();
        let (a, c) = (app.clone(), cancel.clone());
        let owner = tokio::spawn(async move { a.run_vendor_playback(c).await });
        tokio::task::yield_now().await;
        Self {
            app,
            origins,
            cancel,
            owner: Some(owner),
            _dir: dir,
        }
    }
    pub async fn get(&self, path: &str) -> Response {
        tokio::time::timeout(
            Duration::from_secs(12),
            router(self.app.clone())
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap()),
        )
        .await
        .unwrap()
        .unwrap()
    }
    pub async fn playlist(&self) -> String {
        tokio::time::timeout(Duration::from_secs(12), async {
            loop {
                let reply = self.get("/owned/index.m3u8").await;
                if reply.status() == StatusCode::SERVICE_UNAVAILABLE {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                assert_eq!(reply.status(), StatusCode::OK);
                let bytes = reply.into_body().collect().await.unwrap().to_bytes();
                let text = String::from_utf8(bytes.to_vec()).unwrap();
                if text.contains("#EXTINF:") {
                    return text;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("owned HLS never became ready")
    }
    pub async fn wait_private(&self, index: usize, count: usize) {
        tokio::time::timeout(Duration::from_secs(8), async {
            while self.origins[index].private_calls.load(Ordering::SeqCst) < count {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("background recovery did not pull the expected replica");
    }
    pub async fn stop(mut self) {
        self.cancel.cancel();
        assert!(self.owner.take().unwrap().await.unwrap().is_ok());
        self.app.media.stop_all().await;
        for origin in self.origins.drain(..) {
            origin.stop().await;
        }
        assert_eq!(self.app.playback_auth.live_grants(), 0);
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(owner) = &self.owner {
            owner.abort();
        }
    }
}
