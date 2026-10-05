//! Shared listener playback; the listener is disabled unless explicitly enabled.
mod native;
mod selection;
use crate::{
    playback_auth::ViewerRequest,
    server::{App, ts_access::Playback},
};
pub use native::{Listener, Socket};
pub use selection::Selection;
use std::{
    net::SocketAddr,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

struct Slot {
    _permit: OwnedSemaphorePermit,
    app: Arc<App>,
}
impl Drop for Slot {
    fn drop(&mut self) {
        self.app.srt_connections.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Listener lifetime owns all pending admission and delivery tasks. A
/// handshake can finish before policy admission, but media cannot start then.
pub async fn serve(
    listener: Listener,
    app: Arc<App>,
    cancel: CancellationToken,
) -> std::io::Result<()> {
    app.set_srt_playback(Some(&listener));
    let stop = cancel.child_token();
    let slots = Arc::new(Semaphore::new(listener.settings().client_limit()));
    let mut tasks = JoinSet::new();
    let mut tick = tokio::time::interval(Duration::from_millis(5));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let result = 'serving: loop {
        tokio::select! {biased;
            _=cancel.cancelled()=>break Ok(()),
            result=tasks.join_next(),if !tasks.is_empty()=>{if result.is_some_and(|r|r.is_err()){break Err(std::io::Error::other("SRT viewer task failed"));}},
            _=tick.tick()=>{
                // Bound work per loop even when hostile callers keep the queue full.
                for _ in 0..32 {
                    let accepted=match listener.accept(){Ok(v)=>v,Err(e)=>break 'serving Err(e)};
                    let Some((socket,peer))=accepted else {break};
                    let Ok(permit)=slots.clone().try_acquire_owned() else {drop(socket);continue;};
                    app.srt_connections.fetch_add(1,Ordering::Relaxed);let slot=Slot{_permit:permit,app:app.clone()};let a=app.clone();let c=stop.clone();let address=listener.address();
                    tasks.spawn(async move {let _slot=slot;connection(socket,peer,address,a,c).await;});
                }
            }
        }
    };
    stop.cancel();
    while tasks.join_next().await.is_some() {}
    app.set_srt_playback(None);
    result
}
async fn connection(
    socket: Socket,
    peer: SocketAddr,
    address: SocketAddr,
    app: Arc<App>,
    cancel: CancellationToken,
) {
    let Ok(id) = socket.stream_id() else { return };
    let Ok(selection) = Selection::parse(&id) else {
        return;
    };
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("streamid", &id)
        .finish();
    let viewer = ViewerRequest {
        name: selection.name,
        proto: "srt".into(),
        ip: peer.ip().to_string(),
        token: selection.token,
        qs: query,
        host: address.to_string(),
        ..Default::default()
    };
    let admitted = tokio::select! {biased;_=cancel.cancelled()=>return,result=tokio::time::timeout(Duration::from_secs(10),app.ts_admit(viewer))=>result};
    let Ok(Ok(mut playback)) = admitted else {
        return;
    };
    if cancel.is_cancelled() || !socket.is_connected() || !app.ts_current(&playback).await {
        return;
    }
    let mut receiver = playback.worker.subscribe();
    playback.attach();
    let mut check = tokio::time::interval(Duration::from_millis(250));
    check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut deadline = Instant::now() + Duration::from_secs(10);
    loop {
        tokio::select! {biased;
            _=cancel.cancelled()=>break,
            _=playback.grant.cancelled()=>break,
            _=playback.worker.closed()=>break,
            _=tokio::time::sleep_until(deadline)=>break,
            _=check.tick()=>{if !socket.is_connected()||!app.ts_current(&playback).await {break;}},
            data=receiver.recv()=>{
                let Ok(data)=data else {break}; // Overflow closes just this viewer.
                if !app.ts_current(&playback).await {break;}
                let mut revision=app.config.revision();let mut sent=true;
                for chunk in data.chunks(1316) {
                    if send(&socket,chunk,&app,&playback,&cancel,&mut revision).await.is_err(){sent=false;break;}
                }
                if !sent {break;}deadline=Instant::now()+Duration::from_secs(10);
            }
        }
    }
}
async fn send(
    socket: &Socket,
    data: &[u8],
    app: &App,
    playback: &Playback,
    cancel: &CancellationToken,
    revision: &mut u64,
) -> Result<(), ()> {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if cancel.is_cancelled() || playback.grant.is_cancelled() || playback.worker.is_closed() {
            return Err(());
        }
        if app.config.revision() != *revision {
            if !app.ts_current(playback).await {
                return Err(());
            }
            *revision = app.config.revision();
        }
        match socket.try_send(data) {
            Ok(true) => {
                playback.grant.add_bytes(data.len());
                app.srt_egress
                    .fetch_add(data.len() as u64, Ordering::Relaxed);
                return Ok(());
            }
            Ok(false) => {}
            Err(_) => return Err(()),
        }
        // A pending write may span a policy/source update even without a local
        // revision change. Recheck before retrying instead of queuing stale media.
        let current = tokio::select! {biased;_=cancel.cancelled()=>return Err(()),_=playback.grant.cancelled()=>return Err(()),_=playback.worker.closed()=>return Err(()),value=app.ts_current(playback)=>value};
        if !current {
            return Err(());
        }
        tokio::select! {biased;_=cancel.cancelled()=>return Err(()),_=playback.grant.cancelled()=>return Err(()),_=playback.worker.closed()=>return Err(()),_=tokio::time::sleep_until(deadline)=>return Err(()),_=tokio::time::sleep(Duration::from_millis(2))=>{}}
    }
}
#[derive(Clone)]
pub struct Settings {
    latency_millis: u32,
    client_limit: usize,
    passphrase: String,
}
impl Settings {
    pub fn new(
        latency_millis: u32,
        client_limit: usize,
        passphrase: String,
    ) -> std::io::Result<Self> {
        if !(1..=10000).contains(&latency_millis) || !(1..=4096).contains(&client_limit) {
            return Err(std::io::Error::other("invalid SRT playback limits"));
        }
        if !passphrase.is_empty()
            && (!(10..=79).contains(&passphrase.len())
                || !passphrase.bytes().all(|b| (0x20..=0x7e).contains(&b)))
        {
            return Err(std::io::Error::other("invalid SRT playback passphrase"));
        }
        Ok(Self {
            latency_millis,
            client_limit,
            passphrase,
        })
    }
    pub fn latency_millis(&self) -> u32 {
        self.latency_millis
    }
    pub fn client_limit(&self) -> usize {
        self.client_limit
    }
    pub fn encrypted(&self) -> bool {
        !self.passphrase.is_empty()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn settings_validate_exact_bounds_without_secret_diagnostics() {
        assert!(Settings::new(120, 128, String::new()).is_ok());
        assert!(Settings::new(1, 1, "0123456789".into()).is_ok());
        assert!(Settings::new(10000, 4096, "a".repeat(79)).is_ok());
        for (latency, limit, secret) in [
            (0, 128, ""),
            (10001, 128, ""),
            (120, 0, ""),
            (120, 4097, ""),
            (120, 128, "short"),
            (120, 128, "owned\nsecret"),
            (120, 128, "nonASCIIésecret"),
        ] {
            let error = Settings::new(latency, limit, secret.into())
                .err()
                .unwrap()
                .to_string();
            assert!(!error.contains(secret) || secret.is_empty());
        }
        assert!(Settings::new(120, 128, "a".repeat(80)).is_err());
    }
}
