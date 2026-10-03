use clap::Parser;
use flussonix::server::{App, Options, router};
use std::{future::IntoFuture, net::SocketAddr, path::PathBuf, time::Duration};
#[derive(Parser)]
#[command(name = "flussonix", version, about = "Independent live media server")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:18210")]
    listen: SocketAddr,
    /// Optional RTSP/1.0 TCP playback listener (disabled by default).
    #[arg(long)]
    rtsp_listen: Option<SocketAddr>,
    /// Optional RTSPS playback listener; control and interleaved media use TLS.
    #[arg(long, requires_all = ["rtsps_cert", "rtsps_key"])]
    rtsps_listen: Option<SocketAddr>,
    #[arg(long, requires = "rtsps_listen")]
    rtsps_cert: Option<PathBuf>,
    #[arg(long, requires = "rtsps_listen")]
    rtsps_key: Option<PathBuf>,
    /// Opt-in inclusive UDP RTP/RTCP port range (even-first/odd-last, 2..256 ports).
    #[arg(long, requires = "rtsp_listen")]
    rtsp_udp_ports: Option<flussonix::rtsp::udp::PortRange>,
    /// Per-viewer RTP application-data cap in Mbps (1..10000; default 100).
    #[arg(long, requires = "rtsp_udp_ports")]
    rtsp_udp_mbps: Option<f64>,
    #[arg(long, default_value = "config.json")]
    config: PathBuf,
    #[arg(long, default_value = "runtime/media")]
    media_dir: PathBuf,
    #[arg(long, default_value = "web/dist")]
    web_dir: PathBuf,
    #[arg(long, default_value = "ffmpeg")]
    ffmpeg: String,
    #[arg(long, env = "FLUSSONIX_ADMIN_USER", default_value = "admin")]
    admin_user: String,
    #[arg(long, env = "FLUSSONIX_ADMIN_PASSWORD", hide_env_values = true)]
    admin_password: String,
    #[arg(long, env = "FLUSSONIX_VIEW_USER")]
    view_user: Option<String>,
    #[arg(long, env = "FLUSSONIX_VIEW_PASSWORD", hide_env_values = true)]
    view_password: Option<String>,
    #[arg(long, env = "FLUSSONIX_PEER_KEY", hide_env_values = true)]
    peer_key: String,
    #[arg(long, default_value = "standalone")]
    role: String,
    #[arg(long, default_value = "local")]
    node_name: String,
    #[arg(long, default_value_t = 1000.0)]
    uplink_mbps: f64,
    #[arg(long, default_value_t = 1000)]
    client_limit: u64,
    #[arg(long, default_value = "auto")]
    uplink_interface: String,
    #[arg(long)]
    drain: bool,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let a = Args::parse();
    let udp_rate = a.rtsp_udp_mbps.unwrap_or(100.0);
    flussonix::rtsp::udp::Pacer::new(udp_rate)?;
    let tls_config = match (&a.rtsps_cert, &a.rtsps_key) {
        (Some(cert), Some(key)) => Some(flussonix::rtsp::tls::server(cert, key)?),
        _ => None,
    };
    let options = Options {
        admin_user: a.admin_user,
        admin_password: a.admin_password,
        view_user: a.view_user,
        view_password: a.view_password,
        peer_key: a.peer_key,
        ffmpeg: a.ffmpeg,
        role: a.role,
        node_name: a.node_name,
        uplink_mbps: a.uplink_mbps,
        uplink_interface: a.uplink_interface,
        client_limit: a.client_limit,
        web_dir: a.web_dir,
        drain: a.drain,
    };
    let app = App::new(a.config, a.media_dir, options)?;
    let listener = tokio::net::TcpListener::bind(a.listen).await?;
    let rtsp_listener = match a.rtsp_listen {
        Some(address) => Some(tokio::net::TcpListener::bind(address).await?),
        None => None,
    };
    let tls_listener = match a.rtsps_listen {
        Some(address) => Some(tokio::net::TcpListener::bind(address).await?),
        None => None,
    };
    let udp_pool = match (a.rtsp_udp_ports, rtsp_listener.as_ref()) {
        (Some(range), Some(listener)) => {
            Some(flussonix::rtsp::udp::Pool::bind(listener.local_addr()?.ip(), range).await?)
        }
        _ => None,
    };
    println!(
        "{}",
        serde_json::json!({"service":"FlussoniX","listen":listener.local_addr()?.to_string(),"rtsp_listen":rtsp_listener.as_ref().map(|l|l.local_addr().map(|a|a.to_string())).transpose()?,"rtsps_listen":tls_listener.as_ref().map(|l|l.local_addr().map(|a|a.to_string())).transpose()?,"rtsp_udp_ports":a.rtsp_udp_ports.map(|p|p.to_string()),"rtsp_udp_mbps":a.rtsp_udp_ports.map(|_|udp_rate),"version":env!("CARGO_PKG_VERSION")})
    );
    app.reconcile().await;
    let background = app.clone();
    let cancel = tokio_util::sync::CancellationToken::new();
    let mut rtsp_task = rtsp_listener.map(|listener| {
        tokio::spawn(flussonix::rtsp::serve_with_udp(
            listener,
            app.clone(),
            cancel.clone(),
            udp_pool,
            udp_rate,
        ))
    });
    let mut tls_task = tls_listener.map(|listener| {
        tokio::spawn(flussonix::rtsp::serve_tls(
            listener,
            app.clone(),
            cancel.clone(),
            tls_config.unwrap(),
        ))
    });
    let bg_cancel = cancel.clone();
    let supervisor = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            tokio::select! {_=bg_cancel.cancelled()=>break,_=interval.tick()=>background.reconcile().await}
        }
    });
    let auth_app = app.clone();
    let auth_cancel = cancel.clone();
    let authorization = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! { _=auth_cancel.cancelled()=>break, _=interval.tick()=>auth_app.playback_auth.renew_due().await }
        }
    });
    let telemetry_app = app.clone();
    let telemetry_cancel = cancel.clone();
    let telemetry = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! { _=telemetry_cancel.cancelled()=>break, _=interval.tick()=>telemetry_app.sample_metrics() }
        }
    });
    let serving = axum::serve(
        listener,
        router(app.clone()).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(cancel.clone().cancelled_owned())
    .into_future();
    tokio::pin!(serving);
    let mut rtsp_result = None;
    let mut tls_result = None;
    let completed = tokio::select! {
        result=&mut serving=>Some(result),
        result=async{match rtsp_task.as_mut(){Some(task)=>task.await,None=>std::future::pending().await}}=>{rtsp_result=Some(result);None},
        result=async{match tls_task.as_mut(){Some(task)=>task.await,None=>std::future::pending().await}}=>{tls_result=Some(result);None},
        _=shutdown()=>None
    };
    cancel.cancel();
    let _ = supervisor.await;
    let _ = authorization.await;
    let _ = telemetry.await;
    app.media.stop_all().await;
    if rtsp_result.is_none() {
        if let Some(task) = rtsp_task {
            rtsp_result = Some(task.await);
        }
    }
    if tls_result.is_none() {
        if let Some(task) = tls_task {
            tls_result = Some(task.await);
        }
    }
    if let Some(result) = completed {
        result?;
    } else if tokio::time::timeout(Duration::from_secs(5), &mut serving)
        .await
        .is_err()
    {
        tracing::warn!("connection drain exceeded five seconds");
    }
    if let Some(result) = rtsp_result {
        result??;
    }
    if let Some(result) = tls_result {
        result??;
    }
    Ok(())
}
async fn shutdown() {
    #[cfg(unix)]
    {
        let mut term =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        tokio::select! {_=tokio::signal::ctrl_c()=>{},_=term.recv()=>{}}
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
