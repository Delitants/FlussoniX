use clap::Parser;
use flussonix::server::{App, Options, router};
use std::{net::SocketAddr, path::PathBuf};
#[derive(Parser)]
#[command(name = "flussonix", version, about = "Independent live media server")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:18210")]
    listen: SocketAddr,
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
    #[arg(long)]
    drain: bool,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let a = Args::parse();
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
        client_limit: a.client_limit,
        web_dir: a.web_dir,
        drain: a.drain,
    };
    let app = App::new(a.config, a.media_dir, options)?;
    let listener = tokio::net::TcpListener::bind(a.listen).await?;
    println!(
        "{}",
        serde_json::json!({"service":"FlussoniX","listen":listener.local_addr()?.to_string(),"version":env!("CARGO_PKG_VERSION")})
    );
    app.reconcile().await;
    let background = app.clone();
    let cancel = tokio_util::sync::CancellationToken::new();
    let bg_cancel = cancel.clone();
    let supervisor = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            tokio::select! {_=bg_cancel.cancelled()=>break,_=interval.tick()=>background.reconcile().await}
        }
    });
    axum::serve(
        listener,
        router(app.clone()).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown())
    .await?;
    cancel.cancel();
    let _ = supervisor.await;
    app.media.stop_all().await;
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
