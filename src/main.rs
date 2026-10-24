use clap::Parser;
use flussonix::server::{App, Options};
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::{net::SocketAddr, path::PathBuf, time::Duration};
#[derive(Parser)]
#[command(name = "flussonix", version, about = "Independent live media server")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:18210")]
    listen: SocketAddr,
    /// Optional HTTPS listener for media, publication, APIs and admin.
    #[arg(long, requires_all = ["https_cert", "https_key"])]
    https_listen: Option<SocketAddr>,
    #[arg(long, requires = "https_listen")]
    https_cert: Option<PathBuf>,
    #[arg(long, requires = "https_listen")]
    https_key: Option<PathBuf>,
    /// Disable plaintext HTTP binding; requires an HTTPS listener.
    #[arg(long, requires = "https_listen")]
    https_only: bool,
    /// Public HTTPS origin when a trusted reverse proxy terminates TLS for the admin UI.
    #[arg(long, env = "FLUSSONIX_ADMIN_PUBLIC_ORIGIN")]
    admin_public_origin: Option<url::Url>,
    /// Optional RTSP/1.0 playback and publication listener (disabled by default).
    #[arg(long)]
    rtsp_listen: Option<SocketAddr>,
    /// Optional RTSPS playback and publication listener; control and media use TLS.
    #[arg(long, requires_all = ["rtsps_cert", "rtsps_key"])]
    rtsps_listen: Option<SocketAddr>,
    #[arg(long, requires = "rtsps_listen")]
    rtsps_cert: Option<PathBuf>,
    #[arg(long, requires = "rtsps_listen")]
    rtsps_key: Option<PathBuf>,
    /// Optional encrypted FXLink native peer listener (disabled by default).
    #[arg(long, requires_all = ["fxlink_cert", "fxlink_key"])]
    fxlink_listen: Option<SocketAddr>,
    #[arg(long, requires = "fxlink_listen")]
    fxlink_cert: Option<PathBuf>,
    #[arg(long, requires = "fxlink_listen")]
    fxlink_key: Option<PathBuf>,
    /// Advertise experimental v6 trimmed records on FXLink clients and listeners.
    #[arg(long, env = "FLUSSONIX_FXLINK_TRIMMED_RECORDS")]
    fxlink_trimmed_records: bool,
    /// Advertise experimental v7 cached parity repairs; requires v6 layouts.
    #[arg(
        long,
        env = "FLUSSONIX_FXLINK_REPAIR_PARITY",
        requires_if("true", "fxlink_trimmed_records")
    )]
    fxlink_repair_parity: bool,
    /// Advertise experimental v8 bounded bootstrap repairs; requires v6/v7 opt-ins.
    #[arg(long, env = "FLUSSONIX_FXLINK_BOOTSTRAP_REPAIR",
        requires_ifs = [("true", "fxlink_trimmed_records"), ("true", "fxlink_repair_parity")])]
    fxlink_bootstrap_repair: bool,
    /// Advertise experimental v9 weak geometry; requires all v6/v7/v8 opt-ins.
    #[arg(long, env = "FLUSSONIX_FXLINK_GEOMETRY_HINTS",
        requires_ifs = [("true", "fxlink_trimmed_records"), ("true", "fxlink_repair_parity"), ("true", "fxlink_bootstrap_repair")])]
    fxlink_geometry_hints: bool,
    /// Shared UDP playback/publication port range (even-first/odd-last, 2..256 ports).
    #[arg(long, requires = "rtsp_listen")]
    rtsp_udp_ports: Option<flussonix::rtsp::udp::PortRange>,
    /// Per-viewer RTP application-data cap in Mbps (1..10000; default 100).
    #[arg(long, requires = "rtsp_udp_ports")]
    rtsp_udp_mbps: Option<f64>,
    /// Optional shared SRT playback UDP listener (disabled by default).
    #[arg(long)]
    srt_play_listen: Option<SocketAddr>,
    #[arg(long, requires = "srt_play_listen")]
    srt_play_latency: Option<u32>,
    #[arg(long, requires = "srt_play_listen")]
    srt_play_client_limit: Option<usize>,
    #[arg(
        long,
        env = "FLUSSONIX_SRT_PLAY_PASSPHRASE",
        hide_env_values = true,
        requires = "srt_play_listen"
    )]
    srt_play_passphrase: Option<String>,
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

fn bind_tcp_listener(address: SocketAddr) -> std::io::Result<tokio::net::TcpListener> {
    let socket = if address.is_ipv4() {
        tokio::net::TcpSocket::new_v4()?
    } else {
        tokio::net::TcpSocket::new_v6()?
    };
    // Preserve the generic bind path's Unix rebinding behavior. On Windows,
    // SO_REUSEADDR could allow another process to hijack an occupied address.
    #[cfg(unix)]
    socket.set_reuseaddr(true)?;
    socket.bind(address)?;
    // Queue short admission bursts without changing application client limits.
    // The kernel may cap this backlog at its configured maximum.
    socket.listen(1024)
}

impl Args {
    fn fxlink_features(&self) -> Result<flussonix::fxlink::Features, String> {
        if self.fxlink_repair_parity && !self.fxlink_trimmed_records {
            return Err("FXLink parity repair requires --fxlink-trimmed-records".into());
        }
        if self.fxlink_bootstrap_repair
            && !(self.fxlink_trimmed_records && self.fxlink_repair_parity)
        {
            return Err("FXLink bootstrap repair requires --fxlink-trimmed-records and --fxlink-repair-parity".into());
        }
        if self.fxlink_geometry_hints
            && !(self.fxlink_trimmed_records
                && self.fxlink_repair_parity
                && self.fxlink_bootstrap_repair)
        {
            return Err("FXLink geometry hints require --fxlink-trimmed-records, --fxlink-repair-parity and --fxlink-bootstrap-repair".into());
        }
        Ok(flussonix::fxlink::Features {
            trimmed_records: self.fxlink_trimmed_records,
            repair_parity: self.fxlink_repair_parity,
            bootstrap_repair: self.fxlink_bootstrap_repair,
            geometry_hints: self.fxlink_geometry_hints,
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let a = Args::parse();
    let fxlink_features = a.fxlink_features()?;
    let udp_rate = a.rtsp_udp_mbps.unwrap_or(100.0);
    let https_config = match (&a.https_cert, &a.https_key) {
        (Some(cert), Some(key)) => Some(flussonix::rtsp::tls::server(cert, key)?),
        _ => None,
    };
    flussonix::rtsp::udp::Pacer::new(udp_rate)?;
    let tls_config = match (&a.rtsps_cert, &a.rtsps_key) {
        (Some(cert), Some(key)) => Some(flussonix::rtsp::tls::server(cert, key)?),
        _ => None,
    };
    // Validate encryption/limits and bind UDP before config/media ownership starts.
    let srt_listener = match a.srt_play_listen {
        Some(address) => Some(flussonix::srt_playback::Listener::bind(
            address,
            flussonix::srt_playback::Settings::new(
                a.srt_play_latency.unwrap_or(120),
                a.srt_play_client_limit.unwrap_or(128),
                a.srt_play_passphrase.unwrap_or_default(),
            )?,
        )?),
        None => None,
    };
    let fxlink_listener = match a.fxlink_listen {
        Some(address) => Some(flussonix::fxlink::Listener::bind_with_features(
            address,
            a.fxlink_cert
                .as_deref()
                .expect("clap requires FXLink certificate"),
            a.fxlink_key.as_deref().expect("clap requires FXLink key"),
            flussonix::fxlink::Settings::default(),
            fxlink_features,
        )?),
        None => None,
    };
    let options = Options {
        admin_user: a.admin_user,
        admin_password: a.admin_password,
        admin_public_origin: a.admin_public_origin,
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
        fxlink_features,
    };
    let app = App::new(a.config, a.media_dir, options)?;
    let listener = if a.https_only {
        None
    } else {
        Some(bind_tcp_listener(a.listen)?)
    };
    let https_listener = match a.https_listen {
        Some(address) => Some(bind_tcp_listener(address)?),
        None => None,
    };
    let rtsp_listener = match a.rtsp_listen {
        Some(address) => Some(bind_tcp_listener(address)?),
        None => None,
    };
    let tls_listener = match a.rtsps_listen {
        Some(address) => Some(bind_tcp_listener(address)?),
        None => None,
    };
    let udp_pool = match (a.rtsp_udp_ports, rtsp_listener.as_ref()) {
        (Some(range), Some(listener)) => {
            Some(flussonix::rtsp::udp::Pool::bind(listener.local_addr()?.ip(), range).await?)
        }
        _ => None,
    };
    let http_address = listener.as_ref().map(|l| l.local_addr()).transpose()?;
    let https_address = https_listener
        .as_ref()
        .map(|l| l.local_addr())
        .transpose()?;
    app.set_http_delivery(http_address, https_address);
    app.set_srt_playback(srt_listener.as_ref());
    app.set_fxlink_listener(fxlink_listener.as_ref());
    app.set_rtsp_publication(
        rtsp_listener.as_ref().map(|l| l.local_addr()).transpose()?,
        tls_listener.as_ref().map(|l| l.local_addr()).transpose()?,
    );
    app.set_rtsp_publication_udp(udp_pool.is_some());
    println!(
        "{}",
        serde_json::json!({"service":"FlussoniX","listen":http_address.map(|a|a.to_string()),"https_listen":https_address.map(|a|a.to_string()),"rtsp_listen":rtsp_listener.as_ref().map(|l|l.local_addr().map(|a|a.to_string())).transpose()?,"rtsps_listen":tls_listener.as_ref().map(|l|l.local_addr().map(|a|a.to_string())).transpose()?,"fxlink_listen":fxlink_listener.as_ref().map(|l|l.address().to_string()),"srt_play_listen":srt_listener.as_ref().map(|l|l.address().to_string()),"srt_play_encrypted":srt_listener.as_ref().map(|l|l.settings().encrypted()),"rtsp_udp_ports":a.rtsp_udp_ports.map(|p|p.to_string()),"rtsp_udp_mbps":a.rtsp_udp_ports.map(|_|udp_rate),"version":env!("CARGO_PKG_VERSION")})
    );
    app.reconcile().await;
    let background = app.clone();
    let cancel = tokio_util::sync::CancellationToken::new();
    let vendor_app = app.clone();
    let vendor_cancel = cancel.clone();
    let vendor_playback =
        tokio::spawn(async move { vendor_app.run_vendor_playback(vendor_cancel).await });
    tokio::task::yield_now().await;
    let mut fxlink_task = fxlink_listener
        .map(|listener| tokio::spawn(listener.serve(app.fxlink_provider(), cancel.clone())));
    let mut srt_task = srt_listener.map(|listener| {
        tokio::spawn(flussonix::srt_playback::serve(
            listener,
            app.clone(),
            cancel.clone(),
        ))
    });
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
        // Short callback grants can become due after 100 ms; sample often
        // enough to start renewal before the hard expiry deadline.
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            tokio::select! { _=auth_cancel.cancelled()=>break, _=interval.tick()=>auth_app.playback_auth.schedule_due() }
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
    let memory_app = app.clone();
    let memory_cancel = cancel.clone();
    let memory_maintenance = tokio::spawn(async move {
        memory_app.media.maintain_idle_memory(memory_cancel).await;
    });
    type Server = std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<()>> + Send>>;
    let mut serving = FuturesUnordered::<Server>::new();
    if let Some(listener) = listener {
        serving.push(Box::pin(flussonix::http_transport::serve(
            listener,
            app.clone(),
            cancel.clone(),
        )));
    }
    if let Some(listener) = https_listener {
        serving.push(Box::pin(flussonix::http_tls::serve(
            listener,
            https_config.unwrap(),
            app.clone(),
            cancel.clone(),
        )));
    }
    let mut rtsp_result = None;
    let mut tls_result = None;
    let mut srt_result = None;
    let mut fxlink_result = None;
    let completed = tokio::select! {
        result=serving.next()=>result,
        result=async{match rtsp_task.as_mut(){Some(task)=>task.await,None=>std::future::pending().await}}=>{rtsp_result=Some(result);None},
        result=async{match tls_task.as_mut(){Some(task)=>task.await,None=>std::future::pending().await}}=>{tls_result=Some(result);None},
        result=async{match srt_task.as_mut(){Some(task)=>task.await,None=>std::future::pending().await}}=>{srt_result=Some(result);None},
        result=async{match fxlink_task.as_mut(){Some(task)=>task.await,None=>std::future::pending().await}}=>{fxlink_result=Some(result);None},
        _=shutdown()=>None
    };
    cancel.cancel();
    let _ = supervisor.await;
    let _ = authorization.await;
    let _ = telemetry.await;
    let _ = memory_maintenance.await;
    let vendor_result = vendor_playback.await;
    if srt_result.is_none() {
        if let Some(task) = srt_task {
            srt_result = Some(task.await);
        }
    }
    if fxlink_result.is_none() {
        if let Some(task) = fxlink_task {
            fxlink_result = Some(task.await);
        }
    }
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
    let drain = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(result) = serving.next().await {
            result?;
        }
        Ok::<(), std::io::Error>(())
    })
    .await;
    if let Ok(result) = drain {
        result?;
    } else {
        tracing::warn!("connection drain exceeded five seconds");
    }
    if let Some(result) = completed {
        result?;
    }
    if let Some(result) = rtsp_result {
        result??;
    }
    if let Some(result) = tls_result {
        result??;
    }
    if let Some(result) = srt_result {
        result??;
    }
    if let Some(result) = fxlink_result {
        result??;
    }
    vendor_result??;
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fxlink_geometry_startup_matrix() {
        const TEST: &str = "tests::fxlink_geometry_startup_matrix";
        const KEYS: [&str; 4] = [
            "FLUSSONIX_FXLINK_TRIMMED_RECORDS",
            "FLUSSONIX_FXLINK_REPAIR_PARITY",
            "FLUSSONIX_FXLINK_BOOTSTRAP_REPAIR",
            "FLUSSONIX_FXLINK_GEOMETRY_HINTS",
        ];
        const FLAGS: [&str; 4] = [
            "--fxlink-trimmed-records",
            "--fxlink-repair-parity",
            "--fxlink-bootstrap-repair",
            "--fxlink-geometry-hints",
        ];
        if let Ok(case) = std::env::var("FXLINK_GEOMETRY_MATRIX_CHILD") {
            let mut digits: usize = case.parse().unwrap();
            let mode = std::env::var("FXLINK_GEOMETRY_MATRIX_MODE").unwrap();
            let mut values = [false; 4];
            let mut args = vec![
                "flussonix",
                "--admin-password",
                "owned-admin",
                "--peer-key",
                "owned-peer-key-123",
            ];
            for (index, flag) in FLAGS.iter().enumerate() {
                let value = digits % 3;
                digits /= 3;
                values[index] = value == 2;
                if mode == "flags" && value == 2 {
                    args.push(flag);
                }
            }
            let [t, p, b, g] = values;
            let parsed = Args::try_parse_from(args)
                .map_err(|error| error.to_string())
                .and_then(|args| args.fxlink_features());
            assert_eq!(
                parsed.is_ok(),
                (!p || t) && (!b || (t && p)) && (!g || (t && p && b)),
                "case{case} mode{mode}:{}",
                parsed.as_ref().err().map_or("", String::as_str)
            );
            if let Ok(features) = parsed {
                assert_eq!(
                    [
                        features.trimmed_records,
                        features.repair_parity,
                        features.bootstrap_repair,
                        features.geometry_hints
                    ],
                    values
                );
            }
            return;
        }
        for mode in ["environment", "flags"] {
            for case in 0..81 {
                let mut child = std::process::Command::new(std::env::current_exe().unwrap());
                child
                    .args([TEST, "--exact", "--test-threads=1"])
                    .env("FXLINK_GEOMETRY_MATRIX_CHILD", case.to_string())
                    .env("FXLINK_GEOMETRY_MATRIX_MODE", mode);
                let mut digits = case;
                for key in KEYS {
                    let value = digits % 3;
                    digits /= 3;
                    child.env_remove(key);
                    if value != 0 {
                        child.env(
                            key,
                            if mode == "environment" && value == 2 {
                                "true"
                            } else {
                                "false"
                            },
                        );
                    }
                }
                let output = child.output().unwrap();
                assert!(
                    output.status.success(),
                    "case{case} mode{mode}:{}{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        }
    }
    #[test]
    fn fxlink_bootstrap_flags_require_both_prerequisites() {
        const TEST: &str = "tests::fxlink_bootstrap_flags_require_both_prerequisites";
        if std::env::var_os("FXLINK_BOOTSTRAP_FLAGS_CHILD").is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([TEST, "--exact", "--test-threads=1"])
                .env("FXLINK_BOOTSTRAP_FLAGS_CHILD", "1")
                .env_remove("FLUSSONIX_FXLINK_TRIMMED_RECORDS")
                .env_remove("FLUSSONIX_FXLINK_REPAIR_PARITY")
                .env_remove("FLUSSONIX_FXLINK_BOOTSTRAP_REPAIR")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stdout)
            );
            return;
        }
        for trimmed in [false, true] {
            for parity in [false, true] {
                for bootstrap in [false, true] {
                    let mut args = vec![
                        "flussonix",
                        "--admin-password",
                        "owned-admin",
                        "--peer-key",
                        "owned-peer-key-123",
                    ];
                    if trimmed {
                        args.push("--fxlink-trimmed-records");
                    }
                    if parity {
                        args.push("--fxlink-repair-parity");
                    }
                    if bootstrap {
                        args.push("--fxlink-bootstrap-repair");
                    }
                    let parsed = Args::try_parse_from(args)
                        .map_err(|e| e.to_string())
                        .and_then(|a| a.fxlink_features());
                    let valid = (!parity || trimmed) && (!bootstrap || (trimmed && parity));
                    assert_eq!(
                        parsed.is_ok(),
                        valid,
                        "flags:{trimmed}/{parity}/{bootstrap}"
                    );
                    if valid {
                        assert_eq!(parsed.unwrap().bootstrap_repair, bootstrap);
                    }
                }
            }
        }
    }
    #[test]
    fn fxlink_bootstrap_environment_matrix() {
        const TEST: &str = "tests::fxlink_bootstrap_environment_matrix";
        if let Ok(bits) = std::env::var("FXLINK_BOOTSTRAP_ENV_CHILD") {
            let bits: u8 = bits.parse().unwrap();
            let parsed = Args::try_parse_from([
                "flussonix",
                "--admin-password",
                "owned-admin",
                "--peer-key",
                "owned-peer-key-123",
            ])
            .map_err(|e| e.to_string())
            .and_then(|a| a.fxlink_features());
            let (t, p, b) = (bits & 1 != 0, bits & 2 != 0, bits & 4 != 0);
            assert_eq!(
                parsed.is_ok(),
                (!p || t) && (!b || (t && p)),
                "env bits:{bits}"
            );
            if let Ok(f) = parsed {
                assert_eq!(f.bootstrap_repair, b);
                assert_eq!(f.repair_parity, p);
                assert_eq!(f.trimmed_records, t);
            }
            return;
        }
        // Absent and explicitly false are distinct Clap argument sources.
        for case in 0u32..27 {
            let mut child = std::process::Command::new(std::env::current_exe().unwrap());
            child.args([TEST, "--exact", "--test-threads=1"]);
            let mut digits = case;
            let mut bits = 0u8;
            for (bit, key) in [
                (1, "FLUSSONIX_FXLINK_TRIMMED_RECORDS"),
                (2, "FLUSSONIX_FXLINK_REPAIR_PARITY"),
                (4, "FLUSSONIX_FXLINK_BOOTSTRAP_REPAIR"),
            ] {
                let value = digits % 3;
                digits /= 3;
                child.env_remove(key);
                if value != 0 {
                    child.env(key, if value == 2 { "true" } else { "false" });
                }
                if value == 2 {
                    bits |= bit;
                }
            }
            child.env("FXLINK_BOOTSTRAP_ENV_CHILD", bits.to_string());
            let output = child.output().unwrap();
            assert!(
                output.status.success(),
                "env case{case} bits{bits}:{}",
                String::from_utf8_lossy(&output.stdout)
            );
        }
    }
    #[test]
    fn fxlink_parity_flag_requires_explicit_trimmed_layout_optin() {
        let base = [
            "flussonix",
            "--admin-password",
            "owned-admin",
            "--peer-key",
            "owned-peer-key-123",
        ];
        assert!(
            Args::try_parse_from(
                base.into_iter()
                    .chain(["--fxlink-trimmed-records", "--fxlink-repair-parity"])
            )
            .is_ok(),
            "new opt-in flag must be accepted"
        );
        assert!(
            Args::try_parse_from(base.into_iter().chain(["--fxlink-repair-parity"])).is_err(),
            "v7 cannot silently enable trimmed layouts"
        );
    }
    #[test]
    fn fxlink_parity_environment_is_opt_in_and_requires_trimmed() {
        const TEST: &str = "tests::fxlink_parity_environment_is_opt_in_and_requires_trimmed";
        if let Ok(expected) = std::env::var("FXLINK_PARITY_ENV_EXPECTED") {
            let parsed = Args::try_parse_from([
                "flussonix",
                "--admin-password",
                "owned-admin",
                "--peer-key",
                "owned-peer-key-123",
            ])
            .map_err(|error| error.to_string())
            .and_then(|args| args.fxlink_features());
            if expected == "rejected" {
                assert!(parsed.is_err());
            } else {
                assert_eq!(parsed.unwrap().repair_parity, expected == "true");
            }
            return;
        }
        for (parity, trimmed, expected) in [
            (None, None, "false"),
            (Some("false"), None, "false"),
            (Some("true"), None, "rejected"),
            (Some("true"), Some("false"), "rejected"),
            (Some("true"), Some("true"), "true"),
        ] {
            let mut child = std::process::Command::new(std::env::current_exe().unwrap());
            child
                .args([TEST, "--exact", "--test-threads=1"])
                .env("FXLINK_PARITY_ENV_EXPECTED", expected)
                .env_remove("FLUSSONIX_FXLINK_REPAIR_PARITY")
                .env_remove("FLUSSONIX_FXLINK_TRIMMED_RECORDS");
            if let Some(value) = parity {
                child.env("FLUSSONIX_FXLINK_REPAIR_PARITY", value);
            }
            if let Some(value) = trimmed {
                child.env("FLUSSONIX_FXLINK_TRIMMED_RECORDS", value);
            }
            let output = child.output().unwrap();
            assert!(
                output.status.success(),
                "owned env case:{expected}:{}",
                String::from_utf8_lossy(&output.stdout)
            );
        }
    }
    #[test]
    fn fxlink_trimmed_flag_is_an_independent_startup_optin() {
        let parsed = Args::try_parse_from([
            "flussonix",
            "--admin-password",
            "owned-admin",
            "--peer-key",
            "owned-peer-key-123",
            "--fxlink-trimmed-records",
        ]);
        assert!(
            parsed.is_ok(),
            "client-only trimmed opt-in was rejected: {:?}",
            parsed.err()
        );
    }
    #[test]
    fn fxlink_trimmed_environment_binding_is_parse_once_and_defaults_off() {
        const TEST: &str =
            "tests::fxlink_trimmed_environment_binding_is_parse_once_and_defaults_off";
        if let Ok(expected) = std::env::var("FXLINK_OWNED_ENV_EXPECTED") {
            let args = Args::try_parse_from([
                "flussonix",
                "--admin-password",
                "owned-admin",
                "--peer-key",
                "owned-peer-key-123",
            ])
            .unwrap();
            assert_eq!(args.fxlink_trimmed_records, expected == "true");
            assert!(args.fxlink_listen.is_none());
            return;
        }
        for (value, expected) in [(None, false), (Some("false"), false), (Some("true"), true)] {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([TEST, "--exact", "--nocapture", "--test-threads=1"])
                .env(
                    "FXLINK_OWNED_ENV_EXPECTED",
                    if expected { "true" } else { "false" },
                )
                .env_remove("FLUSSONIX_FXLINK_TRIMMED_RECORDS");
            if let Some(value) = value {
                command.env("FLUSSONIX_FXLINK_TRIMMED_RECORDS", value);
            }
            let output = command.output().unwrap();
            assert!(
                output.status.success(),
                "owned env case {value:?}: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
    #[test]
    fn fxlink_cli_requires_all_tls_material() {
        let base = [
            "flussonix",
            "--admin-password",
            "owned-admin",
            "--peer-key",
            "owned-peer-key-123",
        ];
        let args =
            |extra: &[&str]| Args::try_parse_from(base.into_iter().chain(extra.iter().copied()));
        assert!(args(&[]).is_ok());
        assert!(
            args(&[
                "--fxlink-listen",
                "127.0.0.1:0",
                "--fxlink-cert",
                "cert.pem",
                "--fxlink-key",
                "key.pem"
            ])
            .is_ok()
        );
        assert!(args(&["--fxlink-listen", "127.0.0.1:0"]).is_err());
        assert!(args(&["--fxlink-cert", "cert.pem"]).is_err());
        assert!(args(&["--fxlink-key", "key.pem"]).is_err());
    }
}
