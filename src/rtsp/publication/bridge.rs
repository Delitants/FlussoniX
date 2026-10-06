//! Private loopback decoder bridge. No publisher-advertised address is used.
use crate::{
    direct_rtp::{
        config::Settings,
        elementary::{packet, readiness, sdp::Session},
        sockets::Pair,
    },
    media::Worker,
    rtsp::protocol::Transport,
};
use std::{
    os::fd::AsRawFd,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{io::AsyncWriteExt, process::ChildStdin};
use tokio_util::sync::CancellationToken;
struct Lane {
    pair: Pair,
    transport: Transport,
    ssrc: Option<u32>,
    pending_report: Option<(Vec<u8>, Instant)>,
    reports: super::reports::Reports,
}
pub(super) struct Bridge {
    session: Session,
    ports: Vec<u16>,
    reserved: Vec<Pair>,
    lanes: Vec<Lane>,
}
impl Bridge {
    pub async fn bind(session: &Session, transports: &[Option<Transport>]) -> Result<Self, String> {
        let mut ports = Vec::new();
        let mut reserved = Vec::new();
        let mut lanes = Vec::new();
        for transport in transports {
            let mut held = None;
            for _ in 0..64 {
                let r = std::net::UdpSocket::bind("127.0.0.1:0")
                    .map_err(|_| "Private RTP bind failed")?;
                let port = r
                    .local_addr()
                    .map_err(|_| "Private RTP bind failed")?
                    .port();
                drop(r);
                if port == 65535 {
                    continue;
                }
                let settings = Settings::parse(
                    &serde_json::json!({"url":format!("rtp://127.0.0.1:{port}"),"flussonix_rtp":{"interface":"127.0.0.1"}}),
                )?;
                if let Ok(p) = Pair::receive(&settings) {
                    held = Some((port, p, settings));
                    break;
                }
            }
            let (port, p, settings) = held.ok_or("Private RTP pair unavailable")?;
            let pair = Pair::send(&settings).await?;
            ports.push(port);
            reserved.push(p);
            lanes.push(Lane {
                pair,
                transport: transport.ok_or("Track not negotiated")?,
                ssrc: None,
                pending_report: None,
                reports: super::reports::Reports::new(),
            });
        }
        Ok(Self {
            session: session.clone(),
            ports,
            reserved,
            lanes,
        })
    }
    pub async fn start(
        &mut self,
        mut stdin: ChildStdin,
        worker: &Arc<Worker>,
        cancel: &CancellationToken,
    ) -> Result<(), String> {
        let sdp = self.session.decoder_sdp(&self.ports);
        let excluded = self
            .reserved
            .iter()
            .flat_map(|pair| [&pair.rtp, &pair.rtcp])
            .map(|socket| readiness::socket_inode(socket.as_raw_fd()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(String::from)?;
        self.reserved.clear();
        tokio::select! {biased;_=cancel.cancelled()=>return Err("Publication cancelled".into()),_=worker.closed()=>return Err("Decoder stopped".into()),r=tokio::time::timeout(Duration::from_secs(2),stdin.write_all(sdp.as_bytes()))=>{r.map_err(|_|"Decoder SDP stalled")?.map_err(|_|"Decoder SDP unavailable")?;}}
        drop(stdin);
        let mut ports = Vec::new();
        for p in &self.ports {
            ports.extend([*p, *p + 1]);
        }
        let ready = async {
            loop {
                if readiness::owned(&ports, worker.pid(), &excluded).map_err(String::from)? {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::select! {biased;_=cancel.cancelled()=>Err("Publication cancelled".into()),_=worker.closed()=>Err("Decoder stopped".into()),r=tokio::time::timeout(Duration::from_secs(8),ready)=>r.map_err(|_|"Private RTP decoder not ready".to_string())?}
    }
    pub async fn forward(&mut self, channel: u8, body: &[u8]) -> Result<bool, &'static str> {
        let Some(n) = self
            .lanes
            .iter()
            .position(|l| l.transport.rtp == channel || l.transport.rtcp == channel)
        else {
            return Err("Unconfigured interleaved channel");
        };
        let lane = &mut self.lanes[n];
        if channel == lane.transport.rtp {
            let p = packet::parse_interleaved(body, &self.session.tracks[n])?;
            if lane.ssrc.is_some_and(|s| s != p.ssrc) {
                return Err("RTP SSRC changed");
            }
            lane.ssrc = Some(p.ssrc);
            lane.reports.packet(&p, self.session.tracks[n].clock);
            if let Some((report, at)) = lane.pending_report.take() {
                if u32::from_be_bytes(report[4..8].try_into().unwrap()) == p.ssrc {
                    lane.reports.sender_report(&report, at);
                    lane.pair
                        .rtcp
                        .send(&report)
                        .await
                        .map_err(|_| "Private sender report send failed")?;
                }
            }
            lane.pair
                .rtp
                .send(body)
                .await
                .map_err(|_| "Private RTP send failed")?;
            Ok(true)
        } else {
            if !crate::direct_rtp::packet::valid_rtcp(body) || body.len() < 8 {
                return Err("Invalid publisher RTCP");
            }
            let source = u32::from_be_bytes(body[4..8].try_into().unwrap());
            match lane.ssrc {
                Some(s) if s == source => {
                    lane.reports.sender_report(body, Instant::now());
                    lane.pair
                        .rtcp
                        .send(body)
                        .await
                        .map_err(|_| "Private RTCP send failed")?;
                }
                Some(_) => return Err("RTCP SSRC changed"),
                None => {
                    if body[1] == 200 {
                        lane.pending_report = Some((body.to_vec(), Instant::now()));
                    }
                }
            };
            Ok(false)
        }
    }
    pub fn reports(&mut self) -> Vec<u8> {
        let mut frames = Vec::new();
        for lane in &mut self.lanes {
            if let Some(source) = lane.ssrc {
                let body = lane.reports.report(source);
                frames.extend([b'$', lane.transport.rtcp]);
                frames.extend((body.len() as u16).to_be_bytes());
                frames.extend(body);
            }
        }
        frames
    }
    pub async fn feedback(&self) -> Result<(u8, Vec<u8>), &'static str> {
        let futures = self
            .lanes
            .iter()
            .map(|lane| {
                Box::pin(async move {
                    let mut body = vec![0; 2049];
                    let n = lane
                        .pair
                        .rtcp
                        .recv(&mut body)
                        .await
                        .map_err(|_| "Private RTCP feedback receive failed")?;
                    body.truncate(n);
                    if !crate::direct_rtp::packet::valid_rtcp(&body) {
                        return Err("Invalid private RTCP feedback");
                    }
                    Ok((lane.transport.rtcp, body))
                })
            })
            .collect::<Vec<_>>();
        if futures.is_empty() {
            return std::future::pending().await;
        }
        futures_util::future::select_all(futures).await.0
    }
}
