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
use std::{sync::Arc, time::Duration};
use tokio::{io::AsyncWriteExt, process::ChildStdin};
use tokio_util::sync::CancellationToken;
struct Lane {
    pair: Pair,
    transport: Transport,
    ssrc: Option<u32>,
    pending_report: Option<Vec<u8>>,
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
        self.reserved.clear();
        tokio::select! {biased;_=cancel.cancelled()=>return Err("Publication cancelled".into()),_=worker.closed()=>return Err("Decoder stopped".into()),r=tokio::time::timeout(Duration::from_secs(2),stdin.write_all(sdp.as_bytes()))=>{r.map_err(|_|"Decoder SDP stalled")?.map_err(|_|"Decoder SDP unavailable")?;}}
        drop(stdin);
        let mut ports = Vec::new();
        for p in &self.ports {
            ports.extend([*p, *p + 1]);
        }
        let ready = async {
            loop {
                if readiness::bound(&ports).map_err(String::from)? {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::select! {biased;_=cancel.cancelled()=>Err("Publication cancelled".into()),_=worker.closed()=>Err("Decoder stopped".into()),r=tokio::time::timeout(Duration::from_secs(8),ready)=>r.map_err(|_|"Private RTP decoder not ready".to_string())?}
    }
    pub async fn forward(&mut self, channel: u8, body: &[u8]) -> Result<bool, ()> {
        let Some(n) = self
            .lanes
            .iter()
            .position(|l| l.transport.rtp == channel || l.transport.rtcp == channel)
        else {
            return Err(());
        };
        let lane = &mut self.lanes[n];
        if channel == lane.transport.rtp {
            let p = packet::parse_interleaved(body, &self.session.tracks[n]).map_err(|_| ())?;
            if lane.ssrc.is_some_and(|s| s != p.ssrc) {
                return Err(());
            }
            lane.ssrc = Some(p.ssrc);
            if let Some(report) = lane.pending_report.take() {
                if u32::from_be_bytes(report[4..8].try_into().unwrap()) == p.ssrc {
                    lane.pair.rtcp.send(&report).await.map_err(|_| ())?;
                }
            }
            lane.pair.rtp.send(body).await.map_err(|_| ())?;
            Ok(true)
        } else {
            if !crate::direct_rtp::packet::valid_rtcp(body) || body.len() < 8 {
                return Err(());
            }
            let source = u32::from_be_bytes(body[4..8].try_into().unwrap());
            match lane.ssrc {
                Some(s) if s == source => {
                    lane.pair.rtcp.send(body).await.map_err(|_| ())?;
                }
                Some(_) => return Err(()),
                None => {
                    if body[1] == 200 {
                        lane.pending_report = Some(body.to_vec());
                    }
                }
            };
            Ok(false)
        }
    }
    pub async fn feedback(&self) -> Result<(u8, Vec<u8>), ()> {
        let futures = self
            .lanes
            .iter()
            .map(|lane| {
                Box::pin(async move {
                    let mut body = vec![0; 2049];
                    let n = lane.pair.rtcp.recv(&mut body).await.map_err(|_| ())?;
                    body.truncate(n);
                    if !crate::direct_rtp::packet::valid_rtcp(&body) {
                        return Err(());
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
