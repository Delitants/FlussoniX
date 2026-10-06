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
// A lane represents one media source. Report-block SSRCs describe receivers'
// observations and are not senders; SDES chunks and BYE lists are source-bearing.
// Validate the whole compound before it can be queued or sent to the decoder.
fn control_source(mut body: &[u8]) -> Result<u32, &'static str> {
    if !crate::direct_rtp::packet::valid_rtcp(body) {
        return Err("Invalid publisher RTCP");
    }
    let mut source = None;
    let mut bind = |bytes: &[u8]| {
        let next = u32::from_be_bytes(bytes.try_into().unwrap());
        if source.is_some_and(|s| s != next) {
            return Err("RTCP SSRC changed");
        }
        source = Some(next);
        Ok(())
    };
    while !body.is_empty() {
        // Structural validation above bounds every member, chunk and item.
        let len = (usize::from(u16::from_be_bytes([body[2], body[3]])) + 1) * 4;
        let packet = &body[..len];
        let count = usize::from(packet[0] & 31);
        match packet[1] {
            200 | 201 | 204 => bind(&packet[4..8])?,
            202 => {
                let mut at = 4;
                for _ in 0..count {
                    bind(&packet[at..at + 4])?;
                    at += 4;
                    while packet[at] != 0 {
                        at += 2 + usize::from(packet[at + 1]);
                    }
                    at = (at + 4) & !3;
                }
            }
            203 => {
                for bytes in packet[4..4 + 4 * count].chunks_exact(4) {
                    bind(bytes)?;
                }
            }
            _ => return Err("Invalid publisher RTCP"),
        }
        body = &body[len..];
    }
    source.ok_or("Publisher RTCP has no source")
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
            let source = control_source(body)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    const SOURCE: u32 = 0x12345678;
    async fn bridge() -> Bridge {
        let description = super::super::sdp::parse(
            b"v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=owned\r\nc=IN IP4 0.0.0.0\r\nt=0 0\r\nm=video 0 RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\na=fmtp:96 packetization-mode=1\r\na=control:streamid=0\r\n",
            &url::Url::parse("rtsp://127.0.0.1/owned").unwrap(),
        ).unwrap();
        Bridge::bind(&description.media, &[Some(Transport { rtp: 0, rtcp: 1 })])
            .await
            .unwrap()
    }
    fn rtp() -> Vec<u8> {
        let mut p = vec![0x80, 96, 0, 1, 0, 0, 0, 0];
        p.extend(SOURCE.to_be_bytes());
        p.extend([0x65, 0x88, 0x84]);
        p
    }
    fn sr(source: u32) -> Vec<u8> {
        let mut p = vec![0x80, 200, 0, 6];
        p.extend(source.to_be_bytes());
        p.extend([0; 20]);
        p
    }
    fn sdes(source: u32) -> Vec<u8> {
        let mut p = vec![0x81, 202, 0, 3];
        p.extend(source.to_be_bytes());
        p.extend([1, 4, b't', b'e', b's', b't', 0, 0]);
        p
    }
    async fn received(bridge: &Bridge) -> Option<Vec<u8>> {
        let mut p = vec![0; 2048];
        let n = tokio::time::timeout(
            Duration::from_millis(100),
            bridge.reserved[0].rtcp.recv(&mut p),
        )
        .await
        .ok()?
        .ok()?;
        p.truncate(n);
        Some(p)
    }
    // Checking only the first RTCP source forwards foreign clock mappings into
    // the real decoder UDP endpoint, both immediately and from pending reports.
    #[tokio::test]
    async fn compound_rtcp_rejects_foreign_sender_after_media() {
        let mut b = bridge().await;
        b.forward(0, &rtp()).await.unwrap();
        let compound = [sr(SOURCE), sr(SOURCE + 1)].concat();
        let result = b.forward(1, &compound).await;
        let forwarded = received(&b).await;
        assert!(
            result.is_err(),
            "Every compound sender must match pinned media"
        );
        assert!(forwarded.is_none(), "Foreign sender must not reach decoder");
    }
    #[tokio::test]
    async fn compound_rtcp_rejects_foreign_sender_before_media() {
        let mut b = bridge().await;
        let compound = [sr(SOURCE), sr(SOURCE + 1)].concat();
        let result = b.forward(1, &compound).await;
        b.forward(0, &rtp()).await.unwrap();
        let forwarded = received(&b).await;
        assert!(
            result.is_err(),
            "Pending reports must have one media identity"
        );
        assert!(
            forwarded.is_none(),
            "Foreign pending report must not reach decoder"
        );
    }
    #[tokio::test]
    async fn compound_rtcp_binds_sdes_bye_and_app_sources() {
        let foreign = SOURCE + 1;
        let mut bye = vec![0x81, 203, 0, 1];
        bye.extend(foreign.to_be_bytes());
        let mut app = vec![0x80, 204, 0, 2];
        app.extend(foreign.to_be_bytes());
        app.extend(*b"test");
        for packet in [sdes(foreign), bye, app] {
            let mut b = bridge().await;
            b.forward(0, &rtp()).await.unwrap();
            let result = b.forward(1, &[sr(SOURCE), packet].concat()).await;
            let forwarded = received(&b).await;
            assert!(
                result.is_err(),
                "Every forwarded control source must match media"
            );
            assert!(forwarded.is_none());
        }
    }
    #[tokio::test]
    async fn matching_compound_sender_reports_and_sdes_reach_decoder() {
        for pending in [false, true] {
            let mut b = bridge().await;
            if !pending {
                b.forward(0, &rtp()).await.unwrap();
            }
            let compound = [sr(SOURCE), sdes(SOURCE)].concat();
            assert!(!b.forward(1, &compound).await.unwrap());
            if pending {
                b.forward(0, &rtp()).await.unwrap();
            }
            assert_eq!(received(&b).await.unwrap(), compound);
        }
    }
}
