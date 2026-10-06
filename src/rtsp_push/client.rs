//! Native publisher consumes immutable shared RTP packets and codec metadata.
use super::bridge::{self, Frame};
use crate::rtp::Description;
use bytes::Bytes;
use std::{collections::HashMap, time::Duration};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpStream, tcp::OwnedWriteHalf},
    sync::mpsc,
};
struct Lane {
    rtp: u8,
    rtcp: u8,
    ssrc: u32,
    stamp: u32,
    clock: u32,
    packets: u32,
    octets: u32,
}
pub(super) struct Client {
    write: OwnedWriteHalf,
    read: mpsc::Receiver<Result<Frame, std::io::Error>>,
    url: String,
    seq: u32,
    session: String,
    lanes: HashMap<u32, Lane>,
    report_origin: (tokio::time::Instant, std::time::SystemTime),
    keepalive: Option<(u32, tokio::time::Instant)>,
}
impl Client {
    pub async fn publish(
        url: &str,
        description: &Description,
        stamp_origin: u64,
        readers: &mut tokio::task::JoinSet<()>,
    ) -> Result<Self, &'static str> {
        let parsed = url::Url::parse(url).map_err(|_| "push_setup_failed")?;
        let socket = TcpStream::connect(("127.0.0.1", parsed.port().ok_or("push_setup_failed")?))
            .await
            .map_err(|_| "push_setup_failed")?;
        socket.set_nodelay(true).map_err(|_| "push_setup_failed")?;
        let (read, write) = socket.into_split();
        let (tx, rx) = mpsc::channel(16);
        readers.spawn(async move {
            let mut reader = BufReader::new(read);
            loop {
                let item = bridge::frame(&mut reader).await;
                let failed = item.is_err();
                if tx.try_send(item).is_err() || failed {
                    break;
                }
            }
        });
        let mut client = Self {
            write,
            read: rx,
            url: url.into(),
            seq: 0,
            session: String::new(),
            lanes: HashMap::new(),
            report_origin: (tokio::time::Instant::now(), std::time::SystemTime::now()),
            keepalive: None,
        };
        let sdp = description.sdp();
        client
            .exchange(
                "ANNOUNCE",
                url,
                "Content-Type: application/sdp\r\n",
                sdp.as_bytes(),
            )
            .await?;
        for (i, track) in description.tracks.iter().enumerate() {
            let rtp = (i * 2) as u8;
            let rtcp = rtp + 1;
            // Match ANNOUNCE's relative control URI, including an existing
            // query: FFmpeg and Flussonic resolve it by aggregate concatenation.
            let control = format!("{url}/trackID={}", track.id);
            let offer =
                format!("Transport: RTP/AVP/TCP;unicast;interleaved={rtp}-{rtcp};mode=record\r\n");
            let response = client.exchange("SETUP", &control, &offer, &[]).await?;
            let t = bridge::transport_response(
                response
                    .header("transport")
                    .ok_or("push_transport_rejected")?,
            )
            .map_err(|_| "push_transport_rejected")?;
            if t.rtp != rtp || t.rtcp != rtcp {
                return Err("push_transport_rejected");
            }
            client.lanes.insert(
                track.id,
                Lane {
                    rtp,
                    rtcp,
                    ssrc: track.ssrc,
                    clock: track.clock,
                    stamp: ((u128::from(stamp_origin) * u128::from(track.clock)) / 90000) as u32,
                    packets: 0,
                    octets: 0,
                },
            );
        }
        if client.session.is_empty() {
            return Err("push_session_rejected");
        }
        client
            .exchange("RECORD", url, "Range: npt=0-\r\n", &[])
            .await?;
        // RECORD receivers need an initial common clock before media arrives.
        client.report_origin = (tokio::time::Instant::now(), std::time::SystemTime::now());
        client.reports().await?;
        Ok(client)
    }
    async fn request(
        &mut self,
        method: &str,
        url: &str,
        headers: &str,
        body: &[u8],
    ) -> Result<u32, &'static str> {
        self.seq = self.seq.checked_add(1).ok_or("push_setup_failed")?;
        let session = if self.session.is_empty() {
            String::new()
        } else {
            format!("Session: {}\r\n", self.session)
        };
        let mut wire=format!("{method} {url} RTSP/1.0\r\nCSeq: {}\r\nUser-Agent: FlussoniX\r\n{session}{headers}Content-Length: {}\r\n\r\n",self.seq,body.len()).into_bytes();
        wire.extend_from_slice(body);
        tokio::time::timeout(Duration::from_secs(2), self.write.write_all(&wire))
            .await
            .map_err(|_| "push_stalled")?
            .map_err(|_| "push_connection_closed")?;
        Ok(self.seq)
    }
    fn accepted(&mut self, frame: Frame, seq: u32) -> Result<bridge::Control, &'static str> {
        let Frame::Control(response) = frame else {
            return Err("push_response_rejected");
        };
        if response.cseq().map_err(|_| "push_response_rejected")? != seq
            || !response.start.starts_with("RTSP/1.0 200 ")
        {
            return Err("push_rejected");
        }
        if let Some(session) = response.header("session") {
            let session = session.split(';').next().unwrap_or("");
            if session.is_empty()
                || session.len() > 256
                || !session.bytes().all(|b| (33..127).contains(&b))
                || !self.session.is_empty() && self.session != session
            {
                return Err("push_session_rejected");
            }
            self.session = session.to_owned();
        }
        Ok(response)
    }
    async fn exchange(
        &mut self,
        method: &str,
        url: &str,
        headers: &str,
        body: &[u8],
    ) -> Result<bridge::Control, &'static str> {
        let seq = self.request(method, url, headers, body).await?;
        let frame = tokio::time::timeout(Duration::from_secs(3), self.read.recv())
            .await
            .map_err(|_| "push_setup_timeout")?
            .ok_or("push_connection_closed")?
            .map_err(|_| "push_connection_closed")?;
        self.accepted(frame, seq)
    }
    pub async fn packet(&mut self, packet: &Bytes) -> Result<(), &'static str> {
        if packet.len() < 16 {
            return Err("push_packet_rejected");
        }
        let id = u32::from_be_bytes(packet[..4].try_into().unwrap());
        let lane = self.lanes.get_mut(&id).ok_or("push_packet_rejected")?;
        tokio::time::timeout(
            Duration::from_secs(2),
            bridge::media(&mut self.write, lane.rtp, &packet[4..]),
        )
        .await
        .map_err(|_| "push_stalled")?
        .map_err(|_| "push_connection_closed")?;
        lane.packets = lane.packets.wrapping_add(1);
        lane.octets = lane.octets.wrapping_add((packet.len() - 16) as u32);
        Ok(())
    }
    pub async fn reports(&mut self) -> Result<(), &'static str> {
        // All tracks map the same advancing media instant to the same NTP instant.
        // A last-sent packet may be stale or buffered and is not the current clock.
        let elapsed = self.report_origin.0.elapsed();
        let time = self.report_origin.1 + elapsed;
        for lane in self.lanes.values() {
            let stamp = lane
                .stamp
                .wrapping_add((elapsed.as_nanos() * u128::from(lane.clock) / 1_000_000_000) as u32);
            let report =
                crate::rtsp::sender_report_at(lane.ssrc, stamp, lane.packets, lane.octets, time);
            tokio::time::timeout(
                Duration::from_secs(2),
                bridge::media(&mut self.write, lane.rtcp, &report),
            )
            .await
            .map_err(|_| "push_stalled")?
            .map_err(|_| "push_connection_closed")?;
        }
        Ok(())
    }
    pub async fn keepalive(&mut self) -> Result<(), &'static str> {
        if self.keepalive.is_some() {
            return Err("push_keepalive_timeout");
        }
        let url = self.url.clone();
        let seq = self.request("OPTIONS", &url, "", &[]).await?;
        self.keepalive = Some((seq, tokio::time::Instant::now() + Duration::from_secs(5)));
        Ok(())
    }
    pub async fn feedback(&mut self) -> Result<(), &'static str> {
        let deadline = self.keepalive.map(|(_, at)| at);
        let expired = async {
            if let Some(at) = deadline {
                tokio::time::sleep_until(at).await
            } else {
                std::future::pending().await
            }
        };
        let frame = tokio::select! {biased;_=expired=>return Err("push_keepalive_timeout"),frame=self.read.recv()=>frame.ok_or("push_connection_closed")?.map_err(|_|"push_connection_closed")?};
        match frame {
            Frame::Media(channel, body) => {
                if !self.lanes.values().any(|t| t.rtcp == channel)
                    || body.len() > 2048
                    || !valid_rtcp(&body)
                {
                    return Err("push_feedback_rejected");
                }
                Ok(())
            }
            Frame::Control(response) => {
                let (seq, _) = self.keepalive.take().ok_or("push_response_rejected")?;
                self.accepted(Frame::Control(response), seq)?;
                Ok(())
            }
        }
    }
}
fn valid_rtcp(body: &[u8]) -> bool {
    let mut at = 0;
    while at < body.len() {
        if body.len() - at < 4 || body[at] >> 6 != 2 {
            return false;
        }
        let size = (u16::from_be_bytes([body[at + 2], body[at + 3]]) as usize + 1) * 4;
        if size > body.len() - at {
            return false;
        }
        at += size;
    }
    at > 0
}
