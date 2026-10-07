//! Native publisher consumes immutable shared RTP packets and codec metadata.
use super::{
    auth::{Auth, Credentials},
    bridge::{self, Frame},
};
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
    udp: Option<super::udp::Pair>,
}
pub(super) struct Client {
    write: OwnedWriteHalf,
    read: mpsc::Receiver<Result<Frame, std::io::Error>>,
    url: String,
    route: bridge::Route,
    auth: Auth,
    seq: u32,
    session: String,
    lanes: HashMap<u32, Lane>,
    report_origin: (tokio::time::Instant, std::time::SystemTime),
    keepalive: Option<(u32, tokio::time::Instant, u8)>,
    udp_bytes: u64,
}
impl Client {
    pub async fn publish(
        bridge: &bridge::Bridge,
        credentials: Option<Credentials>,
        description: &Description,
        stamp_origin: u64,
        readers: &mut tokio::task::JoinSet<()>,
    ) -> Result<Self, &'static str> {
        let url = bridge.local_url();
        let parsed = url::Url::parse(url).map_err(|_| "push_setup_failed")?;
        let socket = TcpStream::connect(("127.0.0.1", parsed.port().ok_or("push_setup_failed")?))
            .await
            .map_err(|_| "push_setup_failed")?;
        socket.set_nodelay(true).map_err(|_| "push_setup_failed")?;
        let (read, write) = socket.into_split();
        let (tx, rx) = mpsc::channel(16);
        let udp_tx = tx.clone();
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
            route: bridge.route(),
            auth: Auth::new(credentials),
            seq: 0,
            session: String::new(),
            lanes: HashMap::new(),
            report_origin: (tokio::time::Instant::now(), std::time::SystemTime::now()),
            keepalive: None,
            udp_bytes: 0,
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
            let mut pair = if bridge.udp {
                Some(
                    super::udp::Pair::bind(bridge.local, bridge.egress.clone())
                        .await
                        .map_err(|_| "push_setup_failed")?,
                )
            } else {
                None
            };
            let offer = if let Some(pair) = &pair {
                let p = pair.ports();
                format!(
                    "Transport: RTP/AVP;unicast;client_port={}-{};mode=record\r\n",
                    p.rtp, p.rtcp
                )
            } else {
                format!("Transport: RTP/AVP/TCP;unicast;interleaved={rtp}-{rtcp};mode=record\r\n")
            };
            let response = client.exchange("SETUP", &control, &offer, &[]).await?;
            let value = response
                .header("transport")
                .ok_or("push_transport_rejected")?;
            if let Some(pair) = &mut pair {
                let ports = super::udp::response(value, pair.ports(), bridge.peer)
                    .map_err(|_| "push_transport_rejected")?;
                if client
                    .lanes
                    .values()
                    .filter_map(|l| l.udp.as_ref())
                    .any(|p| p.remote == Some(ports))
                {
                    return Err("push_transport_rejected");
                }
                pair.connect(bridge.peer, ports)
                    .await
                    .map_err(|_| "push_transport_rejected")?;
            } else {
                let t = bridge::transport_response(value).map_err(|_| "push_transport_rejected")?;
                if t.rtp != rtp || t.rtcp != rtcp {
                    return Err("push_transport_rejected");
                }
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
                    udp: pair,
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
        for lane in client.lanes.values() {
            if let Some(pair) = &lane.udp {
                pair.drain().map_err(|_| "push_feedback_rejected")?;
                let socket = pair.rtcp.clone();
                let tx = udp_tx.clone();
                let channel = lane.rtcp;
                readers.spawn(async move {
                    let mut data = [0u8; 2049];
                    loop {
                        let item = match socket.recv(&mut data).await {
                            Ok(n) if n <= 2048 => Ok(Frame::Media(channel, data[..n].to_vec())),
                            _ => Err(std::io::Error::other("invalid UDP feedback")),
                        };
                        let failed = item.is_err();
                        if tx.try_send(item).is_err() {
                            let _ = tx
                                .send(Err(std::io::Error::other("UDP feedback queue overflow")))
                                .await;
                            break;
                        }
                        if failed {
                            break;
                        }
                    }
                });
            }
        }
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
        let target = self.route.upstream(url).map_err(|_| "push_setup_failed")?;
        let authorization = self.auth.authorization(method, &target)?;
        let mut wire=format!("{method} {url} RTSP/1.0\r\nCSeq: {}\r\nUser-Agent: FlussoniX\r\n{session}{headers}{authorization}Content-Length: {}\r\n\r\n",self.seq,body.len()).into_bytes();
        wire.extend_from_slice(body);
        tokio::time::timeout(Duration::from_secs(2), self.write.write_all(&wire))
            .await
            .map_err(|_| "push_stalled")?
            .map_err(|_| "push_connection_closed")?;
        Ok(self.seq)
    }
    fn checked(frame: Frame, seq: u32) -> Result<bridge::Control, &'static str> {
        let Frame::Control(response) = frame else {
            return Err("push_response_rejected");
        };
        if response.cseq().map_err(|_| "push_response_rejected")? != seq {
            return Err("push_response_rejected");
        }
        Ok(response)
    }
    fn challenge(&mut self, response: &bridge::Control) -> Result<(), &'static str> {
        self.auth.challenge(
            response
                .headers
                .iter()
                .filter(|(k, _)| k == "www-authenticate")
                .map(|(_, v)| v.as_str()),
        )
    }
    fn accepted(&mut self, response: bridge::Control) -> Result<bridge::Control, &'static str> {
        if !response.start.starts_with("RTSP/1.0 200 ") {
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
        for retries in 0..=2 {
            let seq = self.request(method, url, headers, body).await?;
            let frame = tokio::time::timeout(Duration::from_secs(3), self.read.recv())
                .await
                .map_err(|_| "push_setup_timeout")?
                .ok_or("push_connection_closed")?
                .map_err(|_| "push_connection_closed")?;
            let response = Self::checked(frame, seq)?;
            if response.start.starts_with("RTSP/1.0 401 ") {
                if retries == 2 {
                    return Err("push_auth_rejected");
                }
                self.challenge(&response)?;
            } else {
                return self.accepted(response);
            }
        }
        Err("push_auth_rejected")
    }
    pub async fn packet(&mut self, packet: &Bytes) -> Result<(), &'static str> {
        if packet.len() < 16 {
            return Err("push_packet_rejected");
        }
        let id = u32::from_be_bytes(packet[..4].try_into().unwrap());
        let lane = self.lanes.get_mut(&id).ok_or("push_packet_rejected")?;
        tokio::time::timeout(Duration::from_secs(2), async {
            if let Some(pair) = &lane.udp {
                pair.send(false, &packet[4..]).await
            } else {
                bridge::media(&mut self.write, lane.rtp, &packet[4..]).await
            }
        })
        .await
        .map_err(|_| "push_stalled")?
        .map_err(|_| "push_connection_closed")?;
        if lane.udp.is_some() {
            self.udp_bytes = self.udp_bytes.saturating_add((packet.len() - 4) as u64);
        }
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
            tokio::time::timeout(Duration::from_secs(2), async {
                if let Some(pair) = &lane.udp {
                    pair.send(true, &report).await
                } else {
                    bridge::media(&mut self.write, lane.rtcp, &report).await
                }
            })
            .await
            .map_err(|_| "push_stalled")?
            .map_err(|_| "push_connection_closed")?;
        }
        Ok(())
    }
    pub fn take_udp_bytes(&mut self) -> u64 {
        std::mem::take(&mut self.udp_bytes)
    }
    pub async fn keepalive(&mut self) -> Result<(), &'static str> {
        if self.keepalive.is_some() {
            return Err("push_keepalive_timeout");
        }
        let url = self.url.clone();
        let seq = self.request("OPTIONS", &url, "", &[]).await?;
        self.keepalive = Some((seq, tokio::time::Instant::now() + Duration::from_secs(5), 0));
        Ok(())
    }
    pub async fn feedback(&mut self) -> Result<Frame, &'static str> {
        let deadline = self.keepalive.map(|(_, at, _)| at);
        let expired = async {
            if let Some(at) = deadline {
                tokio::time::sleep_until(at).await
            } else {
                std::future::pending().await
            }
        };
        let frame = tokio::select! {biased;_=expired=>return Err("push_keepalive_timeout"),frame=self.read.recv()=>frame.ok_or("push_connection_closed")?.map_err(|_|"push_connection_closed")?};
        Ok(frame)
    }
    // Process after the outer select commits to this branch: an authentication
    // write cannot be cancelled by a ready media packet midway through a request.
    pub async fn handle_feedback(&mut self, frame: Frame) -> Result<(), &'static str> {
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
                let (seq, deadline, retries) =
                    self.keepalive.take().ok_or("push_response_rejected")?;
                let response = Self::checked(Frame::Control(response), seq)?;
                if response.start.starts_with("RTSP/1.0 401 ") {
                    if retries == 2 || tokio::time::Instant::now() >= deadline {
                        return Err("push_auth_rejected");
                    }
                    self.challenge(&response)?;
                    let url = self.url.clone();
                    let seq =
                        tokio::time::timeout_at(deadline, self.request("OPTIONS", &url, "", &[]))
                            .await
                            .map_err(|_| "push_keepalive_timeout")??;
                    self.keepalive = Some((seq, deadline, retries + 1));
                } else {
                    self.accepted(response)?;
                }
                Ok(())
            }
        }
    }
}
fn valid_rtcp(body: &[u8]) -> bool {
    if body.is_empty() || body.len() > 2048 {
        return false;
    }
    let mut at = 0;
    while at < body.len() {
        if body.len() - at < 4 || body[at] >> 6 != 2 {
            return false;
        }
        let size = (u16::from_be_bytes([body[at + 2], body[at + 3]]) as usize + 1) * 4;
        let Some(packet) = body.get(at..at + size) else {
            return false;
        };
        let padding = if packet[0] & 32 != 0 {
            let pad = usize::from(*packet.last().unwrap());
            if at + size != body.len() || pad == 0 || pad > size - 4 {
                return false;
            }
            pad
        } else {
            0
        };
        let payload = size - padding;
        let count = usize::from(packet[0] & 31);
        let valid = match packet[1] {
            200 => payload == 28 + 24 * count && crate::direct_rtp::packet::valid_rtcp(packet),
            201 => payload == 8 + 24 * count && crate::direct_rtp::packet::valid_rtcp(packet),
            202..=204 => crate::direct_rtp::packet::valid_rtcp(packet),
            205 | 206 => payload >= 12,
            _ => false,
        };
        if !valid {
            return false;
        }
        at += size;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::valid_rtcp;
    #[test]
    fn malformed_receiver_reports_and_padding_are_rejected() {
        assert!(valid_rtcp(&[0x80, 201, 0, 1, 0, 0, 0, 1]));
        for body in [
            &[0x80, 201, 0, 0][..],
            &[0x81, 201, 0, 1, 0, 0, 0, 1],
            &[0xa0, 201, 0, 1, 0, 0, 0, 0],
            &[0x80, 200, 0, 1, 0, 0, 0, 1],
            &[0x81, 202, 0, 1, 0, 0, 0, 1],
        ] {
            assert!(!valid_rtcp(body), "malformed RTCP admitted: {body:?}");
        }
    }
}
