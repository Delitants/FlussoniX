//! Native SRT input: the upstream URL and crypto options never enter FFmpeg argv.
use std::{net::SocketAddr, time::Duration};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use crate::srt_playback::native::{self, ReceiveSettings, Socket};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Caller,
    Listener,
}

pub(crate) struct Input {
    host: String,
    port: u16,
    mode: Mode,
    settings: ReceiveSettings,
    connect_timeout: Duration,
    listen_timeout: Duration,
    idle_timeout: Duration,
}

impl Input {
    pub(crate) fn parse(raw: &str) -> Result<Self, String> {
        if raw.len() > 4096 || raw.chars().any(|c| c.is_control() || c.is_whitespace()) {
            return Err("invalid SRT input URL".into());
        }
        // Flussonic-style examples may use a literal # inside a query value.
        let normalized = match raw.split_once('?') {
            Some((base, query)) => format!("{base}?{}", query.replace('#', "%23")),
            None => match raw.split_once("#passphrase=") {
                Some((base, passphrase)) => format!(
                    "{base}?passphrase={}",
                    url::form_urlencoded::byte_serialize(passphrase.as_bytes()).collect::<String>()
                ),
                None => raw.to_owned(),
            },
        };
        let url = url::Url::parse(&normalized).map_err(|_| "invalid SRT input URL")?;
        if url.scheme() != "srt"
            || url.host().is_none()
            || url.port().is_none_or(|port| port == 0)
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || !["", "/"].contains(&url.path())
        {
            return Err("SRT input requires srt://HOST:PORT without userinfo or path".into());
        }
        let host = match url.host().unwrap() {
            url::Host::Domain(host) => host.to_owned(),
            url::Host::Ipv4(host) => host.to_string(),
            url::Host::Ipv6(host) => host.to_string(),
        };
        let mut mode = Mode::Caller;
        let mut passphrase = String::new();
        let mut streamid = String::new();
        let mut pbkeylen = 16;
        let mut tsbpd = true;
        let mut latency_micros = 120_000u64;
        let mut connect_millis = 3_000u64;
        let mut timeout_micros = 10_000_000u64;
        let mut listen_micros = 15_000_000u64;
        let mut rcvbuf_bytes = None;
        let mut seen = std::collections::HashSet::new();
        if let Some(query) = url.query() {
            let bytes = query.as_bytes();
            for (i, byte) in bytes.iter().enumerate() {
                if *byte == b'%'
                    && (bytes.get(i + 1).is_none_or(|b| !b.is_ascii_hexdigit())
                        || bytes.get(i + 2).is_none_or(|b| !b.is_ascii_hexdigit()))
                {
                    return Err("invalid SRT input query encoding".into());
                }
            }
            percent_encoding::percent_decode_str(query)
                .decode_utf8()
                .map_err(|_| "invalid SRT input query encoding")?;
        }
        for (key, value) in url.query_pairs() {
            let canonical = match key.as_ref() {
                "tsbpd" => "tsbpdmode",
                "srt_streamid" => "streamid",
                key => key,
            };
            if !seen.insert(canonical.to_owned()) {
                return Err("duplicate SRT input option".into());
            }
            match canonical {
                "mode" => {
                    mode = match value.as_ref() {
                        "caller" => Mode::Caller,
                        "listener" => Mode::Listener,
                        _ => return Err("SRT input mode must be caller or listener".into()),
                    }
                }
                "passphrase" => passphrase = value.into_owned(),
                "streamid" => streamid = value.into_owned(),
                "pbkeylen" => {
                    pbkeylen = match value.as_ref() {
                        "16" => 16,
                        "24" => 24,
                        "32" => 32,
                        _ => return Err("SRT input AES key length must be 16, 24 or 32".into()),
                    }
                }
                "tsbpdmode" => {
                    tsbpd = match value.as_ref() {
                        "1" | "true" | "on" => true,
                        "0" | "false" | "off" => false,
                        _ => return Err("SRT input tsbpdmode must be on or off".into()),
                    }
                }
                "latency" => latency_micros = micros(&value, 10_000_000)?,
                "connect_timeout" => connect_millis = number(&value, 30_000)?,
                "timeout" => timeout_micros = micros(&value, 300_000_000)?,
                "listen_timeout" => listen_micros = micros(&value, 300_000_000)?,
                "rcvbuf" => rcvbuf_bytes = Some(number(&value, 128 * 1024 * 1024)?),
                "enforced_encryption" if value == "1" => {}
                _ => return Err("unsupported SRT input option".into()),
            }
        }
        if passphrase.len() > 79
            || (!passphrase.is_empty() && passphrase.len() < 10)
            || !passphrase.bytes().all(|byte| (0x20..=0x7e).contains(&byte))
            || streamid.len() > 512
            || streamid.chars().any(char::is_control)
            || (passphrase.is_empty() && seen.contains("pbkeylen"))
            || (mode == Mode::Listener && !streamid.is_empty())
            || rcvbuf_bytes.is_some_and(|bytes| bytes < 64 * 1024)
        {
            return Err("invalid SRT input secret or stream ID".into());
        }
        let idle_millis = timeout_micros.div_ceil(1000).clamp(1, 300_000);
        let latency_millis = latency_micros.div_ceil(1000);
        // 8 MiB headroom plus about 50 Mb/s of media per latency second.
        // Higher bitrates can request an explicit bounded rcvbuf value.
        let default_buffer = (8 * 1024 * 1024 + latency_millis * 6250).min(64 * 1024 * 1024);
        let rcvbuf_bytes = rcvbuf_bytes.unwrap_or(default_buffer);
        Ok(Self {
            host,
            port: url.port().unwrap(),
            mode,
            settings: ReceiveSettings {
                streamid,
                passphrase,
                pbkeylen,
                tsbpd,
                latency_millis: latency_millis as i32,
                connect_millis: connect_millis as i32,
                idle_millis: idle_millis as i32,
                rcvbuf_bytes: rcvbuf_bytes as i32,
            },
            connect_timeout: Duration::from_millis(connect_millis),
            listen_timeout: Duration::from_micros(listen_micros),
            idle_timeout: Duration::from_millis(idle_millis),
        })
    }

    async fn connect(&self, cancel: &CancellationToken) -> Result<Socket, String> {
        let addresses: Vec<SocketAddr> = tokio::select! {biased;
            _=cancel.cancelled()=>return Err("SRT input stopped".into()),
            result=tokio::time::timeout(self.connect_timeout,tokio::net::lookup_host((self.host.as_str(),self.port)))=>match result {
                Ok(Ok(addresses))=>addresses.take(8).collect(),
                _=>return Err("SRT input DNS failed".into()),
            }
        };
        if addresses.is_empty() {
            return Err("SRT input address unavailable".into());
        }
        match self.mode {
            Mode::Caller => {
                let mut candidates: Vec<_> = addresses
                    .into_iter()
                    .filter_map(|address| native::receiver_caller(address, &self.settings).ok())
                    .collect();
                if candidates.is_empty() {
                    return Err("SRT input connection failed".into());
                }
                let deadline = tokio::time::Instant::now() + self.connect_timeout;
                loop {
                    if let Some(index) = candidates.iter().position(Socket::is_connected) {
                        return Ok(candidates.swap_remove(index));
                    }
                    candidates.retain(Socket::is_connecting);
                    if candidates.is_empty() {
                        return Err("SRT input connection failed".into());
                    }
                    tokio::select! {biased;
                        _=cancel.cancelled()=>return Err("SRT input stopped".into()),
                        _=tokio::time::sleep_until(deadline)=>return Err("SRT input connection timed out".into()),
                        _=tokio::time::sleep(Duration::from_millis(5))=>{},
                    }
                }
            }
            Mode::Listener => {
                let listener = addresses
                    .into_iter()
                    .find_map(|address| {
                        native::ReceiverListener::bind(address, &self.settings).ok()
                    })
                    .ok_or("SRT input listener bind failed")?;
                let deadline = tokio::time::Instant::now() + self.listen_timeout;
                loop {
                    if let Some(socket) =
                        listener.accept().map_err(|_| "SRT input accept failed")?
                    {
                        return Ok(socket);
                    }
                    tokio::select! {biased;
                        _=cancel.cancelled()=>return Err("SRT input stopped".into()),
                        _=tokio::time::sleep_until(deadline)=>return Err("SRT input listener timed out".into()),
                        _=tokio::time::sleep(Duration::from_millis(5))=>{},
                    }
                }
            }
        }
    }

    pub(crate) async fn run<W: AsyncWrite + Unpin>(
        self,
        mut writer: W,
        cancel: CancellationToken,
    ) -> Result<(), String> {
        let socket = self.connect(&cancel).await?;
        let mut buffer = [0u8; 16 * 1024];
        let mut last_media = tokio::time::Instant::now();
        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            let mut drained = false;
            for _ in 0..256 {
                match socket.try_recv(&mut buffer) {
                    Ok(Some(n)) => {
                        drained = true;
                        last_media = tokio::time::Instant::now();
                        tokio::select! {biased;
                            _=cancel.cancelled()=>return Ok(()),
                            result=tokio::time::timeout(Duration::from_secs(2),writer.write_all(&buffer[..n]))=>{
                                result.map_err(|_| "SRT decoder stalled")?.map_err(|_| "SRT decoder closed")?;
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(_) => return Err("SRT input closed".into()),
                }
            }
            if drained {
                tokio::task::yield_now().await;
                continue;
            }
            if !socket.is_connected() {
                return Err("SRT input closed".into());
            }
            let deadline = last_media + self.idle_timeout;
            tokio::select! {biased;
                _=cancel.cancelled()=>return Ok(()),
                _=tokio::time::sleep_until(deadline)=>return Err("SRT input timed out".into()),
                _=tokio::time::sleep(Duration::from_millis(5))=>{},
            }
        }
    }
}

fn number(value: &str, max: u64) -> Result<u64, String> {
    value
        .parse::<u64>()
        .ok()
        .filter(|n| (1..=max).contains(n))
        .ok_or_else(|| "invalid SRT input timeout or latency".into())
}
fn micros(value: &str, max: u64) -> Result<u64, String> {
    number(value, max)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listener_accept_timeout_does_not_override_receive_idle_timeout() {
        let input = Input::parse(
            "srt://127.0.0.1:9000?mode=listener&listen_timeout=1000000&timeout=300000000",
        )
        .unwrap();
        assert_eq!(input.idle_timeout, Duration::from_secs(300));
    }

    #[test]
    fn high_bitrate_inputs_can_request_a_bounded_receive_buffer() {
        assert!(Input::parse("srt://127.0.0.1:9000?latency=1500000&rcvbuf=33554432").is_ok());
        assert!(Input::parse("srt://127.0.0.1:9000?rcvbuf=1073741824").is_err());
    }
}
