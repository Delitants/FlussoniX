//! Short RTSP control probe for copied video startup. No media is consumed.
use std::{path::Path, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VideoCodec {
    H264,
    Hevc,
}

/// Unknown responses retain the conservative copied-video startup filter.
pub(crate) async fn video_codec(input: &str, ca: Option<&Path>) -> Option<VideoCodec> {
    let checked = if input.starts_with("rtsps://") {
        input.to_owned()
    } else {
        crate::media::translate_input(input).ok()?
    };
    crate::media::validate_rtsp_input_url(&checked).ok()?;
    tokio::time::timeout(Duration::from_secs(3), async {
        if input.starts_with("rtsps://") {
            let bridge = crate::tls_input::Bridge::start(input, ca).await.ok()?;
            let result = describe(bridge.local_url(), true).await;
            bridge.close().await;
            result
        } else {
            describe(&checked, false).await
        }
    })
    .await
    .ok()
    .flatten()
}

async fn describe(input: &str, verified_bridge: bool) -> Option<VideoCodec> {
    let mut next = url::Url::parse(input).ok()?;
    if !valid_target(&next, verified_bridge) {
        return None;
    }
    for _ in 0..5 {
        let response = request(&next).await?;
        if response.status == 200 {
            if !response
                .content_type
                .as_deref()
                .is_some_and(|v| v.eq_ignore_ascii_case("application/sdp"))
            {
                return None;
            }
            return parse_sdp(&response.body);
        }
        if response.status != 302 || !verified_bridge {
            return None;
        }
        let redirect = url::Url::parse(response.location.as_deref()?).ok()?;
        if !valid_target(&redirect, true) || redirect == next {
            return None;
        }
        next = redirect;
    }
    None
}

fn valid_target(url: &url::Url, verified_bridge: bool) -> bool {
    if url.scheme() != "rtsp"
        || url.host_str().is_none()
        || url.port() == Some(0)
        || url.fragment().is_some()
    {
        return false;
    }
    // Probe credentials are deliberately unsupported; the normal input can
    // still authenticate through FFmpeg and uses the conservative filter.
    if !url.username().is_empty() || url.password().is_some() {
        return false;
    }
    !verified_bridge || (url.host_str() == Some("127.0.0.1") && url.port().is_some())
}

struct Response {
    status: u16,
    content_type: Option<String>,
    location: Option<String>,
    body: Vec<u8>,
}

async fn request(url: &url::Url) -> Option<Response> {
    let host = url.host_str()?;
    let port = url.port().unwrap_or(554);
    let mut socket = TcpStream::connect((host, port)).await.ok()?;
    let request = format!("DESCRIBE {url} RTSP/1.0\r\nCSeq: 1\r\nAccept: application/sdp\r\n\r\n");
    socket.write_all(request.as_bytes()).await.ok()?;
    let mut header = Vec::with_capacity(1024);
    while !header.ends_with(b"\r\n\r\n") {
        if header.len() >= 16_384 {
            return None;
        }
        header.push(socket.read_u8().await.ok()?);
    }
    let text = std::str::from_utf8(&header).ok()?;
    let mut lines = text[..text.len() - 4].split("\r\n");
    let status = lines.next()?.split_whitespace().collect::<Vec<_>>();
    if status.len() < 2 || status[0] != "RTSP/1.0" {
        return None;
    }
    let status = status[1].parse::<u16>().ok()?;
    let mut size = None;
    let mut content_type = None;
    let mut location = None;
    let mut cseq = false;
    for line in lines {
        let (key, value) = line.split_once(':')?;
        let value = value.trim();
        if key.eq_ignore_ascii_case("Content-Length") {
            if size.is_some() {
                return None;
            }
            size = Some(value.parse::<usize>().ok()?);
        } else if key.eq_ignore_ascii_case("Content-Type") {
            if content_type.is_some() {
                return None;
            }
            content_type = Some(value.to_owned());
        } else if key.eq_ignore_ascii_case("Location") {
            if location.is_some() {
                return None;
            }
            location = Some(value.to_owned());
        } else if key.eq_ignore_ascii_case("CSeq") {
            if cseq || value != "1" {
                return None;
            }
            cseq = true;
        }
    }
    if !cseq {
        return None;
    }
    let size = size.unwrap_or(0);
    if size > 65_536 {
        return None;
    }
    let mut body = vec![0; size];
    socket.read_exact(&mut body).await.ok()?;
    Some(Response {
        status,
        content_type,
        location,
        body,
    })
}

fn parse_sdp(sdp: &[u8]) -> Option<VideoCodec> {
    let text = std::str::from_utf8(sdp).ok()?;
    let mut in_video = false;
    let mut seen_video = false;
    let mut formats = Vec::new();
    let mut codecs = Vec::new();
    for line in text.lines().map(|s| s.trim_end_matches('\r')) {
        if let Some(media) = line.strip_prefix("m=") {
            in_video = media.starts_with("video ");
            if in_video {
                // Multiple video tracks need explicit selection.
                if seen_video {
                    return None;
                }
                seen_video = true;
                let parts = media.split_ascii_whitespace().collect::<Vec<_>>();
                if parts.len() < 4 {
                    return None;
                }
                formats = parts[3..].iter().map(|s| (*s).to_owned()).collect();
            }
        } else if in_video {
            if let Some(map) = line.strip_prefix("a=rtpmap:") {
                let (payload, encoding) = map.split_once(' ')?;
                if formats.iter().any(|format| format == payload) {
                    let codec = encoding.split('/').next()?;
                    codecs.push(if codec.eq_ignore_ascii_case("H264") {
                        VideoCodec::H264
                    } else if codec.eq_ignore_ascii_case("H265")
                        || codec.eq_ignore_ascii_case("HEVC")
                    {
                        VideoCodec::Hevc
                    } else {
                        return None;
                    });
                }
            }
        }
    }
    if formats.len() == 1 && codecs.len() == 1 {
        Some(codecs[0])
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    #[test]
    fn sdp_identifies_one_video_codec_and_rejects_ambiguous_tracks() {
        assert_eq!(parse_sdp(b"v=0\r\nm=video 0 RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\nm=audio 0 RTP/AVP 97\r\na=rtpmap:97 MPEG4-GENERIC/48000/2\r\n"), Some(VideoCodec::H264));
        assert_eq!(
            parse_sdp(b"m=video 0 RTP/AVP 96\r\na=rtpmap:96 H265/90000\r\n"),
            Some(VideoCodec::Hevc)
        );
        assert_eq!(
            parse_sdp(
                b"m=video 0 RTP/AVP 96 97\r\na=rtpmap:96 H264/90000\r\na=rtpmap:97 H265/90000\r\n"
            ),
            None
        );
        assert_eq!(parse_sdp(b"m=video 0 RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\nm=video 0 RTP/AVP 97\r\na=rtpmap:97 H265/90000\r\n"), None);
    }

    #[tokio::test]
    async fn oversized_description_is_rejected_before_reading_its_body() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 256];
            let count = socket.read(&mut request).await.unwrap();
            assert!(request[..count].starts_with(b"DESCRIBE rtsp://"));
            socket.write_all(b"RTSP/1.0 200 OK\r\nCSeq: 1\r\nContent-Type: application/sdp\r\nContent-Length: 65537\r\n\r\n").await.unwrap();
        });
        let url = format!("rtsp://{address}/stream");
        assert_eq!(video_codec(&url, None).await, None);
        server.await.unwrap();
    }
}
