//! Bounded HTTP/1.x response heads; uploads may be acknowledged before EOF.
use super::State;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};

pub(super) async fn watch(io: impl AsyncRead + Unpin, state: Arc<State>) -> &'static str {
    let mut reader = BufReader::new(io);
    for _ in 0..4 {
        let mut head = Vec::with_capacity(1024);
        while !head.ends_with(b"\r\n\r\n") {
            if head.len() == 16384 {
                return "push_response_invalid";
            }
            let mut byte = [0];
            match reader.read_exact(&mut byte).await {
                Ok(_) => head.push(byte[0]),
                Err(_) => return "push_connection_failed",
            }
        }
        let Some(status) = status(&head) else {
            return "push_response_invalid";
        };
        if status == 101 {
            return "push_response_invalid";
        }
        if status < 200 {
            continue;
        }
        state.counters.lock().unwrap().http_status = Some(status);
        if matches!(status, 401 | 403) {
            return "push_auth_denied";
        }
        if (300..400).contains(&status) {
            return "push_redirect_refused";
        }
        if !(200..300).contains(&status) {
            return "push_rejected";
        }
        // A successful early response can coexist with a continuous upload.
        // Keep the socket owned until close, cancellation or body-progress stall.
        let mut buffer = [0; 4096];
        let mut total = 0usize;
        loop {
            match reader.read(&mut buffer).await {
                Ok(0) => return "push_response_ended",
                Ok(n) => {
                    total += n;
                    if total > 16384 {
                        return "push_response_invalid";
                    }
                }
                Err(_) => return "push_connection_failed",
            }
        }
    }
    "push_response_invalid"
}
fn status(head: &[u8]) -> Option<u16> {
    let mut lines = head.strip_suffix(b"\r\n\r\n")?.split(|b| *b == b'\n');
    let first = lines.next()?;
    let first = first.strip_suffix(b"\r").unwrap_or(first);
    let mut fields = first.splitn(3, |b| *b == b' ');
    if ![b"HTTP/1.1".as_slice(), b"HTTP/1.0".as_slice()].contains(&fields.next()?) {
        return None;
    }
    let code = fields.next()?;
    if code.len() != 3
        || !code.iter().all(u8::is_ascii_digit)
        || first.iter().any(|b| *b < 32 || *b == 127)
    {
        return None;
    }
    let code = std::str::from_utf8(code).ok()?.parse::<u16>().ok()?;
    if !(100..=599).contains(&code) {
        return None;
    }
    for (count, line) in lines.enumerate() {
        if count >= 100 {
            return None;
        }
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let colon = line.iter().position(|b| *b == b':')?;
        if colon == 0
            || !line[..colon]
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(b))
            || line[colon + 1..]
                .iter()
                .any(|b| (*b < 32 && *b != b'\t') || *b == 127)
        {
            return None;
        }
    }
    Some(code)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_push::Destination;
    use serde_json::json;
    use std::{sync::atomic::AtomicU64, time::Duration};
    use tokio::io::AsyncWriteExt;
    use tokio_util::task::AbortOnDropHandle;

    #[tokio::test]
    async fn malformed_or_excessive_response_heads_are_bounded() {
        let mut cases = vec![
            b"HTTP/1.1 101 Switching Protocols\r\n\r\n".to_vec(),
            b"HTTP/1.1 200 OK\r\n Bad: folded\r\n\r\n".to_vec(),
            b"HTTP/1.1 100 Continue\r\n\r\n".repeat(4),
            [
                b"HTTP/1.1 200 OK\r\nLarge: ".as_slice(),
                &vec![b'x'; 16384],
                b"\r\n\r\n",
            ]
            .concat(),
        ];
        cases.push(
            [
                b"HTTP/1.1 200 OK\r\n".as_slice(),
                &b"X: x\r\n".repeat(101),
                b"\r\n",
            ]
            .concat(),
        );
        for bytes in cases {
            let (reader, mut writer) = tokio::io::duplex(1024);
            let writing = AbortOnDropHandle::new(tokio::spawn(async move {
                let _ = writer.write_all(&bytes).await;
            }));
            let state = State::new(
                Destination::parse(&json!({"url":"http://127.0.0.1:9/owned"})).unwrap(),
                0,
                Arc::new(AtomicU64::new(0)),
            );
            let reason = tokio::time::timeout(Duration::from_secs(1), watch(reader, state))
                .await
                .unwrap();
            assert_eq!(reason, "push_response_invalid");
            writing.abort();
            let _ = writing.await;
        }
    }
}
