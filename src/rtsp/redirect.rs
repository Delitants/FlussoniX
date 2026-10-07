//! Callback-selected viewer destinations, never upstream connections.
use super::Reply;

pub(crate) fn destination(value: &str) -> Option<url::Url> {
    if value.is_empty()
        || value.len() > 8192
        || value
            .bytes()
            .any(|byte| byte <= 32 || byte >= 127 || byte == b'\\')
    {
        return None;
    }
    let target = url::Url::parse(value).ok()?;
    (matches!(target.scheme(), "rtsp" | "rtsps")
        && target.host_str().is_some()
        && target.username().is_empty()
        && target.password().is_none()
        && target.fragment().is_none())
    .then_some(target)
}

pub(super) fn response(value: String, source: &url::Url, secure: bool) -> Reply {
    let Some(target) = destination(&value) else {
        return Reply::code(403);
    };
    let scheme = if secure { "rtsps" } else { "rtsp" };
    let port = |url: &url::Url| {
        url.port()
            .unwrap_or(if url.scheme() == "rtsps" { 322 } else { 554 })
    };
    let path = |url: &url::Url| {
        percent_encoding::percent_decode_str(url.path().trim_matches('/'))
            .decode_utf8_lossy()
            .into_owned()
    };
    let same = target.scheme() == scheme
        && target
            .host_str()
            .unwrap()
            .eq_ignore_ascii_case(source.host_str().unwrap())
        && port(&target) == source.port().unwrap_or(if secure { 322 } else { 554 })
        && path(&target) == path(source)
        && target.query_pairs().eq(source.query_pairs());
    if (secure && target.scheme() != "rtsps") || same {
        return Reply::code(403);
    }
    let mut reply = Reply::code(302);
    // Preserve the backend's path/query bytes; do not copy API/peer credentials
    // or invent authorization for the destination. The viewer reconnects there.
    reply.headers = vec![("Location", value), ("Cache-Control", "no-store".into())];
    reply.close = true;
    reply
}
