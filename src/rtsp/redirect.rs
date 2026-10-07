//! Strict RTSP destination URI validation and callback-selected viewer responses.
use super::Reply;

pub(crate) fn destination(value: &str) -> Option<url::Url> {
    if value.is_empty() || value.len() > 8192 {
        return None;
    }
    // URL parsers may normalize invalid raw characters. Validate the original
    // RFC3986 URI bytes because those exact bytes go into the Location header.
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'%' {
            if bytes
                .get(index + 1..index + 3)
                .is_none_or(|escape| !escape.iter().all(u8::is_ascii_hexdigit))
            {
                return None;
            }
            index += 3;
        } else if byte.is_ascii_alphanumeric() || b"-._~:/?#[]@!$&'()*+,;=".contains(&byte) {
            index += 1;
        } else {
            return None;
        }
    }
    // Parsers may discard empty userinfo, so reject its original authority
    // marker too. An @ in the path or query is ordinary URI data.
    let authority = value.split_once("://")?.1.split(['/', '?', '#']).next()?;
    if authority.contains('@') {
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
