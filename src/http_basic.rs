//! HTTP input credentials never enter decoder URLs or cross their configured origin.
use axum::http::{HeaderMap, HeaderValue};
use base64::{Engine as _, engine::general_purpose::STANDARD};

fn valid(username: &str, password: &str) -> bool {
    !username.is_empty()
        && username.len() <= 256
        && !username.contains(':')
        && !username.chars().any(char::is_control)
        && password.len() <= 1024
        && !password.chars().any(char::is_control)
}

pub(crate) fn take_url_credentials(
    url: &mut url::Url,
) -> Result<Option<HeaderValue>, &'static str> {
    if url.username().is_empty() && url.password().is_none() {
        return Ok(None);
    }
    let username = percent_encoding::percent_decode_str(url.username())
        .decode_utf8()
        .map_err(|_| "invalid HTTP Basic username")?;
    let password = percent_encoding::percent_decode_str(url.password().unwrap_or(""))
        .decode_utf8()
        .map_err(|_| "invalid HTTP Basic password")?;
    if !valid(&username, &password) {
        return Err("invalid HTTP Basic credentials");
    }
    let mut header = HeaderValue::from_str(&format!(
        "Basic {}",
        STANDARD.encode(format!("{username}:{password}"))
    ))
    .map_err(|_| "invalid HTTP Basic credentials")?;
    header.set_sensitive(true);
    url.set_username("").map_err(|_| "invalid HTTP input URL")?;
    url.set_password(None)
        .map_err(|_| "invalid HTTP input URL")?;
    Ok(Some(header))
}

pub(crate) fn publisher_password(headers: &HeaderMap) -> Result<Option<String>, ()> {
    let Some(header) = headers.get("authorization") else {
        return Ok(None);
    };
    let header = header.to_str().map_err(|_| ())?;
    if header.len() > 8192 {
        return Err(());
    }
    let mut parts = header.split_ascii_whitespace();
    if !parts
        .next()
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("Basic"))
    {
        return Err(());
    }
    let encoded = parts.next().ok_or(())?;
    if parts.next().is_some() {
        return Err(());
    }
    let decoded = STANDARD.decode(encoded).map_err(|_| ())?;
    let decoded = std::str::from_utf8(&decoded).map_err(|_| ())?;
    let (username, password) = decoded.split_once(':').ok_or(())?;
    if !valid(username, password) {
        return Err(());
    }
    Ok(Some(password.into()))
}
