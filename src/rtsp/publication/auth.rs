//! Header credentials authorize only this connection's configured publication.
use super::{Request, Snapshot};
use base64::{Engine, engine::general_purpose::STANDARD};
use md5::{Digest, Md5};
use std::collections::HashMap;
use subtle::ConstantTimeEq;

const REALM: &str = "FlussoniX publisher";
pub(super) struct Auth {
    uri: String,
    nonce: String,
}
impl Auth {
    pub(super) fn new(uri: &str) -> Self {
        Self {
            uri: uri.into(),
            nonce: uuid::Uuid::new_v4().simple().to_string(),
        }
    }
    pub(super) fn challenge(&self) -> String {
        format!(
            "Digest realm=\"{REALM}\", nonce=\"{}\", algorithm=MD5, qop=\"auth\"",
            self.nonce
        )
    }
    pub(super) fn verify(
        &self,
        r: &Request,
        expected: &Snapshot,
        password: Option<&str>,
    ) -> Result<(), u16> {
        if r.uri != self.uri {
            return Err(400);
        }
        let header = r.headers.get("authorization");
        if header.is_some() && password.is_some() {
            return Err(400);
        }
        if let Some(password) = password {
            return expected
                .policy
                .accepts_password(password)
                .then_some(())
                .ok_or(403);
        }
        let Some(secret) = expected.config["password"].as_str() else {
            return Ok(());
        };
        let value = header.filter(|h| h.len() <= 8192).ok_or(401u16)?;
        let (scheme, value) = value.split_once(' ').ok_or(401u16)?;
        if scheme.eq_ignore_ascii_case("Basic") {
            let decoded = STANDARD.decode(value).map_err(|_| 401u16)?;
            let decoded = std::str::from_utf8(&decoded).map_err(|_| 401u16)?;
            let (username, password) = decoded.split_once(':').ok_or(401u16)?;
            if !valid_username(username)
                || password.len() > 1024
                || password.chars().any(char::is_control)
            {
                return Err(401);
            }
            return expected
                .policy
                .accepts_password(password)
                .then_some(())
                .ok_or(401);
        }
        if !scheme.eq_ignore_ascii_case("Digest") {
            return Err(401);
        }
        let p = parameters(value).ok_or(401u16)?;
        if p.keys().any(|k| {
            ![
                "username",
                "realm",
                "nonce",
                "uri",
                "response",
                "algorithm",
                "qop",
                "nc",
                "cnonce",
            ]
            .contains(&k.as_str())
        }) {
            return Err(401);
        }
        let get = |key: &str| p.get(key).map(String::as_str).ok_or(401u16);
        let username = get("username")?;
        if !valid_username(username)
            || get("realm")? != REALM
            || get("nonce")? != self.nonce
            || get("uri")? != r.uri
            || p.get("algorithm")
                .is_some_and(|s| !s.eq_ignore_ascii_case("MD5"))
        {
            return Err(401);
        }
        let response = get("response")?;
        if response.len() != 32 || !response.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(401);
        }
        let hash = |s: &str| format!("{:x}", Md5::digest(s.as_bytes()));
        let a1 = hash(&format!("{username}:{REALM}:{secret}"));
        let a2 = hash(&format!("{}:{}", r.method, r.uri));
        let actual = match (p.get("qop"), p.get("nc"), p.get("cnonce")) {
            (None, None, None) => hash(&format!("{a1}:{}:{a2}", self.nonce)),
            (Some(qop), Some(nc), Some(cnonce))
                if qop == "auth"
                    && nc.len() == 8
                    && nc.bytes().all(|b| b.is_ascii_hexdigit())
                    && nc != "00000000"
                    && !cnonce.is_empty()
                    && cnonce.len() <= 256
                    && cnonce.bytes().all(|b| (32..127).contains(&b)) =>
            {
                // Initial admission is single-use; the accepted connection then
                // moves into session handling. Never normalize these hash inputs.
                hash(&format!("{a1}:{}:{nc}:{cnonce}:auth:{a2}", self.nonce))
            }
            _ => return Err(401),
        };
        bool::from(
            actual
                .as_bytes()
                .ct_eq(response.to_ascii_lowercase().as_bytes()),
        )
        .then_some(())
        .ok_or(401)
    }
}
fn valid_username(s: &str) -> bool {
    !s.is_empty() && s.len() <= 256 && !s.contains(':') && s.bytes().all(|b| (32..127).contains(&b))
}
// Bounded quoted-string parser: no duplicates, empty items or ambiguous escapes.
fn parameters(mut s: &str) -> Option<HashMap<String, String>> {
    let mut fields = HashMap::new();
    loop {
        s = s.trim_start_matches([' ', '\t']);
        let end = s.find('=')?;
        let key = s[..end].trim_end_matches([' ', '\t']);
        if key.is_empty() || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
            return None;
        }
        s = s[end + 1..].trim_start_matches([' ', '\t']);
        let value;
        if let Some(rest) = s.strip_prefix('"') {
            let mut text = String::new();
            let mut chars = rest.char_indices();
            let mut end = None;
            while let Some((i, c)) = chars.next() {
                match c {
                    '"' => {
                        end = Some(i + 1);
                        break;
                    }
                    '\\' => {
                        let (_, escaped) = chars.next()?;
                        if !['"', '\\'].contains(&escaped) {
                            return None;
                        }
                        text.push(escaped);
                    }
                    c if c.is_ascii() && !c.is_ascii_control() => text.push(c),
                    _ => return None,
                }
            }
            s = &rest[end?..];
            value = text;
        } else {
            let end = s.find(',').unwrap_or(s.len());
            let text = s[..end].trim_end_matches([' ', '\t']);
            if text.is_empty() || !text.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
                return None;
            }
            value = text.into();
            s = &s[end..];
        }
        if fields.len() == 9 || fields.insert(key.to_ascii_lowercase(), value).is_some() {
            return None;
        }
        s = s.trim_start_matches([' ', '\t']);
        if s.is_empty() {
            return Some(fields);
        }
        s = s.strip_prefix(',')?;
        if s.trim().is_empty() {
            return None;
        }
    }
}
