//! Bounded, per-connection receiver authentication. Secrets never enter errors.
use base64::{Engine, engine::general_purpose::STANDARD};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

type Parameters = HashMap<String, String>;
type ParsedChallenge = (String, Parameters);

const REJECTED: &str = "push_auth_rejected";
#[derive(Clone)]
pub(super) struct Credentials {
    username: String,
    password: String,
}
impl Credentials {
    pub fn take(url: &mut url::Url) -> Result<Option<Self>, String> {
        if url.username().is_empty() && url.password().is_none() {
            return Ok(None);
        }
        let decode = |s: &str, max: usize| {
            let text = percent_encoding::percent_decode_str(s)
                .decode_utf8()
                .map_err(|_| "Invalid RTSP destination credentials")?;
            if text.len() > max || !text.bytes().all(|b| (32..127).contains(&b)) {
                return Err("Invalid RTSP destination credentials");
            }
            Ok(text.into_owned())
        };
        let username = decode(url.username(), 256)?;
        let password = decode(url.password().unwrap_or(""), 512)?;
        if username.is_empty() || username.contains(':') {
            return Err("Invalid RTSP destination credentials".into());
        }
        url.set_password(None)
            .map_err(|_| "Invalid RTSP destination credentials")?;
        url.set_username("")
            .map_err(|_| "Invalid RTSP destination credentials")?;
        Ok(Some(Self { username, password }))
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Algorithm {
    Md5,
    Md5Sess,
    Sha256,
    Sha256Sess,
}
impl Algorithm {
    fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "md5" => Some(Self::Md5),
            "md5-sess" => Some(Self::Md5Sess),
            "sha-256" => Some(Self::Sha256),
            "sha-256-sess" => Some(Self::Sha256Sess),
            _ => None,
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Md5 => "MD5",
            Self::Md5Sess => "MD5-sess",
            Self::Sha256 => "SHA-256",
            Self::Sha256Sess => "SHA-256-sess",
        }
    }
    fn rank(self) -> u8 {
        match self {
            Self::Md5 => 1,
            Self::Md5Sess => 2,
            Self::Sha256 => 3,
            Self::Sha256Sess => 4,
        }
    }
    fn session(self) -> bool {
        matches!(self, Self::Md5Sess | Self::Sha256Sess)
    }
    fn hash(self, text: &str) -> String {
        if matches!(self, Self::Md5 | Self::Md5Sess) {
            format!("{:x}", md5::Md5::digest(text.as_bytes()))
        } else {
            format!("{:x}", Sha256::digest(text.as_bytes()))
        }
    }
}
#[derive(PartialEq, Eq)]
struct DigestChallenge {
    realm: String,
    nonce: String,
    opaque: Option<String>,
    algorithm: Algorithm,
    qop: bool,
    stale: bool,
}
enum Challenge {
    Basic,
    Digest(DigestChallenge),
}
impl Challenge {
    fn rank(&self) -> u8 {
        match self {
            Self::Basic => 0,
            Self::Digest(d) => d.algorithm.rank(),
        }
    }
}
pub(super) struct Auth {
    credentials: Option<Credentials>,
    challenge: Option<Challenge>,
    cnonce: String,
    nc: u32,
}
impl Auth {
    pub fn new(credentials: Option<Credentials>) -> Self {
        Self {
            credentials,
            challenge: None,
            cnonce: String::new(),
            nc: 0,
        }
    }
    pub fn challenge<'a>(
        &mut self,
        values: impl Iterator<Item = &'a str>,
    ) -> Result<(), &'static str> {
        if self.credentials.is_none() {
            return Err(REJECTED);
        }
        let mut selected: Option<Challenge> = None;
        let mut digest_offered = false;
        let mut count = 0;
        let mut algorithms = HashSet::new();
        for value in values {
            for (scheme, params) in parse(value)? {
                count += 1;
                if count > 8 {
                    return Err(REJECTED);
                }
                if scheme.eq_ignore_ascii_case("digest") {
                    digest_offered = true;
                }
                let Some(candidate) = supported(&scheme, params)? else {
                    continue;
                };
                if !algorithms.insert(candidate.rank()) {
                    return Err(REJECTED);
                }
                if let Some(old) = &selected {
                    if old.rank() > candidate.rank() {
                        continue;
                    }
                }
                selected = Some(candidate);
            }
        }
        let next = selected.ok_or(REJECTED)?;
        if digest_offered && matches!(next, Challenge::Basic) {
            return Err(REJECTED);
        }
        if let Some(previous) = &self.challenge {
            match (previous, &next) {
                (Challenge::Digest(old), Challenge::Digest(new))
                    if new.stale
                        && new.nonce != old.nonce
                        && new.realm == old.realm
                        && new.algorithm == old.algorithm
                        && new.qop == old.qop => {}
                _ => return Err(REJECTED),
            }
        }
        self.cnonce = uuid::Uuid::new_v4().simple().to_string();
        self.nc = 0;
        self.challenge = Some(next);
        Ok(())
    }
    pub fn authorization(&mut self, method: &str, uri: &str) -> Result<String, &'static str> {
        let Some(challenge) = &self.challenge else {
            return Ok(String::new());
        };
        let c = self.credentials.as_ref().ok_or(REJECTED)?;
        let value = match challenge {
            Challenge::Basic => format!(
                "Basic {}",
                STANDARD.encode(format!("{}:{}", c.username, c.password))
            ),
            Challenge::Digest(d) => {
                self.nc = self.nc.checked_add(1).ok_or(REJECTED)?;
                let nc = format!("{:08x}", self.nc);
                let mut a1 = d
                    .algorithm
                    .hash(&format!("{}:{}:{}", c.username, d.realm, c.password));
                if d.algorithm.session() {
                    a1 = d
                        .algorithm
                        .hash(&format!("{a1}:{}:{}", d.nonce, self.cnonce));
                }
                let a2 = d.algorithm.hash(&format!("{method}:{uri}"));
                let response = if d.qop {
                    d.algorithm
                        .hash(&format!("{a1}:{}:{nc}:{}:auth:{a2}", d.nonce, self.cnonce))
                } else {
                    d.algorithm.hash(&format!("{a1}:{}:{a2}", d.nonce))
                };
                let mut value = format!(
                    "Digest username={}, realm={}, nonce={}, uri={}, response={}, algorithm={}",
                    quote(&c.username),
                    quote(&d.realm),
                    quote(&d.nonce),
                    quote(uri),
                    quote(&response),
                    d.algorithm.name()
                );
                if let Some(opaque) = &d.opaque {
                    value.push_str(&format!(", opaque={}", quote(opaque)));
                }
                if d.qop {
                    value.push_str(&format!(
                        ", qop=auth, nc={nc}, cnonce={}",
                        quote(&self.cnonce)
                    ));
                } else if d.algorithm.session() {
                    value.push_str(&format!(", cnonce={}", quote(&self.cnonce)));
                }
                value
            }
        };
        Ok(format!("Authorization: {value}\r\n"))
    }
}
fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
}
fn supported(scheme: &str, mut p: Parameters) -> Result<Option<Challenge>, &'static str> {
    if !scheme.eq_ignore_ascii_case("basic") && !scheme.eq_ignore_ascii_case("digest") {
        return Ok(None);
    }
    let realm = p.remove("realm").ok_or(REJECTED)?;
    if p.get("charset")
        .is_some_and(|s| !s.eq_ignore_ascii_case("UTF-8"))
    {
        return Ok(None);
    }
    if scheme.eq_ignore_ascii_case("basic") {
        return Ok(Some(Challenge::Basic));
    }
    let nonce = p
        .remove("nonce")
        .filter(|n| !n.is_empty())
        .ok_or(REJECTED)?;
    let Some(algorithm) = Algorithm::parse(p.get("algorithm").map_or("MD5", String::as_str)) else {
        return Ok(None);
    };
    if p.get("userhash")
        .is_some_and(|s| !s.eq_ignore_ascii_case("false"))
    {
        return Ok(None);
    }
    let qop = match p.get("qop") {
        None => false,
        Some(s) if s.split(',').any(|q| q.trim() == "auth") => true,
        _ => return Ok(None),
    };
    let stale = match p.get("stale").map(|s| s.to_ascii_lowercase()) {
        None => false,
        Some(s) if s == "false" => false,
        Some(s) if s == "true" => true,
        _ => return Err(REJECTED),
    };
    Ok(Some(Challenge::Digest(DigestChallenge {
        realm,
        nonce,
        opaque: p.remove("opaque"),
        algorithm,
        qop,
        stale,
    })))
}
// Parse combined and repeated challenges, preserving comma lists inside quotes.
// Limits apply before allocation. Unknown auth schemes do not expand this grammar.
fn parse(text: &str) -> Result<Vec<ParsedChallenge>, &'static str> {
    if text.len() > 4096 || !text.bytes().all(|b| (32..127).contains(&b) || b == b'\t') {
        return Err(REJECTED);
    }
    let b = text.as_bytes();
    let mut at = 0;
    let mut out = Vec::new();
    while at < b.len() {
        ws(b, &mut at);
        let scheme = token(b, &mut at)?;
        if at == b.len() || !matches!(b[at], b' ' | b'\t') {
            return Err(REJECTED);
        }
        ws(b, &mut at);
        let mut params = HashMap::new();
        loop {
            let key = token(b, &mut at)?.to_ascii_lowercase();
            ws(b, &mut at);
            if b.get(at) != Some(&b'=') {
                return Err(REJECTED);
            }
            at += 1;
            ws(b, &mut at);
            let value = if b.get(at) == Some(&b'"') {
                at += 1;
                let mut value = String::new();
                loop {
                    let ch = *b.get(at).ok_or(REJECTED)?;
                    at += 1;
                    if ch == b'"' {
                        break;
                    }
                    let ch = if ch == b'\\' {
                        let ch = *b.get(at).ok_or(REJECTED)?;
                        at += 1;
                        ch
                    } else {
                        ch
                    };
                    if !(32..127).contains(&ch) || value.len() >= 1024 {
                        return Err(REJECTED);
                    }
                    value.push(char::from(ch));
                }
                value
            } else {
                token(b, &mut at)?.to_owned()
            };
            if value.len() > 1024 || params.len() >= 16 || params.insert(key, value).is_some() {
                return Err(REJECTED);
            }
            ws(b, &mut at);
            if at == b.len() {
                break;
            }
            if b[at] != b',' {
                return Err(REJECTED);
            }
            at += 1;
            ws(b, &mut at);
            // A token followed by '=' is another parameter; otherwise a scheme.
            let mut look = at;
            token(b, &mut look)?;
            ws(b, &mut look);
            if b.get(look) != Some(&b'=') {
                break;
            }
        }
        out.push((scheme.to_owned(), params));
        if out.len() > 8 {
            return Err(REJECTED);
        }
    }
    Ok(out)
}
fn ws(b: &[u8], at: &mut usize) {
    while b.get(*at).is_some_and(|c| matches!(c, b' ' | b'\t')) {
        *at += 1;
    }
}
fn token<'a>(b: &'a [u8], at: &mut usize) -> Result<&'a str, &'static str> {
    let start = *at;
    while b
        .get(*at)
        .is_some_and(|c| c.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(c))
    {
        *at += 1;
    }
    if *at == start {
        return Err(REJECTED);
    }
    std::str::from_utf8(&b[start..*at]).map_err(|_| REJECTED)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn owned() -> Auth {
        Auth::new(Some(Credentials {
            username: "Mufasa".into(),
            password: "Circle of Life".into(),
        }))
    }
    #[test]
    fn digest_matches_independent_rfc7616_vectors() {
        // RFC7616 section3.9.1, fixed cnonce only for the published test vector.
        for (algorithm, response) in [
            ("MD5", "8ca523f5e9506fed4657c9700eebdbec"),
            (
                "SHA-256",
                "753927fa0e85d155564e2e272a28d1802ca10daf4496794697cf8db5856cb6c1",
            ),
        ] {
            let mut auth = owned();
            let challenge = format!(
                "Digest realm=\"http-auth@example.org\", qop=\"auth, auth-int\", algorithm={algorithm}, nonce=\"7ypf/xlj9XXwfDPEoM4URrv/xwf94BcCAzFZH4GiTo0v\", opaque=\"FQhe/qaU925kfnzjCev0ciny7QMkPqMAFRtzCUYo5tdS\""
            );
            auth.challenge(std::iter::once(challenge.as_str())).unwrap();
            auth.cnonce = "f2/wE4q74E6zIJEtWaHKaf5wv/H5QzzpXusqGemxURZJ".into();
            let value = auth.authorization("GET", "/dir/index.html").unwrap();
            assert!(value.contains(&format!("response=\"{response}\"")));
            assert!(value.contains("nc=00000001"));
            assert!(
                auth.authorization("GET", "/dir/index.html")
                    .unwrap()
                    .contains("nc=00000002")
            );
        }
    }
    #[test]
    fn multiple_challenges_choose_sha256_and_fail_ambiguous_algorithms() {
        let mut auth = owned();
        auth.challenge(
            [
                "Basic realm=\"owned\", Digest realm=\"owned\", nonce=\"one\", algorithm=MD5",
                "Digest realm=\"owned\", nonce=\"two\", algorithm=SHA-256, qop=\"auth\"",
            ]
            .into_iter(),
        )
        .unwrap();
        let header = auth
            .authorization("RECORD", "rtsp://example/stream")
            .unwrap();
        assert!(header.contains("algorithm=SHA-256"));
        assert!(!header.contains("Basic "));
        let mut auth = owned();
        assert!(
            auth.challenge(
                [
                    "Digest realm=\"owned\", nonce=\"two\", algorithm=SHA-256",
                    "Digest realm=\"owned\", nonce=\"one\", algorithm=MD5",
                    "Digest realm=\"other\", nonce=\"three\", algorithm=MD5"
                ]
                .into_iter()
            )
            .is_err(),
            "ambiguous lower-ranked challenges must also fail"
        );
    }
    #[test]
    fn malformed_bounded_and_unsupported_challenges_never_authorize() {
        for challenge in [
            "Digest realm=\"owned\", nonce=\"one\", NONCE=\"two\"",
            "Digest realm=\"owned\", nonce=\"\"",
            "Digest realm=\"unterminated",
            "Digest realm=\"owned\", nonce=\"one\", qop=\"auth-int\"",
            "Digest realm=\"owned\", nonce=\"one\", algorithm=SHA-512",
            "Digest realm=\"owned\", nonce=\"one\", userhash=true",
            "Digest realm=\"owned\", nonce=\"one\", stale=maybe",
            "Basic realm=\"owned\", Digest realm=\"owned\", nonce=\"one\", algorithm=SHA-512",
            "Basic realm=\"owned\",",
            "Basic realm=\"owned\"\r\nAuthorization: leaked",
        ] {
            let mut auth = owned();
            assert_eq!(
                auth.challenge(std::iter::once(challenge)).err(),
                Some(REJECTED)
            );
            assert_eq!(
                auth.authorization("OPTIONS", "rtsp://example/stream")
                    .unwrap(),
                ""
            );
        }
        let mut auth = owned();
        let large = format!("Digest realm=\"{}\", nonce=\"one\"", "a".repeat(1025));
        assert!(auth.challenge(std::iter::once(large.as_str())).is_err());
        let large = "a".repeat(4097);
        assert!(auth.challenge(std::iter::once(large.as_str())).is_err());
        assert!(
            auth.challenge(std::iter::repeat_n("Basic realm=\"owned\"", 9))
                .is_err()
        );
        assert!(
            Auth::new(None)
                .challenge(std::iter::once("Basic realm=\"owned\""))
                .is_err()
        );
    }
    #[test]
    fn nonce_renewal_cannot_change_realm_algorithm_or_downgrade() {
        for next in [
            "Basic realm=\"owned\"",
            "Digest realm=\"owned\", nonce=\"two\", algorithm=MD5, stale=true",
            "Digest realm=\"other\", nonce=\"two\", algorithm=SHA-256, stale=true",
            "Digest realm=\"owned\", nonce=\"one\", algorithm=SHA-256, stale=true",
            "Digest realm=\"owned\", nonce=\"two\", algorithm=SHA-256",
        ] {
            let mut auth = owned();
            auth.challenge(std::iter::once(
                "Digest realm=\"owned\", nonce=\"one\", algorithm=SHA-256",
            ))
            .unwrap();
            assert!(auth.challenge(std::iter::once(next)).is_err());
        }
        let mut auth = owned();
        auth.challenge(std::iter::once(
            "Digest realm=\"owned\", nonce=\"one\", algorithm=SHA-256, qop=\"auth\"",
        ))
        .unwrap();
        auth.authorization("RECORD", "rtsp://example/stream")
            .unwrap();
        let previous = auth.cnonce.clone();
        auth.challenge(std::iter::once(
            "Digest realm=\"owned\", nonce=\"two\", algorithm=SHA-256, qop=\"auth\", stale=true",
        ))
        .unwrap();
        assert_ne!(previous, auth.cnonce);
        assert!(
            auth.authorization("OPTIONS", "rtsp://example/stream")
                .unwrap()
                .contains("nc=00000001")
        );
    }
    #[test]
    fn credentials_are_decoded_once_and_removed_from_network_url() {
        let mut url =
            url::Url::parse("rtsps://user%20name:secret%253A%3Avalue@example/stream?token=one")
                .unwrap();
        let credentials = Credentials::take(&mut url).unwrap().unwrap();
        assert_eq!(credentials.username, "user name");
        assert_eq!(credentials.password, "secret%3A:value");
        assert_eq!(url.as_str(), "rtsps://example/stream?token=one");
        for userinfo in [
            ":secret",
            "user%3Aname:secret",
            "user:secret%0A",
            "user:secret%7F",
            "user:%FF",
            "us%C3%A9r:secret",
        ] {
            let mut url = url::Url::parse(&format!("rtsp://{userinfo}@example/stream")).unwrap();
            assert_eq!(
                Credentials::take(&mut url).err().as_deref(),
                Some("Invalid RTSP destination credentials")
            );
        }
    }
}
