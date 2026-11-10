use std::collections::HashSet;

/// The destinations a configured RTSPS input permits its upstream to select.
pub struct RedirectOrigins {
    original: (String, u16),
    approved: HashSet<(String, u16)>,
    credentialed: bool,
}

impl RedirectOrigins {
    pub fn parse(input: &str, roots: Option<&serde_json::Value>) -> Result<Self, String> {
        let source = url::Url::parse(input).map_err(|_| "invalid RTSPS input URL")?;
        if source.scheme() != "rtsps" {
            return Err("RTSPS redirect origins require an RTSPS input".into());
        }
        let original = origin(&source).ok_or("invalid RTSPS input origin")?;
        let credentialed = !source.username().is_empty() || source.password().is_some();
        let mut approved = HashSet::new();
        if let Some(value) = roots {
            let rows = value
                .as_array()
                .ok_or("RTSPS redirect origins must be an array")?;
            if rows.len() > 16 {
                return Err("RTSPS redirect origins allow at most 16 entries".into());
            }
            for row in rows {
                let text = row
                    .as_str()
                    .ok_or("RTSPS redirect origins require origin URLs")?;
                if text
                    .bytes()
                    .any(|b| b <= 32 || b == 127 || b == b'@' || b == b'*')
                {
                    return Err("invalid RTSPS redirect origin".into());
                }
                let parsed = url::Url::parse(text).map_err(|_| "invalid RTSPS redirect origin")?;
                if parsed.scheme() != "rtsps"
                    || !["", "/"].contains(&parsed.path())
                    || parsed.query().is_some()
                    || parsed.fragment().is_some()
                    || !parsed.username().is_empty()
                    || parsed.password().is_some()
                {
                    return Err("invalid RTSPS redirect origin".into());
                }
                let root = origin(&parsed).ok_or("invalid RTSPS redirect origin")?;
                if !approved.insert(root) {
                    return Err("duplicate RTSPS redirect origin".into());
                }
            }
        }
        Ok(Self {
            original,
            approved,
            credentialed,
        })
    }

    pub fn permits(&self, target: &url::Url) -> bool {
        if target.scheme() != "rtsps" {
            return false;
        }
        let Some(next) = origin(target) else {
            return false;
        };
        next == self.original || !self.credentialed && self.approved.contains(&next)
    }

    pub(crate) fn matches_source(&self, source: &url::Url) -> bool {
        origin(source).is_some_and(|value| value == self.original)
            && self.credentialed == (!source.username().is_empty() || source.password().is_some())
    }
}

fn origin(url: &url::Url) -> Option<(String, u16)> {
    let host = url.host_str()?;
    let port = url.port().unwrap_or(322);
    (port != 0).then(|| (host.to_ascii_lowercase(), port))
}
