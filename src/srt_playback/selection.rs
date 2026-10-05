pub struct Selection {
    pub name: String,
    pub token: String,
}
impl Selection {
    pub fn parse(id: &str) -> Result<Self, &'static str> {
        if id.len() > 512 || id.chars().any(char::is_control) {
            return Err("invalid SRT stream ID");
        }
        let content = id.strip_prefix("#!::").ok_or("invalid SRT stream ID")?;
        let mut fields = std::collections::HashMap::new();
        for field in content.split(',') {
            let (key, value) = field.split_once('=').ok_or("invalid SRT stream ID")?;
            if !["r", "m", "u", "s", "a"].contains(&key) || fields.insert(key, value).is_some() {
                return Err("invalid SRT stream ID");
            }
        }
        if fields.get("m").is_some_and(|v| *v != "request") {
            return Err("SRT publishing is not enabled");
        }
        let name = fields.get("r").ok_or("SRT stream name required")?;
        crate::config::valid_name(name).map_err(|_| "invalid SRT stream name")?;
        Ok(Self {
            name: (*name).to_owned(),
            token: fields.get("u").copied().unwrap_or_default().to_owned(),
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selection_keeps_exact_opaque_token_and_ignores_session_claims() {
        let s = Selection::parse(
            "#!::r=owned/nested,m=request,u=token%2B+é,s=untrusted,a=vendor-version",
        )
        .ok()
        .unwrap();
        assert_eq!(s.name, "owned/nested");
        assert_eq!(s.token, "token%2B+é");
        assert!(
            Selection::parse("#!::r=owned")
                .ok()
                .unwrap()
                .token
                .is_empty()
        );
    }
    #[test]
    fn selection_rejects_ambiguous_or_non_playback_stream_ids() {
        for id in [
            "",
            "owned",
            "#!::r=",
            "#!::r=owned,m=publish",
            "#!::r=owned,m=unknown",
            "#!::r=owned,r=other",
            "#!::r=owned,u=a,u=b",
            "#!::r=owned,unknown=x",
            "#!::r=owned,",
            "#!::r=../owned",
            "#!::r=owned,u=a\nb",
        ] {
            assert!(Selection::parse(id).is_err(), "Invalid ID accepted");
        }
    }
    #[test]
    fn selection_uses_utf8_byte_limit() {
        let prefix = "#!::r=owned,u=";
        let exact = format!("{prefix}{}", "é".repeat(249));
        assert_eq!(exact.len(), 512);
        assert!(Selection::parse(&exact).is_ok());
        assert!(Selection::parse(&(exact + "a")).is_err());
    }
}
