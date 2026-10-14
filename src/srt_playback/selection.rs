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

/// Parse the native extension without repurposing viewer-controlled a/s fields.
pub(crate) fn playback(id: &str) -> Result<(Selection, Option<String>, String), &'static str> {
    if id.len() > 512 || id.chars().any(char::is_control) {
        return Err("invalid SRT stream ID");
    }
    let mut ticket = None;
    let mut fields = Vec::new();
    for field in id
        .strip_prefix("#!::")
        .ok_or("invalid SRT stream ID")?
        .split(',')
    {
        let (key, value) = field.split_once('=').ok_or("invalid SRT stream ID")?;
        if key == "flussonix_ticket" {
            if ticket.is_some()
                || uuid::Uuid::parse_str(value)
                    .ok()
                    .is_none_or(|id| id.to_string() != value)
            {
                return Err("invalid SRT ticket");
            }
            ticket = Some(value.to_owned());
        } else {
            fields.push(field);
        }
    }
    let clean = format!("#!::{}", fields.join(","));
    Ok((Selection::parse(&clean)?, ticket, clean))
}

#[cfg(test)]
mod playback_tests {
    use super::*;
    const ID: &str = "00000000-0000-0000-0000-000000000001";
    #[test]
    fn native_ticket_is_removed_without_rewriting_opaque_viewer_fields() {
        let clean = "#!::u=one%2B+é,r=owned,s=claim,a=opaque,m=request";
        let id = format!("{clean},flussonix_ticket={ID}");
        let (selection, ticket, original) = playback(&id).unwrap();
        assert_eq!(selection.token, "one%2B+é");
        assert_eq!(ticket.as_deref(), Some(ID));
        assert_eq!(original, clean);
        assert!(
            Selection::parse(&id).is_err(),
            "lookup must not accept an incoming ticket"
        );
        let (_, ticket, original) = playback(clean).unwrap();
        assert!(ticket.is_none());
        assert_eq!(original, clean);
    }
    #[test]
    fn native_ticket_parser_rejects_duplicates_noncanonical_values_and_full_id_overflow() {
        for raw in [
            format!("#!::r=owned,flussonix_ticket={ID},flussonix_ticket={ID}"),
            "#!::r=owned,flussonix_ticket=".into(),
            "#!::r=owned,flussonix_ticket=00000000000000000000000000000001".into(),
            format!("#!::r=owned,extra=x,flussonix_ticket={ID}"),
            format!("#!::r=owned,r=other,flussonix_ticket={ID}"),
        ] {
            assert!(playback(&raw).is_err());
        }
        let prefix = "#!::r=owned,m=request,u=";
        let suffix = format!(",flussonix_ticket={ID}");
        let remaining = 512 - prefix.len() - suffix.len();
        let exact = format!("{prefix}{}{suffix}", "x".repeat(remaining));
        assert_eq!(exact.len(), 512);
        assert!(playback(&exact).is_ok());
        assert!(playback(&(exact + "x")).is_err());
    }
}
