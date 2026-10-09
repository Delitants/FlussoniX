//! Bounded, opaque value-based pagination for the scalar collection profile.
use crate::api_sort::{Key, Sort};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
};

const MAX_TOKEN: usize = 24_000;
const MAX_DECODED: usize = 16_384;
const NATIVE: &str = "$flussonix_cursor";
const INVALID: &str = "invalid or unsupported collection cursor";

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Direction {
    Forward,
    Backward,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Boundary {
    version: u8,
    context: String,
    direction: Direction,
    keys: Vec<Key>,
}

enum Cursor {
    Start,
    Position(usize),
    Boundary(Boundary),
}

pub(crate) struct Page {
    pub(crate) items: Vec<Value>,
    pub(crate) next: Option<String>,
    pub(crate) prev: Option<String>,
}

pub(crate) fn paginate(
    mut items: Vec<Value>,
    kind: &str,
    query: &HashMap<String, String>,
) -> Result<Page, &'static str> {
    let sort = Sort::new(query.get("sort").map(String::as_str));
    // Bind the collection, canonical sort specification and original filter/search
    // arguments. Projection and page size may change without changing membership.
    let membership = query
        .iter()
        .filter(|(key, _)| !matches!(key.as_str(), "cursor" | "sort" | "select" | "limit"))
        .collect::<BTreeMap<_, _>>();
    let context = format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&(kind, sort.specification(), membership)).map_err(|_| INVALID)?
        )
    );
    let cursor = match query.get("cursor") {
        Some(token) => decode(token, &sort, &context)?,
        None => Cursor::Start,
    };
    items.sort_by(|left, right| sort.compare(left, right));
    let limit = query
        .get("limit")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(100)
        .clamp(1, 1000);
    let (start, end) = match cursor {
        Cursor::Start => (0, limit.min(items.len())),
        Cursor::Position(offset) => {
            let start = offset.min(items.len());
            (start, start.saturating_add(limit).min(items.len()))
        }
        Cursor::Boundary(boundary) => match boundary.direction {
            Direction::Forward => {
                let start = items.partition_point(|row| {
                    sort.compare_keys(row, &boundary.keys) != Ordering::Greater
                });
                (start, start.saturating_add(limit).min(items.len()))
            }
            Direction::Backward => {
                let end = items.partition_point(|row| {
                    sort.compare_keys(row, &boundary.keys) == Ordering::Less
                });
                (end.saturating_sub(limit), end)
            }
        },
    };
    let mut page = Page {
        items: Vec::new(),
        next: None,
        prev: None,
    };
    if start < end {
        if end < items.len() {
            page.next = Some(encode(
                &sort,
                &context,
                &items[end - 1],
                Direction::Forward,
            )?);
        }
        if start > 0 {
            page.prev = Some(encode(&sort, &context, &items[start], Direction::Backward)?);
        }
        page.items = items.drain(start..end).collect();
    }
    Ok(page)
}

fn encode(
    sort: &Sort<'_>,
    context: &str,
    row: &Value,
    direction: Direction,
) -> Result<String, &'static str> {
    let boundary = Boundary {
        version: 1,
        context: context.to_owned(),
        direction,
        keys: sort.keys(row),
    };
    let json = serde_json::to_string(&boundary).map_err(|_| INVALID)?;
    let decoded = url::form_urlencoded::Serializer::new(String::new())
        .append_pair(NATIVE, &json)
        .finish();
    let token = STANDARD.encode(&decoded);
    if decoded.len() > MAX_DECODED || token.len() > MAX_TOKEN {
        return Err("collection cursor exceeds size limit; choose smaller scalar sort fields");
    }
    Ok(token)
}

fn decode(token: &str, sort: &Sort<'_>, context: &str) -> Result<Cursor, &'static str> {
    if token.len() > MAX_TOKEN {
        return Err(INVALID);
    }
    let decoded = STANDARD.decode(token).map_err(|_| INVALID)?;
    if decoded.len() > MAX_DECODED {
        return Err(INVALID);
    }
    let decoded = std::str::from_utf8(&decoded).map_err(|_| INVALID)?;
    let mut pairs = HashMap::new();
    for pair in decoded.split('&') {
        let (key, value) = pair.split_once('=').ok_or(INVALID)?;
        let key = component(key)?;
        if key.is_empty() || pairs.insert(key, component(value)?).is_some() {
            return Err(INVALID);
        }
    }
    if let Some(json) = pairs.get(NATIVE) {
        if pairs.len() != 1 {
            return Err(INVALID);
        }
        let boundary: Boundary = serde_json::from_str(json).map_err(|_| INVALID)?;
        if boundary.version != 1 || boundary.context != context || !sort.valid_keys(&boundary.keys)
        {
            return Err(INVALID);
        }
        return Ok(Cursor::Boundary(boundary));
    }
    if pairs.len() == 1
        && let Some(position) = pairs.get("$position_gt")
    {
        return Ok(Cursor::Position(
            position_value(position)?.saturating_add(1),
        ));
    }
    // Reference name-bound dialect is unambiguous only for a single identity key.
    // Compound reference bounds and their positional semantics are not qualified.
    let descending = sort.identity_direction().ok_or(INVALID)?;
    let direction = match pairs.get("$reversed").map(String::as_str) {
        None => Direction::Forward,
        Some("true") => Direction::Backward,
        _ => return Err(INVALID),
    };
    let expected = if descending ^ matches!(direction, Direction::Backward) {
        "name_lt"
    } else {
        "name_gt"
    };
    let name = pairs.get(expected).ok_or(INVALID)?;
    if pairs.contains_key("$position_gt") && pairs.contains_key("$position_lt") {
        return Err(INVALID);
    }
    for (key, value) in &pairs {
        match key.as_str() {
            "$position_gt" | "$position_lt" => {
                position_value(value)?;
            }
            "$reversed" => (),
            _ if key == expected => (),
            _ => return Err(INVALID),
        }
    }
    Ok(Cursor::Boundary(Boundary {
        version: 1,
        context: context.to_owned(),
        direction,
        keys: vec![Key::Text(name.clone())],
    }))
}

fn position_value(value: &str) -> Result<usize, &'static str> {
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(INVALID);
    }
    value.parse().map_err(|_| INVALID)
}

fn component(value: &str) -> Result<String, &'static str> {
    let bytes = value.as_bytes();
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'%'
            && !(bytes.get(index + 1).is_some_and(u8::is_ascii_hexdigit)
                && bytes.get(index + 2).is_some_and(u8::is_ascii_hexdigit))
        {
            return Err(INVALID);
        }
    }
    percent_encoding::percent_decode_str(&value.replace('+', " "))
        .decode_utf8()
        .map(|v| v.into_owned())
        .map_err(|_| INVALID)
}
