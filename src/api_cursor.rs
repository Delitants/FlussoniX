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
    PositionBound(usize, Direction),
    Boundary(Boundary),
}

pub(crate) struct Page {
    pub(crate) items: Vec<Value>,
    pub(crate) next: Option<String>,
    pub(crate) prev: Option<String>,
}

/// Paginate an already sorted collection using the reference API's positional
/// cursor dialect. Cluster peers and sources have no stable row position field;
/// their positions refer to the current sorted collection.
pub(crate) fn paginate_positions(
    mut items: Vec<Value>,
    query: &HashMap<String, String>,
) -> Result<Page, &'static str> {
    let limit = query
        .get("limit")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(100)
        .clamp(1, 1000);
    let (start, end) = match query.get("cursor") {
        None => (0, limit.min(items.len())),
        Some(token) => {
            let pairs = cursor_pairs(token)?;
            if pairs.len() == 1
                && let Some(position) = pairs.get("$position_gt")
            {
                let start = position_value(position)?.saturating_add(1).min(items.len());
                (start, start.saturating_add(limit).min(items.len()))
            } else if pairs.len() == 2
                && pairs.get("$reversed").is_some_and(|value| value == "true")
                && let Some(position) = pairs.get("$position_lt")
            {
                let end = position_value(position)?.min(items.len());
                (end.saturating_sub(limit), end)
            } else {
                return Err(INVALID);
            }
        }
    };
    let mut page = Page {
        items: Vec::new(),
        next: None,
        prev: None,
    };
    if start < end {
        if end < items.len() {
            page.next = Some(encode_position(end - 1, Direction::Forward));
        }
        if start > 0 {
            page.prev = Some(encode_position(start, Direction::Backward));
        }
        page.items = items.drain(start..end).collect();
    }
    Ok(page)
}

fn encode_position(position: usize, direction: Direction) -> String {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    match direction {
        Direction::Forward => {
            query.append_pair("$position_gt", &position.to_string());
        }
        Direction::Backward => {
            query.append_pair("$position_lt", &position.to_string());
            query.append_pair("$reversed", "true");
        }
    }
    STANDARD.encode(query.finish())
}

pub(crate) fn paginate(
    mut items: Vec<Value>,
    kind: &str,
    query: &HashMap<String, String>,
) -> Result<Page, &'static str> {
    let default_stream_order = kind == "streams" && !query.contains_key("sort");
    let sort = Sort::new(
        query
            .get("sort")
            .map(String::as_str)
            .or(default_stream_order.then_some("position")),
    );
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
        Some(token) => decode(token, &sort, &context, default_stream_order)?,
        None => Cursor::Start,
    };
    items.sort_by(|left, right| sort.compare(left, right));
    let reference_positions = default_stream_order
        && items
            .iter()
            .all(|row| row.get("position").and_then(Value::as_u64).is_some());
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
        Cursor::PositionBound(position, Direction::Forward) => {
            if !reference_positions {
                return Err(INVALID);
            }
            let start =
                items.partition_point(|row| row["position"].as_u64().unwrap() <= position as u64);
            (start, start.saturating_add(limit).min(items.len()))
        }
        Cursor::PositionBound(position, Direction::Backward) => {
            if !reference_positions {
                return Err(INVALID);
            }
            let end =
                items.partition_point(|row| row["position"].as_u64().unwrap() < position as u64);
            (end.saturating_sub(limit), end)
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
            page.next = Some(
                if reference_positions && items[end - 1]["position"] != items[end]["position"] {
                    encode_reference_position(&items[end - 1], Direction::Forward)?
                } else {
                    encode(&sort, &context, &items[end - 1], Direction::Forward)?
                },
            );
        }
        if start > 0 {
            page.prev = Some(
                if reference_positions && items[start - 1]["position"] != items[start]["position"] {
                    encode_reference_position(&items[start], Direction::Backward)?
                } else {
                    encode(&sort, &context, &items[start], Direction::Backward)?
                },
            );
        }
        page.items = items.drain(start..end).collect();
    }
    Ok(page)
}

fn encode_reference_position(row: &Value, direction: Direction) -> Result<String, &'static str> {
    let position = row["position"].as_u64().ok_or(INVALID)?.to_string();
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    match direction {
        Direction::Forward => {
            query.append_pair("$position_gt", &position);
        }
        Direction::Backward => {
            query.append_pair("$position_lt", &position);
            query.append_pair("$reversed", "true");
        }
    }
    Ok(STANDARD.encode(query.finish()))
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

fn decode(
    token: &str,
    sort: &Sort<'_>,
    context: &str,
    default_stream_order: bool,
) -> Result<Cursor, &'static str> {
    let pairs = cursor_pairs(token)?;
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
    if default_stream_order {
        if pairs.len() == 1
            && let Some(position) = pairs.get("$position_gt")
        {
            return Ok(Cursor::PositionBound(
                position_value(position)?,
                Direction::Forward,
            ));
        }
        if pairs.len() == 2
            && pairs.get("$reversed").is_some_and(|value| value == "true")
            && let Some(position) = pairs.get("$position_lt")
        {
            return Ok(Cursor::PositionBound(
                position_value(position)?,
                Direction::Backward,
            ));
        }
        return Err(INVALID);
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

fn cursor_pairs(token: &str) -> Result<HashMap<String, String>, &'static str> {
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
    Ok(pairs)
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
