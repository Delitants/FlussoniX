//! Independent scalar collection filtering for the implemented management profile.
use serde_json::Value;
use std::{cmp::Ordering, collections::HashMap};

#[derive(Clone, Copy)]
enum Scalar {
    String,
    Integer,
    Boolean,
}

enum Condition {
    Any(Vec<Value>),
    Not(Value),
    Compare(Ordering, bool, Value),
    Like(String),
    Missing(bool),
}

struct Predicate<'a> {
    path: Vec<&'a str>,
    condition: Condition,
}

pub(crate) struct Filters<'a>(Vec<Predicate<'a>>);

impl<'a> Filters<'a> {
    pub(crate) fn parse(query: &'a HashMap<String, String>, streams: bool) -> Result<Self, String> {
        let mut predicates = Vec::new();
        for (key, raw) in query {
            let (field, operation) = operation(key, raw);
            let Some(scalar) = field_type(field, streams) else {
                // Unknown fields and unsupported object/array paths are ignored.
                continue;
            };
            let convert = |value: &str| {
                scalar
                    .parse(value)
                    .ok_or_else(|| format!("invalid filter value for {field}"))
            };
            let condition = match operation {
                "is" => Condition::Missing(true),
                "is_not" => Condition::Missing(false),
                "like" => {
                    if !matches!(scalar, Scalar::String) {
                        return Err(format!("substring filter requires a string field: {field}"));
                    }
                    Condition::Like(raw.clone())
                }
                "ne" => Condition::Not(convert(raw)?),
                "lt" => Condition::Compare(Ordering::Less, false, convert(raw)?),
                "lte" => Condition::Compare(Ordering::Less, true, convert(raw)?),
                "gt" => Condition::Compare(Ordering::Greater, false, convert(raw)?),
                "gte" => Condition::Compare(Ordering::Greater, true, convert(raw)?),
                _ => Condition::Any(raw.split(',').map(convert).collect::<Result<_, _>>()?),
            };
            predicates.push(Predicate {
                path: field.split('.').collect(),
                condition,
            });
        }
        Ok(Self(predicates))
    }

    pub(crate) fn matches(&self, row: &Value) -> bool {
        self.0.iter().all(|predicate| {
            let value = predicate.path.iter().try_fold(row, |value, field| {
                value.as_object()?.get(*field).filter(|value| {
                    !value.is_null() && !matches!(value.as_str(), Some("null" | "undefined"))
                })
            });
            match &predicate.condition {
                Condition::Missing(missing) => value.is_none() == *missing,
                Condition::Any(values) => value.is_some_and(|value| values.contains(value)),
                Condition::Not(expected) => value != Some(expected),
                Condition::Like(pattern) => value
                    .and_then(Value::as_str)
                    .is_some_and(|value| value.contains(pattern)),
                Condition::Compare(direction, inclusive, expected) => value
                    .and_then(|value| compare(value, expected))
                    .is_some_and(|order| {
                        order == *direction || (*inclusive && order == Ordering::Equal)
                    }),
            }
        })
    }
}

fn operation<'a>(key: &'a str, raw: &str) -> (&'a str, &'static str) {
    if raw == "null" {
        for (suffix, op) in [("_is_not", "is_not"), ("_is", "is")] {
            if let Some(field) = key.strip_suffix(suffix) {
                return (field, op);
            }
        }
    }
    for (suffix, op) in [
        ("_like", "like"),
        ("_gte", "gte"),
        ("_lte", "lte"),
        ("_gt", "gt"),
        ("_lt", "lt"),
        ("_ne", "ne"),
    ] {
        if let Some(field) = key.strip_suffix(suffix) {
            return (field, op);
        }
    }
    (key, "eq")
}

fn field_type(mut field: &str, streams: bool) -> Option<Scalar> {
    if let Some(disk) = field.strip_prefix("config_on_disk.") {
        if !streams {
            return None;
        }
        field = disk;
    } else if streams {
        match field {
            "stats.status" => return Some(Scalar::String),
            "stats.online_clients" => return Some(Scalar::Integer),
            "stats.alive" => return Some(Scalar::Boolean),
            _ => {}
        }
    }
    match field {
        "name" | "title" | "comment" | "template" | "transcoder.encoder" => Some(Scalar::String),
        "position" | "transcoder.vb" | "transcoder.ab" => Some(Scalar::Integer),
        "static" | "disabled" => Some(Scalar::Boolean),
        _ => None,
    }
}

impl Scalar {
    fn parse(self, raw: &str) -> Option<Value> {
        match self {
            Self::String => Some(Value::String(raw.to_owned())),
            Self::Boolean => match raw {
                "true" => Some(Value::Bool(true)),
                "false" => Some(Value::Bool(false)),
                _ => None,
            },
            Self::Integer => raw
                .parse::<i64>()
                .map(Value::from)
                .ok()
                .or_else(|| raw.parse::<u64>().map(Value::from).ok()),
        }
    }
}

fn compare(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
        (Value::Bool(left), Value::Bool(right)) => Some(left.cmp(right)),
        (Value::Number(_), Value::Number(_)) => {
            let integer = |value: &Value| {
                value
                    .as_i64()
                    .map(i128::from)
                    .or_else(|| value.as_u64().map(i128::from))
            };
            Some(integer(left)?.cmp(&integer(right)?))
        }
        _ => None,
    }
}
