use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use std::cmp::Ordering;

/// Scalar collection ordering. Paths are compiled once per request; identity
/// breaks ties so repeated pages have a deterministic order for a fixed snapshot.
pub(crate) struct Sort<'a> {
    fields: Vec<(Vec<&'a str>, bool)>,
}

impl<'a> Sort<'a> {
    pub(crate) fn new(expression: Option<&'a str>) -> Self {
        let mut fields = expression
            .unwrap_or("name")
            .split(',')
            .map(|field| {
                let (path, descending) = field
                    .strip_prefix('-')
                    .map_or((field, false), |path| (path, true));
                (path.split('.').collect::<Vec<_>>(), descending)
            })
            .collect::<Vec<_>>();
        if !fields.iter().any(|(path, _)| path == &["name"]) {
            fields.push((vec!["name"], false));
        }
        Self { fields }
    }

    pub(crate) fn compare(&self, left: &Value, right: &Value) -> Ordering {
        for (path, descending) in &self.fields {
            let order = scalar(left, path).compare(scalar(right, path));
            if order != Ordering::Equal {
                return if *descending { order.reverse() } else { order };
            }
        }
        Ordering::Equal
    }

    pub(crate) fn specification(&self) -> &[(Vec<&'a str>, bool)] {
        &self.fields
    }

    pub(crate) fn identity_direction(&self) -> Option<bool> {
        match self.fields.as_slice() {
            [(path, descending)] if path == &["name"] => Some(*descending),
            _ => None,
        }
    }

    pub(crate) fn keys(&self, row: &Value) -> Vec<Key> {
        self.fields
            .iter()
            .map(|(path, _)| match scalar(row, path) {
                Scalar::Missing => Key::Missing,
                Scalar::Integer(value) => Key::Integer(if value < 0 {
                    Number::from(value as i64)
                } else {
                    Number::from(value as u64)
                }),
                Scalar::Text(value) => Key::Text(value.to_owned()),
                Scalar::Float(value) => Key::Float(value.to_bits()),
                Scalar::Boolean(value) => Key::Boolean(value),
            })
            .collect()
    }

    pub(crate) fn valid_keys(&self, keys: &[Key]) -> bool {
        keys.len() == self.fields.len()
            && self.fields.iter().zip(keys).all(|((path, _), key)| {
                if path == &["name"] {
                    return matches!(key, Key::Text(_));
                }
                match key {
                    Key::Integer(value) => value.is_i64() || value.is_u64(),
                    Key::Float(bits) => f64::from_bits(*bits).is_finite(),
                    Key::Text(value) => value != "null" && value != "undefined",
                    _ => true,
                }
            })
    }

    /// Only used with keys validated against this exact sort specification.
    pub(crate) fn compare_keys(&self, row: &Value, keys: &[Key]) -> Ordering {
        for ((path, descending), key) in self.fields.iter().zip(keys) {
            let order = scalar(row, path).compare(key.scalar());
            if order != Ordering::Equal {
                return if *descending { order.reverse() } else { order };
            }
        }
        Ordering::Equal
    }
}

/// Preserve number types and exact floating-point bits through JSON round trips.
#[derive(Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum Key {
    Missing,
    Integer(Number),
    Text(String),
    Float(u64),
    Boolean(bool),
}

impl Key {
    fn scalar(&self) -> Scalar<'_> {
        match self {
            Self::Missing => Scalar::Missing,
            Self::Integer(value) => value
                .as_i64()
                .map(i128::from)
                .or_else(|| value.as_u64().map(i128::from))
                .map_or(Scalar::Missing, Scalar::Integer),
            Self::Text(value) => Scalar::Text(value),
            Self::Float(bits) => Scalar::Float(f64::from_bits(*bits)),
            Self::Boolean(value) => Scalar::Boolean(*value),
        }
    }
}

enum Scalar<'a> {
    Missing,
    Integer(i128),
    Text(&'a str),
    Float(f64),
    Boolean(bool),
}

impl Scalar<'_> {
    fn rank(&self) -> u8 {
        match self {
            Self::Missing => 0,
            Self::Integer(_) => 1,
            Self::Text(_) => 2,
            Self::Float(_) => 3,
            Self::Boolean(_) => 4,
        }
    }

    fn compare(self, other: Self) -> Ordering {
        match (self, other) {
            (Self::Integer(a), Self::Integer(b)) => a.cmp(&b),
            (Self::Text(a), Self::Text(b)) => a.cmp(b),
            // JSON numbers are finite. partial_cmp also treats -0.0 and 0.0
            // as equal, allowing the identity key to break that tie.
            (Self::Float(a), Self::Float(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
            (Self::Boolean(a), Self::Boolean(b)) => a.cmp(&b),
            (a, b) => a.rank().cmp(&b.rank()),
        }
    }
}

fn scalar<'a>(row: &'a Value, path: &[&str]) -> Scalar<'a> {
    let mut value = row;
    for part in path {
        let Some(next) = value.as_object().and_then(|object| object.get(*part)) else {
            return Scalar::Missing;
        };
        value = next;
    }
    match value {
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                Scalar::Integer(i128::from(value))
            } else if let Some(value) = number.as_u64() {
                Scalar::Integer(i128::from(value))
            } else {
                number.as_f64().map_or(Scalar::Missing, Scalar::Float)
            }
        }
        // These are valid literal identities. Preserve existing name ordering
        // and its tie-breaker even when an identity spells a null sentinel.
        Value::String(text) if path == ["name"] || (text != "null" && text != "undefined") => {
            Scalar::Text(text)
        }
        Value::Bool(value) => Scalar::Boolean(*value),
        _ => Scalar::Missing,
    }
}

#[cfg(test)]
mod tests {
    use super::Sort;
    use serde_json::json;

    #[test]
    fn distinct_runtime_statistics_drive_composite_scalar_order() {
        let rows = [
            json!({"name":"alpha","stats":{"status":"waiting","online_clients":0}}),
            json!({"name":"beta","stats":{"status":"error","online_clients":20}}),
            json!({"name":"gamma","stats":{"status":"online","online_clients":10}}),
            json!({"name":"delta","stats":{"status":"online","online_clients":2}}),
        ];
        for (expression, expected) in [
            (
                "stats.status,-stats.online_clients",
                ["beta", "gamma", "delta", "alpha"],
            ),
            ("stats.online_clients", ["alpha", "delta", "gamma", "beta"]),
            ("-stats.online_clients", ["beta", "gamma", "delta", "alpha"]),
        ] {
            let mut sorted = rows.clone();
            let sort = Sort::new(Some(expression));
            sorted.sort_by(|left, right| sort.compare(left, right));
            assert_eq!(
                sorted
                    .iter()
                    .map(|row| row["name"].as_str().unwrap())
                    .collect::<Vec<_>>(),
                expected,
                "{expression}"
            );
        }
    }
}
