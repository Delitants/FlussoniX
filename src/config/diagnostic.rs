use serde_json::{Value, json};

use super::{INPUT_FIELDS, INPUT_FIELDS_DETAIL};

/// Attribute only validation errors whose rejected field is present in the
/// submitted collection. Other errors keep the existing empty-path fallback.
pub(crate) fn nested_validation_path(patch: &Value, detail: &str) -> Option<(String, Value)> {
    for kind in ["streams", "templates"] {
        let Some(rows) = patch.get(kind).and_then(Value::as_array) else {
            continue;
        };
        for (index, row) in rows.iter().enumerate() {
            let Some(fields) = row.as_object() else {
                continue;
            };
            if let Some(key) = detail
                .strip_prefix(&format!("{kind} option "))
                .and_then(|rest| rest.strip_suffix(" is not implemented"))
                .filter(|key| fields.contains_key(*key))
            {
                return Some((
                    "unknown_key".into(),
                    json!(["server_config", kind, index, key]),
                ));
            }
            let field = match detail {
                "inputs must be array" if fields.get("inputs").is_some_and(|v| !v.is_array()) => {
                    Some("inputs")
                }
                "template must be a name string"
                    if fields.get("template").is_some_and(|v| !v.is_string()) =>
                {
                    Some("template")
                }
                "static must be boolean"
                    if fields.get("static").is_some_and(|v| !v.is_boolean()) =>
                {
                    Some("static")
                }
                "disabled must be boolean"
                    if fields.get("disabled").is_some_and(|v| !v.is_boolean()) =>
                {
                    Some("disabled")
                }
                _ => None,
            };
            if let Some(field) = field {
                return Some((
                    "invalid_config".into(),
                    json!(["server_config", kind, index, field]),
                ));
            }
            let Some(inputs) = fields.get("inputs").and_then(Value::as_array) else {
                continue;
            };
            for (input_index, input) in inputs.iter().enumerate() {
                if detail == INPUT_FIELDS_DETAIL {
                    if let Some(input_fields) = input.as_object() {
                        if let Some(key) = input_fields
                            .keys()
                            .find(|key| !INPUT_FIELDS.contains(&key.as_str()))
                        {
                            return Some((
                                "unknown_key".into(),
                                json!(["server_config", kind, index, "inputs", input_index, key]),
                            ));
                        }
                    } else {
                        return Some((
                            "invalid_config".into(),
                            json!(["server_config", kind, index, "inputs", input_index]),
                        ));
                    }
                }
                if detail == "input url required"
                    && input
                        .as_object()
                        .is_some_and(|fields| fields.get("url").is_none_or(|url| !url.is_string()))
                {
                    return Some((
                        "invalid_config".into(),
                        json!(["server_config", kind, index, "inputs", input_index, "url"]),
                    ));
                }
                if let Some(scheme) = detail.strip_prefix("unsupported input protocol: ") {
                    if input["url"]
                        .as_str()
                        .and_then(|url| url.split("://").next())
                        == Some(scheme)
                    {
                        return Some((
                            "invalid_config".into(),
                            json!(["server_config", kind, index, "inputs", input_index, "url"]),
                        ));
                    }
                }
            }
        }
    }
    None
}
