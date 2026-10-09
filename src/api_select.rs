use serde_json::{Map, Value};

/// A collection selector compiled once per request. Keep paths flat so an
/// arbitrarily deep unknown query cannot create a recursively dropped tree.
pub(crate) struct Selection<'a> {
    paths: Vec<Vec<&'a str>>,
}

impl<'a> Selection<'a> {
    pub(crate) fn new(select: &'a str) -> Self {
        let mut paths: Vec<Vec<&str>> = Vec::new();
        for field in select.split(',') {
            let path: Vec<_> = field.split('.').collect();
            // Reference selectors resolve overlapping parent/child paths in
            // query order. A later whole parent widens; a later child narrows.
            paths.retain(|old| !old.starts_with(&path) && !path.starts_with(old));
            paths.push(path);
        }
        Self { paths }
    }

    pub(crate) fn project(&self, row: &Value, stream: bool) -> Value {
        let mut selected = Map::new();
        if let Some(source) = row.as_object() {
            for path in &self.paths {
                copy_path(source, &mut selected, path);
            }
            // stream_config_specific requires name even when not selected.
            if stream && let Some(name) = source.get("name") {
                selected.insert("name".into(), name.clone());
            }
        }
        Value::Object(selected)
    }
}

fn copy_path(source: &Map<String, Value>, selected: &mut Map<String, Value>, path: &[&str]) {
    let Some((field, rest)) = path.split_first() else {
        return;
    };
    let Some(value) = source.get(*field) else {
        return;
    };
    if rest.is_empty() {
        selected.insert((*field).into(), value.clone());
    } else if let Some(child) = value.as_object() {
        // Recurse only through existing objects, so depth is bounded by the
        // stored JSON, never by an untrusted selector. Arrays are whole values.
        let target = selected
            .entry((*field).to_owned())
            .or_insert_with(|| Value::Object(Map::new()));
        if let Some(target) = target.as_object_mut() {
            copy_path(child, target, rest);
        }
    }
}
