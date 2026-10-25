use super::*;
use serde::Serialize;

pub(super) async fn dispatch(app: Arc<App>, request: Request, tail: &str, role: Role) -> Response {
    let method = request.method().clone();
    let query = parse_query(request.uri().query());
    let suffix = tail.strip_prefix("iptv").unwrap_or("");
    if suffix.is_empty() {
        return match method {
            axum::http::Method::GET => {
                if app.iptv.snapshot().enabled() {
                    management_read_response(role, json!({}))
                } else {
                    error(StatusCode::NOT_FOUND, "IPTV is disabled")
                }
            }
            axum::http::Method::PUT => {
                let body = match read_json(request).await {
                    Ok(body) => body,
                    Err((status, message)) => return error(status, &message),
                };
                if body.as_object().is_none_or(|fields| !fields.is_empty()) {
                    return error(StatusCode::BAD_REQUEST, "unsupported IPTV root setting");
                }
                let store = app.iptv.clone();
                match mutate(move || store.reload_validated()).await {
                    Ok(revision) => json_response(json!({"revision":revision})),
                    Err(response) => *response,
                }
            }
            axum::http::Method::DELETE => {
                let store = app.iptv.clone();
                match mutate(move || store.set_enabled(false)).await {
                    Ok(revision) => with_revision(StatusCode::NO_CONTENT.into_response(), revision),
                    Err(response) => *response,
                }
            }
            _ => error(StatusCode::METHOD_NOT_ALLOWED, "method not allowed"),
        };
    }
    if !app.iptv.snapshot().enabled() {
        return error(StatusCode::NOT_FOUND, "IPTV is disabled");
    }
    let Some(suffix) = suffix.strip_prefix('/') else {
        return error(StatusCode::NOT_FOUND, "unknown IPTV operation");
    };
    let (kind, name) = suffix
        .split_once('/')
        .map_or((suffix, None), |(kind, name)| (kind, Some(name)));
    if !matches!(kind, "packages" | "subscribers") {
        return error(StatusCode::NOT_FOUND, "unknown IPTV collection");
    }
    if let Some(name) = name {
        if valid_name(name).is_err() {
            return error(StatusCode::BAD_REQUEST, "invalid IPTV item name");
        }
        return item(app, request, kind, name, role).await;
    }
    if method != axum::http::Method::GET {
        return error(
            StatusCode::METHOD_NOT_ALLOWED,
            "IPTV collection is read only",
        );
    }
    collection(app, kind, role, &query)
}

async fn item(app: Arc<App>, request: Request, kind: &str, name: &str, role: Role) -> Response {
    match *request.method() {
        axum::http::Method::GET => {
            let snapshot = app.iptv.snapshot();
            let row = if kind == "packages" {
                snapshot
                    .document()
                    .packages
                    .iter()
                    .find(|row| row.name == name)
                    .and_then(|row| serde_json::to_value(row).ok())
            } else {
                snapshot
                    .document()
                    .subscribers
                    .iter()
                    .find(|row| row.name == name)
                    .and_then(|row| serde_json::to_value(row).ok())
            };
            match row {
                Some(row) => management_read_response(role, row),
                None => error(StatusCode::NOT_FOUND, "IPTV item not found"),
            }
        }
        axum::http::Method::PUT => {
            let body = match read_json(request).await {
                Ok(body) => body,
                Err((status, message)) => return error(status, &message),
            };
            let store = app.iptv.clone();
            let name = name.to_owned();
            let packages = kind == "packages";
            match mutate(move || {
                if packages {
                    store.put_package(&name, body)
                } else {
                    store.put_subscriber(&name, body)
                }
            })
            .await
            {
                Ok((revision, row)) => with_revision(json_response(row), revision),
                Err(response) => *response,
            }
        }
        axum::http::Method::DELETE => {
            let store = app.iptv.clone();
            let name = name.to_owned();
            let packages = kind == "packages";
            match mutate(move || {
                if packages {
                    store.delete_package(&name)
                } else {
                    store.delete_subscriber(&name)
                }
            })
            .await
            {
                Ok(Some(revision)) => {
                    with_revision(StatusCode::NO_CONTENT.into_response(), revision)
                }
                Ok(None) => error(StatusCode::NOT_FOUND, "IPTV item not found"),
                Err(response) => *response,
            }
        }
        _ => error(StatusCode::METHOD_NOT_ALLOWED, "method not allowed"),
    }
}

async fn mutate<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, Box<Response>> {
    match tokio::task::spawn_blocking(operation).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(message)) => Err(Box::new(error(StatusCode::BAD_REQUEST, &message))),
        Err(_) => Err(Box::new(error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "IPTV edit worker failed",
        ))),
    }
}

fn collection(app: Arc<App>, kind: &str, role: Role, query: &HashMap<String, String>) -> Response {
    let snapshot = app.iptv.snapshot();
    if kind == "packages" {
        collection_rows(
            snapshot.document().packages.iter().collect(),
            |row| row.name.as_str(),
            kind,
            role,
            query,
        )
    } else {
        collection_rows(
            snapshot.document().subscribers.iter().collect(),
            |row| row.name.as_str(),
            kind,
            role,
            query,
        )
    }
}

fn collection_rows<T: Serialize>(
    rows: Vec<&T>,
    name: impl Fn(&T) -> &str,
    kind: &str,
    role: Role,
    query: &HashMap<String, String>,
) -> Response {
    if query
        .get("limit")
        .is_some_and(|raw| raw.parse::<usize>().is_err())
    {
        return error(StatusCode::BAD_REQUEST, "invalid IPTV limit");
    }
    let count = rows.len();
    let sort = query.get("sort").map(String::as_str);
    let compact = sort.is_none_or(|value| matches!(value, "name" | "-name"));
    let mut items = Vec::with_capacity(count);
    for (index, row) in rows.iter().enumerate() {
        let item = match sort {
            None => json!(index),
            Some("name" | "-name") => json!({"name":name(row),"$index":index}),
            Some(_) => match serde_json::to_value(row) {
                Ok(mut value) => {
                    if role == Role::View {
                        redact_view_value(&mut value);
                    }
                    value
                }
                Err(_) => return error(StatusCode::INTERNAL_SERVER_ERROR, "invalid IPTV row"),
            },
        };
        items.push(item);
    }
    let page = if query.contains_key("sort") {
        crate::api_cursor::paginate(items, kind, query)
    } else {
        crate::api_cursor::paginate_positions(items, query)
    };
    let page = match page {
        Ok(page) => page,
        Err(message) => return error(StatusCode::BAD_REQUEST, message),
    };
    let mut body = json!({"estimated_count":count,"timing":{"select":0,"sort":0,"load":0,"filter":0,"limit":0}});
    if let Some(next) = page.next {
        body["next"] = json!(next);
    }
    if let Some(prev) = page.prev {
        body["prev"] = json!(prev);
    }
    let mut page_rows = Vec::with_capacity(page.items.len());
    for item in page.items {
        let mut row = if compact {
            let index = if sort.is_none() {
                item.as_u64()
            } else {
                item["$index"].as_u64()
            }
            .and_then(|value| usize::try_from(value).ok())
            .and_then(|index| rows.get(index));
            match index.and_then(|row| serde_json::to_value(row).ok()) {
                Some(row) => row,
                None => return error(StatusCode::INTERNAL_SERVER_ERROR, "invalid IPTV row"),
            }
        } else {
            item
        };
        if role == Role::View {
            redact_view_value(&mut row);
        }
        page_rows.push(row);
    }
    if let Some(select) = query.get("select") {
        let selection = crate::api_select::Selection::new(select);
        for row in &mut page_rows {
            *row = selection.project(row, true);
        }
    }
    body[kind] = json!(page_rows);
    management_read_response(role, body)
}

fn with_revision(mut response: Response, revision: u64) -> Response {
    response.headers_mut().insert(
        "x-flussonix-revision",
        revision.to_string().parse().unwrap(),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::ser::SerializeStruct;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountedRow<'a> {
        name: String,
        serialized: &'a AtomicUsize,
    }

    impl Serialize for CountedRow<'_> {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            self.serialized.fetch_add(1, Ordering::Relaxed);
            let mut row = serializer.serialize_struct("CountedRow", 2)?;
            row.serialize_field("name", &self.name)?;
            row.serialize_field("token", &"owned-token")?;
            row.end()
        }
    }

    #[test]
    fn limit_one_does_not_serialize_the_entire_subscriber_catalog() {
        let serialized = AtomicUsize::new(0);
        let rows = (0..1_000)
            .map(|index| CountedRow {
                name: format!("subscriber-{index:04}"),
                serialized: &serialized,
            })
            .collect::<Vec<_>>();
        let query = HashMap::from([("limit".to_owned(), "1".to_owned())]);
        let response = collection_rows(
            rows.iter().collect(),
            |row| row.name.as_str(),
            "subscribers",
            Role::Edit,
            &query,
        );
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(serialized.load(Ordering::Relaxed), 1);
    }
}
