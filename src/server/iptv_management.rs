use super::*;
use crate::iptv::{
    ha::{HaAuthority, catalog::CatalogEdit, connection::HaError},
    store::IptvSnapshot,
};
use serde::Serialize;

pub(super) async fn dispatch(app: Arc<App>, request: Request, tail: &str, role: Role) -> Response {
    if app.iptv_ha.is_some() {
        return dispatch_ha(app, request, tail, role).await;
    }
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
    collection_for_snapshot(app.iptv.snapshot(), kind, role, query)
}

fn collection_for_snapshot(
    snapshot: Arc<IptvSnapshot>,
    kind: &str,
    role: Role,
    query: &HashMap<String, String>,
) -> Response {
    if kind == "packages" {
        collection_rows(
            snapshot.document().packages.iter().collect(),
            |row| row.name.as_str(),
            |_, _, _| None,
            kind,
            role,
            query,
        )
    } else {
        collection_rows(
            snapshot.document().subscribers.iter().collect(),
            |row| row.name.as_str(),
            |row, field, role| match field {
                "token" if role == Role::View => Some(json!("[redacted]")),
                "token" => Some(json!(row.token)),
                "max_sessions" => Some(json!(row.max_sessions)),
                "flussonix_expires_at" => Some(json!(row.flussonix_expires_at)),
                _ => None,
            },
            kind,
            role,
            query,
        )
    }
}

async fn dispatch_ha(app: Arc<App>, request: Request, tail: &str, role: Role) -> Response {
    let ha = app.iptv_ha.as_ref().expect("HA selected");
    let snapshot = match ha.catalog.load_current().await {
        Ok(snapshot) => snapshot,
        Err(_) => return ha_error(HaError::Unavailable),
    };
    let revision = snapshot.revision();
    let method = request.method().clone();
    let suffix = tail.strip_prefix("iptv").unwrap_or("");
    let query = parse_query(request.uri().query());
    if method != axum::http::Method::GET {
        let expected = match request.headers().get(axum::http::header::IF_MATCH) {
            None => {
                return ha_response(
                    error(
                        StatusCode::PRECONDITION_REQUIRED,
                        "IPTV catalog revision required",
                    ),
                    revision,
                );
            }
            Some(value) if value.to_str().ok() == Some(format!("\"{revision}\"").as_str()) => {
                revision
            }
            Some(_) => {
                return ha_response(
                    error(
                        StatusCode::PRECONDITION_FAILED,
                        "IPTV catalog revision changed",
                    ),
                    revision,
                );
            }
        };
        if suffix.is_empty() {
            return match method {
                axum::http::Method::PUT => {
                    let body = match read_json(request).await {
                        Ok(body) => body,
                        Err((status, message)) => {
                            return ha_response(error(status, &message), revision);
                        }
                    };
                    if body.as_object().is_none_or(|fields| !fields.is_empty()) {
                        return ha_response(
                            error(StatusCode::BAD_REQUEST, "unsupported IPTV root setting"),
                            revision,
                        );
                    }
                    match ha.catalog.edit(expected, CatalogEdit::Enable(true)).await {
                        Ok(result) => ha_response(
                            json_response(json!({"revision":result.revision})),
                            result.revision,
                        ),
                        Err(error) => ha_edit_error(ha, error).await,
                    }
                }
                axum::http::Method::DELETE => {
                    match ha.catalog.edit(expected, CatalogEdit::Enable(false)).await {
                        Ok(result) => {
                            ha_response(StatusCode::NO_CONTENT.into_response(), result.revision)
                        }
                        Err(error) => ha_edit_error(ha, error).await,
                    }
                }
                _ => ha_response(
                    error(StatusCode::METHOD_NOT_ALLOWED, "method not allowed"),
                    revision,
                ),
            };
        }
        let Some(path) = suffix.strip_prefix('/') else {
            return ha_response(
                error(StatusCode::NOT_FOUND, "unknown IPTV operation"),
                revision,
            );
        };
        let Some((kind, name)) = path.split_once('/') else {
            return ha_response(
                error(
                    StatusCode::METHOD_NOT_ALLOWED,
                    "IPTV collection is read only",
                ),
                revision,
            );
        };
        if !matches!(kind, "packages" | "subscribers") || valid_name(name).is_err() {
            return ha_response(
                error(StatusCode::BAD_REQUEST, "invalid IPTV item name"),
                revision,
            );
        }
        let edit = match method {
            axum::http::Method::PUT => {
                let body = match read_json(request).await {
                    Ok(body) => body,
                    Err((status, message)) => {
                        return ha_response(error(status, &message), revision);
                    }
                };
                if kind == "packages" {
                    CatalogEdit::PutPackage(name.into(), body)
                } else {
                    CatalogEdit::PutSubscriber(name.into(), body)
                }
            }
            axum::http::Method::DELETE => {
                if kind == "packages" {
                    CatalogEdit::DeletePackage(name.into())
                } else {
                    CatalogEdit::DeleteSubscriber(name.into())
                }
            }
            _ => {
                return ha_response(
                    error(StatusCode::METHOD_NOT_ALLOWED, "method not allowed"),
                    revision,
                );
            }
        };
        return match ha.catalog.edit(expected, edit).await {
            Ok(result) => {
                let response = match method {
                    axum::http::Method::DELETE => StatusCode::NO_CONTENT.into_response(),
                    _ => json_response(result.item.unwrap_or(Value::Null)),
                };
                ha_response(response, result.revision)
            }
            Err(error) => ha_edit_error(ha, error).await,
        };
    }
    if suffix.is_empty() {
        if snapshot.enabled() {
            return ha_response(management_read_response(role, json!({})), revision);
        }
        return ha_response(error(StatusCode::NOT_FOUND, "IPTV is disabled"), revision);
    }
    if !snapshot.enabled() {
        return ha_response(error(StatusCode::NOT_FOUND, "IPTV is disabled"), revision);
    }
    let Some(path) = suffix.strip_prefix('/') else {
        return ha_response(
            error(StatusCode::NOT_FOUND, "unknown IPTV operation"),
            revision,
        );
    };
    let (kind, name) = path
        .split_once('/')
        .map_or((path, None), |(kind, name)| (kind, Some(name)));
    if !matches!(kind, "packages" | "subscribers") {
        return ha_response(
            error(StatusCode::NOT_FOUND, "unknown IPTV collection"),
            revision,
        );
    }
    match name {
        None => ha_response(
            collection_for_snapshot(snapshot, kind, role, &query),
            revision,
        ),
        Some(name) => {
            if valid_name(name).is_err() {
                return ha_response(
                    error(StatusCode::BAD_REQUEST, "invalid IPTV item name"),
                    revision,
                );
            }
            let item = if kind == "packages" {
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
            ha_response(
                match item {
                    Some(row) => management_read_response(role, row),
                    None => error(StatusCode::NOT_FOUND, "IPTV item not found"),
                },
                revision,
            )
        }
    }
}

fn ha_response(mut response: Response, revision: u64) -> Response {
    response
        .headers_mut()
        .insert("etag", format!("\"{revision}\"").parse().unwrap());
    response
        .headers_mut()
        .insert("cache-control", "no-store".parse().unwrap());
    with_revision(response, revision)
}

fn ha_error(kind: HaError) -> Response {
    let status = match kind {
        HaError::Invalid | HaError::InvalidConfig => StatusCode::BAD_REQUEST,
        HaError::NotFound => StatusCode::NOT_FOUND,
        HaError::PreconditionFailed => StatusCode::PRECONDITION_FAILED,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    let mut response = error(status, &kind.to_string());
    response
        .headers_mut()
        .insert("cache-control", "no-store".parse().unwrap());
    response
}

async fn ha_edit_error(ha: &HaAuthority, kind: HaError) -> Response {
    if kind == HaError::PreconditionFailed {
        return match ha.catalog.load_current().await {
            Ok(snapshot) => ha_response(ha_error(kind), snapshot.revision()),
            Err(error) => ha_error(error),
        };
    }
    ha_error(kind)
}

fn collection_rows<T: Serialize>(
    rows: Vec<&T>,
    name: impl Fn(&T) -> &str,
    scalar: impl Fn(&T, &str, Role) -> Option<Value>,
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
    let sort_fields = match sort.map(crate::api_sort::bounded_fields).transpose() {
        Ok(fields) => fields,
        Err(message) => return error(StatusCode::BAD_REQUEST, message),
    };
    let mut items = Vec::with_capacity(count);
    for (index, row) in rows.iter().enumerate() {
        let item = match sort_fields.as_deref() {
            None => json!(index),
            Some(parts) => {
                let mut fields = serde_json::Map::new();
                fields.insert("name".into(), json!(name(row)));
                for part in parts {
                    let field = part.strip_prefix('-').unwrap_or(part);
                    if field != "name"
                        && let Some(value) = scalar(row, field, role)
                    {
                        fields.insert(field.to_owned(), value);
                    }
                }
                Value::Object(fields)
            }
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
    let by_name = sort.map(|_| {
        rows.iter()
            .map(|row| (name(row), *row))
            .collect::<HashMap<_, _>>()
    });
    let mut page_rows = Vec::with_capacity(page.items.len());
    for item in page.items {
        let source = if sort.is_none() {
            item.as_u64()
                .and_then(|value| usize::try_from(value).ok())
                .and_then(|index| rows.get(index).copied())
        } else {
            item["name"]
                .as_str()
                .and_then(|name| by_name.as_ref().and_then(|rows| rows.get(name).copied()))
        };
        let mut row = match source.and_then(|row| serde_json::to_value(row).ok()) {
            Some(row) => row,
            None => return error(StatusCode::INTERNAL_SERVER_ERROR, "invalid IPTV row"),
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
            |_, _, _| None,
            "subscribers",
            Role::Edit,
            &query,
        );
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(serialized.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn scalar_sort_limit_one_does_not_serialize_the_entire_subscriber_catalog() {
        for sort in ["max_sessions", "-flussonix_expires_at", "token"] {
            let serialized = AtomicUsize::new(0);
            let rows = (0..1_000)
                .map(|index| CountedRow {
                    name: format!("subscriber-{index:04}"),
                    serialized: &serialized,
                })
                .collect::<Vec<_>>();
            let query = HashMap::from([
                ("limit".to_owned(), "1".to_owned()),
                ("sort".to_owned(), sort.to_owned()),
            ]);
            let response = collection_rows(
                rows.iter().collect(),
                |row| row.name.as_str(),
                |_, _, _| None,
                "subscribers",
                Role::View,
                &query,
            );
            assert_eq!(response.status(), StatusCode::OK, "{sort}");
            assert_eq!(serialized.load(Ordering::Relaxed), 1, "{sort}");
        }
    }

    #[test]
    fn excessive_sort_fields_are_rejected_before_visiting_catalog_rows() {
        let serialized = AtomicUsize::new(0);
        let rows = (0..64)
            .map(|index| CountedRow {
                name: format!("subscriber-{index:04}"),
                serialized: &serialized,
            })
            .collect::<Vec<_>>();
        let query = HashMap::from([
            ("limit".to_owned(), "1".to_owned()),
            ("sort".to_owned(), vec!["missing"; 17].join(",")),
        ]);
        let response = collection_rows(
            rows.iter().collect(),
            |row| row.name.as_str(),
            |_, _, _| None,
            "subscribers",
            Role::View,
            &query,
        );
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(serialized.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn oversized_sort_expression_is_rejected_before_visiting_catalog_rows() {
        let serialized = AtomicUsize::new(0);
        let rows = [CountedRow {
            name: "subscriber-one".to_owned(),
            serialized: &serialized,
        }];
        let query = HashMap::from([("sort".to_owned(), "x".repeat(8193))]);
        let response = collection_rows(
            rows.iter().collect(),
            |row| row.name.as_str(),
            |_, _, _| None,
            "subscribers",
            Role::View,
            &query,
        );
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(serialized.load(Ordering::Relaxed), 0);
    }
}
