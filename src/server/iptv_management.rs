use super::*;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
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
    mut rows: Vec<&T>,
    name: impl Fn(&T) -> &str,
    kind: &str,
    role: Role,
    query: &HashMap<String, String>,
) -> Response {
    rows.sort_by(|a, b| name(a).cmp(name(b)));
    let count = rows.len();
    let limit = match query.get("limit") {
        Some(raw) => match raw.parse::<usize>() {
            Ok(value) => value.clamp(1, 1_000),
            Err(_) => return error(StatusCode::BAD_REQUEST, "invalid IPTV limit"),
        },
        None => 100,
    };
    let offset = match query.get("cursor") {
        Some(raw) => match URL_SAFE_NO_PAD
            .decode(raw)
            .ok()
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .and_then(|value| value.parse::<usize>().ok())
        {
            Some(offset) if offset <= count => offset,
            _ => return error(StatusCode::BAD_REQUEST, "invalid IPTV cursor"),
        },
        None => 0,
    };
    let page: Vec<Value> = rows
        .into_iter()
        .skip(offset)
        .take(limit)
        .filter_map(|row| serde_json::to_value(row).ok())
        .collect();
    let next_offset = offset.saturating_add(page.len());
    let next = (next_offset < count).then(|| URL_SAFE_NO_PAD.encode(next_offset.to_string()));
    let mut body = json!({"estimated_count":count,"timing":{},"next":next,"prev":null});
    body[kind] = json!(page);
    management_read_response(role, body)
}

fn with_revision(mut response: Response, revision: u64) -> Response {
    response.headers_mut().insert(
        "x-flussonix-revision",
        revision.to_string().parse().unwrap(),
    );
    response
}
