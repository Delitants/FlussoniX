//! Public HTTPS control-plane issuance of short-lived, tokenless SRT IDs.
use super::*;

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct IssueBody {
    streamid: String,
    encryption: Option<String>,
}

pub(super) async fn issue(State(app): State<Arc<App>>, request: Request) -> Response {
    let _activity = app.media.idle_memory.activity().await;
    let result = issue_inner(&app, request).await;
    let mut response = match result {
        Ok(value) => json_response(value),
        Err(status) => error(
            StatusCode::from_u16(status).unwrap_or(StatusCode::SERVICE_UNAVAILABLE),
            "SRT ticket unavailable",
        ),
    };
    response.headers_mut().insert(
        "cache-control",
        "private, no-store".parse().expect("static cache header"),
    );
    response
}

async fn issue_inner(app: &Arc<App>, request: Request) -> Result<Value, u16> {
    if request.method() != axum::http::Method::POST {
        return Err(405);
    }
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0)
        .ok_or(426u16)?;
    let secure = request
        .extensions()
        .get::<crate::http_tls::SecureHttp>()
        .is_some();
    if !secure && !peer.ip().is_loopback() {
        return Err(426);
    }
    if request.uri().query().is_some() {
        return Err(400);
    }
    if header(request.headers(), "content-type") != Some("application/json") {
        return Err(415);
    }
    let user_agent = header(request.headers(), "user-agent")
        .unwrap_or("")
        .to_owned();
    let referer = header(request.headers(), "referer")
        .unwrap_or("")
        .to_owned();
    let host = header(request.headers(), "host").unwrap_or("").to_owned();
    let bytes = axum::body::to_bytes(request.into_body(), 4096)
        .await
        .map_err(|_| 400u16)?;
    let body: IssueBody = serde_json::from_slice(&bytes).map_err(|_| 400u16)?;
    if !matches!(body.encryption.as_deref(), None | Some("required")) {
        return Err(400);
    }
    let selection = crate::srt_playback::Selection::parse(&body.streamid).map_err(|_| 400u16)?;
    if selection.token.is_empty() || !body.streamid.split(',').any(|part| part == "m=request") {
        return Err(400);
    }
    if !matches!(app.options.role.as_str(), "standalone" | "cdn" | "lb") || app.options.drain {
        return Err(503);
    }
    let viewer = ViewerRequest {
        name: selection.name,
        token: selection.token,
        proto: "srt".into(),
        qs: url::form_urlencoded::Serializer::new(String::new())
            .append_pair("streamid", &body.streamid)
            .finish(),
        ip: peer.ip().to_string(),
        user_agent,
        referer,
        host,
        finite_media: false,
    };
    if app.options.role == "lb" {
        return app.srt_lookup(viewer, &body.streamid, true).await;
    }
    let address = app.options.srt_play_public_url.as_ref().ok_or(503u16)?;
    let profile = app.srt_playback.lock().unwrap().clone();
    if profile["enabled"] != true || profile["encrypted"] != true {
        return Err(503);
    }
    let listener_id = profile["listener_id"].as_str().ok_or(503u16)?.to_owned();
    let route = app
        .config
        .read(|root| crate::vendor_playback::select(root, &viewer.name, app.config.revision()));
    if !matches!(route, crate::vendor_playback::Selection::Native) {
        return Err(503);
    }
    let resolved = app.resolve(&viewer.name).await.ok_or(404u16)?;
    if resolved.config["disabled"] == true {
        return Err(404);
    }
    let grant = match app
        .playback_auth
        .authorize_control(resolved.policy, viewer.clone())
        .await
    {
        AuthOutcome::Allowed(grant) => grant,
        AuthOutcome::Denied | AuthOutcome::Redirect(_) => return Err(403),
    };
    if grant.is_cancelled() || app.config.revision() != resolved.revision {
        return Err(503);
    }
    let ticket = app
        .reserve_srt_credential(viewer.clone(), crate::cluster::FALLBACK_MBPS)
        .await?;
    let valid = !grant.is_cancelled()
        && app.config.revision() == resolved.revision
        && app.srt_playback.lock().unwrap()["listener_id"] == listener_id;
    if !valid {
        app.reservations.lock().await.remove(&ticket);
        return Err(503);
    }
    if app.iptv_authority_client.is_some()
        && !app.playback_auth.release_redirect_lease(&grant).await
    {
        app.reservations.lock().await.remove(&ticket);
        return Err(503);
    }
    drop(grant);
    let Some(streamid) = crate::srt_playback::ticket_only(&viewer.name, &ticket) else {
        app.reservations.lock().await.remove(&ticket);
        return Err(503);
    };
    Ok(json!({
        "address": address.as_str(),
        "streamid": streamid,
        "encryption_required": true,
        "expires_in": 5,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::srt_playback::{Listener, Settings};

    #[tokio::test]
    async fn verified_https_binds_the_socket_ip_not_a_forwarded_header() {
        let dir = tempfile::tempdir().unwrap();
        let app = App::new(
            dir.path().join("config.json"),
            dir.path().join("media"),
            Options {
                admin_password: "owned-admin".into(),
                peer_key: "owned-peer-secret".into(),
                srt_play_public_url: Some(url::Url::parse("srt://127.0.0.1:9000").unwrap()),
                ..Options::default()
            },
        )
        .unwrap();
        app.config
            .put("streams", "owned", json!({"inputs":[{"url":"testsrc://"}]}))
            .unwrap();
        let listener = Listener::bind(
            "127.0.0.1:0".parse().unwrap(),
            Settings::new(120, 8, "owned-test-passphrase".into()).unwrap(),
        )
        .unwrap();
        app.set_srt_playback(Some(&listener));
        app.sample_metrics();
        let actual: SocketAddr = "198.51.100.22:40001".parse().unwrap();
        let mut request = Request::builder()
            .method(axum::http::Method::POST)
            .uri("/flussonix/api/v1/srt-ticket")
            .header("content-type", "application/json")
            .header("x-forwarded-for", "127.0.0.1")
            .body(Body::from(
                r##"{"streamid":"#!::r=owned,m=request,u=owned-secret"}"##,
            ))
            .unwrap();
        request.extensions_mut().insert(ConnectInfo(actual));
        request.extensions_mut().insert(crate::http_tls::SecureHttp);
        let response = issue(State(app.clone()), request).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), 4096)
                .await
                .unwrap(),
        )
        .unwrap();
        let ticket = body["streamid"]
            .as_str()
            .unwrap()
            .split_once("flussonix_ticket=")
            .unwrap()
            .1;
        let listener_id = app.srt_listener_id();
        let credential = super::super::srt_tickets::Ticket {
            id: ticket,
            encrypted: true,
            listener_id: &listener_id,
        };
        assert!(
            app.srt_ticket_viewer(credential, "owned", "127.0.0.1".parse().unwrap())
                .await
                .is_none()
        );
        let viewer = app
            .srt_ticket_viewer(credential, "owned", actual.ip())
            .await
            .unwrap();
        assert_eq!(viewer.ip, "198.51.100.22");
        assert_eq!(viewer.token, "owned-secret");
    }
}
