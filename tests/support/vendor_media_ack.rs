//! Positive vendor fixtures acknowledge accepted media credentials.
//! This adds no request validation; individual fixtures own that. Negative
//! acknowledgment regressions deliberately use raw servers without this layer.
use axum::{
    Router,
    extract::Request,
    http::{HeaderValue, StatusCode},
    middleware::{Next, from_fn},
};

pub fn acknowledge_media(router: Router) -> Router {
    router.layer(from_fn(|request: Request, next: Next| async move {
        let private = request.headers().contains_key("cluster-auth-key");
        let mut response = next.run(request).await;
        if private && response.status() == StatusCode::OK {
            response
                .headers_mut()
                .insert("cluster-auth", HeaderValue::from_static("yes"));
        }
        response
    }))
}
