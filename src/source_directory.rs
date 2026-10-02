//! Bounded native source metadata lookup. App supplies its no-redirect HTTP client.
use serde_json::Value;
use std::time::Duration;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupFailure {
    Unavailable,
    Absent,
    Invalid,
}
pub async fn query(
    client: &reqwest::Client,
    source: &Value,
    name: &str,
    default_key: &str,
) -> Result<Value, LookupFailure> {
    tokio::time::timeout(Duration::from_millis(750), async {
        crate::config::valid_name(name).map_err(|_| LookupFailure::Invalid)?;
        let mut url = url::Url::parse(source["api_url"].as_str().ok_or(LookupFailure::Invalid)?)
            .map_err(|_| LookupFailure::Invalid)?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            return Err(LookupFailure::Invalid);
        }
        let name = name
            .split('/')
            .map(|p| {
                percent_encoding::utf8_percent_encode(p, percent_encoding::NON_ALPHANUMERIC)
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join("/");
        url.set_path(&format!(
            "{}/flussonix/api/v1/stream/{name}",
            url.path().trim_end_matches('/')
        ));
        url.set_fragment(None);
        let key = source["cluster_key"].as_str().unwrap_or(default_key);
        let mut response = client
            .get(url)
            .header("X-Flussonix-Peer", key)
            .send()
            .await
            .map_err(|_| LookupFailure::Unavailable)?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(LookupFailure::Absent);
        }
        if !response.status().is_success() {
            return Err(LookupFailure::Unavailable);
        }
        if response
            .content_length()
            .is_some_and(|len| len > 1024 * 1024)
        {
            return Err(LookupFailure::Invalid);
        }
        let mut data = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| LookupFailure::Unavailable)?
        {
            if data.len().saturating_add(chunk.len()) > 1024 * 1024 {
                return Err(LookupFailure::Invalid);
            }
            data.extend_from_slice(&chunk);
        }
        let value: Value = serde_json::from_slice(&data).map_err(|_| LookupFailure::Invalid)?;
        if !value.is_object() {
            return Err(LookupFailure::Invalid);
        }
        Ok(value)
    })
    .await
    .unwrap_or(Err(LookupFailure::Unavailable))
}
