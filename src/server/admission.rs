use serde_json::Value;

// Admission tickets are small; bound both announced and streamed peer replies.
pub(super) async fn read_json(mut response: reqwest::Response) -> Option<Value> {
    const LIMIT: usize = 16 * 1024;
    if response.content_length().is_some_and(|n| n > LIMIT as u64) {
        return None;
    }
    let mut bytes = vec![];
    while let Some(chunk) = response.chunk().await.ok()? {
        if bytes.len().checked_add(chunk.len())? > LIMIT {
            return None;
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).ok()
}
