//! Direct self-source sanity check. Observed IDs are not topology attestations.
use super::{DEADLINE, InventoryClient, InventoryStream, LookupFailure, MAX_BYTES};
use uuid::Uuid;
impl InventoryClient {
    pub(crate) async fn identity_inventory(
        &self,
        local_id: Uuid,
    ) -> Result<(Uuid, Vec<InventoryStream>), LookupFailure> {
        if local_id.is_nil() {
            return Err(LookupFailure::Invalid);
        }
        tokio::time::timeout(DEADLINE, async {
            let mut remaining = MAX_BYTES;
            let before = self.server_id(&mut remaining).await?;
            if before == local_id {
                return Err(LookupFailure::Invalid);
            }
            let rows = self.fetch(&mut remaining).await?;
            let after = self.server_id(&mut remaining).await?;
            if before != after || after == local_id {
                return Err(LookupFailure::Invalid);
            }
            Ok((before, rows))
        })
        .await
        .unwrap_or(Err(LookupFailure::Unavailable))
    }
    async fn server_id(&self, remaining: &mut usize) -> Result<Uuid, LookupFailure> {
        let mut url = self.endpoint.clone();
        let prefix = url
            .path()
            .strip_suffix("/-/streams")
            .ok_or(LookupFailure::Invalid)?;
        let path = format!("{prefix}/streamer/api/v3/config");
        url.set_path(&path);
        let value = self.json(url, remaining).await?;
        let text = value["stats"]["id"]
            .as_str()
            .filter(|id| id.len() == 36)
            .ok_or(LookupFailure::Invalid)?;
        let id = Uuid::parse_str(text).map_err(|_| LookupFailure::Invalid)?;
        if id.is_nil() {
            return Err(LookupFailure::Invalid);
        }
        Ok(id)
    }
}
