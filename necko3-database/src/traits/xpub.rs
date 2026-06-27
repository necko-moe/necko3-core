use async_trait::async_trait;
use crate::error::DbResult;

#[async_trait]
pub trait XPubStore: Sync + Send {
    async fn next_derivation_index(&self, xpub: &str) -> DbResult<u64>;
}