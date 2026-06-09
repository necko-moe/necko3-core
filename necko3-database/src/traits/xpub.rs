use async_trait::async_trait;

#[async_trait]
pub trait XPubStore: Sync + Send {
    async fn next_derivation_index(&self, xpub: &str) -> anyhow::Result<u64>;
}