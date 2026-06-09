use async_trait::async_trait;

#[async_trait]
pub trait BlockchainWorker: Send + Sync {
    async fn run(mut self, starting_block: u64);
}