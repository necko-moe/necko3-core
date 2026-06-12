use async_trait::async_trait;
use tokio::sync::{mpsc, watch};
use necko3_types::blockchain::{ChainEvent, ChainState};
use crate::traits::worker::BlockchainWorker;

#[async_trait]
pub trait BlockchainAdapter: Send + Sync {
    fn with_rpc_urls(rpc_urls: Vec<String>) -> anyhow::Result<Self> where Self: Sized;
    fn with_rpc_url(rpc_url: String) -> anyhow::Result<Self> where Self: Sized;

    fn derive_address(xpub: String, index: u32) -> anyhow::Result<String>;
    async fn get_tx_block_number(&self, tx_hash: &str) -> anyhow::Result<Option<u64>>;

    fn build_worker(
        &self,
        state_rx: watch::Receiver<ChainState>,
        event_tx: mpsc::Sender<ChainEvent>
    ) -> Box<dyn BlockchainWorker>;
}