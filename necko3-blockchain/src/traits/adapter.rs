use crate::traits::worker::BlockchainWorker;
use alloy::primitives::BlockNumber;
use async_trait::async_trait;
use necko3_types::blockchain::{ChainEvent, ChainState, TrackTransaction};
use std::collections::HashMap;
use tokio::sync::{mpsc, watch};

#[async_trait]
pub trait BlockchainAdapter: Send + Sync {
    fn derive_address(xpub: String, index: u32) -> anyhow::Result<String>;

    fn build_worker(
        block_hashes: HashMap<BlockNumber, String>,
        state_rx: watch::Receiver<ChainState>,
        transactions_rs: mpsc::Receiver<TrackTransaction>,
        event_tx: mpsc::Sender<ChainEvent>
    ) -> anyhow::Result<impl BlockchainWorker>;
}