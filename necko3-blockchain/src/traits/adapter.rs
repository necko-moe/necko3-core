use crate::traits::worker::BlockchainWorker;
use alloy::primitives::BlockNumber;
use async_trait::async_trait;
use necko3_types::blockchain::{ChainEvent, ChainState, StateCommand, TrackTransaction};
use necko3_types::TokenData;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;

#[async_trait]
pub trait BlockchainAdapter: Send + Sync {
    fn derive_address(xpub: String, index: u32) -> anyhow::Result<String>;

    fn build_worker(
        state: ChainState,
        tokens_map: HashMap<String, TokenData>,
        watch_addresses: HashSet<String>,
        block_hashes: HashMap<BlockNumber, String>,
        
        state_rx: mpsc::Receiver<StateCommand>,
        transactions_rx: mpsc::Receiver<TrackTransaction>,
        event_tx: mpsc::Sender<ChainEvent>
    ) -> anyhow::Result<impl BlockchainWorker>;
}