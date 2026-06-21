use crate::traits::worker::BlockchainWorker;
use alloy::primitives::BlockNumber;
use necko3_types::blockchain::{ChainEvent, ChainState, StateCommand, TrackTransaction};
use necko3_types::TokenData;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;

pub trait BlockchainAdapter: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn derive_address(&self, xpub: String, index: u32) -> Result<String, Self::Error>;

    fn build_worker(
        state: ChainState,
        tokens_map: HashMap<String, TokenData>,
        watch_addresses: HashSet<String>,
        block_hashes: HashMap<BlockNumber, String>,
        
        state_rx: mpsc::Receiver<StateCommand>,
        transactions_rx: mpsc::Receiver<TrackTransaction>,
        event_tx: mpsc::Sender<ChainEvent>
    ) -> Result<impl BlockchainWorker, Self::Error>
    where
        Self: Sized;
}