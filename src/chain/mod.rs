use crate::chain::evm::EvmBlockchain;
use crate::chain::Blockchain::Evm;
use crate::db::Database;
use crate::model::{ChainConfig, ChainType, PaymentEvent};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc::Sender;

pub mod evm;

pub trait BlockchainAdapter: Sync + Send {
    fn new(chain_config: ChainConfig) -> anyhow::Result<Self> where Self: Sized;
    fn derive_address(&self, index: u32) -> impl Future<Output = anyhow::Result<String>> + Send;
    fn listen(&self, db: Arc<Database>, sender: Sender<PaymentEvent>)
        -> impl Future<Output = anyhow::Result<()>> + Send;
    fn get_tx_block_number(&self, tx_hash: &str)
                           -> impl Future<Output = anyhow::Result<Option<u64>>> + Send;
    fn config(&self) -> Arc<RwLock<ChainConfig>>;
}

#[derive(Clone)]
pub enum Blockchain {
    Evm(EvmBlockchain),
}

impl BlockchainAdapter for Blockchain {
    fn new(chain_config: ChainConfig) -> anyhow::Result<Self> {
        match chain_config.chain_type {
            ChainType::EVM => Ok(Evm(EvmBlockchain::new(chain_config)?))
        }
    }

    async fn derive_address(&self, index: u32) -> anyhow::Result<String> {
        match self {
            Evm(bc) => bc.derive_address(index).await,
        }
    }

    async fn listen(&self, db: Arc<Database>, sender: Sender<PaymentEvent>) -> anyhow::Result<()> {
        match self {
            Evm(bc) => bc.listen(db, sender).await,
        }
    }

    async fn get_tx_block_number(&self, tx_hash: &str) -> anyhow::Result<Option<u64>> {
        match self {
            Evm(bc) => bc.get_tx_block_number(tx_hash).await,
        }
    }

    fn config(&self) -> Arc<RwLock<ChainConfig>> {
        match self {
            Evm(bc) => bc.config(),
        }
    }
}