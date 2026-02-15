use crate::chain::evm::EvmBlockchain;
use crate::chain::Blockchain::Evm;
use crate::model::PaymentEvent;
use crate::state::AppState;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use strum::{AsRefStr, Display, EnumString};
use tokio::sync::mpsc::Sender;
use utoipa::ToSchema;

pub mod evm;

#[derive(Debug, Copy, Clone, Serialize, Deserialize, ToSchema,
    Display, EnumString, AsRefStr)]
#[strum(serialize_all = "UPPERCASE")]
pub enum ChainType {
    EVM
}

pub trait BlockchainAdapter: Sync + Send {
    fn new(state: Arc<AppState>, chain_type: ChainType, chain_name: &str, 
           sender: Option<Sender<PaymentEvent>>) -> Self;
    fn derive_address(&self, index: u32) -> impl Future<Output = anyhow::Result<String>> + Send;
    fn listen(&self) -> impl Future<Output = anyhow::Result<()>> + Send;
}

#[derive(Clone)]
pub enum Blockchain {
    Evm(EvmBlockchain),
}

impl BlockchainAdapter for Blockchain {
    fn new(state: Arc<AppState>, chain_type: ChainType, chain_name: &str, sender: Option<Sender<PaymentEvent>>) -> Self {
        match chain_type {
            ChainType::EVM => Evm(EvmBlockchain::new(state, chain_type, chain_name, sender))
        }
    }

    async fn derive_address(&self, index: u32) -> anyhow::Result<String> {
        match self {
            Evm(bc) => bc.derive_address(index).await,
        }
    }

    async fn listen(&self) -> anyhow::Result<()> {
        match self {
            Evm(bc) => bc.listen().await,
        }
    }
}