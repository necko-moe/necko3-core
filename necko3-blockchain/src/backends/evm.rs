use std::str::FromStr;
use std::sync::Arc;
use alloy::network::{AnyNetwork, ReceiptResponse};
use alloy::primitives::{Address, TxHash};
use alloy::providers::{DynProvider, Provider, ProviderBuilder, RootProvider};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use coins_bip32::prelude::{Parent, XPub};
use tokio::sync::mpsc::Sender;
use tokio::sync::watch::Receiver;
use tracing::{debug, instrument, trace, warn};
use url::Url;
use necko3_types::blockchain::{ChainEvent, ChainState};
use crate::backends::create_fallback_provider;
use crate::traits::adapter::BlockchainAdapter;
use crate::traits::worker::BlockchainWorker;

pub struct EvmBlockchain {
    provider: ArcSwap<DynProvider<AnyNetwork>>,
}

#[async_trait]
impl BlockchainAdapter for EvmBlockchain {
    #[instrument(level = "warn")]
    fn with_rpc_urls(rpc_urls: Vec<String>) -> anyhow::Result<Self> {
        let provider = create_fallback_provider(&rpc_urls)?;

        Ok(Self { provider: ArcSwap::new(Arc::new(provider)) })
    }

    fn with_rpc_url(rpc_url: String) -> anyhow::Result<Self> {
        Self::with_rpc_urls(vec![rpc_url])
    }

    #[instrument(level = "debug")]
        fn derive_address(xpub: String, index: u32) -> anyhow::Result<String> {
        trace!("Deriving address for index {}", index);

        let xpub = XPub::from_str(&xpub)?;
        let child_xpub = xpub.derive_child(index)?;

        let addr = Address::from_public_key(child_xpub.as_ref()).to_string();
        trace!(address = %addr, "Derived address");

        Ok(addr)
    }

    #[instrument(skip(self), err)]
    async fn get_tx_block_number(&self, tx_hash: &str) -> anyhow::Result<Option<u64>> {
        debug!(tx_hash, "Checking transaction receipt");
        let hash = tx_hash.parse::<TxHash>()?;

        match self.provider.load().get_transaction_receipt(hash).await {
            Ok(Some(receipt)) => {
                if receipt.status() {
                    Ok(receipt.block_number)
                } else {
                    debug!("Transaction failed on-chain");
                    Ok(None)
                }
            }
            Ok(None) => {
                debug!("Transaction receipt not found (yet)");
                Ok(None)
            }
            Err(e) => {
                anyhow::bail!("All RPC nodes failed inside FallbackLayer. Error: {:?}", e)
            }
        }
    }

    fn build_worker(&self, state_rx: Receiver<ChainState>, event_tx: Sender<ChainEvent>) -> Box<dyn BlockchainWorker> {
        Box::new(EvmBlockchainWorker::new(state_rx, event_tx))
    }
}

pub struct EvmBlockchainWorker {
    state_rx: Receiver<ChainState>,
    event_tx: Sender<ChainEvent>
}

impl EvmBlockchainWorker {
    pub fn new(state_rx: Receiver<ChainState>, event_tx: Sender<ChainEvent>) -> Self {
        Self { state_rx, event_tx }
    }
}

#[async_trait]
impl BlockchainWorker for EvmBlockchainWorker {
    async fn run(self, starting_block: u64) {
        todo!()
    }
}