use std::str::FromStr;
use alloy::network::{AnyNetwork, ReceiptResponse};
use alloy::primitives::{Address, TxHash};
use alloy::providers::{Provider, ProviderBuilder};
use async_trait::async_trait;
use coins_bip32::prelude::{Parent, XPub};
use tokio::sync::mpsc::Sender;
use tokio::sync::watch::Receiver;
use tracing::{debug, instrument, trace, warn};
use url::Url;
use necko3_types::blockchain::{ChainEvent, ChainState};
use crate::traits::adapter::BlockchainAdapter;
use crate::traits::worker::BlockchainWorker;

pub struct EvmBlockchain {
    rpc_urls: Vec<String>,
}

#[async_trait]
impl BlockchainAdapter for EvmBlockchain {
    fn with_rpc_urls(rpc_urls: Vec<String>) -> Self {
        Self { rpc_urls }
    }

    fn with_rpc_url(rpc_url: String) -> Self {
        Self::with_rpc_urls(vec![rpc_url])
    }

    #[instrument(skip(self), level = "debug")]
        fn derive_address(&self, xpub: String, index: u32) -> anyhow::Result<String> {
        trace!("Deriving address for index {}", index);

        let xpub = XPub::from_str(&xpub)?;

        let child_xpub = xpub.derive_child(index)?;
        let verifying_key = child_xpub.as_ref();

        let addr = Address::from_public_key(&verifying_key).to_string();
        trace!(address = %addr, "Derived address");

        Ok(addr)
    }

    #[instrument(skip(self), err)]
    async fn get_tx_block_number(&self, tx_hash: &str) -> anyhow::Result<Option<u64>> {
        debug!(tx_hash, "Checking transaction receipt");
        let hash = tx_hash.parse::<TxHash>()?;

        let mut last_err = None;

        for url_str in &self.rpc_urls {
            let url = match Url::parse(url_str)  {
                Ok(u) => u,
                Err(e) => {
                    warn!(error = %e, rpc_url = url_str, "Failed to parse RPC URL");
                    continue
                }
            };

            let provider = ProviderBuilder::new()
                .network::<AnyNetwork>()
                .connect_http(url);

            match provider.get_transaction_receipt(hash).await {
                Ok(Some(receipt)) => {
                    return if receipt.status() {
                        Ok(receipt.block_number)
                    } else {
                        debug!("Transaction failed on-chain");
                        Ok(None)
                    }
                }
                Ok(None) => {
                    debug!("Transaction receipt not found (yet)");
                    return Ok(None);
                }
                Err(e) => {
                    warn!(error = %e, node = url_str, "RPC node failed, trying next...");
                    last_err = Some(e);
                }
            }
        }

        anyhow::bail!("All RPC nodes failed. Last error: {:?}", last_err)
    }

    fn build_worker(&self, state_rx: Receiver<ChainState>, event_tx: Sender<ChainEvent>) -> Box<dyn BlockchainWorker> {
        todo!()
    }
}