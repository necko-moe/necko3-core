use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use alloy::consensus::Transaction;
use alloy::network::{AnyNetwork, AnyRpcTransaction, ReceiptResponse, TransactionResponse};
use alloy::primitives::{Address, TxHash, U256};
use alloy::primitives::utils::format_units;
use alloy::providers::{DynProvider, Provider};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use coins_bip32::prelude::{Parent, XPub};
use tokio::sync::mpsc::Sender;
use tokio::sync::watch::Receiver;
use tokio::time;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, instrument, trace, warn, Instrument};
use necko3_types::blockchain::{Asset, ChainEvent, ChainState};
use necko3_types::TokenData;
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

    fn build_worker(state_rx: Receiver<ChainState>, event_tx: Sender<ChainEvent>) -> anyhow::Result<impl BlockchainWorker> {
        EvmBlockchainWorker::new(state_rx, event_tx)
    }
}

pub struct EvmBlockchainWorker {
    provider: DynProvider<AnyNetwork>,

    state_rx: Receiver<ChainState>,
    watch_addresses: HashSet<Address>,
    tokens_map: HashMap<Address, TokenData>,

    event_tx: Sender<ChainEvent>
}

impl EvmBlockchainWorker {
    pub fn new(state_rx: Receiver<ChainState>, event_tx: Sender<ChainEvent>) -> anyhow::Result<Self> {
        let state = state_rx.borrow().clone();
        let provider = match create_fallback_provider(&state.dynamic_data.rpc_urls) {
            Ok(p) => p,
            Err(e) => {
                anyhow::bail!("Failed to create fallback provider. Error: {:?}", e)
            }
        };

        let watch_addresses = state.watch_addresses.iter()
            .map(|s| s.parse::<Address>())
            .collect::<Result<HashSet<_>, _>>()?;

        let mut tokens_map = HashMap::with_capacity(state.tokens_map.len());

        for (k, v) in state.tokens_map.iter() {
            match k.parse::<Address>() {
                Ok(addr) => {
                    tokens_map.insert(addr, v.clone());
                }
                Err(e) => {
                    warn!(contract_address = k, error = %e,
                        "Failed to parse token contract_address as Address");
                }
            }
        }

        Ok(Self {
            provider,
            watch_addresses,
            tokens_map,
            state_rx,
            event_tx
        })
    }
}

#[async_trait]
impl BlockchainWorker for EvmBlockchainWorker {
    #[instrument(skip(self))]
    async fn run(mut self) {
        let start_state = self.state_rx.borrow().clone();
        let worker_span = tracing::info_span!("worker", chain_name = start_state.static_data.name);

        async {
            // starting point
            let mut latest_block_num = start_state.dynamic_data.last_processed_block;
            if latest_block_num == 0 {
                latest_block_num = loop {
                    match self.provider.get_block_number().await {
                        Ok(n) => break n,
                        Err(e) => {
                            warn!(error = %e, "Failed to get latest block number, retrying in 2s...");
                            tokio::time::sleep(Duration::from_secs(2)).await;
                        }
                    };
                }
            }

            info!(latest_block = latest_block_num, "Starting worker from the latest block");

            // for the self.state_rx.changed()
            let mut active_watch_addresses = start_state.watch_addresses.clone();
            let mut active_tokens_map = start_state.tokens_map.clone();
            let mut active_rpc_urls = start_state.dynamic_data.rpc_urls.clone();
            let mut last_block_state = start_state.dynamic_data.last_processed_block;

            let mut interval = time::interval(Duration::from_millis(1500));
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

            interval.tick().await; // skip first tick

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let tick_state = self.state_rx.borrow().clone();

                        let current_block_num = match self.provider.get_block_number().await {
                            Ok(n) => n,
                            Err(e) => {
                                warn!(error = %e, "Failed to get latest block number, retrying on next tick...");
                                continue
                            }
                        }.saturating_sub(tick_state.dynamic_data.block_lag as u64);

                        if current_block_num <= latest_block_num {
                            trace!(current = current_block_num, last = latest_block_num,
                                "No new blocks, skipping tick...");
                            continue
                        }

                        for block_num in (latest_block_num + 1)..=current_block_num {
                            let transactions = match self.process_block(block_num).await
                            {
                                Ok(transactions) => Some(transactions),
                                Err(e) => {
                                    error!(error = %e, block_number = block_num,
                                        "Failed to process block (very bad)");

                                    None
                                }
                            };

                            // if let Some(txs) = transactions {
                            //     if let Err(e) = self.process_logs(block_num, txs).await {
                            //         error!(error = %e, block_number = block_num,
                            //             "Failed to process logs for block");
                            //     }
                            // } else {
                            //     warn!(block_number = block_num,
                            //         "Skipping logs because process_block returned error instead of transactions");
                            // }

                            latest_block_num = block_num;

                            if let Err(e) = self.event_tx.send(ChainEvent::BlockProcessed {
                                chain_name: tick_state.static_data.name.clone(),
                                block_number: block_num,
                            }).await {
                                warn!(error = %e, block_number = block_num,
                                    "Failed to send BlockProcessed event");
                            }
                        }
                    }

                    change_result = self.state_rx.changed() => {
                        if change_result.is_err() {
                            warn!("State change channel closed. Exiting worker loop.");
                            break
                        }

                        let new_state = self.state_rx.borrow().clone();

                        if new_state.dynamic_data.rpc_urls != active_rpc_urls {
                            active_rpc_urls = new_state.dynamic_data.rpc_urls.clone();

                            match create_fallback_provider(&active_rpc_urls) {
                                Ok(provider) => {
                                    self.provider = provider;
                                    debug!("Provider successfully updated with new RPC URLs");
                                }
                                Err(e) => {
                                    warn!(error = %e, "Failed to create fallback provider. Error: {:?}", e)
                                }
                            }
                        }

                        if new_state.dynamic_data.last_processed_block != last_block_state {
                            latest_block_num = new_state.dynamic_data.last_processed_block;
                            last_block_state = latest_block_num;
                        }

                        if new_state.tokens_map != active_tokens_map {
                            let mut tokens_map = HashMap::with_capacity(
                                new_state.tokens_map.len());

                            for (k, v) in new_state.tokens_map.iter() {
                                match k.parse::<Address>() {
                                    Ok(addr) => {
                                        tokens_map.insert(addr, v.clone());
                                    }
                                    Err(e) => {
                                        warn!(contract_address = k, error = %e,
                                            "Failed to parse token contract_address as Address");
                                    }
                                }
                            }

                            self.tokens_map = tokens_map;
                            active_tokens_map = new_state.tokens_map.clone();
                        }

                        if new_state.watch_addresses != active_watch_addresses {
                            let watch_addresses = match new_state.watch_addresses.iter()
                                .map(|s| s.parse::<Address>())
                                .collect::<Result<HashSet<_>, _>>()
                            {
                                Ok(addrs) => addrs,
                                Err(e) => {
                                    warn!(error = %e, "Failed to parse watch_addresses. Skipping changes.");
                                    continue
                                }
                            };

                            self.watch_addresses = watch_addresses;
                            active_watch_addresses = new_state.watch_addresses.clone();
                        }
                    }
                }
            }
        }.instrument(worker_span).await;
    }
}

impl EvmBlockchainWorker {
    async fn process_block(&self, block_number: u64) -> anyhow::Result<Vec<AnyRpcTransaction>> {
        let state = self.state_rx.borrow().clone();

        let mut delay = Duration::from_secs(1);

        let block = loop {
            match self.provider.get_block_by_number(block_number.into()).full().await {
                Ok(Some(block)) => break block,
                Ok(None) => { // block reorg?
                    warn!(
                        block_number,
                        "Block not found (None). Possible sync lag. Retrying in {:?}...",
                        delay
                    );
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        block_number,
                        "RPC provider error. Retrying in {:?}...",
                        delay
                    );
                }
            };

            tokio::time::sleep(delay).await;
            delay *= 2;
            // we CAN NOT skip blocks btw
        };

        let txs = block.into_inner().transactions.into_transactions_vec();

        for tx in txs.iter() {
            let address_to = if let Some(addr) = tx.to() {
                addr
            } else { continue };

            if !self.watch_addresses.contains(&address_to) {
                continue
            }

            let amount = tx.value();

            if amount > U256::ZERO {
                let from = tx.from().to_string();
                let asset = Asset::Native(state.static_data.native_symbol.clone());
                let tx_hash = tx.tx_hash().to_string();
                let amount_human = format_units(amount, state.static_data.decimals)
                    .unwrap_or_default();

                info!(
                    asset = %asset,
                    %tx_hash,
                    from = %from,
                    to = %address_to.to_string(),
                    amount = %amount_human,
                    "Native payment detected"
                );

                let event = ChainEvent::PaymentDetected {
                    tx_hash,
                    from,
                    to: address_to.to_string(),
                    asset,
                    amount_raw: amount,
                    amount_human,
                    block_number,
                };

                if let Err(e) = self.event_tx.send(event).await {
                    warn!(error = %e, block_number,
                        "Failed to send PaymentDetected event");
                }
            }
        }

        Ok(txs)
    }

    async fn process_logs(&self, block_number: u64) -> anyhow::Result<()> {
        let state = self.state_rx.borrow().clone();



        Ok(())
    }
}