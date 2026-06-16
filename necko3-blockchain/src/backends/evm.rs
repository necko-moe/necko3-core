use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::str::FromStr;
use std::time::Duration;
use alloy::consensus::Transaction;
use alloy::core::sol;
use alloy::network::{AnyNetwork, AnyRpcBlock, TransactionResponse};
use alloy::primitives::{Address, BlockHash, BlockNumber, TxHash, U256};
use alloy::primitives::utils::format_units;
use alloy::providers::{DynProvider, Provider};
use alloy::rpc::types::Filter;
use alloy::sol_types::SolEvent;
use async_trait::async_trait;
use coins_bip32::prelude::{Parent, XPub};
use tokio::sync::{mpsc, watch};
use tokio::time;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, instrument, trace, warn, Instrument};
use necko3_types::blockchain::{Asset, ChainEvent, ChainState, TrackTransaction};
use necko3_types::TokenData;
use crate::backends::create_fallback_provider;
use crate::traits::adapter::BlockchainAdapter;
use crate::traits::worker::BlockchainWorker;

pub const MAX_REORG_DEPTH: u64 = 100;

sol! {
    #[derive(Debug)]
    event Transfer(address indexed from, address indexed to, uint256 value);
}

pub struct EvmBlockchain;

#[async_trait]
impl BlockchainAdapter for EvmBlockchain {
    #[instrument(level = "debug")]
    fn derive_address(xpub: String, index: u32) -> anyhow::Result<String> {
        trace!("Deriving address for index {}", index);

        let xpub = XPub::from_str(&xpub)?;
        let child_xpub = xpub.derive_child(index)?;

        let addr = Address::from_public_key(child_xpub.as_ref()).to_string();
        trace!(address = %addr, "Derived address");

        Ok(addr)
    }

    fn build_worker(
        block_hashes: HashMap<BlockNumber, String>,
        state_rx: watch::Receiver<ChainState>,
        transactions_rx: mpsc::Receiver<TrackTransaction>,
        event_tx: mpsc::Sender<ChainEvent>
    ) -> anyhow::Result<impl BlockchainWorker> {
        EvmBlockchainWorker::new(block_hashes, state_rx, transactions_rx, event_tx)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct TrackedTx {
    tx_hash: String,
    block_hash: String,
    block_number: u64,
}

pub struct EvmBlockchainWorker {
    provider: DynProvider<AnyNetwork>,

    transactions_rs: mpsc::Receiver<TrackTransaction>,
    /// key = block_number+required_confirmations
    tracked_txs: HashMap<BlockNumber, HashSet<TrackedTx>>,
    tx_block_map: HashMap<TxHash, (BlockNumber, TrackedTx)>,

    block_hashes: HashMap<BlockNumber, String>,

    state_rx: watch::Receiver<ChainState>,
    watch_addresses: HashSet<Address>,
    tokens_map: HashMap<Address, TokenData>,

    event_tx: mpsc::Sender<ChainEvent>
}

impl EvmBlockchainWorker {
    pub fn new(
        block_hashes: HashMap<BlockNumber, String>,
        state_rx: watch::Receiver<ChainState>,
        transactions_rx: mpsc::Receiver<TrackTransaction>,
        event_tx: mpsc::Sender<ChainEvent>
    ) -> anyhow::Result<Self> {
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
            transactions_rs: transactions_rx,
            tracked_txs: HashMap::new(),
            watch_addresses,
            tokens_map,
            block_hashes,
            state_rx,
            event_tx,
            tx_block_map: Default::default(),
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

                        if !tick_state.dynamic_data.active {
                            trace!("Chain is not active. Skipping tick");
                            continue
                        }

                        let current_block_num = match self.provider.get_block_number().await {
                            Ok(n) => n,
                            Err(e) => {
                                warn!(error = %e, "Failed to get latest block number, retrying on next tick...");
                                continue
                            }
                        };
                        let block_with_lag = current_block_num
                            .saturating_sub(tick_state.dynamic_data.block_lag as u64);

                        if block_with_lag <= latest_block_num {
                            trace!(current = current_block_num, last = latest_block_num,
                                "No new blocks, skipping tick...");
                            continue
                        }

                        for block_num in (latest_block_num + 1)..=block_with_lag {
                            let block_opt = match self.process_block(block_num).await
                            {
                                Ok(b) => Some(b),
                                Err(e) => {
                                    error!(error = %e, block_number = block_num,
                                        "Failed to process block (very bad)");

                                    None
                                }
                            };

                            if let Some((block_hash, parent_hash)) = block_opt {
                                self.process_blocks_reorg(block_num, parent_hash).await;
                                self.process_tracked_transactions(block_num).await;

                                let actual_lag = current_block_num - block_num;

                                if let Err(e) = self.process_logs(block_hash, actual_lag).await {
                                    error!(error = %e, block_number = block_num, block_hash = %block_hash,
                                        "Failed to process logs for block");
                                }

                                if let Err(e) = self.event_tx.send(ChainEvent::BlockProcessed {
                                    chain_name: tick_state.static_data.name.clone(),
                                    block_number: block_num,
                                    block_hash: block_hash.to_string(),
                                }).await {
                                    warn!(error = %e, block_number = block_num,
                                        "Failed to send BlockProcessed event");
                                }
                            } else {
                                warn!(block_number = block_num,
                                    "Skipping logs because process_block returned error instead of transactions");
                            }

                            latest_block_num = block_num;
                        }
                    }

                    track_request_opt = self.transactions_rs.recv() => {
                        let track = if let Some(r) = track_request_opt { r }
                        else {
                            warn!("Tracking transactions channel closed. Exiting worker loop.");
                            break
                        };

                        let tracked_key = track.block_number + track.confirm_after;
                        let tracked_tx = TrackedTx {
                            tx_hash: track.tx_hash.clone(),
                            block_hash: track.block_hash,
                            block_number: track.block_number,
                        };

                        match track.tx_hash.parse::<TxHash>() {
                            Ok(tx_hash) => self.tx_block_map
                                .insert(tx_hash, (tracked_key, tracked_tx.clone())),
                            Err(e) => {
                                warn!(error = %e, "Failed to parse tx_hash. Skipping transaction.");
                                continue
                            }
                        };

                        self.tracked_txs.entry(tracked_key)
                            .or_default()
                            .insert(tracked_tx);
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
    async fn process_blocks_reorg(
        &mut self,
        latest_block_number: u64,
        latest_parent_hash: String,
    ) {
        let prev_hash = self.block_hashes.get(&(latest_block_number - 1));
        if prev_hash.is_none() || prev_hash.unwrap() == &latest_parent_hash {
            return; // well not my problem if block_hashes are empty
        }

        let mut prev_block_num = latest_block_number - 1;
        let mut new_blocks: Vec<AnyRpcBlock> = Vec::new();

        let mut depth = 0;

        while prev_block_num != 0 && depth < MAX_REORG_DEPTH {
            let block = self.get_block(prev_block_num).await;
            let block_hash = block.header.hash.to_string();

            new_blocks.push(block);

            if self.block_hashes.get(&prev_block_num) == Some(&block_hash) {
                new_blocks.pop();
                break
            }

            prev_block_num -= 1;
            depth += 1;
        }

        if depth >= MAX_REORG_DEPTH {
            warn!("Reorg depth is more than 100! That's bad.")
        }

        let dead_blocks_from = prev_block_num + 1; // everything above is DEAD (reorged)

        let mut dead_txs_info = HashMap::new();

        self.tx_block_map.retain(|tx_hash, (confirm_on, tracked_tx)| {
            if tracked_tx.block_number >= dead_blocks_from {
                dead_txs_info.insert(*tx_hash, (*confirm_on, tracked_tx.clone()));
                false
            } else { true }
        });

        for (old_confirm_on, tracked) in dead_txs_info.values() {
            if let Some(tx_set) = self.tracked_txs.get_mut(old_confirm_on) {
                tx_set.remove(tracked);

                if tx_set.is_empty() {
                    self.tracked_txs.remove(old_confirm_on);
                }
            }
        }

        let mut reorged_blocks = Vec::new();

        for block in new_blocks.into_iter().rev() { // from old to new
            let block_number = block.number();
            let block_hash = block.header.hash.to_string();

            reorged_blocks.push((block_number, block_hash.clone()));

            for tx in block.into_transactions_iter() {
                let tx_hash = tx.tx_hash();

                if let Some((confirm_on, tracked)) = dead_txs_info.remove(&tx_hash) {
                    let new_confirm_on = block_number + (confirm_on - tracked.block_number);

                    let new_tracked_tx = TrackedTx {
                        tx_hash: tx_hash.to_string(),
                        block_hash: block_hash.clone(),
                        block_number,
                    };

                    self.tx_block_map.insert(tx_hash, (new_confirm_on, new_tracked_tx.clone()));
                    self.tracked_txs.entry(new_confirm_on)
                        .or_default()
                        .insert(new_tracked_tx);

                    if let Err(e) = self.event_tx.send(ChainEvent::PaymentReorged {
                        tx_hash: tracked.tx_hash,
                        old_block_number: tracked.block_number,
                        new_block_number: block_number,
                        old_block_hash: tracked.block_hash,
                        new_block_hash: block_hash.clone(),
                    }).await {
                        warn!(error = %e, tx_hash = %tx_hash,
                            "Failed to send PaymentReorged event");
                    };
                }
            }
        }

        let pending_transactions: Vec<String> = dead_txs_info
            .into_values()
            .map(|(_, tracked)| tracked.tx_hash)
            .collect();

        let chain_id = self.state_rx.borrow().static_data.id;
        
        if let Err(e) = self.event_tx.send(ChainEvent::BlocksReorged {
            chain_id,
            new_blocks: reorged_blocks,
            pending_transactions,
        }).await {
            warn!(error = %e, block_number = latest_block_number,
                "Failed to send BlocksReorged event");
        };
    }

    async fn process_tracked_transactions(
        &mut self,
        block_number: u64
    ) {
        let ready_keys: Vec<u64> = self.tracked_txs.keys()
            .filter(|&k| *k <= block_number)
            .copied()
            .collect();

        for k in ready_keys {
            if let Some(txs) = self.tracked_txs.remove(&k) {
                for tx in txs {
                    if let Ok(tx_hash) = tx.tx_hash.parse::<TxHash>() {
                        self.tx_block_map.remove(&tx_hash);
                    }

                    if let Err(e) = self.event_tx.send(ChainEvent::PaymentConfirmed {
                        tx_hash: tx.tx_hash.clone(),
                        block_number: tx.block_number,
                        block_hash: tx.block_hash,
                        confirmed_after: block_number - tx.block_number,
                    }).await {
                        warn!(error = %e, tx_hash = tx.tx_hash, current_block_number = block_number,
                            "Failed to send PaymentConfirmed event");
                    }
                }
            }
        }
    }
}

impl EvmBlockchainWorker {
    async fn get_block(
        &self,
        block_number: u64,
    ) -> AnyRpcBlock {
        loop {
            match self.provider.get_block_by_number(block_number.into()).full().await {
                Ok(Some(block)) => break block,
                Ok(None) => { // block reorg?
                    warn!(
                        block_number,
                        "Block not found (None). Possible sync lag. Retrying in 1s..."
                    );
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        block_number,
                        "RPC provider error. Retrying in 1s..."
                    );
                }
            };

            tokio::time::sleep(Duration::from_secs(1)).await;
            // we CAN NOT skip blocks btw
        }
    }

    async fn process_block(
        &self,
        block_number: u64
    ) -> anyhow::Result<(BlockHash, String)> {
        let state = self.state_rx.borrow().clone();

        let block = self.get_block(block_number).await;

        let block_hash = block.header.hash;
        let parent_hash = block.header.parent_hash.to_string();

        for tx in block.into_transactions_iter() {
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
                    chain_name: state.static_data.name.clone(),
                    tx_hash,
                    from,
                    to: address_to.to_string(),
                    asset,
                    amount_raw: amount,
                    amount_human,
                    block_number,
                    block_hash: block_hash.to_string(),
                    log_index: None,
                    required_confirmations: state.dynamic_data.required_confirmations,
                };

                if let Err(e) = self.event_tx.send(event).await {
                    warn!(error = %e, block_number, block_hash = %block_hash,
                        "Failed to send PaymentDetected event");
                }
            }
        }

        Ok((block_hash, parent_hash))
    }

    async fn process_logs(
        &self,
        block_hash: BlockHash,
        actual_lag: u64,
    ) -> anyhow::Result<()> {
        let state = self.state_rx.borrow().clone();

        if self.tokens_map.is_empty() {
            trace!("No tokens to watch, skipping log processing");
            return Ok(());
        }

        let filter = match self.tokens_map.len() < 15 {
            true => {
                let token_addresses: Vec<Address> = self.tokens_map.keys()
                    .copied()
                    .collect();

                Filter::new()
                    .at_block_hash(block_hash)
                    .address(token_addresses)
                    .event_signature(Transfer::SIGNATURE_HASH)
            }
            false => Filter::new()
                .at_block_hash(block_hash)
                .event_signature(Transfer::SIGNATURE_HASH)
        };

        let mut attempt = 0;
        let max_retries = 3;

        let logs = loop {
            match self.provider.get_logs(&filter).await {
                Ok(l) => {
                    if !l.is_empty()
                        || actual_lag >= state.dynamic_data.safe_lag as u64 {
                        break l
                    }

                    if attempt < max_retries {
                        attempt += 1;
                        debug!(
                            attempt,
                            max_retries,
                            "Tried to get logs with Transfer, but they are empty. Retrying in 1s..."
                        );

                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }

                    debug!("Gave up retrying.");

                    break l;
                },
                Err(e) => {
                    warn!(error = %e, block_hash = %block_hash,
                        "Failed to get logs. Retrying in 1s...");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        };

        if !logs.is_empty() {
            debug!(count = logs.len(), "Received non-empty logs from RPC");
        }

        for log in logs {
            let contract_address = log.address();

            let token_data = match self.tokens_map.get(&contract_address) {
                Some(data) => data,
                None => {
                    continue;
                }
            };

            if let Ok(transfer) = log.log_decode::<Transfer>() {
                let event_data = transfer.inner;

                let address_to = event_data.to;

                if !self.watch_addresses.contains(&address_to) {
                    continue
                }

                let asset = Asset::Token(token_data.symbol.clone(), contract_address.to_string());
                let amount = event_data.value;
                let from = event_data.from.to_string();
                let amount_human = format_units(amount, token_data.decimals)
                    .unwrap_or_default();

                let tx_hash = log.transaction_hash.unwrap_or_default()
                    .to_string();
                let block_number = log.block_number.unwrap_or_default();

                info!(
                    asset = %asset,
                    %tx_hash,
                    from = %from,
                    to = %address_to.to_string(),
                    amount = %amount_human,
                    "Token transfer detected"
                );

                let event = ChainEvent::PaymentDetected {
                    chain_name: state.static_data.name.clone(),
                    tx_hash,
                    from,
                    to: address_to.to_string(),
                    asset,
                    amount_raw: amount,
                    amount_human,
                    block_number,
                    block_hash: block_hash.to_string(),
                    log_index: log.log_index,
                    required_confirmations: state.dynamic_data.required_confirmations,
                };

                if let Err(e) = self.event_tx.send(event).await {
                    warn!(error = %e, block_number, block_hash = %block_hash,
                        "Failed to send PaymentDetected event");
                }
            }
        }

        Ok(())
    }
}