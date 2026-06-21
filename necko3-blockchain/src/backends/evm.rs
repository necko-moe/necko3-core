pub mod fallback_provider;

use std::cmp::min;
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
use tokio::sync::mpsc;
use tokio::time;
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, instrument, trace, warn, Instrument};
use necko3_types::blockchain::{Asset, ChainEvent, ChainState, StateCommand, TrackTransaction};
use necko3_types::TokenData;
use crate::backends::evm::fallback_provider::create_fallback_provider;
use crate::traits::adapter::BlockchainAdapter;
use crate::traits::worker::BlockchainWorker;

pub const MAX_REORG_DEPTH: u64 = 100;

sol! {
    #[derive(Debug)]
    event Transfer(address indexed from, address indexed to, uint256 value);
}

pub struct EvmBlockchain;

impl BlockchainAdapter for EvmBlockchain {
    #[instrument(skip(self), level = "debug")]
    fn derive_address(&self, xpub: String, index: u32) -> anyhow::Result<String> {
        trace!("Deriving address for index {}", index);

        let xpub = XPub::from_str(&xpub)?;
        let child_xpub = xpub.derive_child(index)?;

        let addr = Address::from_public_key(child_xpub.as_ref()).to_string();
        trace!(address = %addr, "Derived address");

        Ok(addr)
    }

    fn build_worker(
        state: ChainState,
        tokens_map: HashMap<String, TokenData>,
        watch_addresses: HashSet<String>,
        block_hashes: HashMap<BlockNumber, String>,

        state_rx: mpsc::Receiver<StateCommand>,
        transactions_rx: mpsc::Receiver<TrackTransaction>,
        event_tx: mpsc::Sender<ChainEvent>
    ) -> anyhow::Result<impl BlockchainWorker> {
        let worker_watch_addresses = watch_addresses.iter()
            .map(|s| s.parse::<Address>())
            .collect::<Result<HashSet<_>, _>>()?;

        let mut worker_tokens_map = HashMap::with_capacity(tokens_map.len());

        for (k, v) in tokens_map.iter() {
            match k.parse::<Address>() {
                Ok(addr) => {
                    worker_tokens_map.insert(addr, v.clone());
                }
                Err(e) => {
                    warn!(contract_address = k, error = %e,
                        "Failed to parse token contract_address as Address");
                }
            }
        }

        EvmBlockchainWorker::new(state, worker_tokens_map, worker_watch_addresses, block_hashes,
                                 state_rx, transactions_rx, event_tx)
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
    state: ChainState,
    state_rx: mpsc::Receiver<StateCommand>,
    latest_block_num: u64,

    transactions_rx: mpsc::Receiver<TrackTransaction>,
    /// key = block_number+required_confirmations
    tracked_txs: HashMap<BlockNumber, HashSet<TrackedTx>>,
    tx_block_map: HashMap<TxHash, (BlockNumber, TrackedTx)>,

    block_hashes: HashMap<BlockNumber, String>,

    watch_addresses: HashSet<Address>,
    /// key = contract_address
    tokens_map: HashMap<Address, TokenData>,

    event_tx: mpsc::Sender<ChainEvent>
}

impl EvmBlockchainWorker {
    pub fn new(
        state: ChainState,
        tokens_map: HashMap<Address, TokenData>,
        watch_addresses: HashSet<Address>,
        block_hashes: HashMap<BlockNumber, String>,

        state_rx: mpsc::Receiver<StateCommand>,
        transactions_rx: mpsc::Receiver<TrackTransaction>,
        event_tx: mpsc::Sender<ChainEvent>
    ) -> anyhow::Result<Self> {
        let provider = match create_fallback_provider(&state.dynamic_data.rpc_urls) {
            Ok(p) => p,
            Err(e) => {
                anyhow::bail!("Failed to create fallback provider. Error: {:?}", e)
            }
        };

        Ok(Self {
            provider,
            state,
            state_rx,
            latest_block_num: 0,
            transactions_rx,
            tracked_txs: HashMap::new(),
            watch_addresses,
            tokens_map,
            block_hashes,
            event_tx,
            tx_block_map: Default::default(),
        })
    }
}

#[async_trait]
impl BlockchainWorker for EvmBlockchainWorker {
    #[instrument(skip(self))]
    async fn run(mut self) {
        let worker_span = tracing::info_span!("worker", chain_name = self.state.static_data.name);

        async {
            self.init_starting_block().await;

            let mut interval = time::interval(Duration::from_millis(1500));
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

            interval.tick().await; // skip first tick

            loop {
                tokio::select! {
                    biased;

                    cmd_opt = self.state_rx.recv() => {
                        let Some(cmd) = cmd_opt
                        else {
                            warn!("State change channel closed. Exiting worker loop.");
                            break
                        };

                        self.handle_state_command(cmd);
                    }

                    track_request_opt = self.transactions_rx.recv() => {
                        let Some(track) = track_request_opt
                        else {
                            warn!("Tracking transactions channel closed. Exiting worker loop.");
                            break
                        };

                        self.handle_track_request(track);
                    }

                    _ = interval.tick() => {
                        self.handle_tick().await;
                    }
                }
            }
        }.instrument(worker_span).await;
    }
}

impl EvmBlockchainWorker {
    async fn init_starting_block(&mut self) {
        let mut latest_block_num = self.state.static_data.starting_block_num;

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

        self.latest_block_num = latest_block_num;
    }

    fn handle_state_command(&mut self, cmd: StateCommand) {
        match cmd {
            StateCommand::AddWatchAddress(addr) => {
                match addr.parse::<Address>() {
                    Ok(addr) => {
                        self.watch_addresses.insert(addr);
                    }
                    Err(e) => {
                        warn!(watch_address = addr, error = %e,
                                            "Failed to parse token watch_address as Address");
                    }
                }
            }
            StateCommand::RemoveWatchAddress(addr) => {
                match addr.parse::<Address>() {
                    Ok(addr) => {
                        self.watch_addresses.remove(&addr);
                    }
                    Err(e) => {
                        warn!(watch_address = addr, error = %e,
                                            "Failed to parse token watch_address as Address");
                    }
                }
            }
            StateCommand::AddTokenData(token_data) => {
                let contract_address = token_data.contract.as_str();

                match contract_address.parse::<Address>() {
                    Ok(addr) => {
                        self.tokens_map.insert(addr, token_data);
                    }
                    Err(e) => {
                        warn!(contract_address, error = %e,
                                            "Failed to parse token contract_address as Address");
                    }
                }
            }
            StateCommand::RemoveToken { contract_address } => {
                match contract_address.parse::<Address>() {
                    Ok(addr) => {
                        self.tokens_map.remove(&addr);
                    }
                    Err(e) => {
                        warn!(contract_address, error = %e,
                                            "Failed to parse token contract_address as Address");
                    }
                }
            }
            StateCommand::ChangeActive(is_active) => {
                self.state.dynamic_data.active = is_active;
            }
            StateCommand::ChangeRpcUrls(rpc_urls) => {
                match create_fallback_provider(&rpc_urls) {
                    Ok(provider) => {
                        self.provider = provider;
                        debug!("Provider successfully updated with new RPC URLs");
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to create fallback provider. Error: {:?}", e)
                    }
                }
            }
            StateCommand::ChangeLastProcessedBlock(last_processed_block) => {
                self.latest_block_num = last_processed_block;
            }
            StateCommand::ChangeBlockLag(block_lag) => {
                self.state.dynamic_data.block_lag = block_lag;
            }
            StateCommand::ChangeSafeLag(safe_lag) => {
                self.state.dynamic_data.safe_lag = safe_lag;
            }
            StateCommand::ChangeRequiredConfirmations(req_confirm) => {
                self.state.dynamic_data.required_confirmations = req_confirm;
            }
        };
    }

    fn handle_track_request(&mut self, track: TrackTransaction) {
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
                error!(error = %e, "Failed to parse tx_hash. Skipping transaction.");
                return;
            }
        };

        self.tracked_txs.entry(tracked_key)
            .or_default()
            .insert(tracked_tx);
    }

    async fn handle_tick(&mut self) {
        if !self.state.dynamic_data.active {
            trace!("Chain is not active. Skipping tick");
            return;
        }

        let current_rpc_block_num = match self.provider.get_block_number().await {
            Ok(n) => n,
            Err(e) => {
                warn!(error = %e, "Failed to get latest block number, retrying on next tick...");
                return;
            }
        };
        let rpc_block_with_lag = current_rpc_block_num
            .saturating_sub(self.state.dynamic_data.block_lag as u64);

        if rpc_block_with_lag <= self.latest_block_num {
            trace!(current = current_rpc_block_num, last = self.latest_block_num,
                "No new blocks, skipping tick...");
            return;
        }

        let range_to = min(rpc_block_with_lag, self.latest_block_num + 5);

        for block_num in (self.latest_block_num + 1)..=range_to {
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
                self.block_hashes.insert(block_num, block_hash.to_string());

                self.process_blocks_reorg(block_num, parent_hash).await;
                self.process_tracked_transactions(block_num).await;

                let actual_lag = current_rpc_block_num - block_num;

                if let Err(e) = self.process_logs(block_hash, actual_lag).await {
                    error!(error = %e, block_number = block_num, block_hash = %block_hash,
                        "Failed to process logs for block");
                }

                if let Err(e) = self.event_tx.send(ChainEvent::BlockProcessed {
                    chain_id: self.state.static_data.id,
                    chain_name: self.state.static_data.name.clone(),
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

            self.latest_block_num = block_num;
        }
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
            self.block_hashes.insert(block_number, block_hash.clone());

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

        let chain_id = self.state.static_data.id;

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
                let asset = Asset::Native(self.state.static_data.native_symbol.clone());
                let tx_hash = tx.tx_hash().to_string();
                let amount_human = format_units(amount, self.state.static_data.decimals)
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
                    chain_name: self.state.static_data.name.clone(),
                    tx_hash,
                    from,
                    to: address_to.to_string(),
                    asset,
                    amount_raw: amount,
                    amount_human,
                    block_number,
                    block_hash: block_hash.to_string(),
                    log_index: None,
                    required_confirmations: self.state.dynamic_data.required_confirmations,
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
                        || actual_lag >= self.state.dynamic_data.safe_lag as u64 {
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

                let asset = Asset::Token(token_data.symbol.clone());
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
                    chain_name: self.state.static_data.name.clone(),
                    tx_hash,
                    from,
                    to: address_to.to_string(),
                    asset,
                    amount_raw: amount,
                    amount_human,
                    block_number,
                    block_hash: block_hash.to_string(),
                    log_index: log.log_index,
                    required_confirmations: self.state.dynamic_data.required_confirmations,
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