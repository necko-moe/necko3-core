use crate::chain::BlockchainAdapter;
use crate::db::{Database, DatabaseAdapter};
use crate::model::TokenConfig;
use crate::model::{ChainConfig, PaymentEvent};
use alloy::primitives::utils::format_units;
use alloy::primitives::{Address, BlockNumber, TxHash, B256, U256};
use alloy::providers::fillers::{BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill,
                                NonceFiller};
use alloy::providers::{Identity, Provider, ProviderBuilder, RootProvider};
use alloy::rpc::types::Filter;
use alloy::sol;
use coins_bip32::prelude::{Parent, XPub};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use url::Url;

use tracing::{debug, error, info, instrument, warn, trace, Instrument};

type EvmProvider = FillProvider<JoinFill<Identity, JoinFill<GasFiller, JoinFill<BlobGasFiller,
    JoinFill<NonceFiller, ChainIdFiller>>>>, RootProvider>;

sol! {
    #[derive(Debug)]
    event Transfer(address indexed from, address indexed to, uint256 value);
}

#[derive(Clone)]
pub struct EvmBlockchain {
    chain_name: String,
    chain_config: Arc<RwLock<ChainConfig>>,
    provider: EvmProvider,
}

impl std::fmt::Debug for EvmBlockchain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EvmBlockchain")
            .field("name", &self.chain_name)
            .finish()
    }
}

impl BlockchainAdapter for EvmBlockchain {
    #[instrument(skip(chain_config), fields(chain = %chain_config.name))]
    fn new(chain_config: ChainConfig) -> anyhow::Result<Self> {
        debug!("Initializing EVM Blockchain adapter");
        let rpc_url = Url::parse(&chain_config.rpc_url).unwrap();
        let provider = ProviderBuilder::new().connect_http(rpc_url);

        Ok(Self {
            chain_name: chain_config.name.clone(),
            chain_config: Arc::new(RwLock::new(chain_config)),
            provider,
        })
    }

    #[instrument(skip(self), level = "debug")]
    async fn derive_address(&self, index: u32) -> anyhow::Result<String> {
        trace!("Deriving address for index {}", index);

        let xpub = XPub::from_str(
            &self.chain_config.read().unwrap().xpub)?;

        let child_xpub = xpub.derive_child(index)?;
        let verifying_key = child_xpub.as_ref();

        let addr = Address::from_public_key(&verifying_key).to_string();
        trace!(address = %addr, "Derived address");

        Ok(addr)
    }

    #[instrument(skip(self, db, sender), fields(chain = %self.chain_name, node_type = "EVM"), err)]
    async fn listen(&self, db: Arc<Database>, sender: Sender<PaymentEvent>) -> anyhow::Result<()> {
        info!("Starting blockchain listener loop");

        let mut last_block_num = self.chain_config.read().unwrap().last_processed_block;
        if last_block_num == 0 {
            debug!("No last processed block found, fetching latest from RPC");

            last_block_num = match self.provider.get_block_number().await {
                Ok(n) => n,
                Err(e) => {
                    warn!(error = %e, "Failed to get latest block number, retrying in 5s...");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    self.provider.get_block_number().await?
                }
            };
        }

        let block_lag = self.chain_config.read().unwrap().block_lag;

        loop {
            let current_block_num = match self.provider.get_block_number().await {
                Ok(n) => n,
                Err(e) => {
                    warn!(error = %e, "failed to get latest block number from RPC. Sleep 2s...");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue
                }
            }.saturating_sub(block_lag as u64);

            if current_block_num <= last_block_num {
                trace!(current = current_block_num, last = last_block_num,
                    "No new blocks, sleep 1s...");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }

            let (decimals, native_symbol) = {
                let guard = self.chain_config.read().unwrap();
                (guard.decimals, guard.native_symbol.clone())
            };

            for block_num in (last_block_num + 1)..=current_block_num {
                let span = tracing::info_span!("process_block", block_number = block_num);

                async {
                    debug!("Processing block...");

                    let transactions: Vec<Value> = loop {
                        let bj: Value = match self.provider.raw_request(
                            "eth_getBlockByNumber".into(),
                            (format!("0x{:x}", block_num), true),
                        ).await {
                            Ok(v) => v,
                            Err(e) => {
                                warn!(error = %e,
                                    "RPC Error during getBlockByNumber. Retrying in 1s...");
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                continue;
                            }
                        };

                        if !bj["error"].is_null() { // actually I don't know if node can return that
                            error!(rpc_error = ?bj["error"], "RPC Node returned error inside response");
                        }

                        match bj["transactions"].as_array() {
                            Some(txs) => break txs.to_owned(),
                            None => {
                                error!("Failed to parse transactions. Retrying in 1s...");
                                // THERE IS NO FUCKING WAY THAT THERE ARE NO TRANSACTIONS
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                continue;
                            }
                        }
                    };

                    let address_set: HashSet<Address> = self.chain_config.read().unwrap()
                        .watch_addresses.read().unwrap()
                        .iter()
                        .map(|s| Address::from_str(&s).unwrap_or_default())
                        .collect();

                    let tx_sender = sender.clone();
                    if let Err(e) = self.process_transactions(
                        &transactions, &address_set, tx_sender,
                        decimals, &native_symbol, block_num).await
                    {
                        error!(error = %e, "Failed to process block transactions");
                    }

                    let logs_sender = sender.clone();
                    if let Err(e) = self.process_logs(block_num, &transactions,
                                                      &address_set, logs_sender).await {
                        error!(error = %e, "Failed to process logs for block");
                    }

                    last_block_num = block_num;
                    self.chain_config.write().unwrap().last_processed_block = last_block_num;

                    if last_block_num % 10 == 0 || last_block_num == current_block_num {
                        debug!("Saving last processed block to DB");
                        if let Err(e) = db.update_chain_block(&self.chain_name, last_block_num).await {
                            error!(error = %e, "Failed to update chain block in DB");
                        }
                    }
                }.instrument(span).await;
            }
        }
    }

    #[instrument(skip(self), err)]
    async fn get_tx_block_number(&self, tx_hash: &str) -> anyhow::Result<Option<u64>> {
        debug!(tx_hash, "Checking transaction receipt");
        let hash = tx_hash.parse::<TxHash>()?;

        match self.provider.get_transaction_receipt(hash).await? {
            Some(receipt) => {
                if receipt.status() {
                    Ok(receipt.block_number)
                } else {
                    debug!("Transaction failed on-chain");
                    Ok(None)
                }
            }
            None => {
                debug!("Transaction receipt not found");
                Ok(None)
            },
        }
    }

    fn config(&self) -> Arc<RwLock<ChainConfig>> {
        self.chain_config.clone()
    }
}

impl EvmBlockchain {
    #[instrument(skip_all, fields(block_number = %block_number))]
    async fn process_logs(
        &self,
        block_number: BlockNumber,
        transactions: &[Value],
        addresses: &HashSet<Address>,
        sender: Sender<PaymentEvent>,
    ) -> anyhow::Result<()> {
        let token_map: HashMap<Address, TokenConfig> = {
            let guard = self.chain_config.read().unwrap();
            let tokens = guard.tokens.read().unwrap();

            tokens.iter()
                .filter_map(|tc| {
                    Address::from_str(&tc.contract).ok().map(|addr| (addr, tc.clone()))
                })
                .collect()
        };

        if token_map.is_empty() {
            trace!("No tokens to watch, skipping log processing");
            return Ok(());
        }

        trace!(count = token_map.len(), "Fetching logs for tokens");

        let token_addresses: Vec<Address> = token_map.keys().cloned().collect();

        let mut suspicious_block = false;
        for tx in transactions {
            if let Some(to_str) = tx["to"].as_str() {
                if let Ok(to_addr) = to_str.parse::<Address>() {
                    if token_map.contains_key(&to_addr) {
                        let input_data = tx["input"].as_str()
                            .or_else(|| tx["data"].as_str())
                            .unwrap_or("");

                        // 0xa9059cbb = transfer(address,uint256)
                        // 0x23b872dd = transferFrom(address,address,uint256)
                        let is_transfer = input_data.starts_with("0xa9059cbb") ||
                            input_data.starts_with("0x23b872dd");

                        if is_transfer {
                            suspicious_block = true;
                            trace!(
                                tx = %tx["hash"],
                                contract = %to_addr,
                                "Found transfer/transferFrom to watched contract. "
                            );
                            break;
                        }
                    }
                }
            }
        }

        let filter = Filter::new()
            .from_block(block_number)
            .to_block(block_number)
            .address(token_addresses)
            .event("Transfer(address,address,uint256)");

        let mut attempt = 0;
        let max_retries = 15; // WHERE IS TRANSACTION?????????

        let logs = loop {
            match self.provider.get_logs(&filter).await {
                Ok(l) => {
                    if !l.is_empty() {
                        break l;
                    }

                    if suspicious_block && attempt < max_retries {
                        attempt += 1;
                        warn!(
                            attempt,
                            max_retries,
                            "SUSPICIOUS: Transaction to contract found, but NO LOGS returned. \
                            Possibly RPC Lag or Revert. Retrying in 1s..."
                        );
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }

                    if suspicious_block && attempt >= max_retries {
                        debug!("Gave up retrying. Assuming transaction reverted or emitted no events.");
                    }

                    break l;
                },
                Err(e) => {
                    warn!(error = %e, "Failed to get logs. Retrying in 1s...");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        };

        if !logs.is_empty() {
            debug!(count = logs.len(), "Received non-empty logs from RPC");
        }

        for log in logs {
            let contract_address = log.address();

            let token_conf = match token_map.get(&contract_address) {
                Some(conf) => conf,
                None => {
                    error!(contract = %contract_address,
                        "Received log from UNKNOWN contract");
                    continue;
                },
            };

            if let Ok(transfer) = log.log_decode::<Transfer>() {
                let event_data = transfer.inner;

                if addresses.contains(&event_data.to) {
                    let amount_human = format_units(event_data.value, token_conf.decimals)
                        .unwrap_or_default();

                    info!(
                        token = %token_conf.symbol,
                        amount = %amount_human,
                        to = %event_data.to,
                        tx_hash = ?log.transaction_hash,
                        "Token transfer detected"
                    );

                    let event = PaymentEvent {
                        network: self.chain_name.clone(),
                        tx_hash: log.transaction_hash.unwrap_or_default(),
                        from: event_data.from.to_string(),
                        to: event_data.to.to_string(),
                        token: token_conf.symbol.clone(),
                        amount: amount_human,
                        amount_raw: event_data.value,
                        decimals: token_conf.decimals,
                        block_number: log.block_number
                            .unwrap_or(u64::MAX),
                        log_index: log.log_index,
                    };

                    if let Err(e) = sender.send(event).await {
                        error!(error = %e, "Failed to send payment event via channel");
                    }
                }
            }
        }

        Ok(())
    }

    async fn process_transactions(
        &self,
        transactions: &[Value],
        addresses: &HashSet<Address>,
        sender: Sender<PaymentEvent>,
        decimals: u8,
        native_symbol: &str,
        block_num: u64
    ) -> anyhow::Result<()> {
        for tx in transactions {
            let to_str = tx["to"].as_str().unwrap_or_default();

            if let Ok(to_addr) = to_str.parse::<Address>() {
                if !addresses.contains(&to_addr) {
                    continue
                }

                let value_hex = tx["value"].as_str().unwrap_or("0x0");
                let tx_hash = tx["hash"].as_str().unwrap_or_default();
                let from_str = tx["from"].as_str().unwrap_or_default();

                let value = U256::from_str_radix(
                    value_hex.trim_start_matches("0x"), 16)
                    .unwrap_or(U256::ZERO);

                if value > U256::ZERO {
                    let amount_human = format_units(value, decimals)
                        .unwrap_or_default();

                    info!(
                        symbol = %native_symbol,
                        %tx_hash,
                        to = %to_addr,
                        amount = %amount_human,
                        "Native payment detected"
                    );

                    let event = PaymentEvent {
                        network: self.chain_name.clone(),
                        tx_hash: tx_hash.parse().unwrap_or_default(),
                        from: from_str.to_string(),
                        to: to_addr.to_string(),
                        token: native_symbol.to_owned(),
                        amount: amount_human,
                        amount_raw: value,
                        decimals,
                        block_number: block_num,
                        log_index: None,
                    };

                    if let Err(e) = sender.send(event).await {
                        error!(error = %e, "Failed to send payment event via channel");
                    }
                }
            }
        }

        Ok(())
    }
}