use crate::chain::BlockchainAdapter;
use crate::db::{Database, DatabaseAdapter};
use crate::model::TokenConfig;
use crate::model::{ChainConfig, PaymentEvent};
use alloy::consensus::Transaction;
use alloy::network::TransactionResponse;
use alloy::primitives::utils::format_units;
use alloy::primitives::{Address, TxHash, B256};
use alloy::providers::fillers::{BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller};
use alloy::providers::{Identity, Provider, ProviderBuilder, RootProvider};
use alloy::rpc::types::Transaction as RpcTransaction;
use alloy::rpc::types::{Block, Filter};
use alloy::sol;
use coins_bip32::prelude::{Parent, XPub};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use url::Url;

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

impl BlockchainAdapter for EvmBlockchain {
    fn new(chain_config: ChainConfig) -> anyhow::Result<Self> {
        let rpc_url = Url::parse(&chain_config.rpc_url).unwrap();
        let provider = ProviderBuilder::new().connect_http(rpc_url);

        Ok(Self {
            chain_name: chain_config.name.clone(),
            chain_config: Arc::new(RwLock::new(chain_config)),
            provider,
        })
    }


    async fn derive_address(&self, index: u32) -> anyhow::Result<String> {
        let xpub = XPub::from_str(
            &self.chain_config.read().unwrap().xpub)?;

        let child_xpub = xpub.derive_child(index)?;
        let verifying_key = child_xpub.as_ref();

        Ok(Address::from_public_key(&verifying_key).to_string())
    }

    async fn listen(&self, db: Arc<Database>, sender: Sender<PaymentEvent>) -> anyhow::Result<()> {
        let mut last_block_num = self.chain_config.read().unwrap().last_processed_block;
        if last_block_num == 0 {
            last_block_num = match self.provider.get_block_number().await {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("failed to get latest block number: {}. retrying in 5s...", e);
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
                    eprintln!("failed to get latest block number: {}. sleep 2s...", e);
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue
                }
            }.saturating_sub(block_lag as u64);

            if current_block_num <= last_block_num {
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }

            let address_set: HashSet<Address> = self.chain_config.read().unwrap()
                .watch_addresses.read().unwrap()
                .iter()
                .map(|s| Address::from_str(&s).unwrap_or_default())
                .collect();
            let (decimals, native_symbol) = {
                let guard = self.chain_config.read().unwrap();
                (guard.decimals, guard.native_symbol.clone())
            };

            for block_num in (last_block_num + 1)..=current_block_num {
                println!("processing block {}...", block_num);

                let block = loop { // NEVER SKIP ANY BLOCK
                    match self.provider.get_block_by_number(block_num.into()).full().await {
                        Ok(Some(b)) => break b,
                        Ok(None) => {
                            eprintln!("Block {} not found (node lag?). Retrying in 1s...",
                                      block_num);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                        Err(e) => {
                            eprintln!("RPC Error fetching block {}: {}. Retrying in 1s...",
                                      block_num, e);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                };

                let block_hash = block.hash();

                let transactions = process_block(&address_set, block)
                    .unwrap_or_default();

                if !transactions.is_empty() {
                    for tx in transactions {
                        let amount_human = format_units(tx.value(), decimals)?;

                        let event = PaymentEvent {
                            network: self.chain_name.clone(),
                            tx_hash: tx.tx_hash(),
                            from: tx.from().to_string(),
                            to: tx.to().unwrap_or_default().to_string(), // default is unreachable,
                            // but it's better to keep this instead of ::unwrap()
                            token: native_symbol.clone(),
                            amount: amount_human,
                            amount_raw: tx.value(),
                            decimals,
                            block_number: block_num,
                            log_index: None,
                        };

                        let _ = sender.send(event).await;
                    }
                }

                let sender = sender.clone();
                self.process_logs(block_hash, &address_set, sender).await?;
            }

            last_block_num = current_block_num;
            self.chain_config.write().unwrap().last_processed_block = last_block_num;
            if last_block_num % 10 == 0 { // database won't send killers to my home (I hope)
                db.update_chain_block(&self.chain_name, last_block_num).await?;
            }
        }
    }

    async fn get_tx_block_number(&self, tx_hash: &str) -> anyhow::Result<Option<u64>> {
        let hash = tx_hash.parse::<TxHash>()?;

        match self.provider.get_transaction_receipt(hash).await? {
            Some(receipt) => {
                if receipt.status() { Ok(receipt.block_number) } else { Ok(None) }
            }
            None => Ok(None),
        }
    }

    fn config(&self) -> Arc<RwLock<ChainConfig>> {
        self.chain_config.clone()
    }
}

impl EvmBlockchain {
    async fn process_logs(
        &self,
        block_hash: B256,
        addresses: &HashSet<Address>,
        sender: Sender<PaymentEvent>,
    ) -> anyhow::Result<()> {
        let token_addresses: Vec<Address> = self.chain_config.read().unwrap()
            .tokens.read().unwrap()
            .iter()
            .map(|tc| Address::from_str(&tc.contract).unwrap_or_default())
            .collect();

        if token_addresses.is_empty() { return Ok(()); }

        let filter = Filter::new()
            .at_block_hash(block_hash)
            .address(token_addresses)
            .event("Transfer(address,address,uint256)");

        let logs = loop { // DO. NOT. SKIP. LOGS.
            match self.provider.get_logs(&filter).await {
                Ok(l) => break l,
                Err(e) => {
                    eprintln!("failed to get logs from {}: {}. Retrying in 2s...", self.chain_name, e);
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        };

        let mut token_configs: HashMap<Address, TokenConfig> = HashMap::new();

        for log in logs {
            if let Ok(transfer) = log.log_decode::<Transfer>() {
                let event_data = transfer.inner;
                let address = event_data.address;

                if addresses.contains(&event_data.to) {
                    let token_conf = match token_configs.entry(event_data.address) {
                        Entry::Occupied(entry) => entry.into_mut(),
                        Entry::Vacant(entry) => {
                            let maybe_conf = self.chain_config.read().unwrap()
                                .tokens.read().unwrap()
                                .iter()
                                .find(|tc| {
                                    let conf_addr = Address::from_str(&tc.contract)
                                        .unwrap_or_default();
                                    conf_addr == address
                                })
                                .cloned();

                            match maybe_conf {
                                Some(tc) => entry.insert(tc),
                                None => {
                                    eprintln!("(should be unreachable) received log from UNKNOWN \
                                    contract {}", address);
                                    continue;
                                }
                            }
                        }
                    };

                    let amount_human = format_units(event_data.value, token_conf.decimals)
                        .unwrap_or_default();

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

                    let _ = sender.send(event).await;
                }
            }
        }

        Ok(())
    }
}

fn process_block(
    addresses: &HashSet<Address>,
    block: Block,
) -> anyhow::Result<Vec<RpcTransaction>> {
    let txs = block.into_transactions_vec();

    let transactions = txs
        .into_iter()
        .filter(|tx| {
            match tx.to() {
                Some(to) => addresses.contains(&to) && tx.value() > 0,
                None => false
            }
        })
        .collect();

    Ok(transactions)
}