use crate::chain::{BlockchainAdapter, ChainType};
use crate::db::{Database, DatabaseAdapter};
use crate::model::PaymentEvent;
use crate::state::AppState;
use alloy::consensus::Transaction;
use alloy::network::TransactionResponse;
use alloy::primitives::utils::format_units;
use alloy::primitives::{Address, BlockNumber};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::Transaction as RpcTransaction;
use alloy::rpc::types::{Block, Filter};
use alloy::sol;
use coins_bip32::prelude::{Parent, XPub};
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use url::Url;
use crate::config::TokenConfig;

sol! {
    #[derive(Debug)]
    event Transfer(address indexed from, address indexed to, uint256 value);
}

#[derive(Clone)]
pub struct EvmBlockchain {
    chain_name: String,
    db: Arc<Database>,
    sender: Option<Sender<PaymentEvent>>,
}

impl BlockchainAdapter for EvmBlockchain {
    fn new(state: Arc<AppState>, _chain_type: ChainType, chain_name: &str, sender: Option<Sender<PaymentEvent>>) -> Self {
        Self {
            chain_name: chain_name.to_owned(),
            db: state.db.clone(),
            sender
        }
    }

    async fn derive_address(&self, index: u32) -> anyhow::Result<String> {
        let xpub = XPub::from_str(&self.db.get_xpub(&self.chain_name).await?
            .ok_or_else(|| anyhow::anyhow!("chain {} does not exists", self.chain_name))?)?;

        let child_xpub = xpub.derive_child(index)?;
        let verifying_key = child_xpub.as_ref();

        Ok(Address::from_public_key(&verifying_key).to_string())
    }

    async fn listen(&self) -> anyhow::Result<()> {
        if self.sender.is_none() {
            anyhow::bail!("mpsc sender is empty")
        }
        let sender = self.sender.as_ref().unwrap();
        
        let rpc_url = Url::parse(&self.db.get_rpc_url(&self.chain_name).await?
            .ok_or_else(|| anyhow::anyhow!("chain {} does not exists", self.chain_name))?)?;
        let provider = ProviderBuilder::new().connect_http(rpc_url);

        let mut last_block_num = self.db.get_latest_block(&self.chain_name).await?
            .ok_or_else(|| anyhow::anyhow!("chain {} does not exists", self.chain_name))?;
        if last_block_num == 0 {
            last_block_num = match provider.get_block_number().await {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("failed to get latest block number: {}. retrying in 5s...", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    provider.get_block_number().await?
                }
            };
        }

        let block_lag = self.db.get_block_lag(&self.chain_name).await?
            .ok_or_else(|| anyhow::anyhow!("chain {} does not exists", self.chain_name))?;

        loop {
            let current_block_num = match provider.get_block_number().await {
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

            let watch_addresses = self.db.get_watch_addresses(&self.chain_name).await?
                .ok_or_else(|| anyhow::anyhow!("chain {} does not exists", self.chain_name))?;
            let address_set: HashSet<Address> = watch_addresses.iter()
                .map(|s| Address::from_str(&s).unwrap_or_default())
                .collect();
            let chain_config = match self.db.get_chain(&self.chain_name).await? {
                Some(cc) => cc,
                None => {
                    anyhow::bail!("failed to get chain config (???)");
                }
            };

            for block_num in (last_block_num + 1)..=current_block_num {
                println!("processing block {}...", block_num);

                if let Some(block) = provider
                    .get_block_by_number(block_num.into())
                    .full()
                    .await
                    .ok()
                    .flatten()
                {
                    let transactions = process_block(&address_set, block)
                        .unwrap_or_default();

                    if !transactions.is_empty() {
                        for tx in transactions {
                            let amount_human = format_units(tx.value(), chain_config.decimals)?;

                            let event = PaymentEvent {
                                network: self.chain_name.clone(),
                                tx_hash: tx.tx_hash(),
                                from: tx.from().to_string(),
                                to: tx.to().unwrap_or_default().to_string(), // default is unreachable,
                                // but it's better to keep this instead of ::unwrap()
                                token: chain_config.native_symbol.clone(),
                                amount: amount_human,
                                amount_raw: tx.value(),
                                decimals: chain_config.decimals,
                            };

                            let _ = sender.send(event).await;
                        }
                    }
                }

                let sender = sender.clone();
                self.process_logs(block_num, &address_set, &provider, sender).await?;
            }

            last_block_num = current_block_num;
            if last_block_num % 10 == 0 { // database won't send killers to my home (I hope)
                self.db.update_chain_block(&self.chain_name, last_block_num).await?;
            }
        }
    }
}

impl EvmBlockchain {
    async fn process_logs(
        &self,
        block_number: BlockNumber,
        addresses: &HashSet<Address>,
        provider: &impl Provider,
        sender: Sender<PaymentEvent>,
    ) -> anyhow::Result<()> {
        let token_addresses: Vec<Address> = self.db.get_token_contracts(&self.chain_name).await?
            .ok_or_else(|| anyhow::anyhow!("chain {} does not exists", self.chain_name))?
            .iter()
            .map(|c| Address::from_str(&c).unwrap_or_default())
            .collect();

        if token_addresses.is_empty() { return Ok(()); }

        let filter = Filter::new()
            .from_block(block_number)
            .to_block(block_number)
            .address(token_addresses)
            .event("Transfer(address,address,uint256)");

        let logs = match provider.get_logs(&filter).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("failed to get logs from {}: {}. Retrying in 3s...", self.chain_name, e);
                tokio::time::sleep(Duration::from_secs(3)).await;
                provider.get_logs(&filter).await.unwrap_or_default()
            }
        };

        let mut token_configs: HashMap<Address, TokenConfig> = HashMap::new();

        for log in logs {
            if let Ok(transfer) = log.log_decode::<Transfer>() {
                let event_data = transfer.inner;

                if addresses.contains(&event_data.to) {
                    let token_conf = match token_configs.entry(event_data.address) {
                        Entry::Occupied(entry) => entry.into_mut(),
                        Entry::Vacant(entry) => {
                            let maybe_conf = self.db.get_token_by_contract(
                                &self.chain_name,
                                event_data.address.to_string().as_str()
                            ).await?;

                            match maybe_conf {
                                Some(tc) => entry.insert(tc),
                                None => {
                                    eprintln!("(should be unreachable) received log from UNKNOWN \
                                    contract {}", event_data.address);
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
            tx.to().map_or(false, |to|
                addresses.contains(&to))
        })
        .collect();

    Ok(transactions)
}