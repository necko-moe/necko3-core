pub mod blockchain;

use std::collections::HashSet;
use alloy_primitives::{BlockHash, U256};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display, EnumString};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainData {
    pub id: i32,
    pub name: String,
    pub active: bool,
    pub rpc_urls: Vec<String>,
    pub chain_type: ChainType,
    pub xpub: String,
    pub native_symbol: String,
    pub decimals: u8,
    pub last_processed_block: u64,
    pub block_lag: u8,
    /// Minimum block depth (lag) required to trust empty log responses from RPC.
    /// Protects against lagging node indexers returning false-empty logs near the chain tip.
    pub safe_lag: u8,
    pub required_confirmations: u64,
    pub logo_url: Option<String>,
    pub watch_addresses: HashSet<String>,
}

impl ChainData {
    pub fn patch(&mut self, update: &PartialChainUpdate) {
        if let Some(x) = update.active { self.active = x; }
        if let Some(x) = &update.rpc_urls { self.rpc_urls = x.to_owned(); }
        if let Some(x) = update.last_processed_block { self.last_processed_block = x; }
        if let Some(x) = &update.xpub { self.xpub = x.to_owned(); }
        if let Some(x) = update.block_lag { self.block_lag = x; }
        if let Some(x) = update.safe_lag { self.safe_lag = x; }
        if let Some(x) = update.required_confirmations { self.required_confirmations = x; }
        if let Some(x) = &update.logo_url { self.logo_url = Some(x.to_owned()); }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PartialChainUpdate {
    pub active: Option<bool>,
    pub rpc_urls: Option<Vec<String>>,
    pub last_processed_block: Option<u64>,
    pub xpub: Option<String>,
    pub block_lag: Option<u8>,
    pub safe_lag: Option<u8>,
    pub required_confirmations: Option<u64>,
    pub logo_url: Option<String>,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize,
    Display, EnumString, AsRefStr)]
#[strum(serialize_all = "UPPERCASE")]
pub enum ChainType {
    EVM
}

#[derive(Debug, Clone, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct TokenData {
    pub id: i32,
    pub chain_id: i32,
    pub symbol: String,
    pub contract: String,
    pub decimals: u8,
    pub logo_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Payment {
    pub id: Uuid,
    pub from: String,
    pub to: String,
    pub network: String,
    pub token: String,
    pub tx_hash: String,
    pub amount_raw: U256,
    pub block_number: u64,
    pub block_hash: BlockHash,
    pub log_index: u64,
    pub status: PaymentStatus,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq,
    Display, EnumString, AsRefStr)]
#[strum(serialize_all = "PascalCase")]
pub enum PaymentStatus {
    Confirming,
    Confirmed,
    Failed,
    Cancelled,
}