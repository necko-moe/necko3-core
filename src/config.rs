use crate::chain::ChainType;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use utoipa::ToSchema;

#[derive(Debug, Clone, Eq, Hash, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct TokenConfig {
    pub symbol: String,
    pub contract: String,
    pub decimals: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ChainConfig {
    pub name: String,
    pub rpc_url: String,
    pub chain_type: ChainType,
    pub xpub: String,
    pub native_symbol: String,
    pub decimals: u8,
    pub last_processed_block: u64,
    pub block_lag: u8,

    #[schema(ignore)]
    #[serde(skip)]
    pub watch_addresses: Arc<RwLock<HashSet<String>>>,

    #[schema(ignore)]
    #[serde(skip)]
    pub tokens: Arc<RwLock<HashSet<TokenConfig>>>,
}
