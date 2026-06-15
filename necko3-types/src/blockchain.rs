use crate::{ChainData, ChainType, TokenData};
use alloy_primitives::{BlockHash, BlockNumber, B256, U256};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use strum::Display;

#[derive(Display, Debug, Clone)]
pub enum Asset {
    Native(String),
    /// symbol, contract_address
    Token(String, String),
}

#[derive(Debug, Clone)]
pub enum ChainEvent {
    PaymentDetected {
        tx_hash: String,
        from: String,
        to: String,
        asset: Asset,
        amount_raw: U256,
        amount_human: String,
        block_number: u64,

        block_hash: B256,
        log_index: Option<u64>,

        required_confirmations: u64,
    },

    PaymentReorged {
        tx_hash: String,
        old_block_number: u64,
        new_block_number: u64,
        old_block_hash: B256,
        new_block_hash: B256,
    },

    PaymentConfirmed {
        tx_hash: String,
        block_number: u64,
        block_hash: B256,
        confirmed_after: u64,
    },

    PaymentFailed {
        tx_hash: String,
    },

    BlockProcessed {
        block_number: u64,
        block_hash: B256,
    },

    BlocksReorged {
        new_blocks: Vec<(BlockNumber, BlockHash)>, 
        pending_transactions: Vec<String> // tx_hash
    },

    FatalError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainStaticData {
    pub name: String,
    pub chain_type: ChainType,
    pub native_symbol: String,
    pub decimals: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainDynamicData {
    pub active: bool,
    pub rpc_urls: Vec<String>,
    pub last_processed_block: u64,
    pub block_lag: u8,
    pub safe_lag: u8,
    pub required_confirmations: u64
}

impl From<ChainData> for (ChainStaticData, ChainDynamicData) {
    fn from(chain: ChainData) -> Self {
        let static_data = ChainStaticData {
            name: chain.name,
            chain_type: chain.chain_type,
            native_symbol: chain.native_symbol,
            decimals: chain.decimals,
        };

        let dynamic_data = ChainDynamicData {
            active: chain.active,
            rpc_urls: chain.rpc_urls,
            last_processed_block: chain.last_processed_block,
            block_lag: chain.block_lag,
            safe_lag: chain.safe_lag,
            required_confirmations: chain.required_confirmations,
        };

        (static_data, dynamic_data)
    }
}

#[derive(Debug, Clone)]
pub struct ChainState {
    pub static_data: Arc<ChainStaticData>,
    pub dynamic_data: Arc<ChainDynamicData>,

    pub tokens_map: Arc<HashMap<String, TokenData>>,
    pub watch_addresses: Arc<HashSet<String>>,

    pub block_hashes: Arc<HashMap<BlockNumber, BlockHash>>
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TrackTransaction {
    pub tx_hash: String,
    pub block_number: u64,
    pub block_hash: B256,
    pub confirm_after: u64,
}

