use std::collections::HashSet;
use serde::{Deserialize, Serialize};
use necko3_types::{ChainData, ChainType, TokenData};

pub use necko3_blockchain::backends::evm::EvmBlockchain;
use crate::builder::token_config::TokenConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainConfig {
    pub name: String,
    pub active: bool,
    pub rpc_urls: Vec<String>,
    pub chain_type: ChainType,
    pub xpub: String,
    pub native_symbol: String,
    pub decimals: u8,
    pub last_processed_block: u64,
    pub block_lag: u8,
    pub safe_lag: u8,
    pub required_confirmations: u64,
    pub logo_url: Option<String>,
    pub watch_addresses: HashSet<String>,
    
    pub tokens: Vec<TokenConfig>,
}

impl Default for ChainConfig {
    fn default() -> Self {
        ChainConfig::from(EvmBlockchain)
    }
}

impl From<ChainConfig> for ChainData {
    fn from(value: ChainConfig) -> Self {
        Self {
            id: 0,
            name: value.name,
            active: value.active,
            rpc_urls: value.rpc_urls,
            chain_type: value.chain_type,
            xpub: value.xpub,
            native_symbol: value.native_symbol,
            decimals: value.decimals,
            last_processed_block: value.last_processed_block,
            block_lag: value.block_lag,
            safe_lag: value.safe_lag,
            required_confirmations: value.required_confirmations,
            logo_url: value.logo_url,
            watch_addresses: value.watch_addresses,
        }
    }
}

impl From<ChainConfig> for (Vec<TokenData>, ChainData) {
    fn from(value: ChainConfig) -> (Vec<TokenData>, ChainData) {
        let tokens = value.tokens.into_iter()
            .map(TokenData::from)
            .collect::<Vec<_>>();
        
        let chain_data = ChainData {
            id: 0,
            name: value.name,
            active: value.active,
            rpc_urls: value.rpc_urls,
            chain_type: value.chain_type,
            xpub: value.xpub,
            native_symbol: value.native_symbol,
            decimals: value.decimals,
            last_processed_block: value.last_processed_block,
            block_lag: value.block_lag,
            safe_lag: value.safe_lag,
            required_confirmations: value.required_confirmations,
            logo_url: value.logo_url,
            watch_addresses: value.watch_addresses,
        };

        (tokens, chain_data)
    }
}

impl ChainConfig {
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    pub fn active(mut self) -> Self {
        self.active = true;
        self
    }

    pub fn not_active(mut self) -> Self {
        self.active = false;
        self
    }

    pub fn add_rpc_url(mut self, url: impl Into<String>) -> Self {
        self.rpc_urls.push(url.into());
        self
    }

    pub fn remove_rpc_url(mut self, url: &str) -> Self {
        self.rpc_urls.retain(|u| u != url);
        self
    }

    pub fn with_rpc_urls(mut self, urls: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.rpc_urls = urls.into_iter()
            .map(|url| url.into())
            .collect();

        self
    }

    pub fn with_chain_type(mut self, chain_type: ChainType) -> Self {
        self.chain_type = chain_type;
        self
    }

    pub fn with_xpub(mut self, xpub: impl Into<String>) -> Self {
        self.xpub = xpub.into();
        self
    }

    pub fn with_symbol(mut self, symbol: impl Into<String>) -> Self {
        self.native_symbol = symbol.into();
        self
    }

    pub fn with_decimals(mut self, decimals: u8) -> Self {
        self.decimals = decimals;
        self
    }

    pub fn with_last_processed_block(mut self, last_processed_block: u64) -> Self {
        self.last_processed_block = last_processed_block;
        self
    }

    pub fn with_block_lag(mut self, block_lag: u8) -> Self {
        self.block_lag = block_lag;
        self
    }

    pub fn with_safe_lag(mut self, safe_lag: u8) -> Self {
        self.safe_lag = safe_lag;
        self
    }

    pub fn with_required_confirmations(mut self, required_confirmations: u64) -> Self {
        self.required_confirmations = required_confirmations;
        self
    }

    pub fn with_logo_url(mut self, logo_url: impl Into<String>) -> Self {
        self.logo_url = Some(logo_url.into());
        self
    }

    pub fn without_logo_url(mut self) -> Self {
        self.logo_url = None;
        self
    }

    pub fn add_watch_address(mut self, watch_address: impl Into<String>) -> Self {
        self.watch_addresses.insert(watch_address.into());
        self
    }

    pub fn remove_watch_address(mut self, watch_address: &str) -> Self {
        self.watch_addresses.remove(watch_address);
        self
    }

    pub fn with_watch_addresses(mut self, watch_addresses: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.watch_addresses = watch_addresses.into_iter()
            .map(|url| url.into())
            .collect();

        self
    }
    
    pub fn add_token(mut self, token: impl Into<TokenConfig>) -> Self {
        self.tokens.push(token.into());
        self
    }
    
    pub fn remove_token(mut self, token_symbol: impl Into<String>) -> Self {
        let token_symbol = token_symbol.into();
        self.tokens.retain(|u| u.symbol != token_symbol);
        self
    }
    
    pub fn with_tokens(mut self, tokens: impl IntoIterator<Item = impl Into<TokenConfig>>) -> Self {
        self.tokens = tokens.into_iter()
            .map(|token| token.into())
            .collect();
        
        self
    }
}

impl From<EvmBlockchain> for ChainConfig {
    fn from(_value: EvmBlockchain) -> Self {
        Self {
            name: "".to_string(),
            active: true,
            rpc_urls: vec![],
            chain_type: ChainType::EVM,
            xpub: "".to_string(),
            native_symbol: "".to_string(),
            decimals: 18,
            last_processed_block: 0,
            block_lag: 3,
            safe_lag: 15,
            required_confirmations: 5,
            logo_url: None,
            watch_addresses: HashSet::new(),
            tokens: vec![],
        }
    }
}