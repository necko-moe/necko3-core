use serde::{Deserialize, Serialize};
use necko3_types::TokenData;

#[derive(Debug, Clone, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct TokenConfig {
    pub symbol: String,
    pub contract: String,
    pub decimals: u8,
    pub logo_url: Option<String>,
}

impl From<TokenConfig> for TokenData {
    fn from(value: TokenConfig) -> Self {
        Self {
            id: 0,
            chain_id: 0,
            symbol: value.symbol,
            contract: value.contract,
            decimals: value.decimals,
            logo_url: value.logo_url,
        }
    }
}

impl TokenConfig {
    pub fn new(symbol: impl Into<String>, contract: impl Into<String>, decimals: u8) -> Self {
        Self {
            symbol: symbol.into(),
            contract: contract.into(),
            decimals,
            logo_url: None,
        }
    }

    pub fn with_symbol(mut self, symbol: impl Into<String>) -> Self {
        self.symbol = symbol.into();
        self
    }

    pub fn with_contract(mut self, contract: impl Into<String>) -> Self {
        self.contract = contract.into();
        self
    }

    pub fn with_decimals(mut self, decimals: u8) -> Self {
        self.decimals = decimals;
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
}