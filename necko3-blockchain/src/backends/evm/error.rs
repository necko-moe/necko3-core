use alloy::hex::FromHexError;
use coins_bip32::Bip32Error;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProviderError {
    #[error("Failed to build HTTP client: {0}")]
    HttpClient(#[from] reqwest::Error),

    #[error("No valid RPC URLs provided to build the fallback provider")]
    NoValidUrls,
}

#[derive(Debug, Error)]
pub enum EvmAdapterError {
    #[error("BIP32 derivation error: {0}")]
    Bip32(#[from] Bip32Error),

    #[error("Failed to parse EVM address '{address}': {source}")]
    AddressParse {
        address: String,
        #[source]
        source: FromHexError,
    },

    #[error("Provider initialization failed: {0}")]
    ProviderInit(#[from] ProviderError),
}