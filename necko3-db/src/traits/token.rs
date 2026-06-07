use crate::model::TokenConfig;
use async_trait::async_trait;

#[async_trait]
pub trait TokenStore: Sync + Send {
    async fn get_tokens(&self, chain_name: &str) -> anyhow::Result<Vec<TokenConfig>>;
    async fn get_token(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<TokenConfig>>;
    async fn get_token_by_id(&self, id: i32) -> anyhow::Result<Option<TokenConfig>>;
    async fn get_token_by_contract(&self, chain_name: &str, contract_address: &str) -> anyhow::Result<Option<TokenConfig>>;
    async fn get_tokens_with_symbol(&self, token_symbol: &str) -> anyhow::Result<Vec<TokenConfig>>;
    async fn remove_token(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<bool>;
    async fn add_token(&self, chain_name: &str, token_config: &TokenConfig) -> anyhow::Result<()>;
    
    async fn get_token_decimals(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<u8>>;
}