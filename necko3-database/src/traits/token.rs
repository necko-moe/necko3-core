use crate::model::TokenData;
use async_trait::async_trait;

#[async_trait]
pub trait TokenStore: Sync + Send {
    async fn get_tokens(&self, chain_name: &str) -> anyhow::Result<Vec<TokenData>>;
    async fn get_token(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<TokenData>>;
    async fn get_token_by_id(&self, id: i32) -> anyhow::Result<Option<TokenData>>;
    async fn get_token_by_contract(&self, contract_address: &str) -> anyhow::Result<Option<TokenData>>;
    async fn get_tokens_with_symbol(&self, token_symbol: &str) -> anyhow::Result<Vec<TokenData>>;
    async fn remove_token(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<TokenData>>;
    async fn add_token(&self, chain_name: &str, token_config: &TokenData) -> anyhow::Result<()>;
    
    async fn get_token_decimals(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<u8>>;
}