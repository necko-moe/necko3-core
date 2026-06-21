use necko3_types::TokenData;
use async_trait::async_trait;
use crate::error::DbResult;

pub type DbTokenId = i32;

#[async_trait]
pub trait TokenStore: Sync + Send {
    async fn get_tokens(&self, chain_name: &str) -> DbResult<Vec<TokenData>>;
    async fn get_token(&self, chain_name: &str, token_symbol: &str) -> DbResult<Option<TokenData>>;
    async fn get_token_by_id(&self, id: i32) -> DbResult<Option<TokenData>>;
    async fn get_token_by_contract(&self, contract_address: &str) -> DbResult<Option<TokenData>>;
    async fn get_tokens_with_symbol(&self, token_symbol: &str) -> DbResult<Vec<TokenData>>;
    async fn remove_token(&self, chain_name: &str, token_symbol: &str) -> DbResult<TokenData>;
    async fn add_token(&self, chain_name: &str, token_config: &TokenData) -> DbResult<DbTokenId>;
}