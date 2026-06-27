use necko3_types::{ChainData, PartialChainUpdate};
use async_trait::async_trait;
use crate::error::DbResult;

pub type DbChainId = i32;

#[async_trait]
pub trait ChainStore: Sync + Send {
    async fn get_chains(&self) -> DbResult<Vec<ChainData>>;
    async fn get_chain(&self, chain_name: &str) -> DbResult<Option<ChainData>>;
    async fn get_chain_by_id(&self, id: i32) -> DbResult<Option<ChainData>>;
    async fn add_chain(&self, chain_config: &ChainData) -> DbResult<ChainData>;
    async fn remove_chain(&self, chain_name: &str) -> DbResult<ChainData>;
    async fn chain_exists(&self, chain_name: &str) -> DbResult<bool>;
    async fn update_chain_partial(&self, chain_name: &str, chain_update: &PartialChainUpdate) -> DbResult<()>;
    async fn update_chain_active(&self, chain_name: &str, active: bool) -> DbResult<()>;

    async fn update_chain_block(&self, chain_name: &str, block_num: u64) -> DbResult<()>;

    async fn add_watch_address(&self, chain_name: &str, address: String) -> DbResult<bool>;
    async fn remove_watch_address(&self, chain_name: &str, address: &str) -> DbResult<bool>;
    async fn remove_watch_addresses(&self, chain_name: &str, addresses: &[String]) -> DbResult<Vec<String>>;
}