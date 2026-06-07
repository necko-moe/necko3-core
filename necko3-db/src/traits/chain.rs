use crate::model::{ChainConfig, PartialChainUpdate};
use async_trait::async_trait;

#[async_trait]
pub trait ChainStore: Sync + Send {
    async fn get_chains(&self) -> anyhow::Result<Vec<ChainConfig>>;
    async fn get_chain(&self, chain_name: &str) -> anyhow::Result<Option<ChainConfig>>;
    async fn get_chain_by_id(&self, id: i32) -> anyhow::Result<Option<ChainConfig>>;
    async fn add_chain(&self, chain_config: &ChainConfig) -> anyhow::Result<()>;
    async fn remove_chain(&self, chain_name: &str) -> anyhow::Result<bool>;
    async fn chain_exists(&self, chain_name: &str) -> anyhow::Result<bool>;
    async fn update_chain_partial(&self, chain_name: &str, chain_update: &PartialChainUpdate) -> anyhow::Result<()>;
    async fn update_chain_active(&self, chain_name: &str, active: bool) -> anyhow::Result<()>;

    async fn update_chain_block(&self, chain_name: &str, block_num: u64) -> anyhow::Result<()>;
}