use necko3_types::{ChainData, PartialChainUpdate};
use async_trait::async_trait;

#[async_trait]
pub trait ChainStore: Sync + Send {
    async fn get_chains(&self) -> anyhow::Result<Vec<ChainData>>;
    async fn get_chain(&self, chain_name: &str) -> anyhow::Result<Option<ChainData>>;
    async fn get_chain_by_id(&self, id: i32) -> anyhow::Result<Option<ChainData>>;
    async fn add_chain(&self, chain_config: &ChainData) -> anyhow::Result<()>;
    async fn remove_chain(&self, chain_name: &str) -> anyhow::Result<Option<ChainData>>;
    async fn chain_exists(&self, chain_name: &str) -> anyhow::Result<bool>;
    async fn update_chain_partial(&self, chain_name: &str, chain_update: &PartialChainUpdate) -> anyhow::Result<()>;
    async fn update_chain_active(&self, chain_name: &str, active: bool) -> anyhow::Result<()>;

    async fn update_chain_block(&self, chain_name: &str, block_num: u64) -> anyhow::Result<()>;

    async fn add_watch_address(&self, chain_name: &str, address: String) -> anyhow::Result<bool>;
    async fn remove_watch_address(&self, chain_name: &str, address: &str) -> anyhow::Result<bool>;
    async fn remove_watch_addresses(&self, chain_name: &str, addresses: &[String]) -> anyhow::Result<Vec<String>>;
}