use crate::model::{ChainConfig, PartialChainUpdate};
use async_trait::async_trait;

#[async_trait]
pub trait ChainStore: Sync + Send {
    async fn get_chains(&self) -> anyhow::Result<Vec<ChainConfig>>;
    async fn get_chain(&self, chain_name: &str) -> anyhow::Result<Option<ChainConfig>>;
    async fn add_chain(&self, chain_config: &ChainConfig) -> anyhow::Result<()>;
    async fn remove_chain(&self, chain_name: &str) -> anyhow::Result<()>;
    async fn chain_exists(&self, chain_name: &str) -> anyhow::Result<bool>;
    async fn update_chain_partial(&self, chain_name: &str, chain_update: &PartialChainUpdate) -> anyhow::Result<()>;
    async fn set_chain_active(&self, chain_name: &str, active: bool) -> anyhow::Result<()>;

    async fn update_chain_block(&self, chain_name: &str, block_num: u64) -> anyhow::Result<()>;

    // SELECT address FROM invoices WHERE status = 'Pending' AND network = $1
    async fn get_watch_addresses(&self, chain_name: &str) -> anyhow::Result<Vec<String>>;
    // async fn remove_watch_address(&self, chain_name: &str, address: &str) -> anyhow::Result<()>;
    // async fn remove_watch_addresses_bulk(&self, chain_name: &str, addresses: &[String]) -> anyhow::Result<()>;
    // async fn add_watch_address(&self, chain_name: &str, address: &str) -> anyhow::Result<()>;
}