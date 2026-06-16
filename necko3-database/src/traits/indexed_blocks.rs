use alloy_primitives::BlockNumber;
use async_trait::async_trait;
use std::collections::HashMap;

#[async_trait]
pub trait IndexedBlocksStore: Sync + Send {
    async fn get_latest_indexed_blocks(&self, chain_id: i32, limit: u16) -> anyhow::Result<HashMap<BlockNumber, String>>;
    async fn upsert_indexed_block(&self, chain_id: i32, block_number: u64, block_hash: String) -> anyhow::Result<()>;
    async fn upsert_indexed_blocks_batch(&self, chain_id: i32, blocks: &[(BlockNumber, String)]) -> anyhow::Result<()>;
}