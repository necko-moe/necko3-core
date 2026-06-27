use alloy_primitives::BlockNumber;
use async_trait::async_trait;
use std::collections::HashMap;
use crate::error::DbResult;

#[async_trait]
pub trait IndexedBlocksStore: Sync + Send {
    async fn get_latest_indexed_blocks(&self, chain_id: i32, limit: u16) -> DbResult<HashMap<BlockNumber, String>>;
    async fn upsert_indexed_block(&self, chain_id: i32, block_number: u64, block_hash: String) -> DbResult<()>;
    async fn upsert_indexed_blocks_batch(&self, chain_id: i32, blocks: &[(BlockNumber, String)]) -> DbResult<()>;
}