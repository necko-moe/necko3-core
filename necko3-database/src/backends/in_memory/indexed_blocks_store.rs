use std::collections::HashMap;
use alloy_primitives::BlockNumber;
use async_trait::async_trait;
use crate::backends::in_memory::InMemoryAdapter;
use crate::error::DbResult;
use crate::traits::IndexedBlocksStore;

#[async_trait]
impl IndexedBlocksStore for InMemoryAdapter {
    async fn get_latest_indexed_blocks(&self, chain_id: i32, limit: u16) -> DbResult<HashMap<BlockNumber, String>> {
        let mut blocks = self.indexed_blocks.iter()
            .filter(|block| block.key().0 == chain_id)
            .map(|block| (block.key().1, block.value().clone()))
            .collect::<Vec<_>>();

        blocks.sort_unstable_by(|a, b| b.0.cmp(&a.0));

        let result = blocks
            .into_iter()
            .take(limit as usize)
            .collect::<HashMap<BlockNumber, String>>();

        Ok(result)
    }

    async fn upsert_indexed_block(&self, chain_id: i32, block_number: u64, block_hash: String) -> DbResult<()> {
        self.indexed_blocks.insert((chain_id, block_number), block_hash);
        Ok(())
    }

    async fn upsert_indexed_blocks_batch(&self, chain_id: i32, blocks: &[(BlockNumber, String)]) -> DbResult<()> {
        for (block_number, block_hash) in blocks {
            self.indexed_blocks.insert((chain_id, *block_number), block_hash.clone());
        }

        Ok(())
    }
}