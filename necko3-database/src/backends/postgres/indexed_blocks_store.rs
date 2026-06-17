use std::collections::HashMap;
use alloy_primitives::BlockNumber;
use async_trait::async_trait;
use sqlx::QueryBuilder;
use crate::backends::postgres::PostgresAdapter;
use crate::traits::IndexedBlocksStore;

#[async_trait]
impl IndexedBlocksStore for PostgresAdapter {
    async fn get_latest_indexed_blocks(&self, chain_id: i32, limit: u16) -> anyhow::Result<HashMap<BlockNumber, String>> {
        let blocks_db = sqlx::query_as::<_, (i64, String)>(
            r#"SELECT block_number, block_hash
                   FROM indexed_blocks
                   WHERE chain_id = $1
                   ORDER BY block_number DESC
                   LIMIT $2"#
        )
            .bind(chain_id)
            .bind(limit as i16)
            .fetch_all(&self.pool)
            .await?;

        let blocks = blocks_db
            .into_iter()
            .map(|(block_number, block_hash)| (block_number as u64, block_hash))
            .collect::<HashMap<BlockNumber, String>>();

        Ok(blocks)
    }

    async fn upsert_indexed_block(&self, chain_id: i32, block_number: u64, block_hash: String) -> anyhow::Result<()> {
        sqlx::query(
            r#"INSERT INTO indexed_blocks (chain_id, block_number, block_hash)
                   VALUES ($1, $2, $3)
                   ON CONFLICT (chain_id, block_number)
                       DO UPDATE SET block_hash = EXCLUDED.block_hash"#
        )
            .bind(chain_id)
            .bind(block_number as i64)
            .bind(block_hash)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn upsert_indexed_blocks_batch(&self, chain_id: i32, blocks: &[(BlockNumber, String)]) -> anyhow::Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }

        let mut query_builder = QueryBuilder::new(
            "INSERT INTO indexed_blocks (chain_id, block_number, block_hash) "
        );

        query_builder.push_values(blocks, |mut binder, (block_number, block_hash)| {
            binder.push_bind(chain_id)
                .push_bind(*block_number as i64)
                .push_bind(block_hash);
        });

        query_builder.push(
            " ON CONFLICT (chain_id, block_number) DO UPDATE SET block_hash = EXCLUDED.block_hash"
        );

        let query = query_builder.build();
        query.execute(&self.pool).await?;

        Ok(())
    }
}