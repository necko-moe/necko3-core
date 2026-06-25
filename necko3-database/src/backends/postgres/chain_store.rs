use crate::backends::postgres::PostgresAdapter;
use crate::traits::*;
use async_trait::async_trait;
use necko3_types::{ChainData, PartialChainUpdate};
use std::collections::HashSet;
use crate::error::{DbError, DbResult};

#[async_trait]
impl ChainStore for PostgresAdapter {
    async fn get_chains(&self) -> DbResult<Vec<ChainData>> {
        let rows = sqlx::query(
            r#"SELECT * FROM chains"#
        )
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter()
            .map(Self::map_row_to_chain)
            .collect()
    }

    async fn get_chain(&self, chain_name: &str) -> DbResult<Option<ChainData>> {
        let row = sqlx::query(
            r#"SELECT * FROM chains WHERE name = $1"#
        )
            .bind(chain_name)
            .fetch_optional(&self.pool)
            .await?;

        row.map(Self::map_row_to_chain).transpose()
    }

    async fn get_chain_by_id(&self, id: i32) -> DbResult<Option<ChainData>> {
        let row = sqlx::query(
            r#"SELECT * FROM chains WHERE id = $1"#
        )
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        row.map(Self::map_row_to_chain).transpose()
    }

    async fn add_chain(&self, chain_config: &ChainData) -> DbResult<ChainData> {
        let row = sqlx::query(
            r#"INSERT INTO chains
                    (name, rpc_urls, chain_type, xpub, native_symbol, decimals,
                     last_processed_block, block_lag, required_confirmations, active, logo_url,
                     watch_addresses, safe_lag)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    RETURNING *"#,
        )
            .bind(&chain_config.name)
            .bind(&chain_config.rpc_urls)
            .bind(chain_config.chain_type.to_string())
            .bind(&chain_config.xpub)
            .bind(&chain_config.native_symbol)
            .bind(chain_config.decimals as i16)
            .bind(chain_config.last_processed_block as i64)
            .bind(chain_config.block_lag as i16)
            .bind(chain_config.required_confirmations as i64)
            .bind(chain_config.active)
            .bind(&chain_config.logo_url)
            .bind(chain_config.watch_addresses.iter()
                .collect::<Vec<_>>())
            .bind(chain_config.safe_lag as i16)
            .fetch_one(&self.pool)
            .await?;

        Self::map_row_to_chain(row)
    }

    async fn remove_chain(&self, chain_name: &str) -> DbResult<ChainData> {
        let result_opt = sqlx::query(
            "DELETE FROM chains WHERE name = $1 RETURNING *"
        )
            .bind(chain_name)
            .fetch_optional(&self.pool)
            .await?;

        let Some(result) = result_opt else {
            return Err(DbError::NotFound {
                entity: "Chain",
                id: chain_name.to_string(),
            })
        };

        Self::map_row_to_chain(result)
    }

    async fn chain_exists(&self, chain_name: &str) -> DbResult<bool> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM chains WHERE name = $1)"
        )
            .bind(chain_name)
            .fetch_one(&self.pool)
            .await?;

        Ok(exists)
    }

    async fn update_chain_partial(&self, chain_name: &str, chain_update: &PartialChainUpdate) -> DbResult<()> {
        let result = sqlx::query(
            r#"UPDATE chains SET
                       rpc_urls = COALESCE($1, rpc_urls),
                       last_processed_block = COALESCE($2, last_processed_block),
                       xpub = COALESCE($3, xpub),
                       block_lag = COALESCE($4, block_lag),
                       required_confirmations = COALESCE($5, required_confirmations),
                       active = COALESCE($6, active),
                       logo_url = COALESCE($7, logo_url),
                       safe_lag = COALESCE($8, safe_lag)
                   WHERE name = $9"#
        )
            .bind(chain_update.rpc_urls.clone())
            .bind(chain_update.last_processed_block.map(|x| x as i64))
            .bind(chain_update.xpub.to_owned())
            .bind(chain_update.block_lag.map(|x| x as i16))
            .bind(chain_update.required_confirmations.map(|x| x as i64))
            .bind(chain_update.active)
            .bind(chain_update.logo_url.clone())
            .bind(chain_update.safe_lag.map(|x| x as i16))
            .bind(chain_name)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
            return Err(DbError::NotFound {
                entity: "Chain",
                id: chain_name.to_string(),
            });
        }

        Ok(())
    }

    async fn update_chain_active(&self, chain_name: &str, active: bool) -> DbResult<()> {
        let result = sqlx::query(
            "UPDATE chains SET active = $1 WHERE name = $2"
        )
            .bind(active)
            .bind(chain_name)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
            return Err(DbError::NotFound {
                entity: "Chain",
                id: chain_name.to_string(),
            });
        }

        Ok(())
    }

    async fn update_chain_block(&self, chain_name: &str, block_num: u64) -> DbResult<()> {
        let result = sqlx::query(
            "UPDATE chains SET last_processed_block = $1 WHERE name = $2"
        )
            .bind(block_num as i64)
            .bind(chain_name)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
            return Err(DbError::NotFound {
                entity: "Chain",
                id: chain_name.to_string(),
            });
        }

        Ok(())
    }

    async fn add_watch_address(&self, chain_name: &str, address: String) -> DbResult<bool> {
        let result = sqlx::query(
            r#"UPDATE chains SET watch_addresses = ARRAY_APPEND(watch_addresses, $1)
                   WHERE name = $2 AND NOT ($1 = ANY(watch_addresses))"#
        )
            .bind(address)
            .bind(chain_name)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() > 0 {
            return Ok(true)
        }

        let exists = self.chain_exists(chain_name).await?;
        if !exists {
            return Err(DbError::NotFound {
                entity: "Chain",
                id: chain_name.to_string(),
            });
        }

        Ok(false)
    }

    async fn remove_watch_address(&self, chain_name: &str, address: &str) -> DbResult<bool> {
        let result = sqlx::query(
            r#"UPDATE chains SET watch_addresses = ARRAY_REMOVE(watch_addresses, $1)
                   WHERE name = $2 AND ($1 = ANY(watch_addresses))"#
        )
            .bind(address)
            .bind(chain_name)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() > 0 {
            return Ok(true);
        }

        let exists = self.chain_exists(chain_name).await?;
        if !exists {
            return Err(DbError::NotFound {
                entity: "Chain",
                id: chain_name.to_string(),
            });
        }

        Ok(false)
    }

    async fn remove_watch_addresses(&self, chain_name: &str, addresses: &[String]) -> DbResult<Vec<String>> {
        let to_remove: HashSet<&str> = addresses.iter()
            .map(|s| s.as_str())
            .collect();

        let mut tx = self.pool.begin().await?;

        let old_addresses_opt: Option<Vec<String>> = sqlx::query_scalar(
            "SELECT chains.watch_addresses FROM chains WHERE name = $1 FOR UPDATE"
        )
            .bind(chain_name)
            .fetch_optional(&mut *tx)
            .await?;

        let old_addresses = match old_addresses_opt {
            Some(addrs) => addrs,
            None => {
                tx.rollback().await?;
                return Err(DbError::NotFound {
                    entity: "Chain",
                    id: chain_name.to_string(),
                });
            }
        };

        let (removed, to_keep) = old_addresses.into_iter()
            .partition(|address| to_remove.contains(address.as_str()));

        sqlx::query("UPDATE chains SET watch_addresses = $1 WHERE name = $2")
            .bind(to_keep)
            .bind(chain_name)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;

        Ok(removed)
    }
}