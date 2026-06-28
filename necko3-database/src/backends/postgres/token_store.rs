use crate::backends::postgres::PostgresAdapter;
use crate::error::{DbError, DbQueryError, DbResult};
use crate::traits::{ChainStore, TokenStore};
use async_trait::async_trait;
use necko3_types::TokenData;

#[async_trait]
impl TokenStore for PostgresAdapter {
    async fn get_tokens(&self, chain_name: &str) -> DbResult<Vec<TokenData>> {
        let rows = sqlx::query(
            r#"SELECT t.*
                   FROM tokens t
                   JOIN chains c ON t.chain_id = c.id
                   WHERE c.name = $1"#
        )
            .bind(chain_name)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows.into_iter()
            .map(Self::map_row_to_token)
            .collect())
    }

    async fn get_token(&self, chain_name: &str, token_symbol: &str) -> DbResult<Option<TokenData>> {
        let row = sqlx::query(
            r#"SELECT t.*
                   FROM tokens t
                   JOIN chains c ON t.chain_id = c.id
                   WHERE c.name = $1 AND t.symbol = $2"#
        )
            .bind(chain_name)
            .bind(token_symbol)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(Self::map_row_to_token))
    }

    async fn get_token_by_id(&self, id: i32) -> DbResult<Option<TokenData>> {
        let row = sqlx::query(
            r#"SELECT * FROM tokens WHERE id = $1"#
        )
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(Self::map_row_to_token))
    }

    async fn get_token_by_contract(&self, contract_address: &str) -> DbResult<Option<TokenData>> {
        let row = sqlx::query(
            r#"SELECT * FROM tokens WHERE contract_address = $1"#
        )
            .bind(contract_address)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(Self::map_row_to_token))
    }

    async fn get_tokens_with_symbol(&self, token_symbol: &str) -> DbResult<Vec<TokenData>> {
        let rows = sqlx::query(
            r#"SELECT * FROM tokens WHERE symbol = $1"#
        )
            .bind(token_symbol)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows.into_iter()
            .map(Self::map_row_to_token)
            .collect())
    }

    async fn remove_token(&self, chain_name: &str, token_symbol: &str) -> DbResult<TokenData> {
        let result_opt = sqlx::query(
            r#"DELETE FROM tokens t
                   USING chains c
                   WHERE t.chain_id = c.id
                       AND t.symbol = $1
                       AND c.name = $2
                   RETURNING t.*"#
        )
            .bind(token_symbol)
            .bind(chain_name)
            .fetch_optional(&self.pool)
            .await?;

        if let Some(row) = result_opt {
            return Ok(Self::map_row_to_token(row));
        }

        let chain_exists = self.chain_exists(chain_name).await?;
        if !chain_exists {
            return Err(DbError::NotFound {
                entity: "Chain",
                id: chain_name.to_string(),
            });
        }

        Err(DbError::NotFound {
            entity: "Token",
            id: token_symbol.to_string(),
        })
    }

    async fn add_token(&self, chain_name: &str, token_config: TokenData) -> DbResult<TokenData> {
        let row_opt = sqlx::query(
            r#"INSERT INTO tokens
               (chain_id, symbol, contract_address, decimals, logo_url)
               SELECT chains.id, $2, $3, $4, $5
               FROM chains
               WHERE name = $1
               RETURNING tokens.*"#
        )
            .bind(chain_name)
            .bind(&token_config.symbol)
            .bind(&token_config.contract)
            .bind(token_config.decimals as i16)
            .bind(&token_config.logo_url)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| {
                let db_error = match &e {
                    sqlx::Error::Database(db_err) if db_err.is_unique_violation() => {
                        db_err.constraint().and_then(|constraint| match constraint {
                            "unique_token_per_chain" => Some(DbError::Sqlx(
                                DbQueryError::TokenSymbolAlreadyExists {
                                    symbol: token_config.symbol.to_string(),
                                    chain: chain_name.to_string(),
                                }
                            )),
                            "uq_token_contract_address" => Some(DbError::Sqlx(
                                DbQueryError::TokenContractConflict(token_config.contract.to_string())
                            )),
                            _ => None,
                        })
                    }

                    _ => None,
                };

                match db_error {
                    Some(e) => e,
                    None => DbError::from(e)
                }
            })?;

        let Some(row) = row_opt else {
            return Err(DbError::NotFound {
                entity: "Chain",
                id: chain_name.to_string(),
            })
        };

        Ok(Self::map_row_to_token(row))
    }
}