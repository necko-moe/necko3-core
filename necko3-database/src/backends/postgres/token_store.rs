use async_trait::async_trait;
use necko3_types::TokenData;
use crate::backends::postgres::PostgresAdapter;
use crate::traits::TokenStore;

#[async_trait]
impl TokenStore for PostgresAdapter {
    async fn get_tokens(&self, chain_name: &str) -> anyhow::Result<Vec<TokenData>> {
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

    async fn get_token(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<TokenData>> {
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

    async fn get_token_by_id(&self, id: i32) -> anyhow::Result<Option<TokenData>> {
        let row = sqlx::query(
            r#"SELECT * FROM tokens WHERE id = $1"#
        )
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(Self::map_row_to_token))
    }

    async fn get_token_by_contract(&self, contract_address: &str) -> anyhow::Result<Option<TokenData>> {
        let row = sqlx::query(
            r#"SELECT * FROM tokens WHERE contract_address = $1"#
        )
            .bind(contract_address)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(Self::map_row_to_token))
    }

    async fn get_tokens_with_symbol(&self, token_symbol: &str) -> anyhow::Result<Vec<TokenData>> {
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

    async fn remove_token(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<TokenData>> {
        let result = sqlx::query(
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

        Ok(result.map(Self::map_row_to_token))
    }

    async fn add_token(&self, chain_name: &str, token_config: &TokenData) -> anyhow::Result<()> {
        let result = sqlx::query(
            r#"INSERT INTO tokens
               (chain_id, symbol, contract_address, decimals, logo_url)
               SELECT id, $2, $3, $4, $5
               FROM chains
               WHERE name = $1"#
        )
            .bind(chain_name)
            .bind(&token_config.symbol)
            .bind(&token_config.contract)
            .bind(token_config.decimals as i16)
            .bind(&token_config.logo_url)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
            anyhow::bail!("Chain {} not found in DB", chain_name)
        }

        Ok(())
    }

    async fn get_token_decimals(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<u8>> {
        // native
        let native: Option<(i32, String, i16)> = sqlx::query_as(
            r#"SELECT id, chains.native_symbol, chains.decimals FROM chains WHERE name = $1"#
        )
            .bind(chain_name)
            .fetch_optional(&self.pool)
            .await?;

        let (chain_id, native_symbol, native_decimals) = match native {
            Some((ci, ns, nd)) => (ci, ns, nd as u8),
            None => { return Ok(None) }
        };

        if native_symbol == token_symbol {
            return Ok(Some(native_decimals));
        }

        // token
        let token_decimals: Option<i16> = sqlx::query_scalar(
            r#"SELECT tokens.decimals FROM tokens WHERE symbol = $1 AND chain_id = $2"#
        )
            .bind(token_symbol)
            .bind(chain_id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(token_decimals.map(|dec| dec as u8))
    }
}