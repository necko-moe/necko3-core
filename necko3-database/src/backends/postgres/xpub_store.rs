use async_trait::async_trait;
use crate::backends::postgres::PostgresAdapter;
use crate::traits::XPubStore;

#[async_trait]
impl XPubStore for PostgresAdapter {
    async fn next_derivation_index(&self, xpub: &str) -> anyhow::Result<u64> {
        let index: i64 = sqlx::query_scalar(
            r#"INSERT INTO xpub_states (xpub, last_used_index)
                   VALUES ($1, 0)
                   ON CONFLICT (xpub)
                   DO UPDATE SET last_used_index = last_used_index + 1
                   RETURNING last_used_index;"#
        )
            .bind(xpub)
            .fetch_one(&self.pool)
            .await?;

        Ok(index as u64)
    }
}