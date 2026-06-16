use std::collections::{HashMap, HashSet};
use crate::model::{ChainData, ChainType, ExpiredInvoiceInfo, FinalizedPaymentInfo, Invoice, InvoiceFilter, InvoiceStatus, PaginatedVec, PartialChainUpdate, Payment, PaymentFilter, PaymentStatus, TokenData, Webhook, WebhookEvent, WebhookFilter, WebhookJob, WebhookStatus};
use crate::traits::*;
use alloy_primitives::utils::format_units;
use alloy_primitives::{BlockNumber, U256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::types::BigDecimal;
use sqlx::{PgPool, QueryBuilder, Row};
use std::str::FromStr;
use uuid::Uuid;
use necko3_types::UpsertPayment;

pub struct PostgresAdapter {
    pool: PgPool
}

#[async_trait]
impl DatabaseAdapter for PostgresAdapter {
    async fn new(database_url: &str, max_connections: u32) -> anyhow::Result<Self>
    where
        Self: Sized
    {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(database_url)
            .await?;

        sqlx::query(
            "UPDATE webhooks SET status = 'Pending' WHERE status = 'Processing'"
        )
            .execute(&pool)
            .await?;

        Ok(Self { pool })
    }
}

impl PostgresAdapter {
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    fn map_row_to_chain(
        row: PgRow,
    ) -> anyhow::Result<ChainData> {
        let chain_str: String = row.get("chain_type");
        let chain_type: ChainType = chain_str.parse()
            .map_err(|e| anyhow::anyhow!("Invalid chain type: {}", e))?;

        Ok(ChainData {
            id: row.get("id"),
            name: row.get("name"),
            active: row.get("active"),
            rpc_urls: row.get("rpc_urls"),
            chain_type,
            xpub: row.get("xpub"),
            native_symbol: row.get("native_symbol"),
            decimals: row.get::<i16, _>("decimals") as u8,
            last_processed_block: row.get::<i64, _>("last_processed_block") as u64,
            block_lag: row.get::<i16, _>("block_lag") as u8,
            safe_lag: row.get::<i16, _>("safe_lag") as u8,
            required_confirmations: row.get::<i64, _>("required_confirmations") as u64,
            logo_url: row.get("logo_url"),
            watch_addresses: row.get::<Vec<String>, _>("watch_addresses")
                .into_iter().collect(),
        })
    }

    fn map_row_to_token(
        row: PgRow,
    ) -> TokenData {
        TokenData {
            id: row.get("id"),
            chain_id: row.get("chain_id"),
            symbol: row.get("symbol"),
            contract: row.get("contract_address"),
            decimals: row.get::<i16, _>("decimals") as u8,
            logo_url: row.get("logo_url"),
        }
    }

    fn map_row_to_invoice(
        row: PgRow
    ) -> anyhow::Result<Invoice> {
        let status_str: String = row.get("status");
        let status: InvoiceStatus = status_str.parse()
            .map_err(|e| anyhow::anyhow!("Unknown invoice status '{}' from DB: {}", status_str, e))?;

        let amount_str: String = row.get("amount_raw");
        let paid_str: String = row.get("paid_raw");

        let amount_raw = U256::from_str(&amount_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse amount_raw: {}", e))?;
        let paid_raw = U256::from_str(&paid_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse paid_raw: {}", e))?;

        let network: String = row.get("network");
        let token: String = row.get("token");

        let decimals = row.get::<i16, _>("decimals") as u8;

        let amount_human = format_units(amount_raw, decimals)?;
        let paid_human = format_units(paid_raw, decimals)?;

        Ok(Invoice {
            id: row.get::<Uuid, _>("id"),
            address: row.get("address"),
            address_index: row.get::<i32, _>("address_index") as u32,
            network,
            token,
            amount_raw,
            paid_raw,
            amount: amount_human,
            paid: paid_human,
            status,
            decimals,
            webhook_url: row.get("webhook_url"),
            webhook_secret: row.get("webhook_secret"),
            webhook_max_retries: row.get::<Option<i32>, _>("webhook_max_retries")
                .map(|x| x as u32),
            created_at: row.get("created_at"),
            expires_at: row.get("expires_at"),
        })
    }

    fn map_row_to_payment(
        row: PgRow
    ) -> anyhow::Result<Payment> {
        let status_str: String = row.get("status");
        let status: PaymentStatus = status_str.parse()
            .map_err(|e| anyhow::anyhow!("Unknown payment status '{}' from DB: {}", status_str, e))?;

        let amount_str: String = row.get("amount_raw");
        let amount_raw = U256::from_str(&amount_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse amount_raw: {}", e))?;

        Ok(Payment {
            id: row.get::<Uuid, _>("id"),
            from: row.get("from"),
            to: row.get("to"),
            network: row.get("network"),
            token: row.get("token"),
            tx_hash: row.get("tx_hash"),
            amount_raw,
            block_number: row.get::<i64, _>("block_number") as u64,
            block_hash: row.get("block_hash"),
            status,
            created_at: row.get("created_at"),
            log_index: row.get::<Option<i64>, _>("log_index").map(|x| x as u64),
        })
    }

    fn map_row_to_webhook(
        row: PgRow
    ) -> anyhow::Result<Webhook> {
        let status_str: String = row.get("status");
        let status: WebhookStatus = status_str.parse()
            .map_err(|e| anyhow::anyhow!("Unknown webhook status '{}' from DB: {}", status_str, e))?;

        let db_payload: Value = row.get("payload");

        let payload_enum: WebhookEvent = serde_json::from_value(db_payload)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize webhook payload: {}", e))?;

        Ok(Webhook {
            id: row.get::<Uuid, _>("id"),
            invoice_id: row.get::<Uuid, _>("invoice_id"),
            url: row.get("url"),
            payload: payload_enum,
            status,
            attempts: row.get::<i32, _>("attempts") as u32,
            max_retries: row.get::<i32, _>("max_retries") as u32,
            next_retry: row.get("next_retry"),
            created_at: row.get("created_at"),
        })
    }
}

#[async_trait]
impl ChainStore for PostgresAdapter {
    async fn get_chains(&self) -> anyhow::Result<Vec<ChainData>> {
        let rows = sqlx::query(
            r#"SELECT * FROM chains"#
        )
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter()
            .map(Self::map_row_to_chain)
            .collect()
    }

    async fn get_chain(&self, chain_name: &str) -> anyhow::Result<Option<ChainData>> {
        let row = sqlx::query(
            r#"SELECT * FROM chains WHERE name = $1"#
        )
            .bind(chain_name)
            .fetch_optional(&self.pool)
            .await?;

        row.map(Self::map_row_to_chain).transpose()
    }

    async fn get_chain_by_id(&self, id: i32) -> anyhow::Result<Option<ChainData>> {
        let row = sqlx::query(
            r#"SELECT * FROM chains WHERE id = $1"#
        )
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        row.map(Self::map_row_to_chain).transpose()
    }

    async fn add_chain(&self, chain_config: &ChainData) -> anyhow::Result<()> {
        sqlx::query(
            r#"INSERT INTO chains
                    (name, rpc_urls, chain_type, xpub, native_symbol, decimals,
                     last_processed_block, block_lag, required_confirmations, active, logo_url,
                     watch_addresses, safe_lag)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)"#,
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
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn remove_chain(&self, chain_name: &str) -> anyhow::Result<Option<ChainData>> {
        let result = sqlx::query(
            "DELETE FROM chains WHERE name = $1 RETURNING *"
        )
            .bind(chain_name)
            .fetch_optional(&self.pool)
            .await?;

        result.map(Self::map_row_to_chain).transpose()
    }

    async fn chain_exists(&self, chain_name: &str) -> anyhow::Result<bool> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM chains WHERE name = $1)"
        )
            .bind(chain_name)
            .fetch_one(&self.pool)
            .await?;

        Ok(exists)
    }

    async fn update_chain_partial(&self, chain_name: &str, chain_update: &PartialChainUpdate) -> anyhow::Result<()> {
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
            return Err(anyhow::anyhow!("Chain {} not found in DB", chain_name));
        }

        Ok(())
    }

    async fn update_chain_active(&self, chain_name: &str, active: bool) -> anyhow::Result<()> {
        let result = sqlx::query(
            "UPDATE chains SET active = $1 WHERE name = $2"
        )
            .bind(active)
            .bind(chain_name)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
            return Err(anyhow::anyhow!("Chain {} not found in DB", chain_name));
        }

        Ok(())
    }

    async fn update_chain_block(&self, chain_name: &str, block_num: u64) -> anyhow::Result<()> {
        let result = sqlx::query(
            "UPDATE chains SET last_processed_block = $1 WHERE name = $2"
        )
            .bind(block_num as i64)
            .bind(chain_name)
            .execute(&self.pool)
            .await?;

        if result.rows_affected() == 0 {
            return Err(anyhow::anyhow!("Chain {} not found in DB", chain_name));
        }

        Ok(())
    }

    async fn add_watch_address(&self, chain_name: &str, address: String) -> anyhow::Result<bool> {
        let result = sqlx::query(
            r#"UPDATE chains SET watch_addresses = ARRAY_APPEND(watch_addresses, $1)
                   WHERE name = $2 AND NOT ($1 = ANY(watch_addresses))"#
        )
            .bind(address)
            .bind(chain_name)
            .execute(&self.pool)
            .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn remove_watch_address(&self, chain_name: &str, address: &str) -> anyhow::Result<bool> {
        let result = sqlx::query(
            r#"UPDATE chains SET watch_addresses = ARRAY_REMOVE(watch_addresses, $1)
                   WHERE name = $2 AND ($1 = ANY(watch_addresses))"#
        )
            .bind(address)
            .bind(chain_name)
            .execute(&self.pool)
            .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn remove_watch_addresses(&self, chain_name: &str, addresses: &[String]) -> anyhow::Result<Vec<String>> {
        let to_remove: HashSet<&str> = addresses.iter()
            .map(|s| s.as_str())
            .collect();

        let mut tx = self.pool.begin().await?;

        let old_addresses: Vec<String> = sqlx::query_scalar(
            "SELECT chains.watch_addresses FROM chains WHERE name = $1 FOR UPDATE"
        )
            .bind(chain_name)
            .fetch_one(&mut *tx)
            .await?;

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

#[async_trait]
impl InvoiceStore for PostgresAdapter {
    async fn get_invoices(&self, filter: InvoiceFilter) -> anyhow::Result<PaginatedVec<Invoice>> {
        fn apply_filters(
            builder: &mut QueryBuilder<sqlx::Postgres>,
            filter: &InvoiceFilter
        ) {
            if let Some(ref status) = filter.status {
                builder.push(" AND status = ");
                builder.push_bind(status.to_string());
            }

            if let Some(ref address) = filter.address {
                builder.push(" AND address = ");
                builder.push_bind(address);
            }

            if let Some(ref network) = filter.network {
                builder.push(" AND network = ");
                builder.push_bind(network);
            }

            if let Some(ref token) = filter.token {
                builder.push(" AND token = ");
                builder.push_bind(token);
            }
        }

        let mut count_builder: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
            "SELECT count(*) FROM invoices WHERE TRUE");

        apply_filters(&mut count_builder, &filter);

        let total: i64 = count_builder
            .build_query_as::<(i64,)>()
            .fetch_one(&self.pool)
            .await?
            .0;

        let mut data_builder: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
            r#"SELECT
                    id, address, address_index, network, token, amount_raw::TEXT, paid_raw::TEXT,
                    status, decimals, webhook_url, webhook_secret, webhook_max_retries, created_at, expires_at
                FROM invoices WHERE TRUE"# // WHERE TRUE so that arguments don't have to think that they can be first
        );

        apply_filters(&mut data_builder, &filter);

        data_builder.push(" ORDER BY created_at DESC ");
        data_builder.push(" LIMIT ");
        data_builder.push_bind(filter.pagination.limit as i64);
        data_builder.push(" OFFSET ");
        data_builder.push_bind(filter.pagination.offset as i64);

        let rows = data_builder
            .build()
            .fetch_all(&self.pool)
            .await?;

        let invoices: Vec<Invoice> = rows
            .into_iter()
            .map(Self::map_row_to_invoice)
            .collect::<anyhow::Result<_>>()?;

        Ok(PaginatedVec::new(
            invoices,
            total as u64,
            filter.pagination.offset,
            filter.pagination.limit,
        ))
    }

    async fn get_invoice(&self, invoice_id: Uuid) -> anyhow::Result<Option<Invoice>> {
        sqlx::query(
            r#"SELECT
                       id, address, address_index, network, token, amount_raw::TEXT, paid_raw::TEXT,
                       status, decimals, webhook_url, webhook_secret, webhook_max_retries, created_at, expires_at
                   FROM invoices WHERE id = $1"#
        )
            .bind(invoice_id)
            .fetch_optional(&self.pool)
            .await?
            .map(Self::map_row_to_invoice)
            .transpose()
    }

    async fn add_invoice(&self, invoice: &Invoice) -> anyhow::Result<()> {
        let amount_bd = BigDecimal::from_str(&invoice.amount_raw.to_string())?;
        let paid_bd = BigDecimal::from_str(&invoice.paid_raw.to_string())?;

        sqlx::query(
            r#"INSERT INTO invoices
                   (id, address, address_index, network, token, amount_raw, paid_raw, status,
                    created_at, expires_at, decimals, webhook_url, webhook_secret, webhook_max_retries)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)"#
        )
            .bind(invoice.id)
            .bind(&invoice.address)
            .bind(invoice.address_index as i32)
            .bind(&invoice.network)
            .bind(&invoice.token)
            .bind(&amount_bd)
            .bind(&paid_bd)
            .bind(invoice.status.to_string())
            .bind(invoice.created_at)
            .bind(invoice.expires_at)
            .bind(invoice.decimals as i16)
            .bind(&invoice.webhook_url)
            .bind(&invoice.webhook_secret)
            .bind(invoice.webhook_max_retries.map(|x| x as i32))
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn update_invoice_status(&self, invoice_id: Uuid, status: InvoiceStatus) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE invoices SET status = $1 WHERE id = $2"
        )
            .bind(status.to_string())
            .bind(invoice_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn get_invoice_by_address(&self, address: &str) -> anyhow::Result<Option<Invoice>> {
        let row = sqlx::query(
            r#"SELECT
                       id, address, address_index, network, token, amount_raw::TEXT, paid_raw::TEXT,
                       status, decimals, created_at, expires_at, webhook_url, webhook_secret, webhook_max_retries
                   FROM invoices WHERE address = $1"#
        )
            .bind(address)
            .fetch_optional(&self.pool)
            .await?;

        row.map(Self::map_row_to_invoice).transpose()
    }

    async fn expire_old_invoices(&self) -> anyhow::Result<Vec<ExpiredInvoiceInfo>> {
        let rows = sqlx::query_as::<_, ExpiredInvoiceInfo>(
            r#"UPDATE invoices
                   SET status = 'Expired'
                   WHERE status = 'Pending' AND expires_at <= now()
                   RETURNING id, network, address"#
        )
            .fetch_all(&self.pool)
            .await?;

        Ok(rows)
    }

    async fn update_invoice_paid(&self, invoice_id: Uuid, paid_raw: U256, new_status: Option<InvoiceStatus>) -> anyhow::Result<()> {
        let paid_bd = BigDecimal::from_str(&paid_raw.to_string())?;

        sqlx::query(
            r#"UPDATE invoices SET paid_raw = $1,
                    status = COALESCE($2, status)
                WHERE id = $3"#
        )
            .bind(paid_bd)
            .bind(new_status.map(|x| x.to_string()))
            .bind(invoice_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

#[async_trait]
impl PaymentStore for PostgresAdapter {
    async fn get_payments(&self, filter: PaymentFilter) -> anyhow::Result<PaginatedVec<Payment>> {
        fn apply_filters(
            builder: &mut QueryBuilder<sqlx::Postgres>,
            filter: &PaymentFilter
        ) -> anyhow::Result<()> {
            if let Some(ref from) = filter.from {
                builder.push(r#" AND "from" = "#);
                builder.push_bind(from);
            }

            if let Some(ref to) = filter.to {
                builder.push(r#" AND "to" = "#);
                builder.push_bind(to);
            }

            if let Some(ref network) = filter.network {
                builder.push(" AND network = ");
                builder.push_bind(network);
            }

            if let Some(ref token) = filter.token {
                builder.push(" AND token = ");
                builder.push_bind(token);
            }

            if let Some(ref block_number) = filter.block_number {
                builder.push(" AND block_number = ");
                builder.push_bind(*block_number as i64);
            }

            if let Some(ref block_hash) = filter.block_hash {
                builder.push(" AND block_hash = ");
                builder.push_bind(block_hash.to_string());
            }

            if let Some(ref status) = filter.status {
                builder.push(" AND status = ");
                builder.push_bind(status.to_string());
            }

            Ok(())
        }

        let mut count_builder: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
            "SELECT count(*) FROM payments WHERE TRUE");

        apply_filters(&mut count_builder, &filter)?;

        let total: i64 = count_builder
            .build_query_as::<(i64,)>()
            .fetch_one(&self.pool)
            .await?
            .0;

        let mut data_builder: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
            r#"SELECT
                    id, "from", "to", network, tx_hash, token, amount_raw::TEXT,
                    block_number, block_hash::TEXT, status, created_at, log_index
                FROM payments WHERE TRUE"#
        );

        apply_filters(&mut data_builder, &filter)?;

        data_builder.push(" ORDER BY created_at DESC LIMIT ");
        data_builder.push_bind(filter.pagination.limit as i64);
        data_builder.push(" OFFSET ");
        data_builder.push_bind(filter.pagination.offset as i64);

        let rows = data_builder
            .build()
            .fetch_all(&self.pool)
            .await?;

        let payments: Vec<Payment> = rows
            .into_iter()
            .map(Self::map_row_to_payment)
            .collect::<anyhow::Result<_>>()?;

        Ok(PaginatedVec::new(
            payments,
            total as u64,
            filter.pagination.offset,
            filter.pagination.limit,
        ))
    }

    async fn get_payment(&self, payment_id: Uuid) -> anyhow::Result<Option<Payment>> {
        sqlx::query(
            r#"SELECT id, "from", "to", network, tx_hash, token, amount_raw::TEXT,
                       block_number, block_hash::TEXT, status, created_at, log_index
                   FROM payments WHERE id = $1"#
        )
            .bind(payment_id)
            .fetch_optional(&self.pool)
            .await?
            .map(Self::map_row_to_payment)
            .transpose()
    }

    async fn get_confirming_payments(&self) -> anyhow::Result<Vec<Payment>> {
        let rows = sqlx::query(
            r#"SELECT id, "from", "to", network, tx_hash, token, amount_raw::TEXT,
                       block_number, block_hash::TEXT, status, created_at, log_index
                   FROM payments WHERE status = $1"#)
            .bind(PaymentStatus::Confirming.as_ref())
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter().map(Self::map_row_to_payment).collect()
    }

    async fn upsert_payment(&self, payment: &UpsertPayment) -> anyhow::Result<(Uuid, bool)> {
        let amount_bd = BigDecimal::from_str(&payment.amount_raw.to_string())?;

        let row = sqlx::query(
            r#"INSERT INTO payments ("from", "to", network, tx_hash, amount_raw,
                      block_number, block_hash, status, log_index, token)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, 'Confirming', $9, $10)
                   ON CONFLICT (tx_hash, log_index, network)
                   DO UPDATE SET block_number = excluded.block_number
                   RETURNING id, (xmax = 0) AS inserted"#
        )
            .bind(&payment.from)
            .bind(&payment.to)
            .bind(&payment.network)
            .bind(&payment.tx_hash)
            .bind(amount_bd)
            .bind(payment.block_number as i64)
            .bind(payment.block_hash.to_string())
            .bind(payment.log_index.map(|index| index as i64))
            .bind(&payment.token)
            .fetch_one(&self.pool)
            .await?;

        let id: Uuid = row.get("id");
        let inserted: bool = row.get("inserted");

        Ok((id, inserted))
    }

    async fn update_payment_status(&self, payment_id: Uuid, status: PaymentStatus) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE payments SET status = $1 WHERE id = $2"
        )
            .bind(status.to_string())
            .bind(payment_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn update_payment_block_number(&self, payment_id: Uuid, block_num: u64) -> anyhow::Result<()> {
        sqlx::query("UPDATE payments SET block_number = $1 WHERE id = $2")
            .bind(block_num as i64)
            .bind(payment_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

#[async_trait]
impl WebhookStore for PostgresAdapter {
    async fn get_webhooks(&self, filter: WebhookFilter) -> anyhow::Result<PaginatedVec<Webhook>> {
        fn apply_filters(
            builder: &mut QueryBuilder<sqlx::Postgres>,
            filter: &WebhookFilter
        ) -> anyhow::Result<()> {
            if let Some(ref invoice_id) = filter.invoice_id {
                builder.push(" AND invoice_id = ");
                builder.push_bind(invoice_id);
            }

            if let Some(ref event_type) = filter.event_type {
                builder.push(" AND event_type = ");
                builder.push_bind(event_type);
            }

            if let Some(ref url) = filter.url {
                builder.push(" AND url = ");
                builder.push_bind(url);
            }

            if let Some(ref status) = filter.status {
                builder.push(" AND status = ");
                builder.push_bind(status.to_string());
            }

            Ok(())
        }

        let mut count_builder: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
            "SELECT count(*) FROM webhooks WHERE TRUE");

        apply_filters(&mut count_builder, &filter)?;

        let total: i64 = count_builder
            .build_query_as::<(i64,)>()
            .fetch_one(&self.pool)
            .await?
            .0;

        let mut data_builder: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
            r#"SELECT * FROM webhooks WHERE TRUE"#);

        apply_filters(&mut data_builder, &filter)?;

        data_builder.push(" ORDER BY created_at DESC LIMIT ");
        data_builder.push_bind(filter.pagination.limit as i64);
        data_builder.push(" OFFSET ");
        data_builder.push_bind(filter.pagination.offset as i64);

        let rows = data_builder
            .build()
            .fetch_all(&self.pool)
            .await?;

        let webhooks: Vec<Webhook> = rows
            .into_iter()
            .map(Self::map_row_to_webhook)
            .collect::<anyhow::Result<_>>()?;

        Ok(PaginatedVec::new(
            webhooks,
            total as u64,
            filter.pagination.offset,
            filter.pagination.limit,
        ))
    }

    async fn get_webhook(&self, webhook_id: Uuid) -> anyhow::Result<Option<Webhook>> {
        sqlx::query(
            r#"SELECT * FROM webhooks WHERE id = $1"#
        )
            .bind(webhook_id)
            .fetch_optional(&self.pool)
            .await?
            .map(Self::map_row_to_webhook)
            .transpose()
    }

    async fn add_webhook(&self, webhook: &Webhook) -> anyhow::Result<()> {
        let event_type = webhook.payload.as_ref();
        let payload = serde_json::to_value(&webhook.payload)?;

        sqlx::query(
            r#"INSERT INTO webhooks (id, invoice_id, event_type, url, payload, max_retries, status)
                       VALUES ($1, $2, $3, $4, $5, $6, $7)"#
        )
            .bind(webhook.id)
            .bind(webhook.invoice_id)
            .bind(event_type)
            .bind(&webhook.url)
            .bind(payload)
            .bind(webhook.max_retries as i32)
            .bind(webhook.status.to_string())
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn select_pending_webhooks(&self, limit: usize) -> anyhow::Result<Vec<WebhookJob>> {
        let jobs = sqlx::query_as::<_, WebhookJob>(
            r#"UPDATE webhooks w
                   SET status = 'Processing'
                   FROM invoices i
                   WHERE w.invoice_id = i.id
                       AND w.id IN (
                           SELECT id FROM webhooks
                           WHERE status = 'Pending' AND next_retry <= NOW()
                           LIMIT $1
                           FOR UPDATE SKIP LOCKED
                       )
                   RETURNING w.id, w.url, w.payload, w.max_retries, w.attempts,
                       COALESCE(i.webhook_secret, 'default_secret') as secret_key"#
        )
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?;

        Ok(jobs)
    }

    async fn update_webhook_status(&self, webhook_id: Uuid, status: WebhookStatus) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE webhooks SET status = $1 WHERE id = $2"
        )
            .bind(status.to_string())
            .bind(webhook_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn schedule_webhook_retry(&self, webhook_id: Uuid, attempts: i32, next_retry: DateTime<Utc>) -> anyhow::Result<()> {
        sqlx::query(
            r#"UPDATE webhooks SET status = 'Pending', attempts = $1,
                       next_retry = $2 WHERE id = $3"#
        )
            .bind(attempts)
            .bind(next_retry)
            .bind(webhook_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

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

#[async_trait]
impl DatabaseExt for PostgresAdapter {
    async fn finalize_payment(&self, payment_id: Uuid) -> anyhow::Result<FinalizedPaymentInfo> {
        let mut tx = self.pool.begin().await?;

        let row = sqlx::query(
            r#"UPDATE payments SET status = 'Confirmed' WHERE id = $1
                                         RETURNING "to", amount_raw::TEXT"#
        )
            .bind(payment_id)
            .fetch_one(&mut *tx)
            .await?;

        let to_address: String = row.get("to");
        let pay_amount_bd = BigDecimal::from_str(&row.get::<String, _>("amount_raw"))?;

        let inv = sqlx::query(
            r#"UPDATE invoices SET paid_raw = paid_raw + $1 WHERE address = $2
                   RETURNING (paid_raw - $1)::TEXT as old_paid_raw,
                       paid_raw::TEXT as new_paid_raw,
                       amount_raw::TEXT,
                       status,
                       id"# // could've used OLD.paid_raw and NEW.paid_raw but i wouldn't (sorry pg18)
        )
            .bind(pay_amount_bd)
            .bind(to_address)
            .fetch_one(&mut *tx)
            .await?;

        let inv_paid_before = U256::from_str(&inv.get::<String, _>("old_paid_raw"))
            .map_err(|e| anyhow::anyhow!("Failed to parse old_paid_raw: {}", e))?;
        let inv_paid_after = U256::from_str(&inv.get::<String, _>("new_paid_raw"))
            .map_err(|e| anyhow::anyhow!("Failed to parse new_paid_raw: {}", e))?;
        let inv_amount = U256::from_str(&inv.get::<String, _>("amount_raw"))
            .map_err(|e| anyhow::anyhow!("Failed to parse amount_raw: {}", e))?;

        let old_status_str: String = inv.get("status");
        let old_status: InvoiceStatus = old_status_str.parse()
            .map_err(|e| anyhow::anyhow!("Unknown invoice status '{}' from DB: {}", old_status_str, e))?;

        let inv_id: Uuid = inv.get("id");

        let is_fully_paid = inv_paid_after >= inv_amount;
        let new_status = if is_fully_paid {
            sqlx::query("UPDATE invoices SET status = 'Paid' WHERE id = $1")
                .bind(inv_id)
                .execute(&mut *tx)
                .await?;

            InvoiceStatus::Paid
        } else { old_status };

        tx.commit().await?;

        Ok(FinalizedPaymentInfo {
            is_fully_paid,
            invoice_id: inv_id,
            paid_raw_before: inv_paid_before,
            paid_raw_after: inv_paid_after,
            old_invoice_status: old_status,
            new_invoice_status: new_status,
        })
    }
}