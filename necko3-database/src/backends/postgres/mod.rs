pub mod chain_store;
pub mod token_store;
pub mod invoice_store;
pub mod payment_store;
pub mod webhook_store;
pub mod xpub_store;
pub mod indexed_blocks_store;
pub mod database_ext;

use crate::traits::*;
use alloy_primitives::utils::format_units;
use alloy_primitives::U256;
use async_trait::async_trait;
use serde_json::Value;
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{PgPool, Row};
use std::str::FromStr;
use uuid::Uuid;
use necko3_types::{ChainData, ChainType, Invoice, InvoiceStatus, Payment, PaymentStatus, TokenData, Webhook, WebhookEvent, WebhookStatus};

pub struct PostgresAdapter {
    pub(super) pool: PgPool
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

    pub(super) fn map_row_to_chain(
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

    pub(super) fn map_row_to_token(
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

    pub(super) fn map_row_to_invoice(
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

    pub(super) fn map_row_to_payment(
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

    pub(super) fn map_row_to_webhook(
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