use alloy_primitives::U256;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crate::traits::*;
use dashmap::DashMap;
use sqlx::PgPool;
use crate::model::{ChainConfig, ExpiredInvoiceInfo, Invoice, InvoiceFilter, InvoiceStatus, PaginatedVec, PartialChainUpdate, Payment, PaymentFilter, PaymentStatus, TokenConfig, Webhook, WebhookFilter, WebhookJob, WebhookStatus};

pub struct PostgresDatabase {
    pool: PgPool,

    // no cache here
    token_decimals: DashMap<(String, String), u8>,
}

impl PostgresDatabase {
    pub async fn new(pool: PgPool) -> anyhow::Result<Self> {
        sqlx::query(
            "UPDATE webhooks SET status = 'Pending' WHERE status = 'Processing'"
        )
            .execute(&pool)
            .await?;

        Ok(Self {
            pool,
            token_decimals: DashMap::new(),
        })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl ChainStore for PostgresDatabase {
    async fn get_chains(&self) -> anyhow::Result<Vec<ChainConfig>> {
        todo!()
    }

    async fn get_chain(&self, chain_name: &str) -> anyhow::Result<Option<ChainConfig>> {
        todo!()
    }

    async fn get_chain_by_id(&self, id: i32) -> anyhow::Result<Option<ChainConfig>> {
        todo!()
    }

    async fn add_chain(&self, chain_config: &ChainConfig) -> anyhow::Result<()> {
        todo!()
    }

    async fn remove_chain(&self, chain_name: &str) -> anyhow::Result<()> {
        todo!()
    }

    async fn chain_exists(&self, chain_name: &str) -> anyhow::Result<bool> {
        todo!()
    }

    async fn update_chain_partial(&self, chain_name: &str, chain_update: &PartialChainUpdate) -> anyhow::Result<()> {
        todo!()
    }

    async fn set_chain_active(&self, chain_name: &str, active: bool) -> anyhow::Result<()> {
        todo!()
    }

    async fn update_chain_block(&self, chain_name: &str, block_num: u64) -> anyhow::Result<()> {
        todo!()
    }
}

#[async_trait]
impl TokenStore for PostgresDatabase {
    async fn get_tokens(&self, chain_name: &str) -> anyhow::Result<Option<Vec<TokenConfig>>> {
        todo!()
    }

    async fn get_token(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<TokenConfig>> {
        todo!()
    }

    async fn get_token_by_id(&self, id: i32) -> anyhow::Result<Option<TokenConfig>> {
        todo!()
    }

    async fn get_token_by_contract(&self, chain_name: &str, contract_address: &str) -> anyhow::Result<Option<TokenConfig>> {
        todo!()
    }

    async fn get_tokens_with_symbol(&self, token_symbol: &str) -> anyhow::Result<Vec<TokenConfig>> {
        todo!()
    }

    async fn remove_token(&self, chain_name: &str, id: u32) -> anyhow::Result<()> {
        todo!()
    }

    async fn add_token(&self, chain_name: &str, token_config: &TokenConfig) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_token_decimals(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<u8> {
        todo!()
    }
}

#[async_trait]
impl InvoiceStore for PostgresDatabase {
    async fn get_invoices(&self, filter: InvoiceFilter) -> anyhow::Result<PaginatedVec<Invoice>> {
        todo!()
    }

    async fn get_invoice(&self, uuid: &str) -> anyhow::Result<Option<Invoice>> {
        todo!()
    }

    async fn add_invoice(&self, invoice: &Invoice) -> anyhow::Result<()> {
        todo!()
    }

    async fn update_invoice_status(&self, uuid: &str, status: InvoiceStatus) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_pending_invoice_by_address(&self, chain_name: &str, address: &str) -> anyhow::Result<Option<Invoice>> {
        todo!()
    }

    async fn expire_old_invoices(&self) -> anyhow::Result<Vec<ExpiredInvoiceInfo>> {
        todo!()
    }

    async fn update_invoice_paid(&self, id: &str, paid_raw: U256, paid: &str, new_status: Option<InvoiceStatus>) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_watch_addresses(&self, chain_name: &str) -> anyhow::Result<Vec<String>> {
        todo!()
    }
}

#[async_trait]
impl PaymentStore for PostgresDatabase {
    async fn get_payments(&self, filter: PaymentFilter) -> anyhow::Result<PaginatedVec<Payment>> {
        todo!()
    }

    async fn get_payment(&self, payment_id: &str) -> anyhow::Result<Option<Payment>> {
        todo!()
    }

    async fn get_confirming_payments(&self) -> anyhow::Result<Vec<Payment>> {
        todo!()
    }

    async fn upsert_payment(&self, payment: &Payment) -> anyhow::Result<()> {
        todo!()
    }

    async fn update_payment_status(&self, payment_id: &str, status: PaymentStatus) -> anyhow::Result<()> {
        todo!()
    }

    async fn update_payment_block_number(&self, payment_id: &str, block_num: u64) -> anyhow::Result<()> {
        todo!()
    }
}

#[async_trait]
impl WebhookStore for PostgresDatabase {
    async fn get_webhooks(&self, filter: WebhookFilter) -> anyhow::Result<PaginatedVec<Webhook>> {
        todo!()
    }

    async fn get_webhook(&self, webhook_id: &str) -> anyhow::Result<Option<Webhook>> {
        todo!()
    }

    async fn add_webhook(&self, webhook: &Webhook) -> anyhow::Result<()> {
        todo!()
    }

    async fn select_pending_webhooks(&self, limit: usize) -> anyhow::Result<Vec<WebhookJob>> {
        todo!()
    }

    async fn update_webhook_status(&self, id: &str, status: WebhookStatus) -> anyhow::Result<()> {
        todo!()
    }

    async fn schedule_webhook_retry(&self, id: &str, attempts: i32, next_retry: DateTime<Utc>) -> anyhow::Result<()> {
        todo!()
    }
}

#[async_trait]
impl DatabaseExt for PostgresDatabase {}