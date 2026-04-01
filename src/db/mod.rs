use crate::chain::Blockchain;
use crate::db::mock::MockDatabase;
use crate::db::postgres::Postgres;
use crate::model::{ChainConfig, Invoice, InvoiceFilter, InvoiceStatus, PaginatedVec,
                   PartialChainUpdate, Payment, PaymentFilter, TokenConfig, Webhook,
                   WebhookEvent, WebhookFilter, WebhookJob, WebhookStatus};
use alloy::primitives::U256;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

pub mod postgres;
pub mod mock;

pub trait DatabaseAdapter: Send + Sync {
    // chain
    fn get_chains_map(&self) -> impl Future<Output = anyhow::Result<HashMap<String, Arc<Blockchain>>>> + Send;
    fn get_chains(&self) -> impl Future<Output = anyhow::Result<Vec<Arc<Blockchain>>>> + Send;
    fn get_chain(&self, chain_name: &str) -> impl Future<Output = anyhow::Result<Option<Arc<Blockchain>>>> + Send;
    fn get_chain_by_id(&self, id: u32) -> impl Future<Output = anyhow::Result<Option<Arc<Blockchain>>>> + Send;
    fn add_chain(&self, chain_config: &ChainConfig) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn update_chain_block(&self, chain_name: &str, block_num: u64) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn get_latest_block(&self, chain_name: &str) -> impl Future<Output = anyhow::Result<Option<u64>>> + Send;
    fn get_chains_with_token(&self, token_symbol: &str) -> impl Future<Output = anyhow::Result<Vec<Arc<Blockchain>>>> + Send;
    fn remove_chain(&self, chain_name: &str) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn remove_chain_by_id(&self, id: u32) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn chain_exists(&self, chain_name: &str) -> impl Future<Output = anyhow::Result<bool>> + Send;
    fn update_chain_partial(&self, chain_name: &str, chain_update: &PartialChainUpdate)
        -> impl Future<Output = anyhow::Result<()>> + Send;

    fn get_watch_addresses(&self, chain_name: &str) -> impl Future<Output = anyhow::Result<Option<Vec<String>>>> + Send;
    fn remove_watch_address(&self, chain_name: &str, address: &str) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn remove_watch_addresses_bulk(&self, chain_name: &str, addresses: &[String])
        -> impl Future<Output = anyhow::Result<()>> + Send;
    fn add_watch_address(&self, chain_name: &str, address: &str) -> impl Future<Output = anyhow::Result<()>> + Send;

    fn get_xpub(&self, chain_name: &str) -> impl Future<Output = anyhow::Result<Option<String>>> + Send;
    fn get_rpc_urls(&self, chain_name: &str) -> impl Future<Output = anyhow::Result<Option<Vec<String>>>> + Send;
    fn get_block_lag(&self, chain_name: &str) -> impl Future<Output = anyhow::Result<Option<u8>>> + Send;
    fn get_required_confirmations(&self, chain_name: &str) -> impl Future<Output = anyhow::Result<Option<u64>>> + Send;

    fn set_chain_active(&self, chain_name: &str, active: bool) -> impl Future<Output = anyhow::Result<()>> + Send;

    // token
    fn get_tokens(&self, chain_name: &str) -> impl Future<Output = anyhow::Result<Option<Vec<TokenConfig>>>> + Send;
    fn get_token_contracts(&self, chain_name: &str) -> impl Future<Output = anyhow::Result<Option<Vec<String>>>> + Send;
    fn get_token(&self, chain_name: &str, token_symbol: &str)
        -> impl Future<Output = anyhow::Result<Option<TokenConfig>>> + Send;
    fn get_token_by_id(&self, chain_name: &str, id: u32)
        -> impl Future<Output = anyhow::Result<Option<TokenConfig>>> + Send;
    fn get_token_by_contract(&self, chain_name: &str, contract_address: &str)
        -> impl Future<Output = anyhow::Result<Option<TokenConfig>>> + Send;
    fn remove_token(&self, chain_name: &str, token_symbol: &str) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn remove_token_by_id(&self, chain_name: &str, id: u32) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn add_token(&self, chain_name: &str, token_config: &TokenConfig) -> impl Future<Output = anyhow::Result<()>> + Send;

    // invoice
    fn get_invoices(&self, filter: InvoiceFilter) -> impl Future<Output = anyhow::Result<PaginatedVec<Invoice>>> + Send;
    fn get_invoice(&self, uuid: &str) -> impl Future<Output = anyhow::Result<Option<Invoice>>> + Send;
    fn get_busy_indexes(&self, chain_name: &str) -> impl Future<Output = anyhow::Result<Vec<u32>>> + Send;
    fn add_invoice(&self, invoice: &Invoice) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn set_invoice_status(&self, uuid: &str, status: InvoiceStatus) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn get_pending_invoice_by_address(&self, chain_name: &str, address: &str)
        -> impl Future<Output = anyhow::Result<Option<Invoice>>> + Send;
    fn expire_old_invoices(&self)
        -> impl Future<Output = anyhow::Result<Vec<(String, String, String)>>> + Send; // (uuid, network, address)
    fn is_invoice_expired(&self, uuid: &str) -> impl Future<Output = anyhow::Result<Option<bool>>> + Send;
    fn is_invoice_paid(&self, uuid: &str) -> impl Future<Output = anyhow::Result<Option<bool>>> + Send;
    fn is_invoice_pending(&self, uuid: &str) -> impl Future<Output = anyhow::Result<Option<bool>>> + Send;
    fn cancel_invoice(&self, uuid: &str) -> impl Future<Output = anyhow::Result<()>> + Send;

    // payments
    fn add_payment_attempt(&self, invoice_id: &str, from: &str, to: &str, tx_hash: &str,
                           amount_raw: U256, block_number: u64, network: &str, token: &str, log_index: Option<u64>)
        -> impl Future<Output = anyhow::Result<()>> + Send;
    fn finalize_payment(&self, payment_id: &str) -> impl Future<Output = anyhow::Result<bool>> + Send;
    fn update_payment_block(&self, payment_id: &str, block_num: u64) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn cancel_payment(&self, payment_id: &str) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn get_payments(&self, filter: PaymentFilter) -> impl Future<Output = anyhow::Result<PaginatedVec<Payment>>> + Send;
    fn get_payment(&self, payment_id: &str) -> impl Future<Output = anyhow::Result<Option<Payment>>> + Send;
    fn get_confirming_payments(&self) -> impl Future<Output = anyhow::Result<Vec<Payment>>> + Send;

    // webhooks
    fn select_webhooks_job(&self) -> impl Future<Output = anyhow::Result<Vec<WebhookJob>>> + Send;
    fn set_webhook_status(&self, id: &str, status: WebhookStatus) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn schedule_webhook_retry(&self, id: &str, attempts: i32, next_retry_in_secs: f64)
        -> impl Future<Output = anyhow::Result<()>> + Send;
    fn add_webhook_job(&self, invoice_id: &str, event: &WebhookEvent) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn get_webhooks(&self, filter: WebhookFilter) -> impl Future<Output = anyhow::Result<PaginatedVec<Webhook>>> + Send;
    fn cancel_webhook(&self, webhook_id: &str) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn get_webhook(&self, webhook_id: &str) -> impl Future<Output = anyhow::Result<Option<Webhook>>> + Send;

    // other
    fn get_token_decimals(&self, chain_name: &str, token_symbol: &str) -> impl Future<Output = anyhow::Result<Option<u8>>> + Send;
}

pub enum Database {
    Mock(MockDatabase),
    Postgres(Postgres)
}

impl Database {
    pub async fn init(
        database_url: &str,
        max_connections: u32,
        db_type: &str
    ) -> anyhow::Result<Self> {
        match db_type {
            "postgres" => {
                let pool = PgPoolOptions::new()
                    .max_connections(max_connections)
                    .connect(&database_url)
                    .await?;

                sqlx::migrate!("./migrations/postgres")
                    .run(&pool)
                    .await?;

                Ok(Database::Postgres(Postgres::init(pool).await?))
            }
            "mock" => Ok(Database::Mock(MockDatabase::new())),
            _ => Err(anyhow::anyhow!("Unknown DB type"))
        }
    }
}

impl DatabaseAdapter for Database {

    async fn get_chains_map(&self) -> anyhow::Result<HashMap<String, Arc<Blockchain>>> {
        match self {
            Database::Mock(db) => db.get_chains_map().await,
            Database::Postgres(db) => db.get_chains_map().await,
        }
    }

    async fn get_chains(&self) -> anyhow::Result<Vec<Arc<Blockchain>>> {
        match self {
            Database::Mock(db) => db.get_chains().await,
            Database::Postgres(db) => db.get_chains().await,
        }
    }

    async fn get_chain(&self, chain_name: &str) -> anyhow::Result<Option<Arc<Blockchain>>> {
        match self {
            Database::Mock(db) => db.get_chain(chain_name).await,
            Database::Postgres(db) => db.get_chain(chain_name).await,
        }
    }

    async fn get_chain_by_id(&self, id: u32) -> anyhow::Result<Option<Arc<Blockchain>>> {
        match self {
            Database::Mock(db) => db.get_chain_by_id(id).await,
            Database::Postgres(db) => db.get_chain_by_id(id).await,
        }
    }

    async fn add_chain(&self, chain_config: &ChainConfig) -> anyhow::Result<()> {
        match self {
            Database::Mock(db) => db.add_chain(chain_config).await,
            Database::Postgres(db) => db.add_chain(chain_config).await,
        }
    }

    async fn update_chain_block(&self, chain_name: &str, block_num: u64) -> anyhow::Result<()> {
        match self {
            Database::Mock(db) => db.update_chain_block(chain_name, block_num).await,
            Database::Postgres(db) => db.update_chain_block(chain_name, block_num).await,
        }
    }

    async fn get_latest_block(&self, chain_name: &str) -> anyhow::Result<Option<u64>> {
        match self {
            Database::Mock(db) => db.get_latest_block(chain_name).await,
            Database::Postgres(db) => db.get_latest_block(chain_name).await,
        }
    }

    async fn get_chains_with_token(&self, token_symbol: &str) -> anyhow::Result<Vec<Arc<Blockchain>>> {
        match self {
            Database::Mock(db) => db.get_chains_with_token(token_symbol).await,
            Database::Postgres(db) => db.get_chains_with_token(token_symbol).await,
        }
    }

    async fn remove_chain(&self, chain_name: &str) -> anyhow::Result<()> {
        match self {
            Database::Mock(db) => db.remove_chain(chain_name).await,
            Database::Postgres(db) => db.remove_chain(chain_name).await,
        }
    }

    async fn remove_chain_by_id(&self, id: u32) -> anyhow::Result<()> {
        match self {
            Database::Mock(db) => db.remove_chain_by_id(id).await,
            Database::Postgres(db) => db.remove_chain_by_id(id).await,
        }
    }

    async fn chain_exists(&self, chain_name: &str) -> anyhow::Result<bool> {
        match self {
            Database::Mock(db) => db.chain_exists(chain_name).await,
            Database::Postgres(db) => db.chain_exists(chain_name).await,
        }
    }

    async fn update_chain_partial(&self, chain_name: &str, chain_update: &PartialChainUpdate) -> anyhow::Result<()> {
        match self {
            Database::Mock(db) => db.update_chain_partial(chain_name, chain_update).await,
            Database::Postgres(db) => db.update_chain_partial(chain_name, chain_update).await,
        }
    }

    async fn get_watch_addresses(&self, chain_name: &str) -> anyhow::Result<Option<Vec<String>>> {
        match self {
            Database::Mock(db) => db.get_watch_addresses(chain_name).await,
            Database::Postgres(db) => db.get_watch_addresses(chain_name).await,
        }
    }

    async fn remove_watch_address(&self, chain_name: &str, address: &str) -> anyhow::Result<()> {
        match self {
            Database::Mock(db) => db.remove_watch_address(chain_name, address).await,
            Database::Postgres(db) => db.remove_watch_address(chain_name, address).await,
        }
    }

    async fn remove_watch_addresses_bulk(&self, chain_name: &str, addresses: &[String]) -> anyhow::Result<()> {
        match self {
            Database::Mock(db) => db.remove_watch_addresses_bulk(chain_name, addresses).await,
            Database::Postgres(db) => db.remove_watch_addresses_bulk(chain_name, addresses).await,
        }
    }

    async fn add_watch_address(&self, chain_name: &str, address: &str) -> anyhow::Result<()> {
        match self {
            Database::Mock(db) => db.add_watch_address(chain_name, address).await,
            Database::Postgres(db) => db.add_watch_address(chain_name, address).await,
        }
    }

    async fn get_xpub(&self, chain_name: &str) -> anyhow::Result<Option<String>> {
        match self {
            Database::Mock(db) => db.get_xpub(chain_name).await,
            Database::Postgres(db) => db.get_xpub(chain_name).await,
        }
    }

    async fn get_rpc_urls(&self, chain_name: &str) -> anyhow::Result<Option<Vec<String>>> {
        match self {
            Database::Mock(db) => db.get_rpc_urls(chain_name).await,
            Database::Postgres(db) => db.get_rpc_urls(chain_name).await,
        }
    }

    async fn get_block_lag(&self, chain_name: &str) -> anyhow::Result<Option<u8>> {
        match self {
            Database::Mock(db) => db.get_block_lag(chain_name).await,
            Database::Postgres(db) => db.get_block_lag(chain_name).await,
        }
    }

    async fn get_required_confirmations(&self, chain_name: &str) -> anyhow::Result<Option<u64>> {
        match self {
            Database::Mock(db) => db.get_required_confirmations(chain_name).await,
            Database::Postgres(db) => db.get_required_confirmations(chain_name).await,
        }
    }

    async fn set_chain_active(&self, chain_name: &str, active: bool) -> anyhow::Result<()> {
        match self {
            Database::Mock(db) => db.set_chain_active(chain_name, active).await,
            Database::Postgres(db) => db.set_chain_active(chain_name, active).await,
        }
    }

    async fn get_tokens(&self, chain_name: &str) -> anyhow::Result<Option<Vec<TokenConfig>>> {
        match self {
            Database::Mock(db) => db.get_tokens(chain_name).await,
            Database::Postgres(db) => db.get_tokens(chain_name).await,
        }
    }

    async fn get_token_contracts(&self, chain_name: &str) -> anyhow::Result<Option<Vec<String>>> {
        match self {
            Database::Mock(db) => db.get_token_contracts(chain_name).await,
            Database::Postgres(db) => db.get_token_contracts(chain_name).await,
        }
    }

    async fn get_token(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<TokenConfig>> {
        match self {
            Database::Mock(db) => db.get_token(chain_name, token_symbol).await,
            Database::Postgres(db) => db.get_token(chain_name, token_symbol).await,
        }
    }

    async fn get_token_by_id(&self, chain_name: &str, id: u32) -> anyhow::Result<Option<TokenConfig>> {
        match self {
            Database::Mock(db) => db.get_token_by_id(chain_name, id).await,
            Database::Postgres(db) => db.get_token_by_id(chain_name, id).await,
        }
    }

    async fn get_token_by_contract(&self, chain_name: &str, contract_address: &str) -> anyhow::Result<Option<TokenConfig>> {
        match self {
            Database::Mock(db) => db.get_token_by_contract(chain_name, contract_address).await,
            Database::Postgres(db) => db.get_token_by_contract(chain_name, contract_address).await,
        }
    }

    async fn remove_token(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<()> {
        match self {
            Database::Mock(db) => db.remove_token(chain_name, token_symbol).await,
            Database::Postgres(db) => db.remove_token(chain_name, token_symbol).await,
        }
    }

    async fn remove_token_by_id(&self, chain_name: &str, id: u32) -> anyhow::Result<()> {
        match self {
            Database::Mock(db) => db.remove_token_by_id(chain_name, id).await,
            Database::Postgres(db) => db.remove_token_by_id(chain_name, id).await,
        }
    }

    async fn add_token(&self, chain_name: &str, token_config: &TokenConfig) -> anyhow::Result<()> {
        match self {
            Database::Mock(db) => db.add_token(chain_name, token_config).await,
            Database::Postgres(db) => db.add_token(chain_name, token_config).await,
        }
    }

    async fn get_invoices(&self, filter: InvoiceFilter) -> anyhow::Result<PaginatedVec<Invoice>> {
        match self {
            Database::Mock(db) => db.get_invoices(filter).await,
            Database::Postgres(db) => db.get_invoices(filter).await,
        }
    }

    async fn get_invoice(&self, uuid: &str) -> anyhow::Result<Option<Invoice>> {
        match self {
            Database::Mock(db) => db.get_invoice(uuid).await,
            Database::Postgres(db) => db.get_invoice(uuid).await,
        }
    }

    async fn get_busy_indexes(&self, chain_name: &str) -> anyhow::Result<Vec<u32>> {
        match self {
            Database::Mock(db) => db.get_busy_indexes(chain_name).await,
            Database::Postgres(db) => db.get_busy_indexes(chain_name).await,
        }
    }

    async fn add_invoice(&self, invoice: &Invoice) -> anyhow::Result<()> {
        match self {
            Database::Mock(db) => db.add_invoice(invoice).await,
            Database::Postgres(db) => db.add_invoice(invoice).await,
        }
    }

    async fn set_invoice_status(&self, uuid: &str, status: InvoiceStatus) -> anyhow::Result<()> {
        match self {
            Database::Mock(db) => db.set_invoice_status(uuid, status).await,
            Database::Postgres(db) => db.set_invoice_status(uuid, status).await,
        }
    }

    async fn get_pending_invoice_by_address(&self, chain_name: &str, address: &str) -> anyhow::Result<Option<Invoice>> {
        match self {
            Database::Mock(db) => db.get_pending_invoice_by_address(chain_name, address).await,
            Database::Postgres(db) => db.get_pending_invoice_by_address(chain_name, address).await,
        }
    }

    async fn expire_old_invoices(&self) -> anyhow::Result<Vec<(String, String, String)>> {
        match self {
            Database::Mock(db) => db.expire_old_invoices().await,
            Database::Postgres(db) => db.expire_old_invoices().await,
        }
    }

    async fn is_invoice_expired(&self, uuid: &str) -> anyhow::Result<Option<bool>> {
        match self {
            Database::Mock(db) => db.is_invoice_expired(uuid).await,
            Database::Postgres(db) => db.is_invoice_expired(uuid).await,
        }
    }

    async fn is_invoice_paid(&self, uuid: &str) -> anyhow::Result<Option<bool>> {
        match self {
            Database::Mock(db) => db.is_invoice_paid(uuid).await,
            Database::Postgres(db) => db.is_invoice_paid(uuid).await,
        }
    }

    async fn is_invoice_pending(&self, uuid: &str) -> anyhow::Result<Option<bool>> {
        match self {
            Database::Mock(db) => db.is_invoice_pending(uuid).await,
            Database::Postgres(db) => db.is_invoice_pending(uuid).await,
        }
    }

    async fn cancel_invoice(&self, uuid: &str) -> anyhow::Result<()> {
        match self {
            Database::Mock(db) => db.cancel_invoice(uuid).await,
            Database::Postgres(db) => db.cancel_invoice(uuid).await,
        }
    }

    async fn add_payment_attempt(&self, invoice_id: &str, from: &str, to: &str, tx_hash: &str,
                                 amount_raw: U256, block_number: u64, network: &str, token: &str,
                                 log_index: Option<u64>) -> anyhow::Result<()> {
        match self {
            Database::Mock(db) => db.add_payment_attempt(invoice_id, from, to, tx_hash,
                                                         amount_raw, block_number, network, token,
                                                         log_index).await,
            Database::Postgres(db) => db.add_payment_attempt(invoice_id, from, to, tx_hash,
                                                             amount_raw, block_number, network, token,
                                                             log_index).await,
        }
    }

    async fn finalize_payment(&self, payment_id: &str) -> anyhow::Result<bool> {
        match self {
            Database::Mock(db) => db.finalize_payment(payment_id).await,
            Database::Postgres(db) => db.finalize_payment(payment_id).await,
        }
    }

    async fn update_payment_block(&self, payment_id: &str, block_num: u64) -> anyhow::Result<()> {
        match self {
            Database::Mock(db) => db.update_payment_block(payment_id, block_num).await,
            Database::Postgres(db) => db.update_payment_block(payment_id, block_num).await,
        }
    }

    async fn cancel_payment(&self, payment_id: &str) -> anyhow::Result<()> {
        match self {
            Database::Mock(db) => db.cancel_payment(payment_id).await,
            Database::Postgres(db) => db.cancel_payment(payment_id).await,
        }
    }

    async fn get_payments(&self, filter: PaymentFilter) -> anyhow::Result<PaginatedVec<Payment>> {
        match self {
            Database::Mock(db) => db.get_payments(filter).await,
            Database::Postgres(db) => db.get_payments(filter).await,
        }
    }

    async fn get_payment(&self, payment_id: &str) -> anyhow::Result<Option<Payment>> {
        match self {
            Database::Mock(db) => db.get_payment(payment_id).await,
            Database::Postgres(db) => db.get_payment(payment_id).await,
        }
    }

    async fn get_confirming_payments(&self) -> anyhow::Result<Vec<Payment>> {
        match self {
            Database::Mock(db) => db.get_confirming_payments().await,
            Database::Postgres(db) => db.get_confirming_payments().await,
        }
    }

    async fn select_webhooks_job(&self) -> anyhow::Result<Vec<WebhookJob>> {
        match self {
            Database::Mock(db) => db.select_webhooks_job().await,
            Database::Postgres(db) => db.select_webhooks_job().await,
        }
    }

    async fn set_webhook_status(&self, id: &str, status: WebhookStatus) -> anyhow::Result<()> {
        match self {
            Database::Mock(db) => db.set_webhook_status(id, status).await,
            Database::Postgres(db) => db.set_webhook_status(id, status).await,
        }
    }

    async fn schedule_webhook_retry(&self, id: &str, attempts: i32, next_retry_in_secs: f64) -> anyhow::Result<()> {
        match self {
            Database::Mock(db) => db.schedule_webhook_retry(id, attempts, next_retry_in_secs).await,
            Database::Postgres(db) => db.schedule_webhook_retry(id, attempts, next_retry_in_secs).await,
        }
    }

    async fn add_webhook_job(&self, invoice_id: &str, event: &WebhookEvent) -> anyhow::Result<()> {
        match self {
            Database::Mock(db) => db.add_webhook_job(invoice_id, event).await,
            Database::Postgres(db) => db.add_webhook_job(invoice_id, event).await,
        }
    }

    async fn get_webhooks(&self, filter: WebhookFilter) -> anyhow::Result<PaginatedVec<Webhook>> {
        match self {
            Database::Mock(db) => db.get_webhooks(filter).await,
            Database::Postgres(db) => db.get_webhooks(filter).await,
        }
    }

    async fn cancel_webhook(&self, webhook_id: &str) -> anyhow::Result<()> {
        match self {
            Database::Mock(db) => db.cancel_webhook(webhook_id).await,
            Database::Postgres(db) => db.cancel_webhook(webhook_id).await,
        }
    }

    async fn get_webhook(&self, webhook_id: &str) -> anyhow::Result<Option<Webhook>> {
        match self {
            Database::Mock(db) => db.get_webhook(webhook_id).await,
            Database::Postgres(db) => db.get_webhook(webhook_id).await,
        }
    }

    async fn get_token_decimals(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<u8>> {
        match self {
            Database::Mock(db) => db.get_token_decimals(chain_name, token_symbol).await,
            Database::Postgres(db) => db.get_token_decimals(chain_name, token_symbol).await,
        }
    }
}