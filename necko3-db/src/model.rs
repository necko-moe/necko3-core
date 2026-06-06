use alloy_primitives::{TxHash, U256};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use std::collections::HashSet;
use strum::{AsRefStr, Display, EnumString};

#[derive(Debug, Clone, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct TokenConfig {
    pub symbol: String,
    pub contract: String,
    pub decimals: u8,
    pub logo_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainConfig {
    pub name: String,
    pub active: bool,
    pub rpc_urls: Vec<String>,
    pub chain_type: ChainType,
    pub xpub: String,
    pub native_symbol: String,
    pub decimals: u8,
    pub last_processed_block: u64,
    pub block_lag: u8,
    pub required_confirmations: u64,
    pub logo_url: Option<String>,
    pub tokens: HashSet<TokenConfig>,
}

impl ChainConfig {
    pub fn patch(&mut self, update: &PartialChainUpdate) {
        if let Some(x) = &update.rpc_urls { self.rpc_urls = x.to_owned(); }
        if let Some(x) = update.last_processed_block { self.last_processed_block = x; }
        if let Some(x) = &update.xpub { self.xpub = x.to_owned(); }
        if let Some(x) = update.block_lag { self.block_lag = x; }
        if let Some(x) = update.required_confirmations { self.required_confirmations = x; }
        if let Some(x) = update.active { self.active = x; }
        if let Some(x) = &update.logo_url { self.logo_url = Some(x.to_owned()); }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Payment {
    pub id: String,
    pub invoice_id: String,
    pub from: String,
    pub to: String,
    pub network: String,
    pub token: String,
    pub tx_hash: String,
    pub amount_raw: U256,
    pub block_number: u64,
    pub log_index: u64,
    pub status: PaymentStatus,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize,
    Display, EnumString, AsRefStr)]
#[strum(serialize_all = "UPPERCASE")]
pub enum ChainType {
    EVM
}

#[derive(Debug, Clone)]
pub struct PaymentEvent {
    pub network: String,
    pub tx_hash: TxHash,
    pub from: String,
    pub to: String,
    pub token: String,
    pub amount: String,
    pub amount_raw: U256,
    pub decimals: u8,
    pub block_number: u64,
    pub log_index: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq,
    Display, EnumString, AsRefStr)]
#[strum(serialize_all = "PascalCase")]
pub enum InvoiceStatus {
    Pending,
    Paid,
    Expired,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq,
    Display, EnumString, AsRefStr)]
#[strum(serialize_all = "PascalCase")]
pub enum PaymentStatus {
    Confirming,
    Confirmed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Invoice {
    pub id: String,
    pub address_index: u32,
    pub address: String,
    pub amount: String,
    pub amount_raw: U256,
    pub paid: String,
    pub paid_raw: U256,
    pub token: String,
    pub network: String,
    pub decimals: u8,
    pub webhook_url: Option<String>,
    pub webhook_secret: Option<String>,
    pub webhook_max_retries: Option<u32>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub status: InvoiceStatus,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PartialChainUpdate {
    pub active: Option<bool>,
    pub rpc_urls: Option<Vec<String>>,
    pub last_processed_block: Option<u64>,
    pub xpub: Option<String>,
    pub block_lag: Option<u8>,
    pub required_confirmations: Option<u64>,
    pub logo_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Webhook {
    pub id: String,
    pub invoice_id: String,
    pub url: String,
    pub payload: WebhookEvent,
    pub status: WebhookStatus,
    pub attempts: u32,
    pub max_retries: u32,
    pub next_retry: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct WebhookJob {
    pub id: uuid::Uuid,
    pub url: String,
    pub secret_key: String,
    pub payload: Json<WebhookEvent>,
    pub attempts: i32,
    pub max_retries: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq,
    Display, EnumString, AsRefStr)]
#[serde(tag = "event_type", content = "data", rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum WebhookEvent {
    TxDetected {
        invoice_id: String,
        tx_hash: String,
        amount: String,
        currency: String,
    },
    TxConfirmed {
        invoice_id: String,
        tx_hash: String,
        confirmations: u64,
    },
    InvoicePaid {
        invoice_id: String,
        paid_amount: String,
    },
    InvoiceExpired {
        invoice_id: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq,
    Display, EnumString, AsRefStr)]
#[strum(serialize_all = "PascalCase")]
pub enum WebhookStatus {
    Pending,
    Processing,
    Sent,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
pub struct Pagination {
    pub limit: u32,
    pub offset: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PaginatedVec<T> {
    pub items: Vec<T>,
    pub total: u64,
    pub offset: u64,
    pub limit: u32,
}

impl<T> IntoIterator for PaginatedVec<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

impl<T> PaginatedVec<T> {
    pub fn new(items: Vec<T>, total: u64, offset: u64, limit: u32) -> Self {
        Self {
            items,
            total,
            offset,
            limit,
        }
    }
}

#[derive(Default, Clone, Debug, Deserialize, Serialize)]
pub struct InvoiceFilter {
    pub status: Option<InvoiceStatus>,
    pub address: Option<String>,
    pub network: Option<String>,
    pub token: Option<String>,

    pub pagination: Pagination,
}

#[derive(Default, Clone, Debug, Deserialize, Serialize)]
pub struct PaymentFilter {
    pub invoice_id: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub network: Option<String>,
    pub token: Option<String>,
    pub block_number: Option<u64>,
    pub status: Option<PaymentStatus>,

    pub pagination: Pagination,
}

#[derive(Default, Clone, Debug, Deserialize, Serialize)]
pub struct WebhookFilter {
    pub invoice_id: Option<String>,
    /// WebhookEvent::to_string()
    pub event_type: Option<String>,
    pub url: Option<String>,
    pub status: Option<WebhookStatus>,

    pub pagination: Pagination,
}

#[derive(Debug, Clone)]
pub struct ExpiredInvoiceInfo {
    pub id: String,
    pub network: String,
    pub address: String,
}