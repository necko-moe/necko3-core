use alloy_primitives::U256;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use sqlx::FromRow;
use strum::{AsRefStr, Display, EnumString};
use uuid::Uuid;

pub use necko3_types::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Payment {
    pub id: Uuid,
    pub invoice_id: Uuid,
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
    pub id: Uuid,
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
pub struct Webhook {
    pub id: Uuid,
    pub invoice_id: Uuid,
    pub url: String,
    pub payload: WebhookEvent,
    pub status: WebhookStatus,
    pub attempts: u32,
    pub max_retries: u32,
    pub next_retry: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct WebhookJob {
    pub id: Uuid,
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
        invoice_id: Uuid,
        tx_hash: String,
        amount: String,
        currency: String,
    },
    TxConfirmed {
        invoice_id: Uuid,
        tx_hash: String,
        confirmations: u64,
    },
    InvoicePaid {
        invoice_id: Uuid,
        paid_amount: String,
    },
    InvoiceExpired {
        invoice_id: Uuid,
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
    pub invoice_id: Option<Uuid>,
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
    pub invoice_id: Option<Uuid>,
    /// WebhookEvent::to_string()
    pub event_type: Option<String>,
    pub url: Option<String>,
    pub status: Option<WebhookStatus>,

    pub pagination: Pagination,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct ExpiredInvoiceInfo {
    pub id: Uuid,
    pub network: String,
    pub address: String,
}

impl ExpiredInvoiceInfo {
    pub fn new(id: Uuid, network: String, address: String) -> Self {
        Self { id, network, address }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizedPaymentInfo {
    pub is_fully_paid: bool,
    pub invoice_id: Uuid,
    pub paid_raw_before: U256,
    pub paid_raw_after: U256,
    pub old_invoice_status: InvoiceStatus,
    pub new_invoice_status: InvoiceStatus,
}