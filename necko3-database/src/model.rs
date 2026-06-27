use alloy_primitives::U256;
use necko3_types::{InvoiceStatus, PaymentStatus, WebhookEvent, WebhookStatus};
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct WebhookJob {
    pub id: Uuid,
    pub url: String,
    pub secret_key: String,
    pub payload: Json<WebhookEvent>,
    pub attempts: i32,
    pub max_retries: i32,
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
    pub from: Option<String>,
    pub to: Option<String>,
    pub network: Option<String>,
    pub token: Option<String>,
    pub block_number: Option<u64>,
    pub block_hash: Option<String>,
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