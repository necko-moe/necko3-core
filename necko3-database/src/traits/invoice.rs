use alloy_primitives::U256;
use crate::model::{ExpiredInvoiceInfo, InvoiceFilter, PaginatedVec};
use async_trait::async_trait;
use uuid::Uuid;
use necko3_types::{Invoice, InvoiceStatus};

#[async_trait]
pub trait InvoiceStore: Sync + Send {
    async fn get_invoices(&self, filter: InvoiceFilter) -> anyhow::Result<PaginatedVec<Invoice>>;
    async fn get_invoice(&self, invoice_id: Uuid) -> anyhow::Result<Option<Invoice>>;
    async fn add_invoice(&self, invoice: &Invoice) -> anyhow::Result<()>;

    async fn update_invoice_status(&self, invoice_id: Uuid, status: InvoiceStatus) -> anyhow::Result<()>;
    async fn get_invoice_by_address(&self, address: &str) -> anyhow::Result<Option<Invoice>>;
    async fn expire_old_invoices(&self) -> anyhow::Result<Vec<ExpiredInvoiceInfo>>;

    async fn update_invoice_paid(&self, invoice_id: Uuid, payment_id: Uuid, paid_raw: U256, new_status: Option<InvoiceStatus>) -> anyhow::Result<()>;

}