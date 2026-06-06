use crate::model::{ExpiredInvoiceInfo, Invoice, InvoiceFilter, InvoiceStatus, PaginatedVec};
use async_trait::async_trait;

#[async_trait]
pub trait InvoiceStore: Sync + Send {
    async fn get_invoices(&self, filter: InvoiceFilter) -> anyhow::Result<PaginatedVec<Invoice>>;
    async fn get_invoice(&self, uuid: &str) -> anyhow::Result<Option<Invoice>>;
    async fn add_invoice(&self, invoice: &Invoice) -> anyhow::Result<()>;

    async fn update_invoice_status(&self, uuid: &str, status: InvoiceStatus) -> anyhow::Result<()>;
    async fn get_pending_invoice_by_address(&self, chain_name: &str, address: &str) -> anyhow::Result<Option<Invoice>>;
    async fn expire_old_invoices(&self) -> anyhow::Result<Vec<ExpiredInvoiceInfo>>;
}