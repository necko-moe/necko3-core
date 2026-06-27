use alloy_primitives::U256;
use alloy_primitives::utils::format_units;
use async_trait::async_trait;
use chrono::Utc;
use uuid::Uuid;
use necko3_types::{Invoice, InvoiceStatus};
use crate::backends::in_memory::InMemoryAdapter;
use crate::error::{DbError, DbResult};
use crate::model::{ExpiredInvoiceInfo, InvoiceFilter, PaginatedVec};
use crate::traits::InvoiceStore;

#[async_trait]
impl InvoiceStore for InMemoryAdapter {
    async fn get_invoices(&self, filter: InvoiceFilter) -> DbResult<PaginatedVec<Invoice>> {
        let mut filtered: Vec<Invoice> = self.invoices.iter()
            .filter(|kv| {
                let inv = kv.value();

                filter.status.as_ref().map_or(true, |s| inv.status == *s)
                    && filter.address.as_ref().map_or(true, |a| inv.address == *a)
                    && filter.network.as_ref().map_or(true, |n| inv.network == *n)
                    && filter.token.as_ref().map_or(true, |t| inv.token == *t)
            })
            .map(|x| x.value().clone())
            .collect();

        let total = filtered.len() as u64;

        filtered.sort_unstable_by(|a, b| b.created_at.cmp(&a.created_at));

        let invoices: Vec<Invoice> = filtered
            .into_iter()
            .skip(filter.pagination.offset as usize)
            .take(filter.pagination.limit as usize)
            .collect();

        Ok(PaginatedVec::new(
            invoices,
            total,
            filter.pagination.offset,
            filter.pagination.limit,
        ))
    }

    async fn get_invoice(&self, invoice_id: Uuid) -> DbResult<Option<Invoice>> {
        Ok(self.invoices.get(&invoice_id)
            .map(|x| x.value().clone()))
    }

    async fn add_invoice(&self, invoice: &Invoice) -> DbResult<()> {
        self.invoices.insert(invoice.id.clone(), invoice.clone());

        Ok(())
    }

    async fn update_invoice_status(&self, invoice_id: Uuid, status: InvoiceStatus) -> DbResult<()> {
        if !self.invoices.contains_key(&invoice_id) {
            return Err(DbError::NotFound {
                entity: "Invoice",
                id: invoice_id.to_string(),
            })
        }

        if let Some(mut invoice) = self.invoices
            .get_mut(&invoice_id) {

            invoice.status = status;
        }

        Ok(())
    }

    async fn get_invoice_by_address(&self, address: &str) -> DbResult<Option<Invoice>> {
        Ok(self.invoices.iter()
            .find(|x| {
                let inv = x.value();

                inv.address == address
            })
            .map(|x| x.value().clone()))
    }

    async fn expire_old_invoices(&self) -> DbResult<Vec<ExpiredInvoiceInfo>> {
        let now = Utc::now();

        let expired_ids: Vec<Uuid> = self.invoices.iter()
            .filter(|x| {
                let inv = x.value();

                inv.status == InvoiceStatus::Pending && inv.expires_at <= now
            })
            .map(|entry| entry.key().clone())
            .collect();

        let mut expired: Vec<ExpiredInvoiceInfo> = Vec::with_capacity(expired_ids.len());

        expired_ids.iter().for_each(|id| {
            if let Some(mut kv) = self.invoices.get_mut(id) {
                let inv = kv.value_mut();

                inv.status = InvoiceStatus::Expired;
                expired.push(ExpiredInvoiceInfo::new(
                    inv.id.clone(), inv.network.clone(), inv.address.clone()))
            }
        });

        Ok(expired)
    }

    async fn update_invoice_paid(&self, invoice_id: Uuid, _payment_id: Uuid, paid_raw: U256, new_status: Option<InvoiceStatus>) -> DbResult<()> {
        if !self.invoices.contains_key(&invoice_id) {
            return Err(DbError::NotFound {
                entity: "Invoice",
                id: invoice_id.to_string(),
            })
        }

        if let Some(mut invoice) = self.invoices.get_mut(&invoice_id) {
            invoice.paid_raw = paid_raw;

            let paid_human = format_units(paid_raw, invoice.decimals)
                .map_err(|e| DbError::DataCorruption(
                    format!("format_units for '{}' ({} decimals) failed: {}", paid_raw, invoice.decimals, e)))?;
            invoice.paid = paid_human;

            if let Some(new_status) = new_status {
                invoice.status = new_status;
            }
        }

        Ok(())
    }
}