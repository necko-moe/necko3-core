use std::collections::HashMap;
use crate::model::{ChainData, ExpiredInvoiceInfo, FinalizedPaymentInfo, Invoice, InvoiceFilter, InvoiceStatus, PaginatedVec, PartialChainUpdate, Payment, PaymentFilter, PaymentStatus, TokenData, Webhook, WebhookFilter, WebhookJob, WebhookStatus};
use crate::traits::{ChainStore, DatabaseExt, IndexedBlocksStore, InvoiceStore, PaymentStore, TokenStore, WebhookStore, XPubStore};
use alloy_primitives::{BlockNumber, U256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use uuid::Uuid;
use necko3_types::UpsertPayment;

pub struct NotifyingDb<D> {
    inner: D,
    tx: mpsc::Sender<DbEvent>
}

pub enum DbEvent {
    ChainAdded { chain_data: ChainData },
    ChainRemoved { chain_data: ChainData },
    ChainPartialUpdated { chain_name: String, partial_update: PartialChainUpdate },
    ChainActiveUpdated { chain_name: String, active: bool },
    ChainBlockUpdated { chain_name: String, block_number: u64 },
    ChainWatchAddressAdded { chain_name: String, address: String },
    ChainWatchAddressesRemoved { chain_name: String, addresses: Vec<String> },

    IndexedBlocksUpserted { chain_id: i32, blocks: Vec<(BlockNumber, String)> },
    
    TokenAdded { token_data: TokenData },
    TokenRemoved { token_data: TokenData },

    InvoiceAdded { invoice: Invoice },
    InvoiceStatusUpdated { invoice_id: Uuid, new_status: InvoiceStatus },
    OldInvoicesExpired { invoices_info: Vec<ExpiredInvoiceInfo> },
    InvoicePaymentApplied { invoice_id: Uuid, paid_raw_before: U256, paid_raw_after: U256,
        old_status: InvoiceStatus, new_status: InvoiceStatus },

    PaymentUpserted { payment_id: Uuid, payment: UpsertPayment, is_new_payment: bool },
    PaymentStatusUpdated { payment_id: Uuid, new_status: PaymentStatus },
    PaymentBlockUpdated { payment_id: Uuid, block_number: u64 },

    WebhookAdded { webhook: Webhook },
    PendingWebhooksSelected { webhook_jobs: Vec<WebhookJob>, prompted_limit: usize },
    WebhookStatusUpdated { webhook_id: Uuid, new_status: WebhookStatus },
    ScheduledNextWebhookRetry { webhook_id: Uuid, attempts_new: i32, next_retry: DateTime<Utc> },
}

impl<D> NotifyingDb<D> {
    pub fn new(inner: D, tx: mpsc::Sender<DbEvent>) -> Self {
        Self { inner, tx }
    }

    pub fn with_channel(inner: D, buffer_size: usize) -> (Self, mpsc::Receiver<DbEvent>) {
        let (tx, rx) = mpsc::channel(buffer_size);
        (Self::new(inner, tx), rx)
    }

    pub fn inner(&self) -> &D {
        &self.inner
    }
}

#[async_trait]
impl<D: DatabaseExt> ChainStore for NotifyingDb<D> {
    async fn get_chains(&self) -> anyhow::Result<Vec<ChainData>> {
        self.inner.get_chains().await
    }

    async fn get_chain(&self, chain_name: &str) -> anyhow::Result<Option<ChainData>> {
        self.inner.get_chain(chain_name).await
    }

    async fn get_chain_by_id(&self, id: i32) -> anyhow::Result<Option<ChainData>> {
        self.inner.get_chain_by_id(id).await
    }

    async fn add_chain(&self, chain_config: &ChainData) -> anyhow::Result<()> {
        self.inner.add_chain(chain_config).await?;

        let _ = self.tx.send(DbEvent::ChainAdded {
            chain_data: chain_config.clone(),
        }).await;

        Ok(())
    }

    async fn remove_chain(&self, chain_name: &str) -> anyhow::Result<Option<ChainData>> {
        let result = self.inner.remove_chain(chain_name).await?;

        if let Some(ref chain) = result {
            let _ = self.tx.send(DbEvent::ChainRemoved {
                chain_data: chain.clone(),
            }).await;
        }

        Ok(result)
    }

    async fn chain_exists(&self, chain_name: &str) -> anyhow::Result<bool> {
        self.inner.chain_exists(chain_name).await
    }

    async fn update_chain_partial(&self, chain_name: &str, chain_update: &PartialChainUpdate) -> anyhow::Result<()> {
        self.inner.update_chain_partial(chain_name, chain_update).await?;

        let _ = self.tx.send(DbEvent::ChainPartialUpdated {
            chain_name: chain_name.to_string(),
            partial_update: chain_update.clone()
        }).await;

        Ok(())
    }

    async fn update_chain_active(&self, chain_name: &str, active: bool) -> anyhow::Result<()> {
        self.inner.update_chain_active(chain_name, active).await?;

        let _ = self.tx.send(DbEvent::ChainActiveUpdated {
            chain_name: chain_name.to_string(),
            active
        }).await;

        Ok(())
    }

    async fn update_chain_block(&self, chain_name: &str, block_num: u64) -> anyhow::Result<()> {
        self.inner.update_chain_block(chain_name, block_num).await?;

        let _ = self.tx.send(DbEvent::ChainBlockUpdated {
            chain_name: chain_name.to_string(),
            block_number: block_num,
        }).await;

        Ok(())
    }

    async fn add_watch_address(&self, chain_name: &str, address: String) -> anyhow::Result<bool> {
        let added = self.inner.add_watch_address(chain_name, address.clone()).await?;

        let _ = self.tx.send(DbEvent::ChainWatchAddressAdded {
            chain_name: chain_name.to_string(),
            address
        }).await;

        Ok(added)
    }

    async fn remove_watch_address(&self, chain_name: &str, address: &str) -> anyhow::Result<bool> {
        let removed = self.inner.remove_watch_address(chain_name, address).await?;

        let _ = self.tx.send(DbEvent::ChainWatchAddressesRemoved {
            chain_name: chain_name.to_string(),
            addresses: vec![address.to_string()],
        }).await;

        Ok(removed)
    }

    async fn remove_watch_addresses(&self, chain_name: &str, addresses: &[String]) -> anyhow::Result<Vec<String>> {
        let removed = self.inner.remove_watch_addresses(chain_name, addresses).await?;

        let _ = self.tx.send(DbEvent::ChainWatchAddressesRemoved {
            chain_name: chain_name.to_string(),
            addresses: removed.clone(),
        }).await;

        Ok(removed)
    }
}

#[async_trait]
impl<D: DatabaseExt> TokenStore for NotifyingDb<D> {
    async fn get_tokens(&self, chain_name: &str) -> anyhow::Result<Vec<TokenData>> {
        self.inner.get_tokens(chain_name).await
    }

    async fn get_token(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<TokenData>> {
        self.inner.get_token(chain_name, token_symbol).await
    }

    async fn get_token_by_id(&self, id: i32) -> anyhow::Result<Option<TokenData>> {
        self.inner.get_token_by_id(id).await
    }

    async fn get_token_by_contract(&self, contract_address: &str) -> anyhow::Result<Option<TokenData>> {
        self.inner.get_token_by_contract(contract_address).await
    }

    async fn get_tokens_with_symbol(&self, token_symbol: &str) -> anyhow::Result<Vec<TokenData>> {
        self.inner.get_tokens_with_symbol(token_symbol).await
    }

    async fn remove_token(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<TokenData>> {
        let result = self.inner.remove_token(chain_name, token_symbol).await?;

        if let Some(ref token) = result {
            let _ = self.tx.send(DbEvent::TokenRemoved {
                token_data: token.clone(),
            }).await;
        }

        Ok(result)
    }

    async fn add_token(&self, chain_name: &str, token_config: &TokenData) -> anyhow::Result<()> {
        self.inner.add_token(chain_name, token_config).await?;

        let _ = self.tx.send(DbEvent::TokenAdded {
            token_data: token_config.clone(),
        }).await;

        Ok(())
    }

    async fn get_token_decimals(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<u8>> {
        self.inner.get_token_decimals(chain_name, token_symbol).await
    }
}

#[async_trait]
impl<D: DatabaseExt> InvoiceStore for NotifyingDb<D> {
    async fn get_invoices(&self, filter: InvoiceFilter) -> anyhow::Result<PaginatedVec<Invoice>> {
        self.inner.get_invoices(filter).await
    }

    async fn get_invoice(&self, invoice_id: Uuid) -> anyhow::Result<Option<Invoice>> {
        self.inner.get_invoice(invoice_id).await
    }

    async fn add_invoice(&self, invoice: &Invoice) -> anyhow::Result<()> {
        self.inner.add_invoice(invoice).await?;

        let _ = self.tx.send(DbEvent::InvoiceAdded {
            invoice: invoice.clone()
        }).await;

        Ok(())
    }

    async fn update_invoice_status(&self, invoice_id: Uuid, status: InvoiceStatus) -> anyhow::Result<()> {
        self.inner.update_invoice_status(invoice_id, status).await?;

        let _ = self.tx.send(DbEvent::InvoiceStatusUpdated {
            invoice_id,
            new_status: status,
        }).await;

        Ok(())
    }

    async fn get_invoice_by_address(&self, address: &str) -> anyhow::Result<Option<Invoice>> {
        self.inner.get_invoice_by_address(address).await
    }

    async fn expire_old_invoices(&self) -> anyhow::Result<Vec<ExpiredInvoiceInfo>> {
        let expired = self.inner.expire_old_invoices().await?;

        if !expired.is_empty() {
            let _ = self.tx.send(DbEvent::OldInvoicesExpired {
                invoices_info: expired.clone()
            }).await;
        }

        Ok(expired)
    }

    async fn update_invoice_paid(&self, invoice_id: Uuid, paid_raw: U256, new_status: Option<InvoiceStatus>) -> anyhow::Result<()> {
        let invoice_opt = self.inner.get_invoice(invoice_id).await?;
        let invoice = if let Some(invoice) = invoice_opt { invoice } else { return Ok(()) };

        self.inner.update_invoice_paid(invoice_id, paid_raw, new_status).await?;

        let _ = self.tx.send(DbEvent::InvoicePaymentApplied {
            invoice_id,
            paid_raw_before: invoice.paid_raw,
            paid_raw_after: paid_raw,
            old_status: invoice.status,
            new_status: new_status.unwrap_or(invoice.status),
        }).await;

        Ok(())
    }
}

#[async_trait]
impl<D: DatabaseExt> PaymentStore for NotifyingDb<D> {
    async fn get_payments(&self, filter: PaymentFilter) -> anyhow::Result<PaginatedVec<Payment>> {
        self.inner.get_payments(filter).await
    }

    async fn get_payment(&self, payment_id: Uuid) -> anyhow::Result<Option<Payment>> {
        self.inner.get_payment(payment_id).await
    }

    async fn get_confirming_payments(&self) -> anyhow::Result<Vec<Payment>> {
        self.inner.get_confirming_payments().await
    }

    async fn upsert_payment(&self, payment: &UpsertPayment) -> anyhow::Result<(Uuid, bool)> {
        let (id, inserted) = self.inner.upsert_payment(payment).await?;

        let _ = self.tx.send(DbEvent::PaymentUpserted {
            payment_id: id,
            payment: payment.clone(),
            is_new_payment: inserted,
        }).await;

        Ok((id, inserted))
    }

    async fn update_payment_status(&self, payment_id: Uuid, status: PaymentStatus) -> anyhow::Result<()> {
        self.inner.update_payment_status(payment_id, status).await?;

        let _ = self.tx.send(DbEvent::PaymentStatusUpdated {
            payment_id,
            new_status: status
        }).await;

        Ok(())
    }

    async fn update_payment_block_number(&self, payment_id: Uuid, block_num: u64) -> anyhow::Result<()> {
        self.inner.update_payment_block_number(payment_id, block_num).await?;

        let _ = self.tx.send(DbEvent::PaymentBlockUpdated {
            payment_id,
            block_number: block_num,
        }).await;

        Ok(())
    }
}

#[async_trait]
impl<D: DatabaseExt> WebhookStore for NotifyingDb<D> {
    async fn get_webhooks(&self, filter: WebhookFilter) -> anyhow::Result<PaginatedVec<Webhook>> {
        self.inner.get_webhooks(filter).await
    }

    async fn get_webhook(&self, webhook_id: Uuid) -> anyhow::Result<Option<Webhook>> {
        self.inner.get_webhook(webhook_id).await
    }

    async fn add_webhook(&self, webhook: &Webhook) -> anyhow::Result<()> {
        self.inner.add_webhook(webhook).await?;

        let _ = self.tx.send(DbEvent::WebhookAdded {
            webhook: webhook.clone()
        }).await;

        Ok(())
    }

    async fn select_pending_webhooks(&self, limit: usize) -> anyhow::Result<Vec<WebhookJob>> {
        let selected = self.inner.select_pending_webhooks(limit).await?;

        if !selected.is_empty() {
            let _ = self.tx.send(DbEvent::PendingWebhooksSelected {
                webhook_jobs: selected.clone(),
                prompted_limit: limit,
            }).await;
        }

        Ok(selected)
    }

    async fn update_webhook_status(&self, webhook_id: Uuid, status: WebhookStatus) -> anyhow::Result<()> {
        self.inner.update_webhook_status(webhook_id, status).await?;

        let _ = self.tx.send(DbEvent::WebhookStatusUpdated {
            webhook_id,
            new_status: status,
        }).await;

        Ok(())
    }

    async fn schedule_webhook_retry(&self, webhook_id: Uuid, attempts: i32, next_retry: DateTime<Utc>) -> anyhow::Result<()> {
        self.inner.schedule_webhook_retry(webhook_id, attempts, next_retry).await?;

        let _ = self.tx.send(DbEvent::ScheduledNextWebhookRetry {
            webhook_id,
            attempts_new: attempts,
            next_retry,
        }).await;

        Ok(())
    }
}

#[async_trait]
impl<D: DatabaseExt> XPubStore for NotifyingDb<D> {
    async fn next_derivation_index(&self, xpub: &str) -> anyhow::Result<u64> {
        self.inner.next_derivation_index(xpub).await
    }
}

#[async_trait]
impl<D: DatabaseExt> IndexedBlocksStore for NotifyingDb<D> {
    async fn get_latest_indexed_blocks(&self, chain_id: i32, limit: u16) -> anyhow::Result<HashMap<BlockNumber, String>> {
        self.inner.get_latest_indexed_blocks(chain_id, limit).await
    }

    async fn upsert_indexed_block(&self, chain_id: i32, block_number: u64, block_hash: String) -> anyhow::Result<()> {
        self.inner.upsert_indexed_block(chain_id, block_number, block_hash.clone()).await?;

        let _ = self.tx.send(DbEvent::IndexedBlocksUpserted {
            chain_id,
            blocks: vec![(block_number, block_hash)],
        }).await;

        Ok(())
    }

    async fn upsert_indexed_blocks_batch(&self, chain_id: i32, blocks: &[(BlockNumber, String)]) -> anyhow::Result<()> {
        self.inner.upsert_indexed_blocks_batch(chain_id, blocks).await?;

        let _ = self.tx.send(DbEvent::IndexedBlocksUpserted {
            chain_id,
            blocks: blocks.to_vec(),
        }).await;

        Ok(())
    }
}

#[async_trait]
impl<D: DatabaseExt> DatabaseExt for NotifyingDb<D> {
    async fn finalize_payment(&self, payment_id: Uuid) -> anyhow::Result<FinalizedPaymentInfo> {
        let info = self.inner.finalize_payment(payment_id).await?;

        let _ = self.tx.send(DbEvent::PaymentStatusUpdated {
            payment_id,
            new_status: PaymentStatus::Confirmed
        }).await;

        let _ = self.tx.send(DbEvent::InvoicePaymentApplied {
            invoice_id: info.invoice_id,
            paid_raw_before: info.paid_raw_before,
            paid_raw_after: info.paid_raw_after,
            old_status: info.old_invoice_status,
            new_status: info.new_invoice_status,
        }).await;

        Ok(info)
    }
}