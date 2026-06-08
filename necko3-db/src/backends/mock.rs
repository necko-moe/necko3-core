use crate::model::{ChainData, ExpiredInvoiceInfo, Invoice, InvoiceFilter, InvoiceStatus, PaginatedVec, PartialChainUpdate, Payment, PaymentFilter, PaymentStatus, TokenData, Webhook, WebhookFilter, WebhookJob, WebhookStatus};
use crate::traits::{ChainStore, DatabaseExt, InvoiceStore, PaymentStore, TokenStore, WebhookStore, XPubStore};
use alloy_primitives::utils::format_units;
use alloy_primitives::U256;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

pub struct MockDatabase {
    chains: RwLock<HashMap<String, ChainData>>,
    tokens: RwLock<HashMap<String, HashMap<String, TokenData>>>,

    invoices: DashMap<Uuid, Invoice>,
    payments: DashMap<Uuid, Payment>,
    webhooks: DashMap<Uuid, Webhook>,

    xpub_states: DashMap<String, AtomicU64>,
}

impl MockDatabase {
    pub fn new() -> Self {
        Self {
            chains: Default::default(),
            tokens: Default::default(),
            invoices: Default::default(),
            payments: Default::default(),
            webhooks: Default::default(),
            xpub_states: Default::default(),
        }
    }
}

#[async_trait]
impl ChainStore for MockDatabase {
    async fn get_chains(&self) -> anyhow::Result<Vec<ChainData>> {
        Ok(self.chains.read().values().cloned().collect())
    }

    async fn get_chain(&self, chain_name: &str) -> anyhow::Result<Option<ChainData>> {
        Ok(self.chains.read().get(chain_name).cloned())
    }

    async fn get_chain_by_id(&self, id: i32) -> anyhow::Result<Option<ChainData>> {
        Ok(self.chains.read().values()
            .find(|c| c.id == id)
            .cloned())
    }

    async fn add_chain(&self, chain_config: &ChainData) -> anyhow::Result<()> {
        self.chains.write()
            .insert(chain_config.id.to_string(), chain_config.clone());

        Ok(())
    }

    async fn remove_chain(&self, chain_name: &str) -> anyhow::Result<bool> {
        let deleted = self.chains.write()
            .remove(chain_name)
            .is_some();

        Ok(deleted)
    }

    async fn chain_exists(&self, chain_name: &str) -> anyhow::Result<bool> {
        Ok(self.chains.read().contains_key(chain_name))
    }

    async fn update_chain_partial(&self, chain_name: &str, chain_update: &PartialChainUpdate) -> anyhow::Result<()> {
        let mut guard = self.chains.write();

        let chain = guard
            .get_mut(chain_name)
            .ok_or_else(|| anyhow::anyhow!("Chain {} not found in DB", chain_name))?;

        chain.patch(chain_update);

        Ok(())
    }

    async fn update_chain_active(&self, chain_name: &str, active: bool) -> anyhow::Result<()> {
        if let Some(chain) = self.chains.write()
            .get_mut(chain_name)
        {
            chain.active = active;
        }

        Ok(())
    }

    async fn update_chain_block(&self, chain_name: &str, block_num: u64) -> anyhow::Result<()> {
        if let Some(chain) = self.chains.write()
            .get_mut(chain_name)
        {
            chain.last_processed_block = block_num;
        }

        Ok(())
    }
}

#[async_trait]
impl TokenStore for MockDatabase {
    async fn get_tokens(&self, chain_name: &str) -> anyhow::Result<Vec<TokenData>> {
        Ok(self.tokens.read().get(chain_name)
            .map(|c| c.values().cloned().collect())
            .unwrap_or_default())
    }

    async fn get_token(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<TokenData>> {
        Ok(self.tokens.read().get(chain_name)
            .and_then(|c|
                c.get(token_symbol).cloned()))
    }

    async fn get_token_by_id(&self, id: i32) -> anyhow::Result<Option<TokenData>> {
        Ok(self.tokens.read()
            .values()
            .flat_map(|c| c.values())
            .find(|t| t.id == id)
            .cloned())
    }

    async fn get_token_by_contract(&self, contract_address: &str) -> anyhow::Result<Option<TokenData>> {
        Ok(self.tokens.read()
            .values()
            .flat_map(|c| c.values())
            .find(|t| t.contract == contract_address)
            .cloned())
    }

    async fn get_tokens_with_symbol(&self, token_symbol: &str) -> anyhow::Result<Vec<TokenData>> {
        Ok(self.tokens.read()
            .values()
            .flat_map(|c| c.values())
            .filter(|t| t.symbol == token_symbol)
            .cloned()
            .collect())
    }

    async fn remove_token(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<bool> {
        let deleted = self.tokens.write()
            .get_mut(chain_name)
            .and_then(|c| c.remove(token_symbol))
            .is_some();

        Ok(deleted)
    }

    async fn add_token(&self, chain_name: &str, token_config: &TokenData) -> anyhow::Result<()> {
        self.tokens.write()
            .get_mut(chain_name)
            .ok_or_else(|| anyhow::anyhow!("Chain {} not found in DB", chain_name))?
            .insert(token_config.symbol.clone(), token_config.clone());

        Ok(())
    }

    async fn get_token_decimals(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<u8>> {
        if let Some(chain) = self.chains.read()
            .get(chain_name)
        {
            if chain.native_symbol == token_symbol {
                return Ok(Some(chain.decimals));
            }
        }

        let decimals = self.tokens.read()
            .get(chain_name)
            .and_then(|c| c.get(token_symbol))
            .map(|token| token.decimals);

        Ok(decimals)
    }
}

#[async_trait]
impl InvoiceStore for MockDatabase {
    async fn get_invoices(&self, filter: InvoiceFilter) -> anyhow::Result<PaginatedVec<Invoice>> {
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

    async fn get_invoice(&self, invoice_id: Uuid) -> anyhow::Result<Option<Invoice>> {
        Ok(self.invoices.get(&invoice_id)
            .map(|x| x.value().clone()))
    }

    async fn add_invoice(&self, invoice: &Invoice) -> anyhow::Result<()> {
        if self.invoices.contains_key(&invoice.id) {
            anyhow::bail!("Invoice {} already exists", invoice.id);
        }

        self.invoices.insert(invoice.id.clone(), invoice.clone());

        Ok(())
    }

    async fn update_invoice_status(&self, invoice_id: Uuid, status: InvoiceStatus) -> anyhow::Result<()> {
        if let Some(mut invoice) = self.invoices
            .get_mut(&invoice_id)
        {
            invoice.status = status;
        }

        Ok(())
    }

    async fn get_pending_invoice_by_address(&self, chain_name: &str, address: &str) -> anyhow::Result<Option<Invoice>> {
        Ok(self.invoices.iter()
            .find(|x| {
                let inv = x.value();

                inv.network == chain_name
                    && inv.address == address
                    && inv.status == InvoiceStatus::Pending
            })
            .map(|x| x.value().clone()))
    }

    async fn expire_old_invoices(&self) -> anyhow::Result<Vec<ExpiredInvoiceInfo>> {
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

    async fn update_invoice_paid(&self, invoice_id: Uuid, paid_raw: U256, new_status: Option<InvoiceStatus>) -> anyhow::Result<()> {
        if let Some(mut invoice) = self.invoices.get_mut(&invoice_id) {
            invoice.paid_raw = paid_raw;
            invoice.paid = format_units(paid_raw, invoice.decimals)?;

            if let Some(new_status) = new_status {
                invoice.status = new_status;
            }
        }

        Ok(())
    }

    async fn get_watch_addresses(&self, chain_name: &str) -> anyhow::Result<Vec<String>> {
        Ok(self.invoices.iter()
            .filter(|x| {
                let inv = x.value();

                inv.status == InvoiceStatus::Pending
                    && inv.network == chain_name
            })
            .map(|x| x.value().address.clone())
            .collect())
    }
}

#[async_trait]
impl PaymentStore for MockDatabase {
    async fn get_payments(&self, filter: PaymentFilter) -> anyhow::Result<PaginatedVec<Payment>> {
        let mut filtered: Vec<Payment> = self.payments.iter()
            .filter(|kv| {
                let pay = kv.value();

                filter.invoice_id.as_ref().map_or(true, |i| pay.invoice_id == *i)
                    && filter.from.as_ref().map_or(true, |f| pay.from == *f)
                    && filter.to.as_ref().map_or(true, |t| pay.to == *t)
                    && filter.network.as_ref().map_or(true, |n| pay.network == *n)
                    && filter.token.as_ref().map_or(true, |t| pay.token == *t)
                    && filter.block_number.as_ref().map_or(true, |b| pay.block_number == *b)
                    && filter.status.as_ref().map_or(true, |s| pay.status == *s)
            })
            .map(|x| x.value().clone())
            .collect();

        let total = filtered.len() as u64;

        filtered.sort_unstable_by(|a, b| b.created_at.cmp(&a.created_at));

        let payments: Vec<Payment> = filtered
            .into_iter()
            .skip(filter.pagination.offset as usize)
            .take(filter.pagination.limit as usize)
            .collect();

        Ok(PaginatedVec::new(
            payments,
            total,
            filter.pagination.offset,
            filter.pagination.limit,
        ))
    }

    async fn get_payment(&self, payment_id: Uuid) -> anyhow::Result<Option<Payment>> {
        Ok(self.payments.get(&payment_id)
            .map(|x| x.value().clone()))
    }

    async fn get_confirming_payments(&self) -> anyhow::Result<Vec<Payment>> {
        Ok(self.payments.iter()
            .filter(|payment|
                payment.value().status == PaymentStatus::Confirming)
            .map(|x| x.value().clone())
            .collect())
    }

    async fn upsert_payment(&self, payment: &Payment) -> anyhow::Result<bool> {
        if let Some(mut existing) = self.payments.get_mut(&payment.id) {
            existing.block_number = payment.block_number;
            return Ok(false)
        }

        self.payments.insert(payment.id.clone(), payment.clone());

        Ok(true)
    }

    async fn update_payment_status(&self, payment_id: Uuid, status: PaymentStatus) -> anyhow::Result<()> {
        if let Some(mut payment) = self.payments
            .get_mut(&payment_id)
        {
            payment.status = status;
        }

        Ok(())
    }

    async fn update_payment_block_number(&self, payment_id: Uuid, block_num: u64) -> anyhow::Result<()> {
        if let Some(mut payment) = self.payments
            .get_mut(&payment_id)
        {
            payment.block_number = block_num;
        }

        Ok(())
    }
}

#[async_trait]
impl WebhookStore for MockDatabase {
    async fn get_webhooks(&self, filter: WebhookFilter) -> anyhow::Result<PaginatedVec<Webhook>> {
        let mut filtered: Vec<Webhook> = self.webhooks.iter()
            .filter(|x| {
                let wh = x.value();

                filter.invoice_id.as_ref().map_or(true, |i| wh.invoice_id == *i)
                    && filter.event_type.as_ref().map_or(true, |e| wh.payload.to_string() == *e)
                    && filter.url.as_ref().map_or(true, |u| wh.url == *u)
                    && filter.status.as_ref().map_or(true, |s| wh.status == *s)
            })
            .map(|w| w.value().clone())
            .collect();

        let total = filtered.len() as u64;

        filtered.sort_unstable_by(|a, b| b.created_at.cmp(&a.created_at));

        let webhooks: Vec<Webhook> = filtered
            .into_iter()
            .skip(filter.pagination.offset as usize)
            .take(filter.pagination.limit as usize)
            .collect();

        Ok(PaginatedVec::new(
            webhooks,
            total,
            filter.pagination.offset,
            filter.pagination.limit,
        ))
    }

    async fn get_webhook(&self, webhook_id: Uuid) -> anyhow::Result<Option<Webhook>> {
        Ok(self.webhooks.get(&webhook_id).map(|x| x.value().clone()))
    }

    async fn add_webhook(&self, webhook: &Webhook) -> anyhow::Result<()> {
        self.webhooks.insert(webhook.id, webhook.clone());
        Ok(())
    }

    async fn select_pending_webhooks(&self, limit: usize) -> anyhow::Result<Vec<WebhookJob>> {
        let now = Utc::now();

        let target_ids: Vec<Uuid> = self.webhooks
            .iter()
            .filter(|r| r.status == WebhookStatus::Pending
                && r.next_retry <= now)
            .take(limit)
            .map(|r| r.key().clone())
            .collect();

        let mut jobs = Vec::with_capacity(target_ids.len());

        target_ids.iter().for_each(|id| {
            if let Some(mut job) = self.webhooks.get_mut(&id) {
                job.status = WebhookStatus::Processing;

                let secret = self.invoices
                    .get(&job.invoice_id)
                    .and_then(|inv| inv.webhook_secret.clone())
                    .unwrap_or_else(|| "default_secret".to_owned());

                jobs.push(WebhookJob {
                    id: job.id,
                    url: job.url.clone(),
                    secret_key: secret,
                    payload: sqlx::types::Json(job.payload.clone()),
                    max_retries: job.max_retries as i32,
                    attempts: job.attempts as i32,
                });
            }
        });

        Ok(jobs)
    }

    async fn update_webhook_status(&self, webhook_id: Uuid, status: WebhookStatus) -> anyhow::Result<()> {
        if let Some(mut job) = self.webhooks
            .get_mut(&webhook_id)
        {
            job.status = status;
        }

        Ok(())
    }

    async fn schedule_webhook_retry(&self, webhook_id: Uuid, attempts: i32, next_retry: DateTime<Utc>) -> anyhow::Result<()> {
        if let Some(mut job) = self.webhooks
            .get_mut(&webhook_id)
        {
            job.status = WebhookStatus::Pending;
            job.attempts = attempts as u32;
            job.next_retry = next_retry;
        }

        Ok(())
    }
}

#[async_trait]
impl XPubStore for MockDatabase {
    async fn next_derivation_index(&self, xpub: &str) -> anyhow::Result<u64> {
        if let Some(last_used_index) = self.xpub_states.get(xpub) {
            return Ok(last_used_index.value()
                .fetch_add(1, Ordering::SeqCst))
        }

        let entry = self.xpub_states
            .entry(xpub.to_string())
            .or_insert_with(|| AtomicU64::new(0));

        Ok(entry.value().fetch_add(1, Ordering::SeqCst))
    }
}

#[async_trait]
impl DatabaseExt for MockDatabase {}