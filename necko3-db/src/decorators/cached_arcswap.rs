use std::collections::HashMap;
use std::sync::Arc;
use alloy_primitives::U256;
use arc_swap::{ArcSwap, ArcSwapOption};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::model::{ChainConfig, ExpiredInvoiceInfo, Invoice, InvoiceFilter, InvoiceStatus, PaginatedVec, PartialChainUpdate, Payment, PaymentFilter, PaymentStatus, TokenConfig, Webhook, WebhookFilter, WebhookJob, WebhookStatus};
use crate::traits::{ChainStore, DatabaseExt, InvoiceStore, PaymentStore, TokenStore, WebhookStore, XPubStore};

pub struct CachedDb<D> {
    inner: Arc<D>,

    chains_cache: ArcSwapOption<HashMap<String, ChainConfig>>,
    tokens_cache: ArcSwap<TokenCacheState>,
}

#[derive(Default, Clone)]
struct TokenCacheState {
    by_chain: HashMap<String, Arc<HashMap<String, TokenConfig>>>,

    by_id: HashMap<i32, Arc<TokenConfig>>,

    by_symbol: HashMap<String, Vec<Arc<TokenConfig>>>,
}

impl<D> CachedDb<D> {
    pub fn new(inner: Arc<D>) -> Self {
        Self {
            inner,
            chains_cache: ArcSwapOption::empty(),
            tokens_cache: ArcSwap::default(),
        }
    }

    pub fn inner(&self) -> &Arc<D> {
        &self.inner
    }
}

impl<D: DatabaseExt> CachedDb<D> {
    async fn store_chains_cache(&self) -> anyhow::Result<Vec<ChainConfig>> {
        let chains = self.inner.get_chains().await?;

        let chains_cache: HashMap<String, ChainConfig> = chains
            .clone()
            .into_iter()
            .map(|c| (c.name.clone(), c))
            .collect();
        self.chains_cache.store(Some(Arc::new(chains_cache)));

        Ok(chains)
    }

    async fn store_tokens_cache(&self, chain_name: &str) -> anyhow::Result<Vec<TokenConfig>> {
        let tokens = self.inner.get_tokens(chain_name).await?;

        let tokens_map: HashMap<String, TokenConfig> = tokens
            .clone()
            .into_iter()
            .map(|c| (c.symbol.clone(), c))
            .collect();

        let tokens_map_arc = Arc::new(tokens_map);

        self.tokens_cache.rcu(|curr| {
            let mut new_state = (**curr).clone();

            if let Some(old_chain_tokens) = new_state.by_chain
                .insert(chain_name.to_string(), tokens_map_arc.clone())
            {
                for token in old_chain_tokens.values() {
                    new_state.by_id.remove(&token.id);

                    if let Some(sym_list) = new_state.by_symbol.get_mut(&token.symbol) {
                        sym_list.retain(|t| t.id != token.id);
                    }

                    if new_state.by_symbol.get(&token.symbol)
                        .map(|v| v.is_empty())
                        .unwrap_or(false)
                    {
                        new_state.by_symbol.remove(&token.symbol);
                    }
                }
            }

            for token in tokens.iter() {
                new_state.by_id.insert(token.id, Arc::new(token.clone()));

                new_state.by_symbol
                    .entry(token.symbol.clone())
                    .or_default()
                    .push(Arc::new(token.clone()));
            }

            Arc::new(new_state)
        });

        Ok(tokens)
    }

    async fn invalidate_tokens_cache(&self, chain_name: &str) -> anyhow::Result<()> {
        self.tokens_cache.rcu(|curr| {
            let mut new_state = (**curr).clone();

            if let Some(removed_chain_tokens) = new_state.by_chain.remove(chain_name) {
                for token in removed_chain_tokens.values() {
                    new_state.by_id.remove(&token.id);

                    if let Some(sym_list) = new_state.by_symbol.get_mut(&token.symbol) {
                        sym_list.retain(|t| t.id != token.id);
                    }

                    if new_state.by_symbol.get(&token.symbol)
                        .map(|v| v.is_empty())
                        .unwrap_or(false)
                    {
                        new_state.by_symbol.remove(&token.symbol);
                    }
                }
            }

            Arc::new(new_state)
        });

        Ok(())
    }
}

#[async_trait]
impl<D: DatabaseExt> ChainStore for CachedDb<D> {
    async fn get_chains(&self) -> anyhow::Result<Vec<ChainConfig>> {
        let guard = self.chains_cache.load();
        if let Some(ref chains_cache) = *guard {
            return Ok(chains_cache.values().cloned().collect());
        }

        self.store_chains_cache().await
    }

    async fn get_chain(&self, chain_name: &str) -> anyhow::Result<Option<ChainConfig>> {
        let guard = self.chains_cache.load();
        if let Some(ref chains_cache) = *guard {
            return Ok(chains_cache.get(chain_name).cloned());
        }

        let chains = self.store_chains_cache().await?;

        Ok(chains.into_iter().find(|c| c.name == chain_name))
    }

    async fn get_chain_by_id(&self, id: i32) -> anyhow::Result<Option<ChainConfig>> {
        let guard = self.chains_cache.load();
        if let Some(ref chains_cache) = *guard {
            return Ok(chains_cache.values()
                .find(|c| c.id == id)
                .cloned());
        }

        let chains = self.store_chains_cache().await?;

        Ok(chains.into_iter().find(|c| c.id == id))
    }

    async fn add_chain(&self, chain_config: &ChainConfig) -> anyhow::Result<()> {
        self.inner.add_chain(chain_config).await?;
        self.chains_cache.store(None);
        Ok(())
    }

    async fn remove_chain(&self, chain_name: &str) -> anyhow::Result<bool> {
        let removed = self.inner.remove_chain(chain_name).await?;

        if removed {
            self.chains_cache.store(None);
        }

        Ok(removed)
    }

    async fn chain_exists(&self, chain_name: &str) -> anyhow::Result<bool> {
        let guard = self.chains_cache.load();
        if let Some(ref chains_cache) = *guard {
            return Ok(chains_cache.contains_key(chain_name));
        }

        let chains = self.store_chains_cache().await?;

        Ok(chains.into_iter().any(|c| c.name == chain_name))
    }

    async fn update_chain_partial(&self, chain_name: &str, chain_update: &PartialChainUpdate) -> anyhow::Result<()> {
        self.inner.update_chain_partial(chain_name, chain_update).await?;
        self.chains_cache.store(None);
        Ok(())
    }

    async fn update_chain_active(&self, chain_name: &str, active: bool) -> anyhow::Result<()> {
        self.inner.update_chain_active(chain_name, active).await?;
        self.chains_cache.store(None);
        Ok(())
    }

    async fn update_chain_block(&self, chain_name: &str, block_num: u64) -> anyhow::Result<()> {
        self.inner.update_chain_block(chain_name, block_num).await?;
        self.chains_cache.store(None);
        Ok(())
    }
}

#[async_trait]
impl<D: DatabaseExt> TokenStore for CachedDb<D> {
    async fn get_tokens(&self, chain_name: &str) -> anyhow::Result<Vec<TokenConfig>> {
        let token_cache = self.tokens_cache.load();
        if let Some(tokens_map) = token_cache.by_chain.get(chain_name) {
            return Ok(tokens_map.values()
                .cloned()
                .collect::<Vec<_>>())
        }

        self.store_tokens_cache(chain_name).await
    }

    async fn get_token(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<TokenConfig>> {
        let tokens_cache = self.tokens_cache.load();
        if let Some(tokens_map) = tokens_cache.by_chain.get(chain_name) {
            return Ok(tokens_map.get(token_symbol).cloned());
        }

        let tokens = self.store_tokens_cache(chain_name).await?;

        Ok(tokens.into_iter().find(|t| t.symbol == token_symbol))
    }

    async fn get_token_by_id(&self, id: i32) -> anyhow::Result<Option<TokenConfig>> {
        let tokens_cache = self.tokens_cache.load();
        if let Some(tokens_cache) = tokens_cache.by_id.get(&id) {
            return Ok(Some((**tokens_cache).clone()))
        }

        self.inner.get_token_by_id(id).await
    }

    async fn get_token_by_contract(&self, chain_name: &str, contract_address: &str) -> anyhow::Result<Option<TokenConfig>> {
        let tokens_cache = self.tokens_cache.load();
        if let Some(tokens_map) = tokens_cache.by_chain.get(chain_name) {
            return Ok(tokens_map.values()
                .find(|t| t.contract == contract_address)
                .cloned())
        }

        let tokens = self.store_tokens_cache(chain_name).await?;

        Ok(tokens.into_iter().find(|t| t.contract == contract_address))
    }

    async fn get_tokens_with_symbol(&self, token_symbol: &str) -> anyhow::Result<Vec<TokenConfig>> {
        let tokens_cache = self.tokens_cache.load();
        if let Some(tokens_cache) = tokens_cache.by_symbol.get(token_symbol) {
            return Ok(tokens_cache.iter()
                .map(|v| (**v).clone())
                .collect());
        }

        self.inner.get_tokens_with_symbol(token_symbol).await
    }

    async fn remove_token(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<bool> {
        let removed = self.inner.remove_token(chain_name, token_symbol).await?;

        if removed {
            self.invalidate_tokens_cache(chain_name).await?;
        }

        Ok(removed)
    }

    async fn add_token(&self, chain_name: &str, token_config: &TokenConfig) -> anyhow::Result<()> {
        self.inner.add_token(chain_name, token_config).await?;
        self.invalidate_tokens_cache(chain_name).await?;
        Ok(())
    }

    async fn get_token_decimals(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<u8>> {
        let tokens_cache = self.tokens_cache.load();
        if let Some(tokens_map) = tokens_cache.by_chain.get(chain_name) {
            return Ok(tokens_map.get(token_symbol)
                .cloned()
                .map(|t| t.decimals))
        }

        let tokens = self.store_tokens_cache(chain_name).await?;

        Ok(tokens.into_iter()
            .find(|t| t.symbol == token_symbol)
            .map(|t| t.decimals))
    }
}

#[async_trait]
impl<D: DatabaseExt> InvoiceStore for CachedDb<D> {
    async fn get_invoices(&self, filter: InvoiceFilter) -> anyhow::Result<PaginatedVec<Invoice>> {
        self.inner.get_invoices(filter).await
    }

    async fn get_invoice(&self, invoice_id: Uuid) -> anyhow::Result<Option<Invoice>> {
        self.inner.get_invoice(invoice_id).await
    }

    async fn add_invoice(&self, invoice: &Invoice) -> anyhow::Result<()> {
        self.inner.add_invoice(invoice).await
    }

    async fn update_invoice_status(&self, invoice_id: Uuid, status: InvoiceStatus) -> anyhow::Result<()> {
        self.inner.update_invoice_status(invoice_id, status).await
    }

    async fn get_pending_invoice_by_address(&self, chain_name: &str, address: &str) -> anyhow::Result<Option<Invoice>> {
        self.inner.get_pending_invoice_by_address(chain_name, address).await
    }

    async fn expire_old_invoices(&self) -> anyhow::Result<Vec<ExpiredInvoiceInfo>> {
        self.inner.expire_old_invoices().await
    }

    async fn update_invoice_paid(&self, invoice_id: Uuid, paid_raw: U256, new_status: Option<InvoiceStatus>) -> anyhow::Result<()> {
        self.inner.update_invoice_paid(invoice_id, paid_raw, new_status).await
    }

    // maybe I should cache this too
    async fn get_watch_addresses(&self, chain_name: &str) -> anyhow::Result<Vec<String>> {
        self.inner.get_watch_addresses(chain_name).await
    }
}

#[async_trait]
impl<D: DatabaseExt> PaymentStore for CachedDb<D> {
    async fn get_payments(&self, filter: PaymentFilter) -> anyhow::Result<PaginatedVec<Payment>> {
        self.inner.get_payments(filter).await
    }

    async fn get_payment(&self, payment_id: Uuid) -> anyhow::Result<Option<Payment>> {
        self.inner.get_payment(payment_id).await
    }

    async fn get_confirming_payments(&self) -> anyhow::Result<Vec<Payment>> {
        self.inner.get_confirming_payments().await
    }

    async fn upsert_payment(&self, payment: &Payment) -> anyhow::Result<bool> {
        self.inner.upsert_payment(payment).await
    }

    async fn update_payment_status(&self, payment_id: Uuid, status: PaymentStatus) -> anyhow::Result<()> {
        self.inner.update_payment_status(payment_id, status).await
    }

    async fn update_payment_block_number(&self, payment_id: Uuid, block_num: u64) -> anyhow::Result<()> {
        self.inner.update_payment_block_number(payment_id, block_num).await
    }
}

#[async_trait]
impl<D: DatabaseExt> WebhookStore for CachedDb<D> {
    async fn get_webhooks(&self, filter: WebhookFilter) -> anyhow::Result<PaginatedVec<Webhook>> {
        self.inner.get_webhooks(filter).await
    }

    async fn get_webhook(&self, webhook_id: Uuid) -> anyhow::Result<Option<Webhook>> {
        self.inner.get_webhook(webhook_id).await
    }

    async fn add_webhook(&self, webhook: &Webhook) -> anyhow::Result<()> {
        self.inner.add_webhook(webhook).await
    }

    async fn select_pending_webhooks(&self, limit: usize) -> anyhow::Result<Vec<WebhookJob>> {
        self.inner.select_pending_webhooks(limit).await
    }

    async fn update_webhook_status(&self, webhook_id: Uuid, status: WebhookStatus) -> anyhow::Result<()> {
        self.inner.update_webhook_status(webhook_id, status).await
    }

    async fn schedule_webhook_retry(&self, webhook_id: Uuid, attempts: i32, next_retry: DateTime<Utc>) -> anyhow::Result<()> {
        self.inner.schedule_webhook_retry(webhook_id, attempts, next_retry).await
    }
}

#[async_trait]
impl<D: DatabaseExt> XPubStore for CachedDb<D> {
    async fn next_derivation_index(&self, xpub: &str) -> anyhow::Result<u64> {
        self.inner.next_derivation_index(xpub).await
    }
}

#[async_trait]
impl<D: DatabaseExt> DatabaseExt for CachedDb<D> {}