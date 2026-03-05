use crate::chain::{Blockchain, BlockchainAdapter};
use crate::db::DatabaseAdapter;
use crate::model::{ChainConfig, Invoice, InvoiceFilter, InvoiceStatus, PaginatedVec, PartialChainUpdate, Payment, PaymentFilter, PaymentStatus, TokenConfig, Webhook, WebhookEvent, WebhookFilter, WebhookJob, WebhookStatus};
use alloy::primitives::utils::format_units;
use alloy::primitives::U256;
use chrono::Utc;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use uuid::Uuid;

pub struct MockDatabase {
    chains: RwLock<HashMap<String, Arc<Blockchain>>>, // key = chain name
    invoices: DashMap<String, Invoice>, // key = id/uuid
    token_decimals: RwLock<HashMap<String, HashMap<String, u8>>>, // (chain_name, (token_symbol, decimals))
    payments: DashMap<String, Payment>, // key = invoice_id
    webhooks: DashMap<String, Webhook>, // key = id/uuid
}

impl MockDatabase {
    pub fn new() -> Self {
        Self {
            chains: RwLock::new(HashMap::new()),
            invoices: DashMap::new(),
            token_decimals: RwLock::new(HashMap::new()),
            payments: DashMap::new(),
            webhooks: DashMap::new(),
        }
    }
}

impl DatabaseAdapter for MockDatabase {


    async fn get_chains_map(&self) -> anyhow::Result<HashMap<String, Arc<Blockchain>>> {
        Ok(self.chains.read().unwrap().clone())
    }

    async fn get_chains(&self) -> anyhow::Result<Vec<Arc<Blockchain>>> {
        Ok(self.chains.read().unwrap().values().cloned().collect())
    }

    async fn get_chain(&self, chain_name: &str) -> anyhow::Result<Option<Arc<Blockchain>>> {
        Ok(self.chains.read().unwrap().get(chain_name).cloned())
    }

    async fn get_chain_by_id(&self, _id: u32) -> anyhow::Result<Option<Arc<Blockchain>>> {
        unimplemented!("mock database does not have ids")
    }

    async fn add_chain(&self, chain_config: &ChainConfig) -> anyhow::Result<()> {
        match self.chains.read().unwrap().contains_key(&chain_config.name) {
            true => anyhow::bail!("chain with name {} already exists", chain_config.name),
            false => {},
        };

        let blockchain = Blockchain::new(chain_config.clone())?;

        self.chains.write().unwrap()
            .insert(chain_config.name.clone(), Arc::new(blockchain));

        Ok(())
    }

    async fn update_chain_block(&self, chain_name: &str, block_num: u64) -> anyhow::Result<()> {
        match self.chains.read().unwrap().get(chain_name) {
            Some(c) => c.config().write().unwrap()
                .last_processed_block = block_num,
            None => anyhow::bail!("chain '{}' does not exist", chain_name),
        }

        Ok(())
    }

    async fn get_latest_block(&self, chain_name: &str) -> anyhow::Result<Option<u64>> {
        Ok(self.chains.read().unwrap().get(chain_name)
            .map(|c| c.config().read().unwrap().last_processed_block))
    }

    async fn get_chains_with_token(&self, token_symbol: &str) -> anyhow::Result<Vec<Arc<Blockchain>>> {
        let guard = self.chains.read().unwrap();

        let result = guard.values()
            .filter(|c| {
                if c.config().read().unwrap()
                    .native_symbol == token_symbol { return true; }
                c.config().read().unwrap()
                    .tokens.read().unwrap().iter()
                    .any(|c| c.symbol == token_symbol)
            })
            .cloned()
            .collect();

        Ok(result)
    }

    async fn remove_chain(&self, chain_name: &str) -> anyhow::Result<()> {
        self.chains.write().unwrap().remove(chain_name);
        Ok(())
    }

    async fn remove_chain_by_id(&self, _id: u32) -> anyhow::Result<()> {
        unimplemented!("mock database does not have ids")
    }

    async fn chain_exists(&self, chain_name: &str) -> anyhow::Result<bool> {
        Ok(self.chains.read().unwrap().contains_key(chain_name))
    }

    async fn update_chain_partial(&self, chain_name: &str, chain_update: &PartialChainUpdate) -> anyhow::Result<()> {
        let mut guard = self.chains.write().unwrap();
        let blockchain = guard.get(chain_name)
            .ok_or_else(|| anyhow::anyhow!("chain '{}' does not exist", chain_name))?;

        let config_lock = blockchain.config();
        let mut chain_config = config_lock.read().unwrap().clone();

        if let Some(xpub) = &chain_update.xpub {
            chain_config.xpub = xpub.to_owned();
        }

        if let Some(rpc_urls) = &chain_update.rpc_urls {
            chain_config.rpc_urls = rpc_urls.clone();
        }

        if let Some(last_processed_block) = chain_update.last_processed_block {
            chain_config.last_processed_block = last_processed_block;
        }

        if let Some(block_lag) = chain_update.block_lag {
            chain_config.block_lag = block_lag;
        }

        if let Some(required_confirmations) = chain_update.required_confirmations {
            chain_config.required_confirmations = required_confirmations;
        }

        let new_blockchain = Arc::new(Blockchain::new(chain_config)?);

        guard.insert(chain_name.to_owned(), new_blockchain);

        Ok(())
    }

    async fn get_watch_addresses(&self, chain_name: &str) -> anyhow::Result<Option<Vec<String>>> {
        Ok(self.chains.read().unwrap().get(chain_name)
            .map(|c| c.config().read().unwrap()
                .watch_addresses.read().unwrap().iter()
                .cloned()
                .collect()))
    }

    async fn remove_watch_address(&self, chain_name: &str, address: &str) -> anyhow::Result<()> {
        match self.chains.read().unwrap().get(chain_name) {
            Some(c) => {
                c.config().read().unwrap()
                    .watch_addresses.write().unwrap().remove(address);
            }
            None => anyhow::bail!("chain '{}' does not exist", chain_name),
        }

        Ok(())
    }

    async fn remove_watch_addresses_bulk(
        &self,
        chain_name: &str,
        addresses: &[String]
    ) -> anyhow::Result<()> {
        match self.chains.read().unwrap().get(chain_name) {
            Some(c) => {
                let config_lock = c.config();
                let guard = config_lock.read().unwrap();
                let mut watch_addresses = guard.watch_addresses.write().unwrap();

                for addr in addresses {
                    watch_addresses.remove::<String>(addr);
                }
            }
            None => anyhow::bail!("chain '{}' does not exist", chain_name)
        }

        Ok(())
    }

    async fn add_watch_address(&self, chain_name: &str, address: &str) -> anyhow::Result<()> {
        match self.chains.read().unwrap().get(chain_name) {
            Some(c) => {
                c.config().read().unwrap()
                    .watch_addresses.write().unwrap().insert(address.to_owned());
            }
            None => anyhow::bail!("chain '{}' does not exist", chain_name),
        }

        Ok(())
    }

    async fn get_xpub(&self, chain_name: &str) -> anyhow::Result<Option<String>> {
        Ok(self.chains.read().unwrap().get(chain_name)
            .map(|c| c.config().read().unwrap().xpub.clone()))
    }

    async fn get_rpc_urls(&self, chain_name: &str) -> anyhow::Result<Option<Vec<String>>> {
        Ok(self.chains.read().unwrap().get(chain_name)
            .map(|c| c.config().read().unwrap()
                .rpc_urls.clone()))
    }

    async fn get_block_lag(&self, chain_name: &str) -> anyhow::Result<Option<u8>> {
        Ok(self.chains.read().unwrap().get(chain_name)
            .map(|c| c.config().read().unwrap()
                .block_lag))
    }

    async fn set_chain_active(&self, chain_name: &str, active: bool) -> anyhow::Result<()> {
        match self.chains.read().unwrap().get(chain_name) {
            Some(c) => {
                c.config().write().unwrap().active = active;
            }
            None => anyhow::bail!("chain '{}' does not exist", chain_name),
        }

        Ok(())
    }

    async fn get_tokens(&self, chain_name: &str) -> anyhow::Result<Option<Vec<TokenConfig>>> {
        Ok(self.chains.read().unwrap().get(chain_name)
            .map(|c| c.config().read().unwrap()
                .tokens.read().unwrap().iter()
                .cloned()
                .collect()))
    }

    async fn get_token_contracts(&self, chain_name: &str) -> anyhow::Result<Option<Vec<String>>> {
        Ok(self.chains.read().unwrap().get(chain_name)
            .map(|c| c.config().read().unwrap()
                .tokens.read().unwrap().iter()
                .map(|tc| tc.contract.clone())
                .collect()))
    }

    async fn get_token(&self, chain_name: &str, token_symbol: &str)
                       -> anyhow::Result<Option<TokenConfig>>
    {
        match self.chains.read().unwrap().get(chain_name) {
            Some(c) => Ok(c.config().read().unwrap()
                .tokens.read().unwrap().iter()
                .find(|tc| tc.symbol == token_symbol)
                .cloned()),
            None => Ok(None),
        }
    }

    async fn get_token_by_id(&self, _chain_name: &str, _id: u32) -> anyhow::Result<Option<TokenConfig>> {
        unimplemented!("mock database does not have ids")
    }

    async fn get_token_by_contract(&self, chain_name: &str, contract_address: &str)
                                   -> anyhow::Result<Option<TokenConfig>>
    {
        match self.chains.read().unwrap().get(chain_name) {
            Some(c) => Ok(c.config().read().unwrap()
                .tokens.read().unwrap().iter()
                .find(|tc| tc.contract == contract_address)
                .cloned()),
            None => Ok(None),
        }
    }

    async fn remove_token(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<()> {
        if let Some(c) = self.chains.read().unwrap().get(chain_name) {
            c.config().read().unwrap()
                .tokens.write().unwrap().retain(|t| t.symbol != token_symbol);
        }

        if let Some(chain_decimals) = self.token_decimals.write().unwrap()
            .get_mut(chain_name)
        {
            chain_decimals.remove(token_symbol);
        }

        Ok(())
    }

    async fn remove_token_by_id(&self, _chain_name: &str, _id: u32) -> anyhow::Result<()> {
        unimplemented!("mock database does not have ids")
    }

    async fn add_token(&self, chain_name: &str, token_config: &TokenConfig) -> anyhow::Result<()> {
        if let Some(c) = self.chains.read().unwrap().get(chain_name) {
            c.config().read().unwrap()
                .tokens.write().unwrap().insert(token_config.clone());
        }
        self._insert_token_decimals(chain_name, &token_config.symbol, token_config.decimals)?;

        Ok(())
    }

    async fn get_invoices(&self, filter: InvoiceFilter) -> anyhow::Result<PaginatedVec<Invoice>> {
        let mut filtered: Vec<Invoice> = self.invoices.iter()
            .map(|x| x.value().clone())
            .filter(|inv| {
                let status_match = filter.status.as_ref()
                    .map_or(true, |x| inv.status == *x);
                let address_match = filter.address.as_ref()
                    .map_or(true, |x| inv.address == *x);
                let network_match = filter.network.as_ref()
                    .map_or(true, |x| inv.network == *x);
                let token_match = filter.token.as_ref()
                    .map_or(true, |x| inv.token == *x);

                status_match && address_match && network_match && token_match
            })
            .collect();

        let total = filtered.len() as u64;

        // ORDER BY created_at DESC
        filtered.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        // pagination :)
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

    async fn get_invoice(&self, uuid: &str) -> anyhow::Result<Option<Invoice>> {
        Ok(self.invoices.get(uuid).map(|x| x.value().clone()))
    }

    async fn get_busy_indexes(&self, chain_name: &str) -> anyhow::Result<Vec<u32>> {
        Ok(self.invoices.iter()
            .filter(|i| i.status == InvoiceStatus::Pending
                && i.network == chain_name)
            .map(|i| i.value().address_index)
            .collect())
    }

    async fn add_invoice(&self, invoice: &Invoice) -> anyhow::Result<()> {
        if self.invoices.contains_key(&invoice.id) {
            anyhow::bail!("invoice '{}' already exists", invoice.id);
        }

        self.invoices.insert(invoice.id.clone(), invoice.clone());

        Ok(())
    }

    async fn set_invoice_status(&self, uuid: &str, status: InvoiceStatus) -> anyhow::Result<()> {
        match self.invoices.get_mut(uuid) {
            Some(mut inv) => inv.status = status,
            None => anyhow::bail!("invoice '{}' does not exist", uuid),
        }

        Ok(())
    }

    async fn get_pending_invoice_by_address(&self, chain_name: &str, address: &str) -> anyhow::Result<Option<Invoice>> {
        Ok(self.invoices.iter()
            .map(|x| x.value().clone())
            .find(|inv| inv.network == chain_name
                && inv.address == address
                && inv.status == InvoiceStatus::Pending))
    }

    async fn expire_old_invoices(&self) -> anyhow::Result<Vec<(String, String, String)>> {
        let now = chrono::Utc::now();

        let mut old_invoices: Vec<(String, String, String)> = vec![];

        self.invoices.iter_mut()
            .filter(|inv| inv.status == InvoiceStatus::Pending
                && inv.expires_at <= now)
            .for_each(|mut inv| {
                inv.status = InvoiceStatus::Expired;
                old_invoices.push((inv.id.clone(), inv.network.clone(), inv.address.clone()))
            });

        Ok(old_invoices)
    }

    async fn is_invoice_expired(&self, uuid: &str) -> anyhow::Result<Option<bool>> {
        Ok(self.invoices.iter()
            .find(|inv| inv.id == uuid)
            .map(|inv| inv.status == InvoiceStatus::Expired))
    }

    async fn is_invoice_paid(&self, uuid: &str) -> anyhow::Result<Option<bool>> {
        Ok(self.invoices.iter()
            .find(|inv| inv.id == uuid)
            .map(|inv| inv.status == InvoiceStatus::Paid))
    }

    async fn is_invoice_pending(&self, uuid: &str) -> anyhow::Result<Option<bool>> {
        Ok(self.invoices.iter()
            .find(|inv| inv.id == uuid)
            .map(|inv| inv.status == InvoiceStatus::Pending))
    }

    async fn cancel_invoice(&self, uuid: &str) -> anyhow::Result<()> {
        self.set_invoice_status(uuid, InvoiceStatus::Cancelled).await
    }

    async fn add_payment_attempt(&self, invoice_id: &str, from: &str, to: &str, tx_hash: &str,
                                 amount_raw: U256, block_number: u64, network: &str, token: &str,
                                 log_index: Option<u64>) -> anyhow::Result<()> {
        let mut contains = false;

        if self.payments.contains_key(invoice_id) {
            contains = true;
        }

        if contains {
            self.payments.get_mut(invoice_id)
                .unwrap().block_number = block_number;
            return Ok(())
        }

        self.payments.insert(invoice_id.to_owned(), Payment {
            id: uuid::Uuid::new_v4().to_string(),
            invoice_id: invoice_id.to_owned(),
            from: from.to_owned(),
            to: to.to_owned(),
            network: network.to_owned(),
            token: token.to_owned(),
            tx_hash: tx_hash.to_owned(),
            amount_raw,
            block_number,
            status: PaymentStatus::Confirming,
            created_at: chrono::Utc::now(),
            log_index: log_index.unwrap_or(u64::MAX),
        });

        Ok(())
    }

    async fn finalize_payment(&self, payment_id: &str) -> anyhow::Result<bool> {
        let (invoice_id, amount_to_add) = {
            let mut payment_ref = self.payments.iter_mut()
                .find(|p| p.id == payment_id)
                .ok_or_else(|| anyhow::anyhow!("Payment {} not found", payment_id))?;

            let p = payment_ref.value_mut();
            p.status = PaymentStatus::Confirmed;
            (p.invoice_id.clone(), p.amount_raw)
        };

        let mut invoice_ref = self.invoices.get_mut(&invoice_id)
            .ok_or_else(|| anyhow::anyhow!("Invoice {} not found", invoice_id))?;

        let inv = invoice_ref.value_mut();

        inv.paid_raw += amount_to_add;
        inv.paid = format_units(inv.paid_raw, inv.decimals)?;

        if inv.paid_raw >= inv.amount_raw {
            inv.status = InvoiceStatus::Paid;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn update_payment_block(&self, payment_id: &str, block_num: u64) -> anyhow::Result<()> {
        self.payments.get_mut(payment_id).unwrap().block_number = block_num;

        Ok(())
    }

    async fn cancel_payment(&self, payment_id: &str) -> anyhow::Result<()> {
        match self.payments.get_mut(payment_id) {
            Some(mut payment) => payment.status = PaymentStatus::Cancelled,
            None => anyhow::bail!("payment '{}' does not exist", payment_id),
        }

        Ok(())
    }

    async fn get_payments(&self, filter: PaymentFilter) -> anyhow::Result<PaginatedVec<Payment>> {
        let mut filtered: Vec<Payment> = self.payments.iter()
            .map(|p| p.value().clone())
            .filter(|p| {
                let invoice_match = filter.invoice_id.as_ref()
                    .map_or(true, |x| p.invoice_id == *x);
                let from_match = filter.from.as_ref()
                    .map_or(true, |x| p.from == *x);
                let to_match = filter.to.as_ref()
                    .map_or(true, |x| p.to == *x);
                let network_match = filter.network.as_ref()
                    .map_or(true, |x| p.network == *x);
                let token_match = filter.token.as_ref()
                    .map_or(true, |x| p.token == *x);
                let block_match = filter.block_number.as_ref()
                    .map_or(true, |x| p.block_number == *x);
                let status_match = filter.status.as_ref()
                    .map_or(true, |x| p.status == *x);

                invoice_match && from_match && to_match && network_match && token_match
                    && block_match && status_match
            })
            .collect();

        let total = filtered.len() as u64;

        filtered.sort_by(|a, b| b.created_at.cmp(&a.created_at));

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

    async fn get_payment(&self, payment_id: &str) -> anyhow::Result<Option<Payment>> {
        Ok(self.payments.get(payment_id).map(|x| x.value().clone()))
    }

    async fn get_confirming_payments(&self) -> anyhow::Result<Vec<Payment>> {
        Ok(self.payments.iter()
            .map(|x| x.value().clone())
            .filter(|payment| payment.status == PaymentStatus::Confirming)
            .collect())
    }

    async fn select_webhooks_job(&self) -> anyhow::Result<Vec<WebhookJob>> {
        let now = Utc::now();
        let mut jobs = Vec::new();

        let target_ids: Vec<String> = self.webhooks.iter()
            .filter(|r| r.status == WebhookStatus::Pending && r.next_retry <= now)
            .take(50)
            .map(|r| r.key().clone())
            .collect();

        for id in target_ids {
            if let Some(mut job) = self.webhooks.get_mut(&id) {
                job.status = WebhookStatus::Processing;

                let secret = self.invoices.get(&job.invoice_id.to_string())
                    .map(|inv| inv.webhook_secret.clone())
                    .flatten()
                    .unwrap_or_else(|| "default_secret".to_owned());

                jobs.push(WebhookJob {
                    id: Uuid::parse_str(&job.id)?,
                    url: job.url.clone(),
                    secret_key: secret,
                    payload: sqlx::types::Json(job.payload.clone()),
                    max_retries: job.max_retries as i32,
                    attempts: job.attempts as i32,
                });
            }
        }

        Ok(jobs)
    }

    async fn set_webhook_status(&self, id: &str, status: WebhookStatus) -> anyhow::Result<()> {
        if let Some(mut job) = self.webhooks.get_mut(id) {
            job.status = status;
            Ok(())
        } else {
            anyhow::bail!("Webhook job {} not found", id)
        }
    }

    async fn schedule_webhook_retry(&self, id: &str, attempts: i32, next_retry_in_secs: f64) -> anyhow::Result<()> {
        if let Some(mut job) = self.webhooks.get_mut(id) {
            job.status = WebhookStatus::Pending;
            job.attempts = attempts as u32;
            job.next_retry = Utc::now() + Duration::from_secs_f64(next_retry_in_secs);
            Ok(())
        } else {
            anyhow::bail!("Webhook job {} not found", id)
        }
    }

    async fn add_webhook_job(&self, invoice_id: &str, event: &WebhookEvent) -> anyhow::Result<()> {
        let inv_id = uuid::Uuid::parse_str(invoice_id)?;

        let invoice = self.invoices.get(invoice_id)
            .ok_or_else(|| anyhow::anyhow!("Invoice {} not found", invoice_id))?;

        if invoice.webhook_url.is_none() {
            return Ok(());
        }

        let job_id = uuid::Uuid::new_v4();
        let job = Webhook {
            id: job_id.to_string(),
            invoice_id: inv_id.to_string(),
            url: invoice.webhook_url.clone().unwrap(),
            payload: event.clone(),
            status: WebhookStatus::Pending,
            attempts: 0,
            max_retries: invoice.webhook_max_retries.unwrap_or(5),
            created_at: Utc::now(),
            next_retry: Utc::now(),
        };

        self.webhooks.insert(job_id.to_string(), job);
        Ok(())
    }

    async fn get_webhooks(&self, filter: WebhookFilter) -> anyhow::Result<PaginatedVec<Webhook>> {
        let mut filtered: Vec<Webhook> = self.webhooks.iter()
            .map(|w| w.value().clone())
            .filter(|wh| {
                let invoice_match = filter.invoice_id.as_ref()
                    .map_or(true, |x| wh.invoice_id == *x);
                let event_type_match = filter.event_type.as_ref()
                    .map_or(true, |x| wh.payload.to_string() == *x);
                let url_match = filter.url.as_ref()
                    .map_or(true, |x| wh.url == *x);
                let status_match = filter.status.as_ref()
                    .map_or(true, |x| wh.status == *x);

                invoice_match && event_type_match && url_match && status_match
            })
            .collect();

        let total = filtered.len() as u64;

        filtered.sort_by(|a, b| b.created_at.cmp(&a.created_at));

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

    async fn cancel_webhook(&self, webhook_id: &str) -> anyhow::Result<()> {
        match self.webhooks.get_mut(webhook_id) {
            Some(mut wh) => wh.status = WebhookStatus::Cancelled,
            None => anyhow::bail!("webhook '{}' does not exist", webhook_id),
        }

        Ok(())
    }

    async fn get_webhook(&self, webhook_id: &str) -> anyhow::Result<Option<Webhook>> {
        Ok(self.webhooks.get(webhook_id).map(|x| x.value().clone()))
    }

    async fn get_token_decimals(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<u8>> {
        if let Some(decimals) = self._get_token_decimals(chain_name, token_symbol)?
        {
            return Ok(Some(decimals))
        }

        let chain_config_lock = {
            let guard = self.chains.read().unwrap();

            let Some(cc) = guard.get(chain_name).cloned() else {
                return Ok(None)
            };

            cc.config()
        };

        let chain_config = chain_config_lock.read().unwrap();

        if token_symbol == chain_config.native_symbol {
            self._insert_token_decimals(chain_name, token_symbol, chain_config.decimals)?;

            return Ok(Some(chain_config.decimals));
        }

        match chain_config.tokens.read().unwrap().iter()
            .find(|tc| tc.symbol == token_symbol) {
            Some(tc) => {
                self._insert_token_decimals(chain_name, token_symbol, tc.decimals)?;

                Ok(Some(tc.decimals))
            },
            None => Ok(None),
        }
    }
}

impl MockDatabase {
    fn _insert_token_decimals(&self, chain_name: &str, token_symbol: &str, decimals: u8) -> anyhow::Result<()> {
        let mut write_guard = self.token_decimals.write().unwrap();
        let inner_map = write_guard
            .entry(chain_name.to_string())
            .or_insert_with(HashMap::new);

        inner_map.insert(token_symbol.to_string(), decimals);

        Ok(())
    }

    fn _get_token_decimals(&self, chain_name: &str, token_symbol: &str) -> anyhow::Result<Option<u8>> {
        Ok(self.token_decimals.read().unwrap()
            .get(chain_name)
            .and_then(|c| c.get(token_symbol)
                .cloned()))
    }
}