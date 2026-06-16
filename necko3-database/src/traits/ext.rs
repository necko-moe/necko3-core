use crate::model::{ChainData, FinalizedPaymentInfo, InvoiceStatus, PaymentStatus, Webhook, WebhookEvent, WebhookStatus};
use crate::traits::DatabaseStore;
use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashSet;
use uuid::Uuid;

#[async_trait]
pub trait DatabaseExt: DatabaseStore {
    // chain
    async fn get_chains_with_token(&self, token_symbol: &str) -> anyhow::Result<Vec<ChainData>> {
        let chains = self.get_chains().await?;
        let tokens = self.get_tokens_with_symbol(token_symbol).await?;

        let token_chain_ids: HashSet<i32> = tokens.into_iter()
            .map(|token| token.chain_id).collect();

        let result = chains.into_iter()
            .filter(|c| {
                c.native_symbol == token_symbol
                    || token_chain_ids.contains(&c.id)
            })
            .collect();

        Ok(result)
    }

    async fn get_latest_block(&self, chain_name: &str) -> anyhow::Result<Option<u64>> {
        Ok(self.get_chain(chain_name).await?
            .map(|c| c.last_processed_block))
    }

    async fn get_xpub(&self, chain_name: &str) -> anyhow::Result<Option<String>> {
        Ok(self.get_chain(chain_name).await?
            .map(|c| c.xpub))
    }

    async fn get_rpc_urls(&self, chain_name: &str) -> anyhow::Result<Option<Vec<String>>> {
        Ok(self.get_chain(chain_name).await?
            .map(|c| c.rpc_urls))
    }

    async fn get_block_lag(&self, chain_name: &str) -> anyhow::Result<Option<u8>> {
        Ok(self.get_chain(chain_name).await?
            .map(|c| c.block_lag))
    }

    async fn get_required_confirmations(&self, chain_name: &str) -> anyhow::Result<Option<u64>> {
        Ok(self.get_chain(chain_name).await?
            .map(|c| c.required_confirmations))
    }

    async fn get_watch_addresses(&self, chain_name: &str) -> anyhow::Result<Option<HashSet<String>>> {
        Ok(self.get_chain(chain_name).await?
            .map(|c| c.watch_addresses))
    }

    // token
    async fn get_token_contracts(&self, chain_name: &str) -> anyhow::Result<Vec<String>> {
        let tokens = self.get_tokens(chain_name).await?;

        Ok(tokens.into_iter()
            .map(|tc| tc.contract)
            .collect())
    }

    // invoice
    async fn cancel_invoice(&self, uuid: Uuid) -> anyhow::Result<()> {
        self.update_invoice_status(uuid, InvoiceStatus::Cancelled).await
    }

    async fn get_invoice_status(&self, uuid: Uuid) -> anyhow::Result<Option<InvoiceStatus>> {
        Ok(self.get_invoice(uuid).await?
            .map(|i| i.status))
    }

    // payment
    async fn finalize_payment(&self, payment_id: Uuid) -> anyhow::Result<Option<FinalizedPaymentInfo>> {
        let payment = self.get_payment(payment_id).await?
            .ok_or_else(|| anyhow::anyhow!("Payment {} not found", payment_id))?;

        self.update_payment_status(payment_id, PaymentStatus::Confirmed).await?;

        let invoice = self.get_invoice_by_address(&payment.to).await?;

        if let Some(invoice) = invoice {
            let old_status = invoice.status;
            let paid_raw_before = invoice.paid_raw;
            let new_paid_raw = invoice.paid_raw + payment.amount_raw;

            let is_fully_paid = new_paid_raw >= invoice.amount_raw;
            let new_status = if is_fully_paid {
                Some(InvoiceStatus::Paid)
            } else { None };

            self.update_invoice_paid(invoice.id, new_paid_raw, new_status).await?;

            Ok(Some(FinalizedPaymentInfo {
                is_fully_paid,
                invoice_id: invoice.id,
                paid_raw_before,
                paid_raw_after: new_paid_raw,
                old_invoice_status: old_status,
                new_invoice_status: new_status.unwrap_or(old_status),
            }))
        } else { Ok(None) }
    }

    async fn mark_txs_as_pending(&self, tx_hashes: &[String]) -> anyhow::Result<Vec<String>> {
        let mut skipped = Vec::new();

        for tx_hash in tx_hashes {
            let payment = match self.get_payment_by_tx_hash(tx_hash.clone()).await? {
                Some(p) => p,
                None => {
                    skipped.push(tx_hash.to_string());
                    continue
                }
            };

            self.update_payment_status(payment.id, PaymentStatus::Pending).await?;
        }
        
        Ok(skipped)
    }
    
    async fn cancel_payment(&self, payment_id: Uuid) -> anyhow::Result<()> {
        self.update_payment_status(payment_id, PaymentStatus::Cancelled).await
    }

    // webhook
    async fn create_webhook_job(&self, invoice_id: Uuid, event: &WebhookEvent) -> anyhow::Result<()> {
        let invoice = self.get_invoice(invoice_id).await?
            .ok_or_else(|| anyhow::anyhow!("Invoice {} not found", invoice_id))?;

        let url = match invoice.webhook_url {
            Some(u) => u,
            None => { return Ok(()) }
        };

        let now = Utc::now();
        let webhook = Webhook {
            id: Uuid::new_v4(),
            invoice_id: invoice_id.to_owned(),
            url,
            payload: event.clone(),
            status: WebhookStatus::Pending,
            attempts: 0,
            max_retries: invoice.webhook_max_retries.unwrap_or(5),
            next_retry: now,
            created_at: now,
        };

        self.add_webhook(&webhook).await
    }

    async fn cancel_webhook(&self, webhook_id: Uuid) -> anyhow::Result<()> {
        self.update_webhook_status(webhook_id, WebhookStatus::Cancelled).await
    }
}