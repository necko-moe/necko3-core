use std::collections::HashSet;
use alloy_primitives::utils::format_units;
use async_trait::async_trait;
use chrono::Utc;
use crate::model::{ChainConfig, InvoiceStatus, PaymentStatus, Webhook, WebhookEvent, WebhookStatus};
use crate::traits::DatabaseAdapter;

#[async_trait]
pub trait DatabaseExt: DatabaseAdapter {
    // chain
    async fn get_chains_with_token(&self, token_symbol: &str) -> anyhow::Result<Vec<ChainConfig>> {
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

    // token
    async fn get_token_contracts(&self, chain_name: &str) -> anyhow::Result<Option<Vec<String>>> {
        let tokens = self.get_tokens(chain_name).await?;
        Ok(tokens.map(|vtc| {
            vtc.into_iter()
                .map(|tc| tc.contract)
                .collect()
        }))
    }

    // invoice
    async fn cancel_invoice(&self, uuid: &str) -> anyhow::Result<()> {
        self.update_invoice_status(uuid, InvoiceStatus::Cancelled).await
    }

    async fn get_invoice_status(&self, uuid: &str) -> anyhow::Result<Option<InvoiceStatus>> {
        Ok(self.get_invoice(uuid).await?
            .map(|i| i.status))
    }

    // payment
    async fn finalize_payment(&self, payment_id: &str) -> anyhow::Result<bool> {
        let payment = self.get_payment(payment_id).await?
            .ok_or_else(|| anyhow::anyhow!("Payment {} not found", payment_id))?;

        self.update_payment_status(payment_id, PaymentStatus::Confirmed).await?;

        let invoice = self.get_invoice(&payment.invoice_id).await?
            .ok_or_else(|| anyhow::anyhow!("Invoice {} not found", payment.invoice_id))?;

        let new_paid_raw = invoice.paid_raw + payment.amount_raw;
        let new_paid = format_units(new_paid_raw, invoice.decimals)?;

        let is_fully_paid = new_paid_raw >= invoice.amount_raw;
        let new_status = if is_fully_paid {
            Some(InvoiceStatus::Paid)
        } else { None };

        self.update_invoice_paid(&invoice.id, new_paid_raw, &new_paid, new_status).await?;

        Ok(is_fully_paid)
    }

    async fn cancel_payment(&self, payment_id: &str) -> anyhow::Result<()> {
        self.update_payment_status(payment_id, PaymentStatus::Cancelled).await
    }

    // webhook
    async fn create_webhook_job(&self, invoice_id: &str, event: &WebhookEvent) -> anyhow::Result<()> {
        let invoice = self.get_invoice(invoice_id).await?
            .ok_or_else(|| anyhow::anyhow!("Invoice {} not found", invoice_id))?;

        let url = match invoice.webhook_url {
            Some(u) => u,
            None => { return Ok(()) }
        };

        let now = Utc::now();
        let webhook = Webhook {
            id: uuid::Uuid::new_v4().to_string(),
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

    async fn cancel_webhook(&self, webhook_id: &str) -> anyhow::Result<()> {
        self.update_webhook_status(webhook_id, WebhookStatus::Cancelled).await
    }
}