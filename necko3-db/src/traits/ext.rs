use async_trait::async_trait;
use crate::model::{ChainConfig, InvoiceStatus, PaymentStatus, WebhookEvent, WebhookStatus};
use crate::traits::DatabaseAdapter;

#[async_trait]
pub trait DatabaseExt: DatabaseAdapter {
    // chain
    async fn get_chains_with_token(&self, token_symbol: &str) -> anyhow::Result<Vec<ChainConfig>> {
        let chains = self.get_chains().await?;

        let result = chains.into_iter()
            .filter(|c| {
                c.native_symbol == token_symbol
                    || c.tokens.iter().any(|token| token.symbol == token_symbol)
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

    async fn get_token_contracts(&self, chain_name: &str) -> anyhow::Result<Option<Vec<String>>> {
        let tokens = self.get_tokens(chain_name).await?;
        Ok(tokens.map(|vtc| {
            vtc.into_iter()
                .map(|tc| tc.contract)
                .collect()
        }))
    }

    async fn cancel_invoice(&self, uuid: &str) -> anyhow::Result<()> {
        self.update_invoice_status(uuid, InvoiceStatus::Cancelled).await
    }

    async fn get_invoice_status(&self, uuid: &str) -> anyhow::Result<Option<InvoiceStatus>> {
        Ok(self.get_invoice(uuid).await?
            .map(|i| i.status))
    }

    async fn finalize_payment(&self, payment_id: &str) -> anyhow::Result<bool> {
        unimplemented!()
    }

    async fn cancel_payment(&self, payment_id: &str) -> anyhow::Result<()> {
        self.update_payment_status(payment_id, PaymentStatus::Cancelled).await
    }

    async fn create_webhook_job(&self, invoice_id: &str, event: &WebhookEvent) -> anyhow::Result<()> {
        unimplemented!()
    }

    async fn cancel_webhook(&self, webhook_id: &str) -> anyhow::Result<()> {
        self.update_webhook_status(webhook_id, WebhookStatus::Cancelled).await
    }
}